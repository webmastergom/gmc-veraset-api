import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig, putConfig } from '@/lib/s3-config';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { homeTableName, homeTableExists } from '@/lib/home-detector';
import {
  buildAudienceCounterSQL,
  parseAudienceCounterResult,
  weeksInRange,
  DEFAULT_HOME_CONFIDENCE_THRESHOLD,
  RESIDENT_WEEK_ACTIVITY_FRACTION,
  type AudienceCounterReport,
} from '@/lib/audience-counter';
import { getCountryParams } from '@/lib/country-params';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * POST /api/datasets/[name]/audience-counter
 *
 * Computes the Resident audience count for a dataset (METHODOLOGY §3.3)
 * using a multiphase polling pattern so we never exceed the 60 s
 * Vercel function timeout on slow Athena scans.
 *
 *   Phase flow (each POST advances one phase):
 *     • start    → kick off the Athena query, store queryId, return
 *                  { phase: 'polling', queryId, percent: 10 }
 *     • polling  → checkQueryStatus(queryId). If RUNNING, return same
 *                  shape with an updated percent. If SUCCEEDED, fetch
 *                  results, build the report, persist to S3, return
 *                  { phase: 'done', report }.
 *     • done     → next POST short-circuits to the cached report.
 *
 *   Body: { country?: string, force?: boolean }
 *     country  Overrides the job's country if explicitly provided. Most
 *              datasets carry country in their basic-analysis blob, in
 *              which case the body can be empty.
 *     force    If true, ignore any cached report and re-run.
 *
 *   Storage:
 *     • config/dataset-reports/{ds}/audience-counter.json — final report
 *       (wiped automatically by the Reset endpoint).
 *     • config/audience-counter-state/{ds}.json — multiphase polling
 *       state.
 *
 * Reset interaction: the audience-counter.json lives under
 * config/dataset-reports/{ds}/, so the Reset endpoint's prefix wipe
 * automatically invalidates it. The state file is keyed under
 * config/audience-counter-state/ so we add it to that wipe list too.
 */

interface AudienceCounterState {
  phase: 'start' | 'polling' | 'done' | 'error';
  queryId?: string;
  country?: string;
  startedAt?: string;
  error?: string;
}

const REPORT_KEY = (ds: string) => `dataset-reports/${ds}/audience-counter`;
const STATE_KEY = (ds: string) => `audience-counter-state/${ds}`;

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> },
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { name: datasetName } = await context.params;

  let body: { country?: string; force?: boolean } = {};
  try { body = await request.json(); } catch {}
  const overrideCountry = body.country?.trim().toUpperCase();
  const force = !!body.force;

  // Short-circuit on cached report unless explicit force.
  if (!force) {
    const cached = await getConfig<AudienceCounterReport>(REPORT_KEY(datasetName));
    if (cached) {
      return NextResponse.json({ phase: 'done', report: cached });
    }
  }

  // Resolve country from the basic dataset analysis if the caller didn't
  // override it. Without a country we can't apply κ_md, so we refuse.
  let country = overrideCountry || '';
  if (!country) {
    const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
    country = (analysis?.country || '').toUpperCase();
  }
  if (!country) {
    return NextResponse.json({
      phase: 'error',
      error: 'Country is not set for this dataset. Assign a country on the job before computing Resident audience.',
    }, { status: 400 });
  }

  // Resolve the date window — needed to compute weeksInWindow. We pull
  // from the basic dataset analysis (which is what the dataset page is
  // already showing the date range from).
  const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
  const dateFrom: string | null = analysis?.summary?.dateFrom || analysis?.dateFrom || null;
  const dateTo: string | null = analysis?.summary?.dateTo || analysis?.dateTo || null;
  const weeksInWindow = weeksInRange(dateFrom, dateTo);
  if (weeksInWindow <= 0) {
    return NextResponse.json({
      phase: 'error',
      error: 'Could not determine the dataset date range. Run "Analyze" first to populate basic dataset analysis.',
    }, { status: 400 });
  }

  // Confirm the home table exists — Resident requires the TC-WK-19-7
  // home-locations output (no legacy first-ping fallback).
  if (!(await homeTableExists(datasetName))) {
    return NextResponse.json({
      phase: 'error',
      error: 'Home table is missing for this dataset. Click Analyze (auto-triggers home detection) and retry.',
    }, { status: 400 });
  }

  let state = (await getConfig<AudienceCounterState>(STATE_KEY(datasetName))) || { phase: 'start' as const };
  if (force) state = { phase: 'start' };

  // Phase: start — fire the Athena CTE.
  if (state.phase === 'start' || !state.queryId) {
    await ensureTableForDataset(datasetName);
    const sql = buildAudienceCounterSQL({
      homeTable: homeTableName(datasetName),
      sourceTable: getTableName(datasetName),
      country,
      weeksInWindow,
    });
    try {
      const queryId = await startQueryAsync(sql);
      state = { phase: 'polling', queryId, country, startedAt: new Date().toISOString() };
      await putConfig(STATE_KEY(datasetName), state);
      return NextResponse.json({
        phase: 'polling',
        queryId,
        progress: { percent: 10, message: `Athena: counting residents of ${country}…` },
      });
    } catch (e: any) {
      state = { phase: 'error', error: e?.message || String(e) };
      await putConfig(STATE_KEY(datasetName), state);
      return NextResponse.json({ phase: 'error', error: state.error }, { status: 500 });
    }
  }

  // Phase: polling — check Athena status, parse + cache on SUCCEEDED.
  if (state.phase === 'polling') {
    let status;
    try {
      status = await checkQueryStatus(state.queryId!);
    } catch (e: any) {
      const msg = e?.message || String(e);
      if (/not found|InvalidRequestException/i.test(msg)) {
        // Athena keeps query metadata ~45 days; an expired query needs
        // a force-restart from the caller.
        state = { phase: 'error', error: `Athena query ${state.queryId} expired. Retry to restart.` };
        await putConfig(STATE_KEY(datasetName), state);
        return NextResponse.json({ phase: 'error', error: state.error, expired: true }, { status: 410 });
      }
      throw e;
    }

    if (status.state === 'RUNNING' || status.state === 'QUEUED') {
      const scanned = status.statistics?.dataScannedBytes ? (status.statistics.dataScannedBytes / 1e9).toFixed(1) : '?';
      const runtimeSec = status.statistics?.engineExecutionTimeMs
        ? Math.round(status.statistics.engineExecutionTimeMs / 1000)
        : 0;
      return NextResponse.json({
        phase: 'polling',
        queryId: state.queryId,
        progress: {
          percent: Math.min(80, 10 + runtimeSec),
          message: `Athena: scanning ${scanned} GB · ${runtimeSec}s`,
        },
      });
    }

    if (status.state === 'FAILED' || status.state === 'CANCELLED') {
      state = { phase: 'error', error: status.error || `Athena query ${state.queryId} ${status.state}` };
      await putConfig(STATE_KEY(datasetName), state);
      return NextResponse.json({ phase: 'error', error: state.error }, { status: 500 });
    }

    // SUCCEEDED — fetch + persist
    const result = await fetchQueryResults(state.queryId!);
    const parsed = parseAudienceCounterResult(result.rows, state.country || country);
    const params = getCountryParams(state.country || country);

    // Pull uniqueMaids from the basic analysis (tier 1) so the report
    // bundles all three tiers in one place for the UI.
    const uniqueMaids = typeof analysis?.summary?.uniqueDevices === 'number'
      ? analysis.summary.uniqueDevices
      : null;
    const uniqueUsers = uniqueMaids != null ? Math.round(uniqueMaids * params.kappa_md) : null;

    const report: AudienceCounterReport = {
      datasetName,
      country: state.country || country,
      analyzedAt: new Date().toISOString(),
      dateFrom,
      dateTo,
      weeksInWindow: parsed.weeksInWindow,
      activeWeeksFloor: parsed.activeWeeksFloor,
      homeConfidenceThreshold: DEFAULT_HOME_CONFIDENCE_THRESHOLD,
      uniqueMaids,
      uniqueUsers,
      residentMaids: parsed.residentMaids,
      residentUsers: parsed.residentUsers,
      maidCeilingM: parsed.ceilingM,
      overCeiling: uniqueMaids != null ? uniqueMaids > parsed.ceilingM * 1_000_000 : false,
      kappaMd: parsed.kappaMd,
    };
    await putConfig(REPORT_KEY(datasetName), report);
    state = { phase: 'done' };
    await putConfig(STATE_KEY(datasetName), state);

    return NextResponse.json({ phase: 'done', report });
  }

  if (state.phase === 'error') {
    return NextResponse.json({ phase: 'error', error: state.error }, { status: 500 });
  }

  // 'done' but cache miss — re-trigger by force.
  return NextResponse.json({
    phase: 'start',
    progress: { percent: 0, message: 'No cached result — click Run again to compute.' },
  });
}

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> },
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { name: datasetName } = await context.params;
  const cached = await getConfig<AudienceCounterReport>(REPORT_KEY(datasetName));
  if (cached) return NextResponse.json({ phase: 'done', report: cached });
  return NextResponse.json({ phase: 'idle' });
}
