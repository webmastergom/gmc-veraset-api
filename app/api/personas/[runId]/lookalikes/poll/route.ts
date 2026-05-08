/**
 * POST /api/personas/[runId]/lookalikes/poll
 *
 * Multi-phase polling endpoint that builds a "look-alike" Master MAIDs
 * contribution from a persona cluster. Three modes (zip / traits / brands)
 * each map to a specific CTAS strategy. State machine:
 *
 *   1. starting        → resolve persona, country, source name, etc.
 *   2. ctas_launch     → build mode-specific SQL, start Athena query
 *   3. ctas_polling    → poll until SUCCEEDED
 *   4. register        → COUNT rows, register as persona_lookalike contribution
 *   5. done            → return contributionId + count
 *
 * Idempotent: state key is `lookalike-state/{runId}-{personaId}-{mode}`.
 * Re-POSTing while in progress polls the same query. POST with body
 * `{ reset: true }` forces a brand-new run.
 */

import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  runQuery,
  runQueryViaS3,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { registerAthenaContribution } from '@/lib/master-maids';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import {
  buildZipLookalikeCTAS,
  buildTraitsLookalikeCTAS,
  buildBrandsLookalikeCTAS,
  lookalikeS3Prefix,
} from '@/lib/persona-lookalike-query';
import type {
  LookalikeMode,
  LookalikeState,
  LookalikeResult,
} from '@/lib/persona-lookalike-types';
import type { PersonaState, PersonaReport } from '@/lib/persona-types';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

const STATE_KEY = (key: string) => `lookalike-state/${key}`;
const PERSONA_STATE_KEY = (runId: string) => `persona-state/${runId}`;
const PERSONA_REPORT_KEY = (runId: string) => `persona-reports/${runId}`;

const VALID_MODES: LookalikeMode[] = ['zip', 'traits', 'brands'];

function nowIso() {
  return new Date().toISOString();
}

/* ─── Helpers ────────────────────────────────────────────────────────── */

/**
 * Resolve country, dateRange, sourceDisplayName from the persona run's
 * source megajobs / jobs. Mirrors helpers in app/api/personas/poll.
 */
async function resolveRunMeta(state: PersonaState): Promise<{
  country: string | null;
  dateRange: { from: string; to: string };
  displayName: string;
}> {
  const names: string[] = [];
  let country: string | null = null;
  let from = '';
  let to = '';

  for (const mjId of state.config.megaJobIds) {
    const mj = await getMegaJob(mjId);
    if (mj?.name) names.push(mj.name);
    else names.push(mjId.slice(0, 8));
    if (!country && mj?.country) country = mj.country;
    const r = mj?.sourceScope?.dateRange;
    if (r?.from && (!from || r.from < from)) from = r.from;
    if (r?.to && (!to || r.to > to)) to = r.to;
  }
  for (const jId of state.config.jobIds || []) {
    const j = await getJob(jId);
    if ((j as any)?.name) names.push((j as any).name);
    else names.push(jId.slice(0, 8));
    if (!country && (j as any)?.country) country = (j as any).country;
    const r = (j as any)?.dateRange;
    if (r?.from && (!from || r.from < from)) from = r.from;
    if (r?.to && (!to || r.to > to)) to = r.to;
  }

  return {
    country,
    dateRange: { from: from || '', to: to || '' },
    displayName: names.length > 0 ? names.join(' + ') : 'persona-run',
  };
}

/**
 * Slugify a human label for safe inclusion in an Athena table name.
 * Athena permits [A-Za-z0-9_] in identifiers; max 256 chars but we keep
 * the budget small to leave room for the timestamp + run prefix.
 */
function slug(s: string, max = 24): string {
  return s
    .replace(/[^a-z0-9]+/gi, '_')
    .toLowerCase()
    .replace(/^_+|_+$/g, '')
    .slice(0, max);
}

/* ─── Phase: starting ─────────────────────────────────────────────── */

async function phaseStarting(state: LookalikeState): Promise<LookalikeState> {
  const personaState = await getConfig<PersonaState>(PERSONA_STATE_KEY(state.runId));
  const personaReport =
    personaState?.report ||
    (await getConfig<PersonaReport>(PERSONA_REPORT_KEY(state.runId)));
  if (!personaState) {
    throw new Error(`Persona run ${state.runId} not found`);
  }
  if (!personaReport) {
    throw new Error(`Persona run ${state.runId} has no report — finish the run first`);
  }
  const persona = personaReport.personas.find((p) => p.id === state.personaId);
  if (!persona) {
    throw new Error(`Persona id ${state.personaId} not found in run ${state.runId}`);
  }
  if (persona.deviceCount === 0) {
    throw new Error(`Persona "${persona.name}" has 0 devices — nothing to lookalike from`);
  }

  const meta = await resolveRunMeta(personaState);
  if (!meta.country) {
    throw new Error('Could not determine country for the persona run');
  }

  return {
    ...state,
    phase: 'ctas_launch',
    personaName: persona.name,
    country: meta.country,
    sourceDisplayName: meta.displayName,
    dateRange: meta.dateRange,
    updatedAt: nowIso(),
  };
}

/* ─── Phase: ctas_launch ────────────────────────────────────────────── */

async function phaseCtasLaunch(state: LookalikeState): Promise<LookalikeState> {
  const personaState = await getConfig<PersonaState>(PERSONA_STATE_KEY(state.runId));
  const personaReport =
    personaState?.report ||
    (await getConfig<PersonaReport>(PERSONA_REPORT_KEY(state.runId)));
  if (!personaState || !personaReport) {
    throw new Error(`Persona run ${state.runId} state missing`);
  }
  const persona = personaReport.personas.find((p) => p.id === state.personaId)!;

  // Feature tables across all sources used in the run.
  const featureTables = Object.values(personaState.featureCtas || {})
    .map((f) => f.tableName)
    .filter(Boolean);
  if (featureTables.length === 0) {
    throw new Error(
      'No persona feature tables found for this run (run state may be too old or was cleaned up)'
    );
  }

  // The cluster's already-exported CTAS (one column: ad_id) — we exclude
  // these from the lookalike so we don't re-include the seed audience.
  const seedExport = personaReport.exports.find((e) => e.personaId === persona.id);
  if (!seedExport?.athenaTable) {
    throw new Error(
      `Persona "${persona.name}" has no exported MAIDs table (master_maids export may not have run)`
    );
  }

  // Build the output table name. Pattern follows persona export:
  //   master_<cc>_persona_lookalike_<mode>_<slug>_<runId6>_<ts>
  const ts = Math.floor(Date.now() / 1000).toString(36);
  const cc = (state.country || 'xx').toLowerCase();
  const personaSlug = slug(persona.name, 20);
  const outTable = `master_${cc}_persona_lookalike_${state.mode}_${personaSlug}_${state.runId.slice(0, 6)}_${ts}`;
  const outS3Prefix = lookalikeS3Prefix(outTable);

  try {
    await runQuery(`DROP TABLE IF EXISTS ${outTable}`);
  } catch {
    /* drop is best-effort */
  }

  // Build mode-specific SQL.
  let sql: string;
  if (state.mode === 'zip') {
    const topZips = persona.topZips.slice(0, 10).map((z) => z.zip).filter(Boolean);
    if (topZips.length === 0) {
      throw new Error(
        `Persona "${persona.name}" has no top home ZIPs — cannot build ZIP lookalike`
      );
    }
    sql = buildZipLookalikeCTAS({
      outTable,
      outS3Prefix,
      featureTables,
      topZips,
      excludeFromTable: seedExport.athenaTable,
    });
  } else if (state.mode === 'traits') {
    sql = buildTraitsLookalikeCTAS({
      outTable,
      outS3Prefix,
      featureTables,
      medians: {
        avg_dwell_min: persona.medians.avg_dwell_min,
        weekend_share: persona.medians.weekend_share,
        recency_days: persona.medians.recency_days,
        gyration_km: persona.medians.gyration_km,
      },
      peakHour: { bucket: persona.peakHour.bucket, share: persona.peakHour.share },
      excludeFromTable: seedExport.athenaTable,
    });
  } else if (state.mode === 'brands') {
    // Top brands by visit-day count; skip empty brand mixes.
    const ranked = Object.entries(persona.brandMix || {})
      .filter(([b, c]) => b && c > 0)
      .sort((a, b) => b[1] - a[1])
      .map(([b]) => b);
    if (ranked.length === 0) {
      throw new Error(
        `Persona "${persona.name}" has no brand mix — cannot build BRAND lookalike`
      );
    }
    sql = buildBrandsLookalikeCTAS({
      outTable,
      outS3Prefix,
      featureTables,
      topBrands: ranked.slice(0, 5),
      minBrandMatches: Math.min(2, Math.ceil(ranked.length / 2)),
      excludeFromTable: seedExport.athenaTable,
    });
  } else {
    throw new Error(`Unknown lookalike mode: ${state.mode}`);
  }

  console.log(
    `[LOOKALIKE ${state.key}] launching CTAS → ${outTable} (mode=${state.mode}, persona="${persona.name}", featureTables=${featureTables.length})`
  );

  const queryId = await startQueryAsync(sql);

  return {
    ...state,
    phase: 'ctas_polling',
    ctasQueryId: queryId,
    ctasTable: outTable,
    ctasS3Prefix: outS3Prefix,
    updatedAt: nowIso(),
  };
}

/* ─── Phase: ctas_polling ──────────────────────────────────────────── */

async function phaseCtasPolling(state: LookalikeState): Promise<LookalikeState> {
  if (!state.ctasQueryId) throw new Error('No ctasQueryId in polling phase');
  const status = await checkQueryStatus(state.ctasQueryId);
  if (status.state === 'FAILED' || status.state === 'CANCELLED') {
    throw new Error(`Lookalike CTAS failed: ${status.error || status.state}`);
  }
  if (status.state !== 'SUCCEEDED') {
    return { ...state, updatedAt: nowIso() };
  }
  return { ...state, phase: 'register', updatedAt: nowIso() };
}

/* ─── Phase: register ──────────────────────────────────────────────── */

async function phaseRegister(state: LookalikeState): Promise<LookalikeState> {
  if (!state.ctasTable || !state.ctasS3Prefix) {
    throw new Error('ctasTable / ctasS3Prefix missing in register phase');
  }

  // Count MAIDs in the lookalike table — feeds the Master MAIDs UI and the
  // returned result so the frontend can show the lift number.
  const countRes = await runQueryViaS3(
    `SELECT COUNT(DISTINCT ad_id) AS c FROM ${state.ctasTable}`
  );
  const maidCount = Number(countRes.rows[0]?.c || 0);

  // Build the attributeValue. Same convention as persona export:
  //   "<sourceDisplayName> · <persona> · <Mode>"
  const modeLabel: Record<typeof state.mode, string> = {
    zip: 'ZIP lookalike',
    traits: 'Traits lookalike',
    brands: 'Brand lookalike',
  };
  const attributeValue = `${state.sourceDisplayName || 'persona-run'} · ${state.personaName} · ${modeLabel[state.mode]}`;

  if (state.country) {
    try {
      // registerAthenaContribution is void-returning; the index entry is
      // looked up via getCountryContributions if the caller needs the id.
      // Same s3Prefix pattern used by the persona export path:
      //   's3://<bucket>/athena-temp/<table>/' (full s3:// URL).
      await registerAthenaContribution(
        state.country,
        state.sourceDisplayName || 'persona-run',
        'persona_lookalike',
        attributeValue,
        state.ctasTable,
        state.ctasS3Prefix,
        maidCount,
        state.dateRange || { from: '', to: '' }
      );
    } catch (e: any) {
      console.error(
        `[LOOKALIKE ${state.key}] registration failed: ${e?.message || e}`
      );
      // We still mark phase=done so the user sees the result; most failures
      // are duplicate-key noise from re-runs of the same (mode, persona).
    }
  }

  return {
    ...state,
    phase: 'done',
    maidCount,
    attributeValue,
    updatedAt: nowIso(),
  };
}

/* ─── Phase advance ───────────────────────────────────────────────── */

async function advance(state: LookalikeState): Promise<LookalikeState> {
  switch (state.phase) {
    case 'starting':
      return phaseStarting(state);
    case 'ctas_launch':
      return phaseCtasLaunch(state);
    case 'ctas_polling':
      return phaseCtasPolling(state);
    case 'register':
      return phaseRegister(state);
    case 'done':
    case 'error':
      return state;
    default:
      throw new Error(`Unknown phase: ${(state as any).phase}`);
  }
}

function toResult(state: LookalikeState): LookalikeResult {
  return {
    phase: state.phase,
    mode: state.mode,
    personaId: state.personaId,
    personaName: state.personaName,
    maidCount: state.maidCount,
    contributionId: state.contributionId,
    attributeValue: state.attributeValue,
    athenaTable: state.ctasTable,
    error: state.error,
  };
}

/* ─── Route handler ──────────────────────────────────────────────── */

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ runId: string }> }
): Promise<NextResponse> {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    const { runId } = await context.params;
    const body = await request.json().catch(() => ({} as any));
    const personaId = Number(body?.personaId);
    const mode = String(body?.mode || '') as LookalikeMode;
    const reset = !!body?.reset;

    if (!Number.isFinite(personaId)) {
      return NextResponse.json({ error: 'personaId required' }, { status: 400 });
    }
    if (!VALID_MODES.includes(mode)) {
      return NextResponse.json({ error: `mode must be one of ${VALID_MODES.join('|')}` }, { status: 400 });
    }

    const key = `${runId}-${personaId}-${mode}`;
    let state = await getConfig<LookalikeState>(STATE_KEY(key));

    if (reset || !state) {
      state = {
        phase: 'starting',
        key,
        runId,
        personaId,
        personaName: '',
        mode,
        updatedAt: nowIso(),
      };
    }

    // Loop a few advances per request — if a phase completes immediately
    // (zip CTAS often does in <2s), we want to push as far as possible
    // before returning so the UI doesn't have to round-trip 5 times.
    const startedAt = Date.now();
    const MAX_ADVANCES = 8;
    const MAX_WALL_MS = 50_000; // leave ~10s buffer under maxDuration=60
    let iters = 0;
    while (
      iters < MAX_ADVANCES &&
      Date.now() - startedAt < MAX_WALL_MS &&
      state.phase !== 'done' &&
      state.phase !== 'error'
    ) {
      const before = state.phase;
      try {
        state = await advance(state);
      } catch (e: any) {
        state = { ...state, phase: 'error', error: e?.message || String(e), updatedAt: nowIso() };
        break;
      }
      // ctas_polling intentionally returns the same phase when not yet
      // SUCCEEDED — break so the caller can come back later.
      if (before === state.phase) break;
      iters++;
    }

    await putConfig(STATE_KEY(key), state, { compact: true });
    return NextResponse.json(toResult(state));
  } catch (e: any) {
    console.error('[LOOKALIKE] route error:', e?.message || e);
    return NextResponse.json({ error: e?.message || String(e) }, { status: 500 });
  }
}

/* ─── GET — peek at current state without advancing ────────────────── */

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ runId: string }> }
): Promise<NextResponse> {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { runId } = await context.params;
  const personaId = Number(request.nextUrl.searchParams.get('personaId') || '');
  const mode = String(request.nextUrl.searchParams.get('mode') || '') as LookalikeMode;
  if (!Number.isFinite(personaId) || !VALID_MODES.includes(mode)) {
    return NextResponse.json({ error: 'personaId + mode required' }, { status: 400 });
  }
  const key = `${runId}-${personaId}-${mode}`;
  const state = await getConfig<LookalikeState>(STATE_KEY(key));
  if (!state) {
    return NextResponse.json(
      { phase: 'starting', mode, personaId, personaName: '' } as LookalikeResult,
      { status: 200 }
    );
  }
  return NextResponse.json(toResult(state));
}
