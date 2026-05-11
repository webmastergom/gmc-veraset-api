/**
 * POST /api/zip-code-signals/poll
 *
 * Multi-phase replacement for /api/zip-code-signals/analyze/stream.
 * Splits the pipeline into <60s phases persisted in S3 so it works on
 * any Vercel plan (Hobby 300s, Pro 800s, anywhere).
 *
 * Body shapes:
 *
 *   First request (creates run):
 *     {
 *       datasetName?: string,
 *       megaJobId?: string,
 *       country: string,
 *       postalCodes: string[],
 *       dateFrom?: string,
 *       dateTo?: string,
 *       reset?: boolean   // force restart even if a state exists
 *     }
 *
 *   Continuation (with runId from first response):
 *     { runId: string, reset?: boolean }
 *
 * Response:
 *   {
 *     runId: string,
 *     phase: ZcsPhase,
 *     percent: number,
 *     message: string,
 *     details?: string,
 *     elapsedMs: number,
 *     resultKey?: string,   // S3 key when phase=='done'
 *     result?: PostalMaidResult,  // inline when small
 *     error?: string,
 *   }
 *
 * The frontend polls every 2-3s. The endpoint advances multiple phases
 * per call when time allows (most phases run in <5s except the actual
 * polling_queries phase which checks Athena status and returns quickly).
 */

import { NextRequest, NextResponse } from 'next/server';
import { createHash } from 'crypto';
import { isAuthenticated } from '@/lib/auth';
import { getConfig, putConfig, invalidateCache } from '@/lib/s3-config';
import {
  type ZcsState,
  type ZcsRunConfig,
  type ZcsPhase,
  stateKey,
  readResult,
  phaseStarting,
  phasePrepareTable,
  phaseLaunchQueries,
  phasePollingQueries,
  phaseAggregateFull,
  phaseAggregateBasic,
  phasePass1Basic,
  phaseGeocoding,
  phasePass2Basic,
} from '@/lib/zcs-multiphase';

export const dynamic = 'force-dynamic';
// Vercel Pro accepts up to 800. ZCS phases on big megajobs (e.g. France
// Grid 50k with 24M MAIDs → ~5 GB origins CSV) can take 60-120s for the
// streaming aggregate. Hobby silently clamps to 300, which is still
// enough for medium runs. The state machine itself is plan-agnostic —
// if a phase doesn't fit in one invocation, the in-memory work is lost
// and we retry — but we want to make it fit on the first try where
// possible.
export const maxDuration = 800;

// Result inlining: small results go in the JSON response; bigger ones
// only return the S3 key so the frontend can fetch them via the existing
// /api/zip-code-signals/spill endpoint.
const MAX_INLINE_DEVICES = 60_000;

function configToRunId(cfg: ZcsRunConfig): string {
  const norm = {
    megaJobId: cfg.megaJobId || '',
    datasetName: cfg.datasetName || '',
    country: cfg.country.toUpperCase(),
    postalCodes: [...cfg.postalCodes].map((p) => p.trim().toUpperCase()).sort(),
    dateFrom: cfg.dateFrom || '',
    dateTo: cfg.dateTo || '',
  };
  return 'zcs-' + createHash('sha1').update(JSON.stringify(norm)).digest('hex').slice(0, 16);
}

async function advance(state: ZcsState): Promise<ZcsState> {
  switch (state.phase) {
    case 'starting':
      return await phaseStarting(state);
    case 'prepare_table':
      return await phasePrepareTable(state);
    case 'launch_queries':
      return await phaseLaunchQueries(state);
    case 'polling_queries':
      return await phasePollingQueries(state);
    case 'aggregate_full':
      return await phaseAggregateFull(state);
    case 'aggregate_basic':
      return await phaseAggregateBasic(state);
    // Legacy phases — auto-promote in-flight state from the old 3-phase
    // BASIC pipeline. The Athena queries those phases launched are now
    // useless (different SQL shape), so we restart at launch_queries.
    case 'pass1_basic':
    case 'geocoding':
    case 'pass2_basic':
      console.log(`[ZCS-POLL] auto-promoting legacy phase ${state.phase} → launch_queries`);
      return { ...state, phase: 'launch_queries' as ZcsPhase, queryIds: undefined };
    case 'done':
    case 'error':
      return state;
    default:
      throw new Error(`Unknown phase: ${(state as any).phase}`);
  }
}

const PHASE_LABEL: Record<ZcsPhase, string> = {
  starting: 'Validating inputs…',
  prepare_table: 'Preparing Athena tables…',
  launch_queries: 'Launching Athena queries…',
  polling_queries: 'Waiting for Athena…',
  aggregate_full: 'Streaming result + building signatures…',
  aggregate_basic: 'Streaming matched devices from Athena…',
  pass1_basic: 'Pass 1: collecting unique origin coords…',
  geocoding: 'Reverse-geocoding coords to postal codes…',
  pass2_basic: 'Pass 2: matching devices to target postal codes…',
  done: 'Done',
  error: 'Error',
};

export async function POST(request: NextRequest): Promise<NextResponse> {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const startedAt = Date.now();

  let body: any;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  // Resolve runId: either provided (continuation) or computed from config (new run).
  // CRITICAL: runId is concatenated into S3 paths (zcs-state/<runId>,
  // postal-maid-spill/zcs-<runId>, etc.). Reject anything that could
  // path-traverse or break S3 key syntax. Valid runIds we generate are
  // 'zcs-' + 16-hex sha1 prefix; we accept any safe-looking string for
  // forward compatibility but with strict character + length checks.
  const rawRunId: string = body?.runId || '';
  if (rawRunId && !/^[a-zA-Z0-9_-]{1,128}$/.test(rawRunId)) {
    return NextResponse.json(
      { error: 'Invalid runId format (expected alphanumeric / _ / -, max 128 chars)' },
      { status: 400 },
    );
  }
  let runId: string = rawRunId;
  const reset = !!body?.reset;
  const hasNewConfig =
    !!body?.country &&
    Array.isArray(body?.postalCodes) &&
    body.postalCodes.length > 0 &&
    (body?.megaJobId || body?.datasetName);

  if (hasNewConfig) {
    const cfg: ZcsRunConfig = {
      datasetName: body.datasetName || undefined,
      megaJobId: body.megaJobId || undefined,
      country: String(body.country).toUpperCase(),
      postalCodes: body.postalCodes.map((p: string) => p.trim().toUpperCase()),
      dateFrom: body.dateFrom || undefined,
      dateTo: body.dateTo || undefined,
    };
    runId = configToRunId(cfg);

    // Reset on user request, or if a previous run ended in error (we never
    // want to keep showing a stuck 'error' state when the user clicks Run
    // again). New successful runs are idempotent on the same runId.
    invalidateCache(stateKey(runId));
    let existing = await getConfig<ZcsState>(stateKey(runId));
    if (reset || !existing || existing.phase === 'error') {
      const initial: ZcsState = {
        phase: 'starting',
        runId,
        config: cfg,
        sourceLabel: cfg.megaJobId ? `megajob:${cfg.megaJobId}` : cfg.datasetName!,
        updatedAt: new Date().toISOString(),
      };
      await putConfig(stateKey(runId), initial, { compact: true });
      existing = initial;
    }
  }

  if (!runId) {
    return NextResponse.json({ error: 'runId or full config required' }, { status: 400 });
  }

  // Always invalidate before reading — Vercel functions reuse process instances
  // and the in-memory cache can outlive the state file between invocations.
  invalidateCache(stateKey(runId));
  let state = await getConfig<ZcsState>(stateKey(runId));
  if (!state) {
    return NextResponse.json({ error: 'Run not found' }, { status: 404 });
  }

  // Loop multiple advances if time allows (saves frontend round-trips).
  // The poll endpoint has maxDuration=800 (Pro plan), so we have plenty
  // of budget. We still cap MAX_WALL_MS at 700s to leave ~100s of
  // buffer for the response serialization and Vercel overhead.
  const MAX_WALL_MS = 700_000;
  const MAX_ADVANCES = 8;
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
      console.error(`[ZCS-POLL ${runId}] phase ${state.phase} error:`, e?.message);
      state = { ...state, phase: 'error', error: e?.message || String(e), updatedAt: new Date().toISOString() };
      break;
    }
    if (before === state.phase) break; // polling_queries reached but Athena not done
    iters++;
  }

  await putConfig(stateKey(runId), state, { compact: true });

  const elapsedMs = Date.now() - startedAt;

  // If done, optionally inline the result
  let inlineResult: any = undefined;
  if (state.phase === 'done' && state.resultKey) {
    const result = await readResult(runId);
    if (result && result.devices.length <= MAX_INLINE_DEVICES) {
      inlineResult = result;
    } else if (result) {
      // Provide a preview + spill key (compatible with the existing
      // /api/zip-code-signals/spill endpoint).
      inlineResult = {
        ...result,
        devices: result.devices.slice(0, 5_000),
        devicesSpillKey: state.resultKey,
        devicesSpillTotal: result.devices.length,
      };
    }
  }

  return NextResponse.json({
    runId,
    phase: state.phase,
    percent: state.subProgress?.percent ?? phaseDefaultPercent(state.phase),
    message: state.subProgress?.label || PHASE_LABEL[state.phase],
    details: state.subProgress?.details,
    elapsedMs,
    resultKey: state.resultKey,
    result: inlineResult,
    error: state.error,
  });
}

export async function GET(request: NextRequest): Promise<NextResponse> {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const runId = request.nextUrl.searchParams.get('runId');
  if (!runId) {
    return NextResponse.json({ error: 'runId required' }, { status: 400 });
  }
  const state = await getConfig<ZcsState>(stateKey(runId));
  if (!state) {
    return NextResponse.json({ error: 'Run not found' }, { status: 404 });
  }
  let inlineResult: any = undefined;
  if (state.phase === 'done' && state.resultKey) {
    const result = await readResult(runId);
    if (result) inlineResult = result;
  }
  return NextResponse.json({
    runId,
    phase: state.phase,
    percent: state.subProgress?.percent ?? phaseDefaultPercent(state.phase),
    message: state.subProgress?.label || PHASE_LABEL[state.phase],
    details: state.subProgress?.details,
    resultKey: state.resultKey,
    result: inlineResult,
    error: state.error,
  });
}

function phaseDefaultPercent(p: ZcsPhase): number {
  return ({
    starting: 0,
    prepare_table: 8,
    launch_queries: 15,
    polling_queries: 30,
    aggregate_full: 80,
    aggregate_basic: 80,
    pass1_basic: 50,
    geocoding: 70,
    pass2_basic: 90,
    done: 100,
    error: 0,
  } as const)[p];
}
