import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import { checkQueryStatus, ensureTableForDataset, runQueryViaS3, dropTempTable, cleanupTempS3, startQueryAsync } from '@/lib/athena';
import { getConfig, putConfig, s3Client, BUCKET } from '@/lib/s3-config';
import {
  startConsolidatedNseQuery,
  extractPoiCoords,
  materializePoiCoordsTable,
  dropPoiCoordsTable,
  poiCoordsTablePrefix,
  MAX_INLINE_POIS,
  type DwellFilter,
} from '@/lib/mega-consolidation-queries';
import {
  materializeNseBracketMap,
  startBracketUnloads,
  startBracketCountQuery,
  dropNseBracketMap,
  type BracketUnloadHandle,
} from '@/lib/nse-athena';
import { ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

const STATE_KEY = (id: string) => `nse-export-state/mega-${id}`;
const NSE_KEY = (cc: string) => `nse/${cc.toUpperCase()}`;

interface NseRecord {
  postal_code: string;
  population: number;
  nse: number;
}

const NSE_BRACKETS = [
  { label: '0-19 (Low)', min: 0, max: 19 },
  { label: '20-39', min: 20, max: 39 },
  { label: '40-59 (Mid)', min: 40, max: 59 },
  { label: '60-79', min: 60, max: 79 },
  { label: '80-100 (High)', min: 80, max: 100 },
];

interface BracketResult {
  label: string;
  min: number;
  max: number;
  postalCodes: number;
  population: number;
  maidCount: number;
  downloadUrl: string | null;
}

interface NseExportState {
  /**
   * - querying / polling: NSE CTAS pipeline (per-device ad_id + origin coords)
   * - bracket_setup: build bracket map table, launch UNLOAD + COUNT queries
   * - bracket_running: poll the UNLOADs and COUNT until all SUCCEEDED
   * - done: brackets[] populated, downloads ready
   */
  phase: 'querying' | 'polling' | 'bracket_setup' | 'bracket_running' | 'done' | 'error';
  queryId?: string;
  /** CTAS table name (Parquet results) — set when query is started via CTAS path. */
  ctasTable?: string;
  /** Per-run id used to namespace CTAS tables (avoids collisions on retry). */
  runId?: string;
  /** External POI table (when poiCoords > MAX_INLINE_POIS). Cleaned up at the end. */
  poiTableRef?: string;
  /** Bracket-map external table created from country geocode-cache + NSE config. */
  bracketMapTable?: string;
  /** S3 prefix backing the bracket map table — for cleanup. */
  bracketMapS3Prefix?: string;
  /** Per-bracket UNLOAD handles (queryId + S3 prefix where the CSV lands). */
  unloads?: BracketUnloadHandle[];
  /** Single GROUP BY query that returns COUNT(DISTINCT ad_id) per bracket. */
  countQueryId?: string;
  country: string;
  megaJobId: string;
  error?: string;
  brackets?: BracketResult[];
}

/**
 * POST /api/mega-jobs/[id]/nse-poll
 *
 * Multi-phase polling endpoint for NSE MAID segmentation on mega-jobs.
 * First call body: { country: string }
 * Subsequent calls: no body needed (reads state from S3)
 *
 * Phases:
 * 1. querying: load sub-jobs, start Athena query for ad_id + origin coords
 * 2. polling: wait for Athena query
 * 3. geocoding: fetch results, geocode → postal codes, match to NSE brackets, save CSVs
 * 4. done: return bracket counts + download URLs
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { id } = await context.params;

  try {
    const megaJob = await getMegaJob(id);
    if (!megaJob) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    // Parse body to detect new request (has country)
    let body: any;
    try { body = await request.json(); } catch { body = {}; }
    const isNewRequest = !!body.country;

    let state = await getConfig<NseExportState>(STATE_KEY(id));

    // Return cached results if done and not a new request
    if (state?.phase === 'done' && !isNewRequest && state.brackets) {
      const totalMaids = state.brackets.reduce((s, b) => s + b.maidCount, 0);
      return NextResponse.json({
        phase: 'done',
        brackets: state.brackets,
        totalMaids,
        progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
      });
    }

    // Reset on error or new request
    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: start query ──────────────────────────────────────────
    if (!state) {
      const country: string = body.country || '';
      if (!country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      // Verify NSE data exists for this country
      const nseData = await getConfig<NseRecord[]>(NSE_KEY(country));
      if (!nseData?.length) {
        return NextResponse.json({ error: `No NSE data for ${country}. Upload CSV first.` }, { status: 404 });
      }

      // Load synced sub-jobs
      const subJobs = (
        await Promise.all(megaJob.subJobIds.map((jid) => getJob(jid)))
      ).filter((j): j is NonNullable<typeof j> => j !== null);
      const syncedJobs = subJobs.filter((j) => j.status === 'SUCCESS' && j.syncedAt);

      if (syncedJobs.length === 0) {
        return NextResponse.json({ error: 'No synced sub-jobs' }, { status: 400 });
      }

      // Ensure Athena tables
      for (const job of syncedJobs) {
        const dsName = job.s3DestPath?.replace(/\/$/, '').split('/').pop();
        if (dsName) await ensureTableForDataset(dsName);
      }

      // Extract POI coordinates for spatial path
      const poiCoords = extractPoiCoords(syncedJobs);

      console.log(`[MEGA-NSE-POLL] Starting for mega-job ${id}, country=${country}, ${syncedJobs.length} sub-jobs, ${poiCoords.length} POI coords`);

      // Per-run id so CTAS tables never collide with leftover ones
      const runId = Date.now().toString(36);

      // Materialize POIs as external table when over inline limit (e.g. 25k POI grid)
      let poiTableRef: string | undefined;
      if (poiCoords.length > MAX_INLINE_POIS) {
        try {
          poiTableRef = await materializePoiCoordsTable(id, runId, poiCoords);
          console.log(`[MEGA-NSE-POLL] Materialized ${poiCoords.length} POIs to ${poiTableRef}`);
        } catch (err: any) {
          console.error('[MEGA-NSE-POLL] Failed to materialize POI table:', err.message);
          // Continue without it — query will fail gracefully if POIs were needed
        }
      }

      const handle = await startConsolidatedNseQuery(
        id,
        runId,
        syncedJobs,
        undefined, // poiIds
        poiCoords.length > 0 ? poiCoords : undefined,
        undefined, // dwell
        poiTableRef,
      );

      state = {
        phase: 'polling',
        queryId: handle.queryId,
        ctasTable: handle.ctasTable,
        runId,
        poiTableRef,
        country,
        megaJobId: id,
      };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 10, message: 'Running Athena query for device origins...' },
      });
    }

    // ── Phase: polling ──────────────────────────────────────────────
    if (state.phase === 'polling' && state.queryId) {
      try {
        const status = await checkQueryStatus(state.queryId);

        if (status.state === 'RUNNING' || status.state === 'QUEUED') {
          const scannedGB = status.statistics?.dataScannedBytes
            ? (status.statistics.dataScannedBytes / 1e9).toFixed(1)
            : '0';
          const runtimeSec = status.statistics?.engineExecutionTimeMs
            ? Math.round(status.statistics.engineExecutionTimeMs / 1000)
            : 0;

          return NextResponse.json({
            phase: 'polling',
            progress: {
              step: 'polling',
              percent: 30,
              message: `Athena query running... ${scannedGB} GB scanned, ${runtimeSec}s`,
            },
          });
        }

        if (status.state === 'FAILED' || status.state === 'CANCELLED') {
          state = { ...state, phase: 'error', error: status.error || 'Query failed' };
          await putConfig(STATE_KEY(id), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

        // SUCCEEDED → move to bracket_setup (build bracket map + launch UNLOADs)
        state = { ...state, phase: 'bracket_setup' };
        await putConfig(STATE_KEY(id), state, { compact: true });

        return NextResponse.json({
          phase: 'bracket_setup',
          progress: { step: 'bracket_setup', percent: 55, message: 'Building bracket map for server-side bracket assignment...' },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'error', error: 'Query expired — please retry' };
          await putConfig(STATE_KEY(id), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        throw err;
      }
    }

    // ── Phase: bracket_setup ──────────────────────────────────────────
    // Build the bracket-assignment map (lat_key,lng_key → bracket_label) as an
    // Athena external table, then launch 5 UNLOAD queries (one per bracket) +
    // 1 COUNT-by-bracket query in parallel. Athena does the JOIN+aggregation
    // server-side so we never need to download the per-device CSV (which is
    // up to 1+ GB for 50k-POI megajobs).
    if (state.phase === 'bracket_setup' && state.ctasTable && state.runId) {
      console.log(`[MEGA-NSE-POLL] bracket_setup for ${state.country}, runId=${state.runId}`);

      // Build + register the bracket map external table
      const { tableName: bracketMapTable, s3Prefix: bracketMapS3Prefix, perBracket } =
        await materializeNseBracketMap(id, state.runId, state.country, NSE_BRACKETS);
      console.log(`[MEGA-NSE-POLL] Bracket map table: ${bracketMapTable}, perBracket grid cells: ${JSON.stringify(perBracket)}`);

      // Launch 5 UNLOADs + 1 COUNT in parallel
      const unloads = await startBracketUnloads(id, state.runId, state.ctasTable, bracketMapTable, NSE_BRACKETS);
      const countQueryId = await startBracketCountQuery(state.ctasTable, bracketMapTable);
      console.log(`[MEGA-NSE-POLL] Launched ${unloads.length} UNLOADs + 1 COUNT query=${countQueryId}`);

      state = {
        ...state,
        phase: 'bracket_running',
        bracketMapTable,
        bracketMapS3Prefix,
        unloads,
        countQueryId,
      };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'bracket_running',
        progress: { step: 'bracket_running', percent: 70, message: `Computing per-bracket MAIDs (${unloads.length} bracket queries running)...` },
      });
    }

    // ── Phase: bracket_running ────────────────────────────────────────
    // Poll all 5 UNLOADs + the 1 COUNT query. When all SUCCEEDED, build the
    // brackets[] result with maidCount (from COUNT) and downloadUrl (from
    // listing the UNLOAD output prefix in S3).
    if (state.phase === 'bracket_running' && state.unloads && state.countQueryId) {
      // Check status of all queries
      const allQueryIds = [...state.unloads.map(u => u.unloadQueryId), state.countQueryId];
      const statuses = await Promise.all(
        allQueryIds.map(qid => checkQueryStatus(qid).catch(() => ({ state: 'FAILED' as const, error: 'status check failed' })))
      );
      const failed = statuses.find(s => s.state === 'FAILED' || s.state === 'CANCELLED');
      if (failed) {
        state = { ...state, phase: 'error', error: (failed as any).error || 'Bracket query failed' };
        await putConfig(STATE_KEY(id), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      const doneCount = statuses.filter(s => s.state === 'SUCCEEDED').length;
      if (doneCount < statuses.length) {
        return NextResponse.json({
          phase: 'bracket_running',
          progress: { step: 'bracket_running', percent: 75, message: `Bracket queries: ${doneCount}/${statuses.length} done...` },
        });
      }

      // All done — pull the count results from the count query's CSV
      const countCsvKey = `athena-results/${state.countQueryId}.csv`;
      const countObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: countCsvKey }));
      const countBody = countObj.Body as { transformToString: (encoding: string) => Promise<string> } | undefined;
      const countCsv = countBody ? await countBody.transformToString('utf-8') : '';
      const labelToCount = new Map<string, number>();
      for (const line of countCsv.split('\n').slice(1)) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const parts = trimmed.split(',').map((p: string) => p.replace(/^"|"$/g, ''));
        if (parts.length < 2) continue;
        labelToCount.set(parts[0], parseInt(parts[1], 10) || 0);
      }

      // Load NSE data once for postal code count + population per bracket
      const nseData = await getConfig<NseRecord[]>(NSE_KEY(state.country)) || [];
      const stateUnloads = state.unloads;

      // Build brackets array
      const brackets: BracketResult[] = NSE_BRACKETS.map(b => {
        const inBracket = nseData.filter(r => r.nse >= b.min && r.nse <= b.max);
        const population = inBracket.reduce((s, r) => s + r.population, 0);
        const handle = stateUnloads.find(u => u.label === b.label);
        const maidCount = labelToCount.get(b.label) || 0;
        return {
          label: b.label,
          min: b.min,
          max: b.max,
          postalCodes: inBracket.length,
          population,
          maidCount,
          // Frontend hits the download endpoint which lists + concatenates
          // the UNLOAD output files under unloadS3Prefix.
          downloadUrl: handle && maidCount > 0
            ? `/api/mega-jobs/${id}/reports/download?type=nse&prefix=${encodeURIComponent(handle.unloadS3Prefix)}`
            : null,
        };
      });

      const totalMaids = brackets.reduce((s, b) => s + b.maidCount, 0);
      console.log(`[MEGA-NSE-POLL] Done: ${totalMaids} total MAIDs across ${brackets.length} brackets`);

      // Cleanup temp tables (fire-and-forget). The UNLOAD output stays in
      // exports/ for the user to download.
      if (state.ctasTable) {
        const t = state.ctasTable;
        Promise.all([dropTempTable(t), cleanupTempS3(t)]).catch(() => {});
      }
      if (state.poiTableRef && state.runId) {
        const ref = state.poiTableRef;
        const prefix = poiCoordsTablePrefix(id, state.runId);
        dropPoiCoordsTable(ref, prefix).catch(() => {});
      }
      if (state.bracketMapTable && state.bracketMapS3Prefix) {
        const t = state.bracketMapTable; const p = state.bracketMapS3Prefix;
        dropNseBracketMap(t, p).catch(() => {});
      }

      state = { ...state, phase: 'done', brackets };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        brackets,
        totalMaids,
        progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
      });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[MEGA-NSE-POLL] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(id), {
        phase: 'error', error: error.message, country: '', megaJobId: id,
      });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
