/**
 * Compare → Potential Reach analysis.
 *
 * For two datasets A and B, identifies devices that visited A's POIs and
 * have a *cluster* of pings (≥ minPings OR ≥ minDwellMinutes) within
 * `maxDistanceMeters` of any of B's POIs — i.e. A-visitors who plausibly
 * could / did spend meaningful time near B's POIs. Bidirectional: both
 * A→B and B→A run in parallel.
 *
 * Constraints:
 *   - Capped at 500 POIs per side (inlined as VALUES into the SQL).
 *   - Uses the same grid-bucket spatial join as the rest of the
 *     consolidation pipeline so the spatial step is O(N) on pings.
 */

import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
  startCTASAsync,
  tempTableName,
  dropTempTable,
  cleanupTempS3,
} from '@/lib/athena';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

// ── Types ─────────────────────────────────────────────────────────────

interface PoiPosition {
  poiId: string;
  name?: string;
  lat: number;
  lng: number;
}

interface ReachConfig {
  maxDistanceMeters: number;
  minPings: number;
  minDwellMinutes: number;
}

interface ReachByPoi {
  poiId: string;
  poiName: string;
  lat: number;
  lng: number;
  potentialVisitors: number;
  avgPings: number;
  avgDwellMinutes: number;
}

interface ReachDirectionResult {
  source: { datasetName: string; visitorCount: number };
  target: { datasetName: string; poiCount: number };
  totalPotentialVisitors: number;
  byPoi: ReachByPoi[];
  downloadKey: string;
}

interface ReachResult {
  config: ReachConfig;
  aToB?: ReachDirectionResult;
  bToA?: ReachDirectionResult;
}

interface DirectionState {
  ctasTable?: string;          // per-POI aggregation table
  ctasQueryId?: string;        // CTAS query (for polling)
  totalQueryId?: string;       // SELECT COUNT(DISTINCT ad_id) FROM qualified
  exportQueryId?: string;      // UNLOAD-equivalent: SELECT DISTINCT ad_id (CSV)
  done?: boolean;
}

interface ReachState {
  phase: 'querying' | 'polling' | 'reading' | 'done' | 'error';
  stateId: string;
  datasetA: string;
  datasetB: string;
  config: ReachConfig;
  directions: ('aToB' | 'bToA')[];
  aToB?: DirectionState;
  bToA?: DirectionState;
  error?: string;
  result?: ReachResult;
}

const STATE_KEY = (id: string) => `compare-reach-state/${id}`;

// ── SQL helpers ───────────────────────────────────────────────────────

const ACCURACY = 500;
const GRID = 0.01;

/** Build the inline VALUES list for B-side POIs (≤500 expected). */
function poisValues(pois: PoiPosition[]): string {
  // Each row: ('poi-id', 'name with single quotes escaped', lat, lng)
  return pois
    .map((p) => {
      const name = (p.name || p.poiId).replace(/'/g, "''");
      const id = p.poiId.replace(/'/g, "''");
      return `('${id}', '${name}', CAST(${p.lat} AS DOUBLE), CAST(${p.lng} AS DOUBLE))`;
    })
    .join(', ');
}

/**
 * Build the per-(ad_id, target_poi) ping-cluster SQL — used both as the
 * CTAS source and as the basis for the total-distinct-ad_id count and the
 * CSV download.
 */
function clusterSelectSQL(
  sourceTable: string,
  targetPois: PoiPosition[],
  cfg: ReachConfig,
): string {
  return `
    WITH
    source_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${sourceTable}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    ),
    visitor_pings AS (
      SELECT
        p.ad_id, p.utc_timestamp,
        TRY_CAST(p.latitude AS DOUBLE) as lat,
        TRY_CAST(p.longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(p.latitude AS DOUBLE) / ${GRID}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(TRY_CAST(p.longitude AS DOUBLE) / ${GRID}) AS BIGINT) as lng_bucket
      FROM ${sourceTable} p
      INNER JOIN source_visitors v ON p.ad_id = v.ad_id
      WHERE TRY_CAST(p.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(p.longitude AS DOUBLE) IS NOT NULL
        AND (p.horizontal_accuracy IS NULL OR TRY_CAST(p.horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    ),
    target_pois AS (
      SELECT * FROM (VALUES ${poisValues(targetPois)}) AS t(poi_id, poi_name, poi_lat, poi_lng)
    ),
    target_poi_buckets AS (
      SELECT poi_id, poi_name, poi_lat, poi_lng,
        CAST(FLOOR(poi_lat / ${GRID}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(poi_lng / ${GRID}) AS BIGINT) + dlng as lng_bucket
      FROM target_pois
      CROSS JOIN (VALUES (-1), (0), (1)) AS d1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS d2(dlng)
    ),
    matched AS (
      SELECT vp.ad_id, vp.utc_timestamp,
        bp.poi_id, bp.poi_name
      FROM visitor_pings vp
      INNER JOIN target_poi_buckets bp
        ON vp.lat_bucket = bp.lat_bucket
        AND vp.lng_bucket = bp.lng_bucket
      WHERE 111320 * SQRT(
          POW(vp.lat - bp.poi_lat, 2) +
          POW((vp.lng - bp.poi_lng) * COS(RADIANS((vp.lat + bp.poi_lat) / 2)), 2)
        ) <= ${cfg.maxDistanceMeters}
    ),
    visitor_b_poi AS (
      SELECT
        ad_id, poi_id, poi_name,
        COUNT(*) as ping_count,
        DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0 as dwell_minutes
      FROM matched
      GROUP BY ad_id, poi_id, poi_name
    ),
    qualified AS (
      SELECT * FROM visitor_b_poi
      WHERE ping_count >= ${cfg.minPings}
         OR dwell_minutes >= ${cfg.minDwellMinutes}
    )
  `;
}

function buildPerPoiCTAS(sourceTable: string, targetPois: PoiPosition[], cfg: ReachConfig): string {
  return `${clusterSelectSQL(sourceTable, targetPois, cfg)}
    SELECT
      poi_id,
      poi_name,
      COUNT(DISTINCT ad_id) as potential_visitors,
      AVG(ping_count) as avg_pings,
      AVG(dwell_minutes) as avg_dwell_minutes
    FROM qualified
    GROUP BY poi_id, poi_name
  `;
}

function buildTotalDistinctSQL(sourceTable: string, targetPois: PoiPosition[], cfg: ReachConfig): string {
  return `${clusterSelectSQL(sourceTable, targetPois, cfg)}
    SELECT COUNT(DISTINCT ad_id) as total_potential FROM qualified
  `;
}

function buildExportMaidsSQL(sourceTable: string, targetPois: PoiPosition[], cfg: ReachConfig): string {
  return `${clusterSelectSQL(sourceTable, targetPois, cfg)}
    SELECT DISTINCT ad_id FROM qualified
  `;
}

function buildVisitorCountSQL(sourceTable: string): string {
  return `
    SELECT COUNT(DISTINCT ad_id) as visitor_count
    FROM ${sourceTable}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
  `;
}

// ── Phase logic ──────────────────────────────────────────────────────

async function loadPois(datasetName: string): Promise<PoiPosition[]> {
  const { getPOIPositionsForDataset } = await import('@/lib/poi-storage');
  const positions = await getPOIPositionsForDataset(datasetName);
  return positions
    .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lng))
    .map((p) => ({ poiId: p.poiId, name: p.name, lat: p.lat as number, lng: p.lng as number }));
}

async function startDirection(
  stateId: string,
  direction: 'aToB' | 'bToA',
  sourceTable: string,
  targetPois: PoiPosition[],
  cfg: ReachConfig,
): Promise<DirectionState> {
  const ctasTable = tempTableName(`reach_${direction}`, stateId.replace(/-/g, '_'));
  const perPoiSql = buildPerPoiCTAS(sourceTable, targetPois, cfg);
  // Total distinct visitors and CSV export run in parallel — the SAME source
  // SQL is repeated, so Athena is doing duplicate work. Kept simple for v1;
  // optimize later by reading from the per-POI CTAS once it lands.
  const [ctasQueryId, totalQueryId, exportQueryId] = await Promise.all([
    startCTASAsync(perPoiSql, ctasTable),
    startQueryAsync(buildTotalDistinctSQL(sourceTable, targetPois, cfg)),
    startQueryAsync(buildExportMaidsSQL(sourceTable, targetPois, cfg)),
  ]);

  console.log(`[REACH] Started ${direction}: ctas=${ctasQueryId}, total=${totalQueryId}, export=${exportQueryId}`);
  return { ctasTable, ctasQueryId, totalQueryId, exportQueryId, done: false };
}

async function readDirectionResult(
  ds: DirectionState,
  sourceDatasetName: string,
  targetDatasetName: string,
  sourceVisitorCount: number,
  targetPoiCount: number,
): Promise<ReachDirectionResult> {
  // Read per-POI aggregation
  const { runQueryViaS3 } = await import('@/lib/athena');
  const perPoi = await runQueryViaS3(`SELECT * FROM ${ds.ctasTable}`);
  const byPoi: ReachByPoi[] = (perPoi.rows || []).map((r: any) => ({
    poiId: String(r.poi_id || ''),
    poiName: String(r.poi_name || r.poi_id || ''),
    lat: 0, // patched below from target POI list
    lng: 0,
    potentialVisitors: parseInt(r.potential_visitors, 10) || 0,
    avgPings: parseFloat(r.avg_pings) || 0,
    avgDwellMinutes: Math.round((parseFloat(r.avg_dwell_minutes) || 0) * 10) / 10,
  })).sort((a, b) => b.potentialVisitors - a.potentialVisitors);

  // Read total distinct
  const totalRes = await fetchQueryResults(ds.totalQueryId!);
  const totalPotentialVisitors = parseInt((totalRes.rows[0] as any)?.total_potential, 10) || 0;

  return {
    source: { datasetName: sourceDatasetName, visitorCount: sourceVisitorCount },
    target: { datasetName: targetDatasetName, poiCount: targetPoiCount },
    totalPotentialVisitors,
    byPoi,
    downloadKey: ds.exportQueryId ? `athena-results/${ds.exportQueryId}.csv` : '',
  };
}

// ── Route handler ────────────────────────────────────────────────────

export async function POST(request: NextRequest): Promise<NextResponse> {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    let body: any = {};
    try { body = await request.json(); } catch {}

    const isNew = !!body.datasetA && !!body.datasetB;
    const stateId = body.stateId || '';
    let state: ReachState | null = null;

    if (!isNew && stateId) {
      state = await getConfig<ReachState>(STATE_KEY(stateId));
      if (!state) {
        return NextResponse.json({ phase: 'error', error: 'State not found — please rerun' });
      }
      if (state.phase === 'done' && state.result) {
        return NextResponse.json({ phase: 'done', stateId, result: state.result });
      }
    }

    // ── Phase: querying (start) ──────────────────────────────────────
    if (isNew) {
      const datasetA: string = body.datasetA;
      const datasetB: string = body.datasetB;
      const cfg: ReachConfig = {
        maxDistanceMeters: Math.max(10, Math.min(5000, parseInt(body.maxDistanceMeters, 10) || 200)),
        minPings: Math.max(1, parseInt(body.minPings, 10) || 3),
        minDwellMinutes: Math.max(0, parseInt(body.minDwellMinutes, 10) || 5),
      };
      const directions: ('aToB' | 'bToA')[] = body.directions?.length
        ? body.directions.filter((d: string) => d === 'aToB' || d === 'bToA')
        : ['aToB', 'bToA'];

      const newStateId = `${datasetA}-${datasetB}-reach-${Date.now()}`;
      console.log(`[REACH] Starting: ${datasetA} ↔ ${datasetB}, dirs=${directions.join(',')}, cfg=${JSON.stringify(cfg)}`);

      // Ensure both Athena tables exist
      await Promise.all([ensureTableForDataset(datasetA), ensureTableForDataset(datasetB)]);
      const tableA = getTableName(datasetA);
      const tableB = getTableName(datasetB);

      // Load POI lists (we cap at 500 because we inline VALUES)
      const [poisA, poisB] = await Promise.all([loadPois(datasetA), loadPois(datasetB)]);
      if (poisA.length === 0) {
        return NextResponse.json({ phase: 'error', error: `Dataset A (${datasetA}) has no POIs registered` }, { status: 400 });
      }
      if (poisB.length === 0) {
        return NextResponse.json({ phase: 'error', error: `Dataset B (${datasetB}) has no POIs registered` }, { status: 400 });
      }
      if (poisA.length > 500 || poisB.length > 500) {
        return NextResponse.json({ phase: 'error', error: `Reach analysis is capped at 500 POIs per side (A=${poisA.length}, B=${poisB.length}). Use a smaller POI collection.` }, { status: 400 });
      }

      state = {
        phase: 'querying',
        stateId: newStateId,
        datasetA,
        datasetB,
        config: cfg,
        directions,
      };
      await putConfig(STATE_KEY(newStateId), state, { compact: true });

      // Kick off CTAS + supporting queries for each direction
      if (directions.includes('aToB')) {
        state.aToB = await startDirection(newStateId, 'aToB', tableA, poisB, cfg);
      }
      if (directions.includes('bToA')) {
        state.bToA = await startDirection(newStateId, 'bToA', tableB, poisA, cfg);
      }

      state.phase = 'polling';
      await putConfig(STATE_KEY(newStateId), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        stateId: newStateId,
        progress: { step: 'queries_started', percent: 10, message: `Spatial join across ${directions.length} direction(s) started…` },
      });
    }

    if (!state) {
      return NextResponse.json({ phase: 'error', error: 'No request body and no stateId' }, { status: 400 });
    }

    // ── Phase: polling ───────────────────────────────────────────────
    if (state.phase === 'polling') {
      const queryIds: string[] = [];
      const dirs = state.directions;
      for (const d of dirs) {
        const ds = state[d];
        if (!ds) continue;
        if (ds.ctasQueryId) queryIds.push(ds.ctasQueryId);
        if (ds.totalQueryId) queryIds.push(ds.totalQueryId);
        if (ds.exportQueryId) queryIds.push(ds.exportQueryId);
      }

      // Check status of all
      const statuses = await Promise.all(queryIds.map((q) => checkQueryStatus(q)));
      const anyFailed = statuses.find((s) => s.state === 'FAILED' || s.state === 'CANCELLED');
      if (anyFailed) {
        state = { ...state, phase: 'error', error: `Athena query failed: ${anyFailed.error || 'unknown'}` };
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        return NextResponse.json({ phase: 'error', stateId: state.stateId, error: state.error });
      }
      const allDone = statuses.every((s) => s.state === 'SUCCEEDED');
      if (!allDone) {
        const doneCount = statuses.filter((s) => s.state === 'SUCCEEDED').length;
        return NextResponse.json({
          phase: 'polling',
          stateId: state.stateId,
          progress: { step: 'polling', percent: 30 + Math.round((doneCount / statuses.length) * 50), message: `Athena: ${doneCount}/${statuses.length} queries done` },
        });
      }
      state.phase = 'reading';
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
    }

    // ── Phase: reading (build result + cleanup) ──────────────────────
    if (state.phase === 'reading') {
      // Visitor counts (small, fast)
      const tableA = getTableName(state.datasetA);
      const tableB = getTableName(state.datasetB);
      const [poisA, poisB] = await Promise.all([loadPois(state.datasetA), loadPois(state.datasetB)]);
      const { runQueryViaS3 } = await import('@/lib/athena');
      const [vcA, vcB] = await Promise.all([
        state.directions.includes('aToB') ? runQueryViaS3(buildVisitorCountSQL(tableA)) : Promise.resolve({ rows: [{}] }),
        state.directions.includes('bToA') ? runQueryViaS3(buildVisitorCountSQL(tableB)) : Promise.resolve({ rows: [{}] }),
      ]);
      const visitorCountA = parseInt((vcA.rows[0] as any)?.visitor_count, 10) || 0;
      const visitorCountB = parseInt((vcB.rows[0] as any)?.visitor_count, 10) || 0;

      const result: ReachResult = { config: state.config };
      if (state.aToB) {
        const r = await readDirectionResult(state.aToB, state.datasetA, state.datasetB, visitorCountA, poisB.length);
        // Patch lat/lng from the target POI list (B's POIs)
        const poiMap = new Map(poisB.map((p) => [p.poiId, p]));
        for (const e of r.byPoi) {
          const p = poiMap.get(e.poiId);
          if (p) { e.lat = p.lat; e.lng = p.lng; }
        }
        result.aToB = r;
      }
      if (state.bToA) {
        const r = await readDirectionResult(state.bToA, state.datasetB, state.datasetA, visitorCountB, poisA.length);
        const poiMap = new Map(poisA.map((p) => [p.poiId, p]));
        for (const e of r.byPoi) {
          const p = poiMap.get(e.poiId);
          if (p) { e.lat = p.lat; e.lng = p.lng; }
        }
        result.bToA = r;
      }

      // Cleanup CTAS tables (fire-and-forget)
      const tablesToDrop: string[] = [];
      if (state.aToB?.ctasTable) tablesToDrop.push(state.aToB.ctasTable);
      if (state.bToA?.ctasTable) tablesToDrop.push(state.bToA.ctasTable);
      Promise.all([
        ...tablesToDrop.map((t) => dropTempTable(t).catch(() => {})),
        ...tablesToDrop.map((t) => cleanupTempS3(t).catch(() => {})),
      ]).catch(() => {});

      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        stateId: state.stateId,
        result,
        progress: { step: 'done', percent: 100, message: 'Done' },
      });
    }

    return NextResponse.json({ phase: 'error', error: `Unexpected phase: ${state.phase}` });
  } catch (e: any) {
    console.error('[REACH] error:', e?.message, e?.stack);
    return NextResponse.json({ phase: 'error', error: e?.message || String(e) }, { status: 500 });
  }
}
