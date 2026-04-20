/**
 * POST /api/datasets/[name]/routes/poll
 *
 * Device route analysis: what POI categories do visitors go to BEFORE and AFTER
 * visiting the target POIs? Uses Overture Places (lab_pois_gmc) spatial join.
 *
 * Rewritten from scratch 2026-04-20.
 */

import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
  runQuery,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const GRID = 0.01;       // ~1.1km grid cell for spatial join
const RADIUS = 200;      // meters — max distance to count as "at" an Overture POI
const ACCURACY = 500;    // horizontal_accuracy filter
const SAMPLE_SIZE = 1000;

// ── Types ────────────────────────────────────────────────────────────

interface RoutesState {
  phase: 'polling' | 'reading' | 'done' | 'error';
  sankeyQId?: string;
  sampleQId?: string;
  sankeyDone?: boolean;
  sampleDone?: boolean;
  filters: Filters;
  error?: string;
  result?: RoutesResult;
}

interface Filters {
  country: string;
  minDwell: number;
  maxDwell: number;
  hourFrom: number;
  hourTo: number;
  minVisits: number;
}

interface SankeyFlow {
  direction: 'before' | 'after';
  category: string;
  devices: number;
  visits: number;
}

interface SampleStop {
  ad_id: string;
  date: string;
  ts: string;
  direction: 'before' | 'during' | 'after';
  category: string;
  dwell_minutes: number;
}

interface RoutesResult {
  sankey: SankeyFlow[];
  sampleRoutes: SampleStop[];
  totalVisitors: number;
}

// ── SQL builders ─────────────────────────────────────────────────────

function hourWhere(hourFrom: number, hourTo: number, col = 'utc_timestamp'): string {
  if (hourFrom === 0 && hourTo === 23) return '';
  if (hourFrom <= hourTo) return `AND HOUR(${col}) >= ${hourFrom} AND HOUR(${col}) <= ${hourTo}`;
  return `AND (HOUR(${col}) >= ${hourFrom} OR HOUR(${col}) <= ${hourTo})`;
}

/**
 * CTE: target_daily — one row per (ad_id, date) with arrival/departure times.
 * Identifies devices that visited the target POIs with optional dwell/hour/minVisits filters.
 */
function targetDailyCTE(table: string, f: Filters): string {
  const hWhere = hourWhere(f.hourFrom, f.hourTo);
  const dwellParts: string[] = [];
  if (f.minDwell > 0) dwellParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${f.minDwell}`);
  if (f.maxDwell > 0) dwellParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${f.maxDwell}`);
  const having = dwellParts.length > 0 ? `HAVING ${dwellParts.join(' AND ')}` : '';

  const visitFilter = f.minVisits > 1 ? `
    visit_day_filter AS (
      SELECT ad_id FROM (
        SELECT ad_id, COUNT(DISTINCT date) as vd
        FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
        WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        GROUP BY ad_id HAVING COUNT(DISTINCT date) >= ${f.minVisits}
      )
    ),` : '';
  const visitWhere = f.minVisits > 1 ? `AND ad_id IN (SELECT ad_id FROM visit_day_filter)` : '';

  return `${visitFilter}
    all_target_visits AS (
      SELECT ad_id, date,
        MIN(utc_timestamp) as arrival,
        MAX(utc_timestamp) as departure,
        DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${hWhere} ${visitWhere}
      GROUP BY ad_id, date, poi_id
      ${having}
    ),
    target_daily AS (
      SELECT ad_id, date,
        MIN(arrival) as arrival,
        MAX(departure) as departure,
        MAX(dwell) as dwell
      FROM all_target_visits
      GROUP BY ad_id, date
    )`;
}

/**
 * Build Sankey query: aggregate before/after POI category visits across ALL visitors.
 */
function buildSankeySQL(table: string, f: Filters): string {
  return `
    WITH
    ${targetDailyCTE(table, f)},
    before_after_pings AS (
      SELECT
        p.ad_id, p.date,
        TRY_CAST(p.latitude AS DOUBLE) as lat,
        TRY_CAST(p.longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(p.latitude AS DOUBLE) / ${GRID}) AS BIGINT) as lat_b,
        CAST(FLOOR(TRY_CAST(p.longitude AS DOUBLE) / ${GRID}) AS BIGINT) as lng_b,
        CASE WHEN p.utc_timestamp < td.arrival THEN 'before' ELSE 'after' END as direction
      FROM ${table} p
      INNER JOIN target_daily td ON p.ad_id = td.ad_id AND p.date = td.date
      WHERE TRY_CAST(p.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(p.longitude AS DOUBLE) IS NOT NULL
        AND (p.horizontal_accuracy IS NULL OR TRY_CAST(p.horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        AND (p.utc_timestamp < td.arrival OR p.utc_timestamp > td.departure)
    ),
    poi_grid AS (
      SELECT category,
        CAST(latitude AS DOUBLE) as poi_lat,
        CAST(longitude AS DOUBLE) as poi_lng,
        CAST(FLOOR(CAST(latitude AS DOUBLE) / ${GRID}) AS BIGINT) + dlat as lat_b,
        CAST(FLOOR(CAST(longitude AS DOUBLE) / ${GRID}) AS BIGINT) + dlng as lng_b
      FROM lab_pois_gmc
      CROSS JOIN (VALUES (-1),(0),(1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1),(0),(1)) AS t2(dlng)
      WHERE category IS NOT NULL
    ),
    matched AS (
      SELECT p.ad_id, p.date, p.direction, g.category
      FROM before_after_pings p
      INNER JOIN poi_grid g ON p.lat_b = g.lat_b AND p.lng_b = g.lng_b
      WHERE 111320 * SQRT(
        POW(p.lat - g.poi_lat, 2) +
        POW((p.lng - g.poi_lng) * COS(RADIANS((p.lat + g.poi_lat) / 2)), 2)
      ) <= ${RADIUS}
    ),
    per_device AS (
      SELECT ad_id, date, direction, category,
        ROW_NUMBER() OVER (PARTITION BY ad_id, date, direction, category ORDER BY category) as rn
      FROM matched
    )
    SELECT direction, category,
      COUNT(DISTINCT ad_id) as devices,
      COUNT(*) as visits
    FROM per_device WHERE rn = 1
    GROUP BY direction, category
    ORDER BY direction, devices DESC
  `;
}

/**
 * Build Sample query: detailed stops for SAMPLE_SIZE random devices.
 */
function buildSampleSQL(table: string, f: Filters): string {
  return `
    WITH
    ${targetDailyCTE(table, f)},
    sampled AS (
      SELECT ad_id FROM (
        SELECT ad_id, ROW_NUMBER() OVER (ORDER BY XXHASH64(CAST(ad_id AS VARBINARY))) as rn
        FROM (SELECT DISTINCT ad_id FROM target_daily)
      ) WHERE rn <= ${SAMPLE_SIZE}
    ),
    before_after_pings AS (
      SELECT
        p.ad_id, p.date, p.utc_timestamp,
        TRY_CAST(p.latitude AS DOUBLE) as lat,
        TRY_CAST(p.longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(p.latitude AS DOUBLE) / ${GRID}) AS BIGINT) as lat_b,
        CAST(FLOOR(TRY_CAST(p.longitude AS DOUBLE) / ${GRID}) AS BIGINT) as lng_b,
        CASE WHEN p.utc_timestamp < td.arrival THEN 'before' ELSE 'after' END as direction
      FROM ${table} p
      INNER JOIN sampled s ON p.ad_id = s.ad_id
      INNER JOIN target_daily td ON p.ad_id = td.ad_id AND p.date = td.date
      WHERE TRY_CAST(p.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(p.longitude AS DOUBLE) IS NOT NULL
        AND (p.horizontal_accuracy IS NULL OR TRY_CAST(p.horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        AND (p.utc_timestamp < td.arrival OR p.utc_timestamp > td.departure)
    ),
    poi_grid AS (
      SELECT category,
        CAST(latitude AS DOUBLE) as poi_lat,
        CAST(longitude AS DOUBLE) as poi_lng,
        CAST(FLOOR(CAST(latitude AS DOUBLE) / ${GRID}) AS BIGINT) + dlat as lat_b,
        CAST(FLOOR(CAST(longitude AS DOUBLE) / ${GRID}) AS BIGINT) + dlng as lng_b
      FROM lab_pois_gmc
      CROSS JOIN (VALUES (-1),(0),(1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1),(0),(1)) AS t2(dlng)
      WHERE category IS NOT NULL
    ),
    matched AS (
      SELECT p.ad_id, p.date, p.utc_timestamp, p.direction, g.category
      FROM before_after_pings p
      INNER JOIN poi_grid g ON p.lat_b = g.lat_b AND p.lng_b = g.lng_b
      WHERE 111320 * SQRT(
        POW(p.lat - g.poi_lat, 2) +
        POW((p.lng - g.poi_lng) * COS(RADIANS((p.lat + g.poi_lat) / 2)), 2)
      ) <= ${RADIUS}
    ),
    categorized AS (
      SELECT ad_id, date, direction, category,
        MIN(utc_timestamp) as first_ts,
        MAX(utc_timestamp) as last_ts,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_min
      FROM matched
      GROUP BY ad_id, date, direction, category

      UNION ALL

      SELECT td.ad_id, td.date, 'during' as direction, 'target' as category,
        td.arrival as first_ts, td.departure as last_ts,
        CAST(td.dwell AS DOUBLE) as dwell_min
      FROM target_daily td
      INNER JOIN sampled s ON td.ad_id = s.ad_id
    )
    SELECT ad_id, date, CAST(first_ts AS VARCHAR) as first_ts,
      direction, category, dwell_min
    FROM categorized
    ORDER BY ad_id, date, first_ts
  `;
}

// ── State helpers ────────────────────────────────────────────────────

const STATE_KEY = (ds: string, f: string) => `routes-state/${ds}-${f}`;
function filtersKey(f: Filters): string {
  return `d${f.minDwell}-${f.maxDwell}_h${f.hourFrom}-${f.hourTo}_v${f.minVisits}`;
}

// ── Handler ──────────────────────────────────────────────────────────

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { name: datasetName } = await context.params;

  try {
    let body: any;
    try { body = await request.json(); } catch { body = {}; }
    const isNewRequest = !!body.country;

    const filters: Filters = {
      country: body.country || '',
      minDwell: parseInt(body.minDwell, 10) || 0,
      maxDwell: parseInt(body.maxDwell, 10) || 0,
      hourFrom: parseInt(body.hourFrom, 10) || 0,
      hourTo: body.hourTo != null ? parseInt(body.hourTo, 10) : 23,
      minVisits: parseInt(body.minVisits, 10) || 1,
    };

    const fKey = filtersKey(filters);
    let stateKey = isNewRequest ? STATE_KEY(datasetName, fKey) : (body.stateKey || '');
    let state = stateKey ? await getConfig<RoutesState>(stateKey) : null;

    // Return cached result on poll
    if (state?.phase === 'done' && !isNewRequest && state.result) {
      return NextResponse.json({
        phase: 'done', stateKey, result: state.result,
        progress: { step: 'done', percent: 100, message: 'Route analysis complete' },
      });
    }

    // Reset on error or new request
    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Start ─────────────────────────────────────────────────
    if (!state) {
      if (!filters.country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      stateKey = STATE_KEY(datasetName, fKey);
      console.log(`[ROUTES] Starting for ${datasetName}`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      // Ensure lab_pois_gmc table exists
      const poiBucket = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
      try {
        await runQuery(`
          CREATE EXTERNAL TABLE IF NOT EXISTS lab_pois_gmc (
            id STRING, name STRING, category STRING, city STRING,
            postal_code STRING, country STRING, latitude DOUBLE, longitude DOUBLE
          ) STORED AS PARQUET
          LOCATION 's3://${poiBucket}/pois_gmc/'
        `);
      } catch (e: any) {
        if (!e.message?.includes('already exists')) console.warn(`[ROUTES] lab_pois_gmc:`, e.message);
      }

      // Fire both queries
      const [sankeyQId, sampleQId] = await Promise.all([
        startQueryAsync(buildSankeySQL(table, filters)).catch(e => { console.error('[ROUTES] Sankey:', e.message); return undefined; }),
        startQueryAsync(buildSampleSQL(table, filters)).catch(e => { console.error('[ROUTES] Sample:', e.message); return undefined; }),
      ]);

      console.log(`[ROUTES] 2 queries launched: sankey=${sankeyQId}, sample=${sampleQId}`);

      state = { phase: 'polling', sankeyQId, sampleQId, sankeyDone: false, sampleDone: false, filters };
      await putConfig(stateKey, state, { compact: true });

      return NextResponse.json({
        phase: 'polling', stateKey,
        progress: { step: 'started', percent: 10, message: 'Queries launched...' },
      });
    }

    // ── Polling ───────────────────────────────────────────────
    if (state.phase === 'polling') {
      let allDone = true;
      let anyFailed = false;
      let errorMsg = '';
      let doneCount = 0;

      for (const { id, key } of [
        { id: state.sankeyQId, key: 'sankeyDone' },
        { id: state.sampleQId, key: 'sampleDone' },
      ]) {
        if (!id || (state as any)[key]) { if ((state as any)[key]) doneCount++; continue; }
        try {
          const s = await checkQueryStatus(id);
          if (s.state === 'RUNNING' || s.state === 'QUEUED') {
            allDone = false;
          } else if (s.state === 'FAILED' || s.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `${key} failed: ${s.error || 'unknown'}`;
          } else {
            (state as any)[key] = true;
            doneCount++;
          }
        } catch (err: any) {
          if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
            anyFailed = true; errorMsg = `${key} expired`;
          } else throw err;
        }
      }

      if (anyFailed) {
        state = { ...state, phase: 'error', error: errorMsg };
        await putConfig(stateKey, state, { compact: true });
        return NextResponse.json({ phase: 'error', stateKey, error: errorMsg });
      }

      if (!allDone) {
        await putConfig(stateKey, state, { compact: true });
        return NextResponse.json({
          phase: 'polling', stateKey,
          progress: { step: 'polling', percent: 10 + doneCount * 30, message: `Queries running (${doneCount}/2 done)...` },
        });
      }

      state = { ...state, phase: 'reading' };
      await putConfig(stateKey, state, { compact: true });
      return NextResponse.json({
        phase: 'reading', stateKey,
        progress: { step: 'reading', percent: 75, message: 'Queries done, reading results...' },
      });
    }

    // ── Reading ───────────────────────────────────────────────
    if (state.phase === 'reading') {
      // Parse Sankey
      let sankeyFlows: SankeyFlow[] = [];
      let totalVisitors = 0;
      if (state.sankeyQId) {
        try {
          const r = await fetchQueryResults(state.sankeyQId);
          sankeyFlows = r.rows.map((row: any) => ({
            direction: row.direction as 'before' | 'after',
            category: row.category || 'unknown',
            devices: parseInt(row.devices, 10) || 0,
            visits: parseInt(row.visits, 10) || 0,
          }));
          // totalVisitors from target_daily count
          totalVisitors = Math.max(...sankeyFlows.map(f => f.devices), 0);
        } catch (err: any) {
          console.error('[ROUTES] Sankey read:', err.message);
        }
      }

      // Parse Sample
      let sampleStops: SampleStop[] = [];
      if (state.sampleQId) {
        try {
          const r = await fetchQueryResults(state.sampleQId);
          sampleStops = r.rows.map((row: any) => ({
            ad_id: row.ad_id,
            date: row.date,
            ts: row.first_ts,
            direction: row.direction as 'before' | 'during' | 'after',
            category: row.category || 'unknown',
            dwell_minutes: parseFloat(row.dwell_min) || 0,
          }));
        } catch (err: any) {
          console.error('[ROUTES] Sample read:', err.message);
        }
      }

      // Get total visitors from a quick count if sankey didn't provide it
      if (totalVisitors === 0 && sampleStops.length > 0) {
        totalVisitors = new Set(sampleStops.filter(s => s.direction === 'during').map(s => s.ad_id)).size;
      }

      const result: RoutesResult = { sankey: sankeyFlows, sampleRoutes: sampleStops, totalVisitors };
      state = { ...state, phase: 'done', result };
      await putConfig(stateKey, state, { compact: true });

      console.log(`[ROUTES] Done: ${sankeyFlows.length} sankey flows, ${sampleStops.length} sample stops`);

      return NextResponse.json({
        phase: 'done', stateKey, result,
        progress: { step: 'done', percent: 100, message: 'Route analysis complete' },
      });
    }

    return NextResponse.json({ phase: 'error', stateKey, error: 'Unknown state — retry' });

  } catch (error: any) {
    console.error(`[ROUTES] Error:`, error?.message, error?.stack);
    return NextResponse.json({ error: error?.message || 'Unknown error' }, { status: 500 });
  }
}
