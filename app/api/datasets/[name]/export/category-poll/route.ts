import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { toIsoCountry } from '@/lib/country-inference';
import { registerAthenaContribution, masterTableName } from '@/lib/master-maids';
import { getAllJobsSummary } from '@/lib/jobs';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const STATE_KEY = (ds: string) => `category-export-state/${ds}`;

const ACCURACY_THRESHOLD_METERS = 500;
const SPATIAL_RADIUS = 200; // meters
const GRID_STEP = 0.01; // ~1.1km grid cells
const BBOX_BUFFER = 0.02; // ~2.2km buffer

interface PoiInfo {
  name: string;
  category: string;
  lat: number;
  lng: number;
}

interface CategoryExportState {
  phase: 'querying' | 'polling' | 'processing' | 'done' | 'error';
  queryId?: string;
  ctasTable?: string;          // v2: Athena CTAS table name
  poiQueryId?: string;
  poiQueryDone?: boolean;
  country: string;
  categories: string[];
  groupKey: string;
  minDwell: number;
  dateRange?: { from: string; to: string };
  error?: string;
  pois?: PoiInfo[];
  result?: {
    maidCount: number;
    downloadKey: string;
    pois: PoiInfo[];
  };
}

/**
 * Build a query to list POIs matching the selected categories + country.
 */
function buildPoiListQuery(categories: string[], country: string): string {
  const catFilter = categories.map(c => `'${c}'`).join(',');
  const countryFilter = country ? `AND country = '${toIsoCountry(country)}'` : '';
  return `
    SELECT name, category, latitude, longitude
    FROM lab_pois_gmc
    WHERE category IN (${catFilter})
      ${countryFilter}
    ORDER BY category, name
  `;
}

/**
 * Build a simplified spatial join query that only returns DISTINCT ad_ids
 * that visited POIs of the given categories with minimum dwell time.
 * Adapted from laboratory-analyzer.ts buildSpatialJoinQueries but much lighter.
 */
/**
 * Build the spatial join SELECT (without CREATE TABLE wrapper).
 * Used both for regular queries (small datasets) and CTAS (large datasets).
 */
function buildSpatialJoinSelect(
  tableName: string,
  categories: string[],
  country: string,
  minDwell: number,
  maxDwell = 0,
  hourFrom = 0,
  hourTo = 23,
): string {
  const catFilter = `AND p.category IN (${categories.map(c => `'${c}'`).join(',')})`;
  const countryFilter = country ? `AND p.country = '${toIsoCountry(country)}'` : '';
  const dwellParts: string[] = [];
  if (minDwell > 0) dwellParts.push(`v.dwell_minutes >= ${minDwell}`);
  if (maxDwell > 0) dwellParts.push(`v.dwell_minutes <= ${maxDwell}`);
  const dwellFilter = dwellParts.length > 0 ? `WHERE ${dwellParts.join(' AND ')}` : '';
  // Hour filter on pings
  let hourFilter = '';
  if (hourFrom > 0 || hourTo < 23) {
    if (hourFrom <= hourTo) {
      hourFilter = `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
    } else {
      hourFilter = `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
    }
  }

  // Optimized for TB-scale datasets: NO window functions (ROW_NUMBER kills memory)
  // Instead of "closest POI per ping", we match all pings within radius and
  // aggregate directly. This uses orders of magnitude less memory.
  return `
    WITH
    poi_base AS (
      SELECT id as poi_id, category, latitude as poi_lat, longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID_STEP}) AS BIGINT) as base_lat_bucket,
        CAST(FLOOR(longitude / ${GRID_STEP}) AS BIGINT) as base_lng_bucket
      FROM lab_pois_gmc p
      WHERE p.category IS NOT NULL
        ${catFilter}
        ${countryFilter}
    ),
    poi_bounds AS (
      SELECT
        MIN(poi_lat) - ${BBOX_BUFFER} as min_lat,
        MAX(poi_lat) + ${BBOX_BUFFER} as max_lat,
        MIN(poi_lng) - ${BBOX_BUFFER} as min_lng,
        MAX(poi_lng) + ${BBOX_BUFFER} as max_lng
      FROM poi_base
    ),
    pings AS (
      SELECT
        ad_id,
        date,
        utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(latitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(TRY_CAST(longitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lng_bucket
      FROM ${tableName}
      WHERE TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(latitude AS DOUBLE) BETWEEN (SELECT min_lat FROM poi_bounds) AND (SELECT max_lat FROM poi_bounds)
        AND TRY_CAST(longitude AS DOUBLE) BETWEEN (SELECT min_lng FROM poi_bounds) AND (SELECT max_lng FROM poi_bounds)
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${hourFilter}
    ),
    poi_buckets AS (
      SELECT poi_id, category, poi_lat, poi_lng,
        base_lat_bucket + dlat as lat_bucket,
        base_lng_bucket + dlng as lng_bucket
      FROM poi_base
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
    ),
    matched AS (
      SELECT
        k.ad_id,
        k.date,
        k.utc_timestamp,
        p.category
      FROM pings k
      INNER JOIN poi_buckets p
        ON k.lat_bucket = p.lat_bucket
        AND k.lng_bucket = p.lng_bucket
      WHERE 111320 * SQRT(
          POW(k.lat - p.poi_lat, 2) +
          POW((k.lng - p.poi_lng) * COS(RADIANS((k.lat + p.poi_lat) / 2)), 2)
        ) <= ${SPATIAL_RADIUS}
    ),
    visits AS (
      SELECT
        ad_id,
        category,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_minutes
      FROM matched
      GROUP BY ad_id, date, category
    )
    SELECT ad_id, category, MAX(dwell_minutes) as dwell_minutes
    FROM visits v
    ${dwellFilter}
    GROUP BY ad_id, category
  `;
}

/**
 * For large datasets (>50GB), use CTAS instead of regular query.
 * CTAS has NO 30-min timeout limit — can run for hours if needed.
 * Results go to S3 as Parquet (much smaller than CSV).
 */
function buildCategoryMaidQuery(
  tableName: string,
  categories: string[],
  country: string,
  minDwell: number,
  maxDwell = 0,
  hourFrom = 0,
  hourTo = 23,
): string {
  return buildSpatialJoinSelect(tableName, categories, country, minDwell, maxDwell, hourFrom, hourTo);
}

function buildCategoryMaidCTAS(
  tableName: string,
  categories: string[],
  country: string,
  minDwell: number,
  ctasTableName: string,
  maxDwell = 0,
  hourFrom = 0,
  hourTo = 23,
): string {
  const selectSql = buildSpatialJoinSelect(tableName, categories, country, minDwell, maxDwell, hourFrom, hourTo);
  return `
    CREATE TABLE ${ctasTableName}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = 's3://${BUCKET}/athena-temp/${ctasTableName}/'
    )
    AS ${selectSql}
  `;
}

/**
 * POST /api/datasets/[name]/export/category-poll
 *
 * Multi-phase polling endpoint for category-based MAID extraction.
 * First call body: { categories: string[], groupKey: string, minDwell: number, country: string }
 * Subsequent calls: no body needed (reads state from S3)
 *
 * Phases:
 * 1. querying: ensure table, start Athena spatial join query
 * 2. polling: wait for Athena query to finish
 * 3. processing: download results, save CSV, return count
 * 4. done: return cached result
 */
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
    const isNewRequest = !!body.categories;

    let state = await getConfig<CategoryExportState>(STATE_KEY(datasetName));

    // If done and NOT a new request, return cached results
    if (state?.phase === 'done' && !isNewRequest && state.result) {
      return NextResponse.json({
        phase: 'done',
        result: state.result,
        progress: { step: 'done', percent: 100, message: `${state.result.maidCount.toLocaleString()} MAIDs found` },
      });
    }

    // Reset on error or new request
    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: start ─────────────────────────────────────────────
    if (!state) {
      const categories: string[] = body.categories || [];
      const country: string = body.country || '';
      const minDwell: number = parseInt(body.minDwell, 10) || 0;
      const maxDwell: number = parseInt(body.maxDwell, 10) || 0;
      const hourFrom: number = parseInt(body.hourFrom, 10) || 0;
      const hourTo: number = body.hourTo != null ? parseInt(body.hourTo, 10) : 23;
      const groupKey: string = body.groupKey || 'custom';

      if (!categories.length) {
        return NextResponse.json({ error: 'categories required' }, { status: 400 });
      }
      if (!country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      const filterLabel = [
        minDwell > 0 || maxDwell > 0 ? `dwell=${minDwell}-${maxDwell || '∞'}` : `dwell=${minDwell}`,
        hourFrom > 0 || hourTo < 23 ? `hours=${hourFrom}-${hourTo}` : '',
      ].filter(Boolean).join(', ');
      console.log(`[CATEGORY-POLL] Starting for ${datasetName}, country=${country}, ${filterLabel}, categories=${categories.length}`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      // ALWAYS use CTAS — no timeout limits, Parquet output, works for any size
      const cc = country.toUpperCase();
      const ctasTable = masterTableName(cc, 'cat_' + (groupKey || 'custom'), datasetName);
      const ctasSql = buildCategoryMaidCTAS(table, categories, country, minDwell, ctasTable, maxDwell, hourFrom, hourTo);
      const poiSql = buildPoiListQuery(categories, country);

      const [queryId, poiQueryId] = await Promise.all([
        startQueryAsync(ctasSql),
        startQueryAsync(poiSql),
      ]);
      console.log(`[CATEGORY-POLL] CTAS started: ${queryId}, pois=${poiQueryId}, table=${ctasTable}`);

      // Resolve dateRange from job metadata (backend-side)
      let dateRange = body.dateRange || { from: 'unknown', to: 'unknown' };
      if (dateRange.from === 'unknown') {
        try {
          const jobs = await getAllJobsSummary();
          const job = jobs.find(j => j.s3DestPath?.includes(datasetName));
          if (job?.actualDateRange) dateRange = { from: job.actualDateRange.from, to: job.actualDateRange.to };
          else if (job?.dateRange) dateRange = { from: (job.dateRange as any).from, to: (job.dateRange as any).to };
        } catch {}
      }
      state = { phase: 'polling', queryId, ctasTable, poiQueryId, poiQueryDone: false, country, categories, groupKey, minDwell, dateRange };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 10, message: 'Running spatial join query...' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    if (state.phase === 'polling' && state.queryId) {
      try {
        // Check main MAID query
        const status = await checkQueryStatus(state.queryId);
        let mainDone = false;

        if (status.state === 'FAILED' || status.state === 'CANCELLED') {
          state = { ...state, phase: 'error', error: status.error || 'Query failed' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

        if (status.state === 'SUCCEEDED') {
          mainDone = true;
        }

        // Check POI listing query (if not already done)
        if (state.poiQueryId && !state.poiQueryDone) {
          const poiStatus = await checkQueryStatus(state.poiQueryId);
          if (poiStatus.state === 'SUCCEEDED') {
            // Parse POI results immediately (small result set)
            const poiCsvKey = `athena-results/${state.poiQueryId}.csv`;
            const poiObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: poiCsvKey }));
            const poiCsvText = await poiObj.Body!.transformToString('utf-8');
            const poiLines = poiCsvText.split('\n');
            const pois: PoiInfo[] = [];
            for (let i = 1; i < poiLines.length; i++) {
              const line = poiLines[i].trim();
              if (!line) continue;
              // CSV: "name","category","latitude","longitude"
              const parts = line.match(/(?:"([^"]*)")|([^,]+)/g)?.map(s => s.replace(/^"|"$/g, '')) || [];
              if (parts.length >= 4) {
                pois.push({
                  name: parts[0] || 'Unknown',
                  category: parts[1],
                  lat: parseFloat(parts[2]),
                  lng: parseFloat(parts[3]),
                });
              }
            }
            state = { ...state, poiQueryDone: true, pois };
            console.log(`[CATEGORY-POLL] POI query done: ${pois.length} POIs found`);
          }
          // If POI query failed, not critical — continue without it
          if (poiStatus.state === 'FAILED' || poiStatus.state === 'CANCELLED') {
            state = { ...state, poiQueryDone: true, pois: [] };
          }
        }

        if (!mainDone) {
          const scannedGB = status.statistics?.dataScannedBytes
            ? (status.statistics.dataScannedBytes / 1e9).toFixed(1)
            : '0';
          const runtimeSec = status.statistics?.engineExecutionTimeMs
            ? Math.round(status.statistics.engineExecutionTimeMs / 1000)
            : 0;

          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          return NextResponse.json({
            phase: 'polling',
            progress: {
              step: 'polling',
              percent: 30,
              message: `Athena query running... ${scannedGB} GB scanned, ${runtimeSec}s`,
            },
          });
        }

        // Main query SUCCEEDED → move to processing
        state = { ...state, phase: 'processing' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });

        return NextResponse.json({
          phase: 'processing',
          progress: { step: 'processing', percent: 70, message: 'Query complete, processing results...' },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'error', error: 'Query expired — please retry' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        throw err;
      }
    }

    // ── Phase: processing ────────────────────────────────────────
    if (state.phase === 'processing' && state.queryId) {
      // Ensure POI list is loaded (may not have finished during polling phase)
      if (state.poiQueryId && !state.poiQueryDone) {
        try {
          const poiStatus = await checkQueryStatus(state.poiQueryId);
          if (poiStatus.state === 'SUCCEEDED') {
            const poiCsvKey = `athena-results/${state.poiQueryId}.csv`;
            const poiObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: poiCsvKey }));
            const poiCsvText = await poiObj.Body!.transformToString('utf-8');
            const poiLines = poiCsvText.split('\n');
            const pois: PoiInfo[] = [];
            for (let i = 1; i < poiLines.length; i++) {
              const line = poiLines[i].trim();
              if (!line) continue;
              const parts = line.match(/(?:"([^"]*)")|([^,]+)/g)?.map(s => s.replace(/^"|"$/g, '')) || [];
              if (parts.length >= 4) {
                pois.push({ name: parts[0] || 'Unknown', category: parts[1], lat: parseFloat(parts[2]), lng: parseFloat(parts[3]) });
              }
            }
            state = { ...state, poiQueryDone: true, pois };
            console.log(`[CATEGORY-POLL] POI query resolved in processing: ${pois.length} POIs`);
          } else if (poiStatus.state === 'RUNNING' || poiStatus.state === 'QUEUED') {
            // POI query still running — return to polling to wait
            state = { ...state, phase: 'polling' };
            await putConfig(STATE_KEY(datasetName), state, { compact: true });
            return NextResponse.json({
              phase: 'polling',
              progress: { step: 'loading_pois', percent: 75, message: 'Loading POI list...' },
            });
          } else {
            state = { ...state, poiQueryDone: true, pois: [] };
          }
        } catch {
          state = { ...state, poiQueryDone: true, pois: [] };
        }
      }

      // v2: CTAS already materialized results as Parquet — get exact count from Athena
      const cc = state.country.toUpperCase();
      const ctasTable = state.ctasTable || '';
      let maidCount = 0;

      if (ctasTable) {
        // Get exact MAID count + actual categories from the CTAS table
        let actualCategories: string[] = [];
        try {
          const { runQuery: runAthenaQuery } = await import('@/lib/athena');
          const countResult = await runAthenaQuery(
            `SELECT COUNT(DISTINCT ad_id) as cnt, ARRAY_JOIN(ARRAY_AGG(DISTINCT category), ',') as cats FROM ${ctasTable}`
          );
          maidCount = parseInt(String(countResult.rows[0]?.cnt)) || 0;
          const catsStr = String(countResult.rows[0]?.cats || '');
          if (catsStr) actualCategories = catsStr.split(',').filter(Boolean);
          console.log(`[CATEGORY-POLL] Exact count from CTAS: ${maidCount.toLocaleString()} MAIDs, categories: ${actualCategories.join(', ')}`);
        } catch (e: any) {
          console.warn(`[CATEGORY-POLL] Count query failed: ${e.message}`);
        }

        // Register with actual category names from the data (not the parent group key)
        const attrValue = actualCategories.length > 0
          ? actualCategories.join(',')
          : state.groupKey;
        const dr = state.dateRange || { from: 'unknown', to: 'unknown' };
        try {
          await registerAthenaContribution(
            cc, datasetName, 'category', attrValue,
            ctasTable, `athena-temp/${ctasTable}/`,
            maidCount, dr,
          );
          console.log(`[CATEGORY-POLL] Registered CTAS contribution: ${ctasTable} (${attrValue})`);
        } catch (e: any) {
          console.warn(`[CATEGORY-POLL] Failed to register: ${e.message}`);
        }
      }

      const result = { maidCount, downloadKey: '', pois: state.pois || [] };

      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        result,
        progress: { step: 'done', percent: 100, message: `${maidCount.toLocaleString()} MAIDs found` },
      });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[CATEGORY-POLL] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(datasetName), {
        phase: 'error', error: error.message,
        country: '', categories: [], groupKey: '', minDwell: 0,
      });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
