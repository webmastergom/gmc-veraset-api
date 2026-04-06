import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { PutObjectCommand, GetObjectCommand, CopyObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { toIsoCountry } from '@/lib/country-inference';
import { registerContribution } from '@/lib/master-maids';
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
function buildCategoryMaidQuery(
  tableName: string,
  categories: string[],
  country: string,
  minDwell: number,
): string {
  const catFilter = `AND p.category IN (${categories.map(c => `'${c}'`).join(',')})`;
  const countryFilter = country ? `AND p.country = '${toIsoCountry(country)}'` : '';
  const dwellFilter = minDwell > 0 ? `WHERE v.dwell_minutes >= ${minDwell}` : '';

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
        p.poi_id,
        p.category,
        111320 * SQRT(
          POW(k.lat - p.poi_lat, 2) +
          POW((k.lng - p.poi_lng) * COS(RADIANS((k.lat + p.poi_lat) / 2)), 2)
        ) as distance_m
      FROM pings k
      INNER JOIN poi_buckets p
        ON k.lat_bucket = p.lat_bucket
        AND k.lng_bucket = p.lng_bucket
    ),
    closest AS (
      SELECT
        ad_id, date, utc_timestamp, poi_id, category, distance_m,
        ROW_NUMBER() OVER (PARTITION BY ad_id, utc_timestamp ORDER BY distance_m) as rn
      FROM matched
      WHERE distance_m <= ${SPATIAL_RADIUS}
    ),
    poi_pings AS (
      SELECT ad_id, date, utc_timestamp, poi_id, category
      FROM closest WHERE rn = 1
    ),
    visits AS (
      SELECT
        ad_id,
        poi_id,
        category,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_minutes
      FROM poi_pings
      GROUP BY ad_id, date, poi_id, category
    )
    SELECT ad_id, category, MAX(dwell_minutes) as dwell_minutes
    FROM visits v
    ${dwellFilter}
    GROUP BY ad_id, category
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
      const minDwell: number = body.minDwell || 0;
      const groupKey: string = body.groupKey || 'custom';

      if (!categories.length) {
        return NextResponse.json({ error: 'categories required' }, { status: 400 });
      }
      if (!country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      console.log(`[CATEGORY-POLL] Starting for ${datasetName}, country=${country}, minDwell=${minDwell}, categories=${categories.length}`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      const sql = buildCategoryMaidQuery(table, categories, country, minDwell);
      const poiSql = buildPoiListQuery(categories, country);
      const [queryId, poiQueryId] = await Promise.all([
        startQueryAsync(sql),
        startQueryAsync(poiSql),
      ]);
      console.log(`[CATEGORY-POLL] Athena queries started: maid=${queryId}, pois=${poiQueryId}`);

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
      state = { phase: 'polling', queryId, poiQueryId, poiQueryDone: false, country, categories, groupKey, minDwell, dateRange };
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

      // ZERO MEMORY approach: never read the CSV in Node.js (can be 2TB+)
      // Use S3 CopyObject (server-side) + Athena for count
      const csvKey = `athena-results/${state.queryId}.csv`;
      const timestamp = Date.now();
      const cc = state.country.toUpperCase();

      // 1. Verify CSV exists
      let csvSizeBytes = 0;
      try {
        const { HeadObjectCommand } = await import('@aws-sdk/client-s3');
        const head = await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: csvKey }));
        csvSizeBytes = head.ContentLength || 0;
        console.log(`[CATEGORY-POLL] CSV: ${(csvSizeBytes / 1e6).toFixed(0)}MB`);
      } catch {
        state = { ...state, phase: 'error', error: 'Query results expired. Please retry.' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      // 2. S3 CopyObject to contributions + exports (server-side, zero memory, up to 5GB)
      const contribKey = `master-maids/${cc}/contributions/${datasetName}-cat-${state.groupKey}-${timestamp}.csv`;
      const exportFileName = `${datasetName}-category-${state.groupKey}-${timestamp}.csv`;
      const exportKey = `exports/${exportFileName}`;

      try {
        await Promise.all([
          s3Client.send(new CopyObjectCommand({
            Bucket: BUCKET, CopySource: `${BUCKET}/${csvKey}`, Key: contribKey, ContentType: 'text/csv',
          })),
          s3Client.send(new CopyObjectCommand({
            Bucket: BUCKET, CopySource: `${BUCKET}/${csvKey}`, Key: exportKey, ContentType: 'text/csv',
          })),
        ]);
        const dr = state.dateRange || { from: 'unknown', to: 'unknown' };
        await registerContribution(cc, datasetName, 'category', state.groupKey, contribKey, dr);
        console.log(`[CATEGORY-POLL] S3 copies done, contribution registered`);
      } catch (e: any) {
        console.warn(`[CATEGORY-POLL] S3 copy/register failed: ${e.message}`);
      }

      // 3. Estimate MAID count from CSV size (~80 bytes per row for "ad_id","category","dwell")
      const estimatedMaids = Math.round(csvSizeBytes / 80);

      const downloadUrl = csvSizeBytes > 0
        ? `/api/datasets/${datasetName}/export/download?file=${encodeURIComponent(exportFileName)}`
        : null;

      const result = { maidCount: estimatedMaids, downloadKey: downloadUrl || '', pois: state.pois || [] };

      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        result,
        progress: { step: 'done', percent: 100, message: `~${estimatedMaids.toLocaleString()} MAIDs found` },
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
