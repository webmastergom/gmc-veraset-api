import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { toIsoCountry } from '@/lib/country-inference';
import { registerAthenaContribution, masterTableName } from '@/lib/master-maids';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const STATE_KEY = (id: string) => `category-export-state/mega-${id}`;

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
  ctasTable?: string;
  poiQueryId?: string;
  poiQueryDone?: boolean;
  country: string;
  categories: string[];
  groupKey: string;
  minDwell: number;
  dateRange?: { from: string; to: string };
  subDatasetNames?: string[];
  error?: string;
  pois?: PoiInfo[];
  result?: {
    maidCount: number;
    downloadKey: string;
    pois: PoiInfo[];
  };
}

/**
 * POI listing query — same as dataset version (POIs are per-country, not per-dataset).
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
 * Build the spatial join SELECT consolidated across multiple sub-job tables.
 * Uses UNION ALL on the pings CTE so a device that visits in multiple sub-jobs
 * is counted once (downstream DISTINCT ad_id).
 */
function buildConsolidatedSpatialJoinSelect(
  subTables: string[],
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
  let hourFilter = '';
  if (hourFrom > 0 || hourTo < 23) {
    if (hourFrom <= hourTo) {
      hourFilter = `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
    } else {
      hourFilter = `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
    }
  }

  // Build UNION ALL of pings from each sub-job table.
  // Pre-filter at the source to keep intermediate result small.
  const pingsUnion = subTables
    .map(t => `
      SELECT
        ad_id,
        date,
        utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(latitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(TRY_CAST(longitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lng_bucket
      FROM ${t}
      WHERE TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(latitude AS DOUBLE) BETWEEN (SELECT min_lat FROM poi_bounds) AND (SELECT max_lat FROM poi_bounds)
        AND TRY_CAST(longitude AS DOUBLE) BETWEEN (SELECT min_lng FROM poi_bounds) AND (SELECT max_lng FROM poi_bounds)
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${hourFilter}
    `)
    .join('\n      UNION ALL\n');

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
      ${pingsUnion}
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

function buildCategoryMaidCTAS(
  subTables: string[],
  categories: string[],
  country: string,
  minDwell: number,
  ctasTableName: string,
  maxDwell = 0,
  hourFrom = 0,
  hourTo = 23,
): string {
  const selectSql = buildConsolidatedSpatialJoinSelect(subTables, categories, country, minDwell, maxDwell, hourFrom, hourTo);
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
 * POST /api/mega-jobs/[id]/export/category-poll
 *
 * Multi-phase polling endpoint for category-based MAID extraction across all
 * synced sub-jobs of a megajob (dedupes ad_ids across sub-jobs via DISTINCT).
 *
 * First call body: { categories: string[], groupKey: string, minDwell: number, country: string, ... }
 * Subsequent calls: no body (reads state from S3).
 *
 * Phases mirror the dataset endpoint:
 * 1. querying: load sub-jobs, ensure tables, start CTAS spatial join + POI listing
 * 2. polling: wait for Athena query
 * 3. processing: get exact MAID count from CTAS, register with master-maids
 * 4. done: return result
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

    let body: any;
    try { body = await request.json(); } catch { body = {}; }
    const isNewRequest = !!body.categories;

    let state = await getConfig<CategoryExportState>(STATE_KEY(id));

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
      const country: string = body.country || megaJob.country || '';
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

      // Load synced sub-jobs and resolve their dataset names
      const subJobs = (
        await Promise.all(megaJob.subJobIds.map((jid) => getJob(jid)))
      ).filter((j): j is NonNullable<typeof j> => j !== null && !!j.s3DestPath);

      if (subJobs.length === 0) {
        return NextResponse.json({ error: 'No synced sub-jobs found for this megajob' }, { status: 400 });
      }

      const subDatasetNames = subJobs.map(j => j.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      console.log(`[MEGA-CATEGORY-POLL] Starting for megajob=${id}, country=${country}, sub-datasets=${subDatasetNames.length}, categories=${categories.length}`);

      // Ensure all sub-job Athena tables exist
      await Promise.all(subDatasetNames.map(ds => ensureTableForDataset(ds)));
      const subTables = subDatasetNames.map(ds => getTableName(ds));

      // CTAS table: include megajob id slice for uniqueness
      const cc = country.toUpperCase();
      const shortId = id.replace(/-/g, '').slice(0, 12);
      const ctasTable = masterTableName(cc, 'cat_' + (groupKey || 'custom'), `mega_${shortId}`);
      const ctasSql = buildCategoryMaidCTAS(subTables, categories, country, minDwell, ctasTable, maxDwell, hourFrom, hourTo);
      const poiSql = buildPoiListQuery(categories, country);

      const [queryId, poiQueryId] = await Promise.all([
        startQueryAsync(ctasSql),
        startQueryAsync(poiSql),
      ]);
      console.log(`[MEGA-CATEGORY-POLL] CTAS started: ${queryId}, pois=${poiQueryId}, table=${ctasTable}`);

      // Aggregate date range across sub-jobs
      const fromDates: string[] = [];
      const toDates: string[] = [];
      for (const j of subJobs) {
        const r: any = (j as any).actualDateRange || (j as any).dateRange;
        if (r?.from) fromDates.push(r.from);
        if (r?.to) toDates.push(r.to);
      }
      const dateRange = fromDates.length && toDates.length
        ? { from: fromDates.sort()[0], to: toDates.sort().slice(-1)[0] }
        : { from: 'unknown', to: 'unknown' };

      state = {
        phase: 'polling',
        queryId,
        ctasTable,
        poiQueryId,
        poiQueryDone: false,
        country,
        categories,
        groupKey,
        minDwell,
        dateRange,
        subDatasetNames,
      };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 10, message: `Running spatial join across ${subDatasetNames.length} sub-jobs...` },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    if (state.phase === 'polling' && state.queryId) {
      try {
        const status = await checkQueryStatus(state.queryId);
        let mainDone = false;

        if (status.state === 'FAILED' || status.state === 'CANCELLED') {
          state = { ...state, phase: 'error', error: status.error || 'Query failed' };
          await putConfig(STATE_KEY(id), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

        if (status.state === 'SUCCEEDED') mainDone = true;

        // Resolve POI listing in parallel
        if (state.poiQueryId && !state.poiQueryDone) {
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
                pois.push({
                  name: parts[0] || 'Unknown',
                  category: parts[1],
                  lat: parseFloat(parts[2]),
                  lng: parseFloat(parts[3]),
                });
              }
            }
            state = { ...state, poiQueryDone: true, pois };
            console.log(`[MEGA-CATEGORY-POLL] POI query done: ${pois.length} POIs found`);
          }
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

          await putConfig(STATE_KEY(id), state, { compact: true });
          return NextResponse.json({
            phase: 'polling',
            progress: {
              step: 'polling',
              percent: 30,
              message: `Athena query running... ${scannedGB} GB scanned, ${runtimeSec}s`,
            },
          });
        }

        state = { ...state, phase: 'processing' };
        await putConfig(STATE_KEY(id), state, { compact: true });

        return NextResponse.json({
          phase: 'processing',
          progress: { step: 'processing', percent: 70, message: 'Query complete, processing results...' },
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

    // ── Phase: processing ────────────────────────────────────────
    if (state.phase === 'processing' && state.queryId) {
      // Backstop: ensure POI list resolved
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
          } else if (poiStatus.state === 'RUNNING' || poiStatus.state === 'QUEUED') {
            state = { ...state, phase: 'polling' };
            await putConfig(STATE_KEY(id), state, { compact: true });
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

      const cc = state.country.toUpperCase();
      const ctasTable = state.ctasTable || '';
      let maidCount = 0;

      if (ctasTable) {
        let actualCategories: string[] = [];
        try {
          const { runQuery: runAthenaQuery } = await import('@/lib/athena');
          const countResult = await runAthenaQuery(
            `SELECT COUNT(DISTINCT ad_id) as cnt, ARRAY_JOIN(ARRAY_AGG(DISTINCT category), ',') as cats FROM ${ctasTable}`
          );
          maidCount = parseInt(String(countResult.rows[0]?.cnt)) || 0;
          const catsStr = String(countResult.rows[0]?.cats || '');
          if (catsStr) actualCategories = catsStr.split(',').filter(Boolean);
          console.log(`[MEGA-CATEGORY-POLL] Exact count from CTAS: ${maidCount.toLocaleString()} MAIDs, categories: ${actualCategories.join(', ')}`);
        } catch (e: any) {
          console.warn(`[MEGA-CATEGORY-POLL] Count query failed: ${e.message}`);
        }

        // Register with master-maids using megajob id as the source dataset name
        const attrValue = actualCategories.length > 0 ? actualCategories.join(',') : state.groupKey;
        const dr = state.dateRange || { from: 'unknown', to: 'unknown' };
        const sourceName = `mega-${id}`;
        try {
          await registerAthenaContribution(
            cc, sourceName, 'category', attrValue,
            ctasTable, `athena-temp/${ctasTable}/`,
            maidCount, dr,
          );
          console.log(`[MEGA-CATEGORY-POLL] Registered CTAS contribution: ${ctasTable} (${attrValue}) for ${sourceName}`);
        } catch (e: any) {
          console.warn(`[MEGA-CATEGORY-POLL] Failed to register: ${e.message}`);
        }
      }

      const result = { maidCount, downloadKey: '', pois: state.pois || [] };

      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        result,
        progress: { step: 'done', percent: 100, message: `${maidCount.toLocaleString()} MAIDs found` },
      });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[MEGA-CATEGORY-POLL] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(id), {
        phase: 'error', error: error.message,
        country: '', categories: [], groupKey: '', minDwell: 0,
      });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
