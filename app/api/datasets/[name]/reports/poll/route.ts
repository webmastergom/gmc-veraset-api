import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import {
  parseConsolidatedOD,
  parseConsolidatedHourly,
  parseConsolidatedMobility,
  buildODReport,
  buildCatchmentReport,
  buildTemporalTrends,
} from '@/lib/mega-report-consolidation';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

const STATE_KEY = (ds: string) => `dataset-report-state/${ds}`;
const REPORT_PREFIX = (ds: string) => `dataset-reports/${ds}`;

// ── Types ─────────────────────────────────────────────────────────────

interface ReportState {
  phase: 'start' | 'polling' | 'parsing' | 'done' | 'error';
  queries: Record<string, string>;  // name → queryId
  error?: string;
}

async function getState(ds: string): Promise<ReportState | null> {
  return await getConfig<ReportState>(STATE_KEY(ds));
}

async function saveState(ds: string, state: ReportState): Promise<void> {
  await putConfig(STATE_KEY(ds), state, { compact: true });
}

// ── Query builders using poi_ids (no spatial join) ────────────────────

const ACCURACY = 500;
const PREC = 4;
const GRID = 0.01;
const POI_GMC_TABLE = 'lab_pois_gmc';

/**
 * Common CTE: filter pings that have poi_ids assigned by Veraset.
 * This is O(N) scan — no spatial join needed.
 */
function atPoiCTE(table: string): string {
  return `at_poi AS (
      SELECT DISTINCT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    )`;
}

function buildHourlySQL(table: string): string {
  return `WITH ${atPoiCTE(table)}
    SELECT HOUR(utc_timestamp) as touch_hour,
      COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY 1 ORDER BY 1`;
}

function buildTemporalSQL(table: string): string {
  return `WITH ${atPoiCTE(table)}
    SELECT date, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY date ORDER BY date`;
}

function buildTotalDevicesSQL(table: string): string {
  return `WITH ${atPoiCTE(table)}
    SELECT COUNT(DISTINCT ad_id) as total_unique_devices FROM at_poi`;
}

function buildODSQL(table: string): string {
  return `WITH ${atPoiCTE(table)},
    poi_visits AS (
      SELECT ad_id, date, MIN(utc_timestamp) as first_poi_visit
      FROM at_poi GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    ),
    device_day AS (
      SELECT p.ad_id, p.date,
        MIN_BY(p.lat, p.utc_timestamp) as origin_lat,
        MIN_BY(p.lng, p.utc_timestamp) as origin_lng,
        MAX_BY(p.lat, p.utc_timestamp) as dest_lat,
        MAX_BY(p.lng, p.utc_timestamp) as dest_lng,
        HOUR(MIN(p.utc_timestamp)) as departure_hour,
        HOUR(v.first_poi_visit) as poi_arrival_hour
      FROM all_pings p
      INNER JOIN poi_visits v ON p.ad_id = v.ad_id AND p.date = v.date
      GROUP BY p.ad_id, p.date, v.first_poi_visit
    )
    SELECT
      ROUND(origin_lat, ${PREC}) as origin_lat,
      ROUND(origin_lng, ${PREC}) as origin_lng,
      ROUND(dest_lat, ${PREC}) as dest_lat,
      ROUND(dest_lng, ${PREC}) as dest_lng,
      departure_hour, poi_arrival_hour,
      COUNT(*) as device_days
    FROM device_day
    GROUP BY 1,2,3,4,5,6
    ORDER BY device_days DESC
    LIMIT 100000`;
}

function buildCatchmentSQL(table: string): string {
  return `WITH ${atPoiCTE(table)},
    poi_visitors AS (SELECT DISTINCT ad_id FROM at_poi),
    valid_pings AS (
      SELECT t.ad_id, t.date, t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${table} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    ),
    first_pings AS (
      SELECT ad_id, date,
        MIN_BY(lat, utc_timestamp) as origin_lat,
        MIN_BY(lng, utc_timestamp) as origin_lng,
        HOUR(MIN(utc_timestamp)) as departure_hour
      FROM valid_pings
      GROUP BY ad_id, date
    )
    SELECT
      ROUND(origin_lat, ${PREC}) as origin_lat,
      ROUND(origin_lng, ${PREC}) as origin_lng,
      departure_hour,
      COUNT(*) as device_days
    FROM first_pings
    WHERE origin_lat IS NOT NULL
    GROUP BY 1,2,3
    ORDER BY device_days DESC
    LIMIT 100000`;
}

function buildMobilitySQL(table: string): string {
  return `WITH ${atPoiCTE(table)},
    target_visits AS (
      SELECT ad_id, date, MIN(utc_timestamp) as visit_time
      FROM at_poi GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(latitude AS DOUBLE) / ${GRID}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(TRY_CAST(longitude AS DOUBLE) / ${GRID}) AS BIGINT) as lng_bucket
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    ),
    nearby_pings AS (
      SELECT a.ad_id, a.date, a.lat, a.lng,
        a.lat_bucket, a.lng_bucket,
        CASE WHEN a.utc_timestamp < t.visit_time THEN 'before' ELSE 'after' END as timing
      FROM all_pings a
      INNER JOIN target_visits t ON a.ad_id = t.ad_id AND a.date = t.date
      WHERE ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) <= 120
        AND ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) > 0
    ),
    gmc_buckets AS (
      SELECT id as poi_id, name as poi_name, category,
        latitude as poi_lat, longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(longitude / ${GRID}) AS BIGINT) + dlng as lng_bucket
      FROM ${POI_GMC_TABLE}
      CROSS JOIN (VALUES (-1),(0),(1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1),(0),(1)) AS t2(dlng)
      WHERE category IS NOT NULL
    ),
    matched AS (
      SELECT n.ad_id, n.date, n.timing, g.category
      FROM nearby_pings n
      INNER JOIN gmc_buckets g ON n.lat_bucket = g.lat_bucket AND n.lng_bucket = g.lng_bucket
      WHERE 111320 * SQRT(
          POW(n.lat - g.poi_lat, 2) +
          POW((n.lng - g.poi_lng) * COS(RADIANS((n.lat + g.poi_lat) / 2)), 2)
        ) <= 200
    )
    SELECT category, timing,
      COUNT(DISTINCT ad_id || '|' || date) as device_days,
      COUNT(DISTINCT ad_id) as unique_devices
    FROM matched
    GROUP BY category, timing
    ORDER BY device_days DESC
    LIMIT 500`;
}

// ── Main handler ──────────────────────────────────────────────────────

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { name: datasetName } = await context.params;

  try {
    let state = await getState(datasetName);

    // Reset if done or error
    if (state?.phase === 'done' || state?.phase === 'error') state = null;

    // ── Phase: start ────────────────────────────────────────────
    if (!state) {
      console.log(`[DS-REPORT] Starting reports for ${datasetName}`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      // Launch all 6 queries in parallel using poi_ids (no spatial join)
      const queries: Record<string, string> = {};
      await Promise.all([
        startQueryAsync(buildODSQL(table)).then(id => { queries.od = id; }).catch(e => console.error('[DS-REPORT] od:', e.message)),
        startQueryAsync(buildHourlySQL(table)).then(id => { queries.hourly = id; }).catch(e => console.error('[DS-REPORT] hourly:', e.message)),
        startQueryAsync(buildCatchmentSQL(table)).then(id => { queries.catchment = id; }).catch(e => console.error('[DS-REPORT] catchment:', e.message)),
        startQueryAsync(buildTemporalSQL(table)).then(id => { queries.temporal = id; }).catch(e => console.error('[DS-REPORT] temporal:', e.message)),
        startQueryAsync(buildTotalDevicesSQL(table)).then(id => { queries.totalDevices = id; }).catch(e => console.error('[DS-REPORT] totalDevices:', e.message)),
        startQueryAsync(buildMobilitySQL(table)).then(id => { queries.mobility = id; }).catch(e => console.error('[DS-REPORT] mobility:', e.message)),
      ]);

      console.log(`[DS-REPORT] Launched ${Object.keys(queries).length} queries for ${datasetName}`);

      state = { phase: 'polling', queries };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'queries_started', percent: 10, message: `Running ${Object.keys(queries).length} Athena queries...` },
      });
    }

    // ── Phase: polling ──────────────────────────────────────────
    if (state.phase === 'polling') {
      const entries = Object.entries(state.queries);
      if (entries.length === 0) {
        state = { ...state, phase: 'error', error: 'No queries started' };
        await saveState(datasetName, state);
        return NextResponse.json({ phase: 'error', error: 'No queries started' });
      }

      const statuses = await Promise.all(
        entries.map(async ([name, queryId]) => {
          try {
            const s = await checkQueryStatus(queryId);
            return { name, state: s.state, error: s.error };
          } catch (err: any) {
            if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
              return { name, state: 'FAILED' as const, error: 'Query expired' };
            }
            return { name, state: 'FAILED' as const, error: err.message };
          }
        })
      );

      let allDone = true;
      let doneCount = 0;
      const running: string[] = [];

      for (const s of statuses) {
        if (s.state === 'RUNNING' || s.state === 'QUEUED') {
          allDone = false;
          running.push(s.name);
        } else {
          doneCount++;
          if (s.state === 'FAILED' || s.state === 'CANCELLED') {
            console.warn(`[DS-REPORT] ${s.name} failed: ${s.error}`);
          }
        }
      }

      if (!allDone) {
        return NextResponse.json({
          phase: 'polling',
          progress: {
            step: 'polling',
            percent: 10 + Math.round((doneCount / entries.length) * 50),
            message: `Athena queries: ${doneCount}/${entries.length} complete`,
            detail: `Running: ${running.join(', ')}`,
          },
        });
      }

      // All done → parse
      state = { ...state, phase: 'parsing' };
      await saveState(datasetName, state);
      return NextResponse.json({
        phase: 'parsing',
        progress: { step: 'parsing', percent: 65, message: 'Queries done, parsing results...' },
      });
    }

    // ── Phase: parsing ──────────────────────────────────────────
    if (state.phase === 'parsing') {
      const queries = state.queries;
      const pfx = REPORT_PREFIX(datasetName);
      const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();

      // Parse hourly
      if (queries.hourly) {
        try {
          const r = await fetchQueryResults(queries.hourly);
          await putConfig(`${pfx}/hourly`, parseConsolidatedHourly(datasetName, r.rows), { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] hourly parse:', e.message); }
      }

      // Parse temporal + totalDevices
      if (queries.temporal) {
        try {
          const r = await fetchQueryResults(queries.temporal);
          const daily = r.rows.map((row: any) => ({
            date: row.date,
            pings: parseInt(row.pings, 10) || 0,
            devices: parseInt(row.devices, 10) || 0,
          }));
          const report: any = buildTemporalTrends(datasetName, [daily]);
          if (queries.totalDevices) {
            try {
              const tr = await fetchQueryResults(queries.totalDevices);
              report.totalUniqueDevices = parseInt(tr.rows[0]?.total_unique_devices, 10) || 0;
            } catch {}
          }
          await putConfig(`${pfx}/temporal`, report, { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] temporal parse:', e.message); }
      }

      // Parse mobility
      if (queries.mobility) {
        try {
          const r = await fetchQueryResults(queries.mobility);
          await putConfig(`${pfx}/mobility`, parseConsolidatedMobility(datasetName, r.rows), { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] mobility parse:', e.message); }
      }

      // Parse OD
      let odClusters: any = null;
      if (queries.od) {
        try {
          const r = await fetchQueryResults(queries.od);
          odClusters = parseConsolidatedOD(r.rows);
          for (const c of odClusters.clusters) {
            coordsToGeocode.set(`${c.originLat},${c.originLng}`, { lat: c.originLat, lng: c.originLng, deviceCount: c.deviceDays });
            coordsToGeocode.set(`${c.destLat},${c.destLng}`, { lat: c.destLat, lng: c.destLng, deviceCount: c.deviceDays });
          }
        } catch (e: any) { console.error('[DS-REPORT] od parse:', e.message); }
      }

      // Parse catchment
      let catchmentRows: any[] | null = null;
      if (queries.catchment) {
        try {
          const r = await fetchQueryResults(queries.catchment);
          catchmentRows = r.rows;
          for (const row of r.rows) {
            const lat = parseFloat(row.origin_lat) || 0;
            const lng = parseFloat(row.origin_lng) || 0;
            coordsToGeocode.set(`${lat},${lng}`, { lat, lng, deviceCount: parseInt(row.device_days, 10) || 0 });
          }
        } catch (e: any) { console.error('[DS-REPORT] catchment parse:', e.message); }
      }

      // Geocode OD + catchment coords
      const coordToZip = new Map<string, { zipCode: string; city: string; country: string }>();
      if (coordsToGeocode.size > 0) {
        try {
          const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
          if (analysis?.country) setCountryFilter([analysis.country]);

          // Round to 1 decimal for geocoding efficiency
          const roundedMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
          for (const p of coordsToGeocode.values()) {
            const rl = Math.round(p.lat * 10) / 10;
            const rn = Math.round(p.lng * 10) / 10;
            const key = `${rl},${rn}`;
            const ex = roundedMap.get(key);
            if (ex) ex.deviceCount += p.deviceCount;
            else roundedMap.set(key, { lat: rl, lng: rn, deviceCount: p.deviceCount });
          }

          const geocoded = await batchReverseGeocode(Array.from(roundedMap.values()));
          const rKeys = Array.from(roundedMap.keys());

          for (const [key, p] of coordsToGeocode.entries()) {
            const rk = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
            const idx = rKeys.indexOf(rk);
            if (idx >= 0 && idx < geocoded.length) {
              const g = geocoded[idx];
              if (g.type === 'geojson_local' || g.type === 'nominatim_match') {
                coordToZip.set(key, { zipCode: g.postcode, city: g.city, country: g.country });
              }
            }
          }
          setCountryFilter(null);
        } catch (e: any) {
          console.error('[DS-REPORT] geocoding:', e.message);
          setCountryFilter(null);
        }
      }

      // Save OD + catchment reports
      if (odClusters) {
        await putConfig(`${pfx}/od`, buildODReport(datasetName, odClusters.clusters, coordToZip), { compact: true });
      }
      if (catchmentRows) {
        await putConfig(`${pfx}/catchment`, buildCatchmentReport(datasetName, catchmentRows, coordToZip), { compact: true });
      }

      state = { ...state, phase: 'done' };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'done',
        progress: { step: 'done', percent: 100, message: 'Reports generated successfully' },
      });
    }

    return NextResponse.json({ phase: state?.phase || 'unknown' });
  } catch (err: any) {
    console.error(`[DS-REPORT] ${datasetName}:`, err.message);
    return NextResponse.json({ error: err.message, phase: 'error' }, { status: 500 });
  }
}
