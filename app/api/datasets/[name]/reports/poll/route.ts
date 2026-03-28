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
  datasetSizeGB?: number;
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
 * Common CTE: filter pings at POIs with dwell calculation.
 * Dwell = time span (minutes) between first and last ping per device-day at POI.
 * Returns per-device-day aggregated rows with dwell_minutes.
 * @param minDwell - minimum dwell time in minutes (0 = no filter)
 */
function atPoiWithDwellCTE(table: string, minDwell = 0): string {
  const dwellFilter = minDwell > 0 ? `\n    HAVING DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}` : '';
  return `raw_poi_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    ),
    at_poi AS (
      SELECT ad_id, date,
        MIN(utc_timestamp) as first_ping,
        MAX(utc_timestamp) as last_ping,
        ROUND(MIN_BY(lat, utc_timestamp), ${PREC}) as lat,
        ROUND(MIN_BY(lng, utc_timestamp), ${PREC}) as lng,
        DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell_minutes,
        COUNT(*) as ping_count
      FROM raw_poi_pings
      GROUP BY ad_id, date${dwellFilter}
    )`;
}

/** Simple CTE for queries that just need the raw pings (hourly, temporal).
 * When minDwell > 0, only includes device-days with sufficient dwell time. */
function atPoiCTE(table: string, minDwell = 0): string {
  if (minDwell > 0) {
    // Need to compute dwell first, then filter, then expand back to individual pings
    return `poi_device_days AS (
      SELECT ad_id, date
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
      GROUP BY ad_id, date
      HAVING DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}
    ),
    at_poi AS (
      SELECT DISTINCT t.ad_id, t.date, t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${table} t
      INNER JOIN poi_device_days dd ON t.ad_id = dd.ad_id AND t.date = dd.date
      CROSS JOIN UNNEST(t.poi_ids) AS x(poi_id)
      WHERE x.poi_id IS NOT NULL AND x.poi_id != ''
        AND TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    )`;
  }
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

function buildHourlySQL(table: string, minDwell = 0): string {
  return `WITH ${atPoiCTE(table, minDwell)}
    SELECT HOUR(utc_timestamp) as touch_hour,
      COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY 1 ORDER BY 1`;
}

function buildTemporalSQL(table: string, minDwell = 0): string {
  return `WITH ${atPoiCTE(table, minDwell)}
    SELECT date, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY date ORDER BY date`;
}

function buildTotalDevicesSQL(table: string, minDwell = 0): string {
  return `WITH ${atPoiCTE(table, minDwell)}
    SELECT COUNT(DISTINCT ad_id) as total_unique_devices FROM at_poi`;
}

function buildODSQL(table: string, minDwell = 0): string {
  return `WITH ${atPoiWithDwellCTE(table, minDwell)},
    poi_visits AS (
      SELECT ad_id, date, first_ping as first_poi_visit
      FROM at_poi
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

function buildCatchmentSQL(table: string, minDwell = 0): string {
  return `WITH ${atPoiWithDwellCTE(table, minDwell)},
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

/**
 * Affinity query: per-origin-cluster, compute total visits, unique devices,
 * avg dwell, and visit frequency. These feed the affinity index calculation
 * after reverse geocoding groups them by postal code.
 */
function buildAffinitySQL(table: string, minDwell = 0): string {
  return `WITH ${atPoiWithDwellCTE(table, minDwell)},
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
    first_pings AS (
      SELECT p.ad_id, p.date,
        MIN_BY(p.lat, p.utc_timestamp) as origin_lat,
        MIN_BY(p.lng, p.utc_timestamp) as origin_lng
      FROM all_pings p
      INNER JOIN (SELECT DISTINCT ad_id FROM at_poi) v ON p.ad_id = v.ad_id
      GROUP BY p.ad_id, p.date
    ),
    device_stats AS (
      SELECT
        a.ad_id,
        fp.origin_lat,
        fp.origin_lng,
        COUNT(DISTINCT a.date) as visit_days,
        AVG(a.dwell_minutes) as avg_dwell,
        SUM(a.ping_count) as total_pings
      FROM at_poi a
      INNER JOIN first_pings fp ON a.ad_id = fp.ad_id AND a.date = fp.date
      GROUP BY a.ad_id, fp.origin_lat, fp.origin_lng
    )
    SELECT
      ROUND(origin_lat, ${PREC}) as origin_lat,
      ROUND(origin_lng, ${PREC}) as origin_lng,
      COUNT(DISTINCT ad_id) as unique_devices,
      SUM(visit_days) as total_visit_days,
      AVG(avg_dwell) as avg_dwell_minutes,
      AVG(visit_days) as avg_frequency
    FROM device_stats
    WHERE origin_lat IS NOT NULL
    GROUP BY 1, 2
    ORDER BY unique_devices DESC
    LIMIT 50000`;
}

function buildMobilitySQL(table: string, minDwell = 0): string {
  // Uses poi_ids from the job itself: pings before/after POI visit on the same day.
  // Identifies all pings from POI visitors, classifies as before/during/after visit.
  return `WITH ${atPoiCTE(table, minDwell)},
    visit_times AS (
      SELECT ad_id, date,
        MIN(utc_timestamp) as first_visit,
        MAX(utc_timestamp) as last_visit
      FROM at_poi GROUP BY ad_id, date
    ),
    poi_device_pings AS (
      SELECT t.ad_id, t.date, t.utc_timestamp,
        HOUR(t.utc_timestamp) as ping_hour,
        COALESCE(TRY(CARDINALITY(FILTER(t.poi_ids, x -> x IS NOT NULL AND x != ''))), 0) as active_poi_count
      FROM ${table} t
      INNER JOIN visit_times v ON t.ad_id = v.ad_id AND t.date = v.date
      WHERE t.ad_id IS NOT NULL AND TRIM(t.ad_id) != ''
        AND TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND ABS(DATE_DIFF('hour', t.utc_timestamp, v.first_visit)) <= 3
    ),
    classified AS (
      SELECT p.ad_id, p.date, p.ping_hour, p.utc_timestamp,
        CASE
          WHEN p.utc_timestamp < v.first_visit AND p.active_poi_count = 0 THEN 'before'
          WHEN p.utc_timestamp > v.last_visit AND p.active_poi_count = 0 THEN 'after'
          ELSE 'during'
        END as timing
      FROM poi_device_pings p
      INNER JOIN visit_times v ON p.ad_id = v.ad_id AND p.date = v.date
    )
    SELECT timing, ping_hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as unique_devices,
      COUNT(DISTINCT ad_id || '|' || date) as device_days
    FROM classified
    WHERE timing IN ('before', 'after')
    GROUP BY timing, ping_hour
    ORDER BY timing, ping_hour`;
}

// ── Affinity index computation ────────────────────────────────────────

interface AffinityByZip {
  zipCode: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  uniqueDevices: number;
  totalVisitDays: number;
  avgDwellMinutes: number;
  avgFrequency: number;
  affinityIndex: number;  // 0-100
}

function computeAffinityReport(
  datasetName: string,
  rows: any[],
  coordToZip: Map<string, { zipCode: string; city: string; country: string }>,
): { analyzedAt: string; datasetName: string; byZipCode: AffinityByZip[] } {
  // Aggregate by postal code
  const zipMap = new Map<string, {
    zipCode: string; city: string; country: string;
    lat: number; lng: number;
    uniqueDevices: number; totalVisitDays: number;
    dwellSum: number; dwellCount: number;
    freqSum: number; freqCount: number;
  }>();

  for (const row of rows) {
    const lat = parseFloat(row.origin_lat) || 0;
    const lng = parseFloat(row.origin_lng) || 0;
    const key = `${lat},${lng}`;
    const geo = coordToZip.get(key) || { zipCode: 'UNKNOWN', city: 'UNKNOWN', country: 'UNKNOWN' };
    const zk = geo.zipCode;

    const devices = parseInt(row.unique_devices, 10) || 0;
    const visitDays = parseInt(row.total_visit_days, 10) || 0;
    const avgDwell = parseFloat(row.avg_dwell_minutes) || 0;
    const avgFreq = parseFloat(row.avg_frequency) || 1;

    const existing = zipMap.get(zk);
    if (existing) {
      existing.uniqueDevices += devices;
      existing.totalVisitDays += visitDays;
      existing.dwellSum += avgDwell * devices;
      existing.dwellCount += devices;
      existing.freqSum += avgFreq * devices;
      existing.freqCount += devices;
    } else {
      zipMap.set(zk, {
        zipCode: zk, city: geo.city, country: geo.country,
        lat, lng,
        uniqueDevices: devices, totalVisitDays: visitDays,
        dwellSum: avgDwell * devices, dwellCount: devices,
        freqSum: avgFreq * devices, freqCount: devices,
      });
    }
  }

  // Compute affinity index per zip
  const zips = Array.from(zipMap.values());

  // Find max dwell and max frequency for normalization
  const maxDwell = Math.max(1, ...zips.map(z => z.dwellCount > 0 ? z.dwellSum / z.dwellCount : 0));
  const maxFreq = Math.max(1, ...zips.map(z => z.freqCount > 0 ? z.freqSum / z.freqCount : 0));

  const result: AffinityByZip[] = zips.map(z => {
    const avgDwell = z.dwellCount > 0 ? z.dwellSum / z.dwellCount : 0;
    const avgFreq = z.freqCount > 0 ? z.freqSum / z.freqCount : 1;

    // Normalize to 0-100: dwell (50%) + frequency (50%)
    const dwellScore = Math.min(100, (avgDwell / maxDwell) * 100);
    const freqScore = Math.min(100, (avgFreq / maxFreq) * 100);
    const affinityIndex = Math.round(dwellScore * 0.5 + freqScore * 0.5);

    return {
      zipCode: z.zipCode,
      city: z.city,
      country: z.country,
      lat: z.lat,
      lng: z.lng,
      uniqueDevices: z.uniqueDevices,
      totalVisitDays: z.totalVisitDays,
      avgDwellMinutes: Math.round(avgDwell * 10) / 10,
      avgFrequency: Math.round(avgFreq * 100) / 100,
      affinityIndex,
    };
  });

  result.sort((a, b) => b.affinityIndex - a.affinityIndex);

  return {
    analyzedAt: new Date().toISOString(),
    datasetName,
    byZipCode: result.filter(z => z.zipCode !== 'UNKNOWN'),
  };
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

    // Parse request body for minDwell filter
    let minDwell = 0;
    try {
      const body = await request.json();
      if (body?.minDwell) minDwell = parseInt(body.minDwell, 10) || 0;
    } catch { /* no body */ }

    // Reset if done or error
    if (state?.phase === 'done' || state?.phase === 'error') state = null;

    // ── Phase: start ────────────────────────────────────────────
    if (!state) {
      console.log(`[DS-REPORT] Starting reports for ${datasetName} (minDwell=${minDwell})`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      // Launch all 7 queries in parallel using poi_ids (no spatial join)
      const queries: Record<string, string> = {};
      await Promise.all([
        startQueryAsync(buildODSQL(table, minDwell)).then(id => { queries.od = id; }).catch(e => console.error('[DS-REPORT] od:', e.message)),
        startQueryAsync(buildHourlySQL(table, minDwell)).then(id => { queries.hourly = id; }).catch(e => console.error('[DS-REPORT] hourly:', e.message)),
        startQueryAsync(buildCatchmentSQL(table, minDwell)).then(id => { queries.catchment = id; }).catch(e => console.error('[DS-REPORT] catchment:', e.message)),
        startQueryAsync(buildTemporalSQL(table, minDwell)).then(id => { queries.temporal = id; }).catch(e => console.error('[DS-REPORT] temporal:', e.message)),
        startQueryAsync(buildTotalDevicesSQL(table, minDwell)).then(id => { queries.totalDevices = id; }).catch(e => console.error('[DS-REPORT] totalDevices:', e.message)),
        startQueryAsync(buildMobilitySQL(table, minDwell)).then(id => { queries.mobility = id; }).catch(e => console.error('[DS-REPORT] mobility:', e.message)),
        startQueryAsync(buildAffinitySQL(table, minDwell)).then(id => { queries.affinity = id; }).catch(e => console.error('[DS-REPORT] affinity:', e.message)),
      ]);

      // Get dataset size for progress estimation
      let datasetSizeGB = 0;
      try {
        const index = await getConfig<Record<string, any>>('jobs-index');
        if (index) {
          const entry = Object.entries(index).find(
            ([_, j]: [string, any]) => j.s3DestPath?.replace(/\/$/, '').split('/').pop() === datasetName
          );
          if (entry) {
            datasetSizeGB = Math.round(((entry[1] as any).totalBytes || 0) / 1e9);
          }
        }
      } catch {}

      console.log(`[DS-REPORT] Launched ${Object.keys(queries).length} queries for ${datasetName} (${datasetSizeGB} GB)`);

      state = { phase: 'polling', queries, datasetSizeGB };
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
            return { name, state: s.state, error: s.error, stats: s.statistics };
          } catch (err: any) {
            if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
              return { name, state: 'FAILED' as const, error: 'Query expired', stats: undefined };
            }
            return { name, state: 'FAILED' as const, error: err.message, stats: undefined };
          }
        })
      );

      let allDone = true;
      let doneCount = 0;
      let totalScannedGB = 0;
      const dsGB = state.datasetSizeGB || 0;
      const queryDetails: string[] = [];

      for (const s of statuses) {
        const scannedGB = s.stats?.dataScannedBytes ? (s.stats.dataScannedBytes / 1e9) : 0;
        totalScannedGB += scannedGB;
        const runtimeSec = s.stats?.engineExecutionTimeMs ? Math.round(s.stats.engineExecutionTimeMs / 1000) : 0;

        if (s.state === 'RUNNING' || s.state === 'QUEUED') {
          allDone = false;
          const pct = dsGB > 0 ? Math.min(99, Math.round((scannedGB / dsGB) * 100)) : 0;
          queryDetails.push(`⏳ ${s.name}: ${scannedGB.toFixed(0)}/${dsGB} GB (${pct}%) · ${runtimeSec}s`);
        } else {
          doneCount++;
          if (s.state === 'SUCCEEDED') {
            queryDetails.push(`✅ ${s.name}: ${scannedGB.toFixed(0)} GB in ${runtimeSec}s`);
          } else {
            queryDetails.push(`❌ ${s.name}: ${s.error || 'failed'}`);
            console.warn(`[DS-REPORT] ${s.name} failed: ${s.error}`);
          }
        }
      }

      // Overall percent: scan phase (10-60%) + completion phase (60-65%)
      let overallPercent = 10;
      if (dsGB > 0 && entries.length > 0) {
        const avgScanPct = Math.min(100, (totalScannedGB / (dsGB * entries.length)) * 100);
        overallPercent = 10 + Math.round(avgScanPct * 0.5);
      }
      overallPercent = Math.max(overallPercent, 10 + Math.round((doneCount / entries.length) * 50));

      if (!allDone) {
        return NextResponse.json({
          phase: 'polling',
          progress: {
            step: 'polling',
            percent: overallPercent,
            message: `Athena queries: ${doneCount}/${entries.length} complete · ${totalScannedGB.toFixed(0)} GB scanned`,
            detail: queryDetails.join('\n'),
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

      // Parse mobility (before/after POI visit by hour)
      if (queries.mobility) {
        try {
          const r = await fetchQueryResults(queries.mobility);
          const before: any[] = [];
          const after: any[] = [];
          for (const row of r.rows) {
            const entry = {
              hour: parseInt(row.ping_hour, 10) || 0,
              pings: parseInt(row.pings, 10) || 0,
              devices: parseInt(row.unique_devices, 10) || 0,
              deviceDays: parseInt(row.device_days, 10) || 0,
            };
            if (row.timing === 'before') before.push(entry);
            else if (row.timing === 'after') after.push(entry);
          }
          before.sort((a, b) => a.hour - b.hour);
          after.sort((a, b) => a.hour - b.hour);
          await putConfig(`${pfx}/mobility`, {
            analyzedAt: new Date().toISOString(),
            datasetName,
            before,
            after,
            totalBeforeDeviceDays: before.reduce((s, e) => s + e.deviceDays, 0),
            totalAfterDeviceDays: after.reduce((s, e) => s + e.deviceDays, 0),
          }, { compact: true });
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

      // Parse affinity rows (geocoded later)
      let affinityRows: any[] | null = null;
      if (queries.affinity) {
        try {
          const r = await fetchQueryResults(queries.affinity);
          affinityRows = r.rows;
          for (const row of r.rows) {
            const lat = parseFloat(row.origin_lat) || 0;
            const lng = parseFloat(row.origin_lng) || 0;
            coordsToGeocode.set(`${lat},${lng}`, { lat, lng, deviceCount: parseInt(row.unique_devices, 10) || 0 });
          }
        } catch (e: any) { console.error('[DS-REPORT] affinity parse:', e.message); }
      }

      // Geocode OD + catchment + affinity coords
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

      // Save affinity report
      if (affinityRows) {
        const affinityReport = computeAffinityReport(datasetName, affinityRows, coordToZip);
        await putConfig(`${pfx}/affinity`, affinityReport, { compact: true });
        console.log(`[DS-REPORT] Affinity report: ${affinityReport.byZipCode.length} postal codes`);
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
