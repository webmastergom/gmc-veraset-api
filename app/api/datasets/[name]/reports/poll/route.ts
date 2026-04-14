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
export const maxDuration = 300;

const STATE_KEY = (ds: string) => `dataset-report-state/${ds}`;
function REPORT_KEY(ds: string, type: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  let key = `dataset-reports/${ds}/${type}`;
  if (minDwell > 0 || maxDwell > 0) key += `-dwell-${minDwell}-${maxDwell}`;
  if (hourFrom > 0 || hourTo < 23) key += `-h${hourFrom}-${hourTo}`;
  return key;
}

// ── Types ─────────────────────────────────────────────────────────────

interface ReportState {
  phase: 'start' | 'polling' | 'parsing' | 'done' | 'error';
  queries: Record<string, string>;  // name → queryId
  datasetSizeGB?: number;
  minDwell?: number;
  maxDwell?: number;
  hourFrom?: number;
  hourTo?: number;
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

/** Build SQL fragment to filter pings by hour of day. Handles cross-midnight ranges (e.g., 22h→6h). */
function hourFilterSQL(hourFrom: number, hourTo: number, tsCol = 'utc_timestamp'): string {
  if (hourFrom === 0 && hourTo === 23) return '';
  if (hourFrom <= hourTo) {
    return `AND HOUR(${tsCol}) >= ${hourFrom} AND HOUR(${tsCol}) <= ${hourTo}`;
  }
  // Cross-midnight (e.g., 22h to 6h)
  return `AND (HOUR(${tsCol}) >= ${hourFrom} OR HOUR(${tsCol}) <= ${hourTo})`;
}

/**
 * Common CTE: filter pings at POIs with dwell calculation.
 * Dwell = time span (minutes) between first and last ping per device-day at POI.
 * Returns per-device-day aggregated rows with dwell_minutes.
 * @param minDwell - minimum dwell time in minutes (0 = no filter)
 * @param maxDwell - maximum dwell time in minutes (0 = no filter)
 * @param hourFrom - start hour filter (0-23, default 0)
 * @param hourTo - end hour filter (0-23, default 23)
 */
function atPoiWithDwellCTE(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  const havingParts: string[] = [];
  if (minDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
  if (maxDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
  const dwellFilter = havingParts.length > 0 ? `\n    HAVING ${havingParts.join(' AND ')}` : '';
  const hFilter = hourFilterSQL(hourFrom, hourTo);
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
        ${hFilter}
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
 * When minDwell/maxDwell > 0, only includes device-days with dwell in range.
 * When hourFrom/hourTo specified, only includes pings within the time window. */
function atPoiCTE(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  const hFilter = hourFilterSQL(hourFrom, hourTo);
  const hFilterT = hourFilterSQL(hourFrom, hourTo, 't.utc_timestamp');
  if (minDwell > 0 || maxDwell > 0) {
    // Need to compute dwell first, then filter, then expand back to individual pings
    const havingParts: string[] = [];
    if (minDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
    if (maxDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
    return `poi_device_days AS (
      SELECT ad_id, date
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        ${hFilter}
      GROUP BY ad_id, date
      HAVING ${havingParts.join(' AND ')}
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
        ${hFilterT}
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
        ${hFilter}
    )`;
}

function buildHourlySQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  return `WITH ${atPoiCTE(table, minDwell, maxDwell, hourFrom, hourTo)}
    SELECT HOUR(utc_timestamp) as touch_hour,
      COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY 1 ORDER BY 1`;
}

function buildTemporalSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  return `WITH ${atPoiCTE(table, minDwell, maxDwell, hourFrom, hourTo)}
    SELECT date, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY date ORDER BY date`;
}

function buildTotalDevicesSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  return `WITH ${atPoiCTE(table, minDwell, maxDwell, hourFrom, hourTo)}
    SELECT COUNT(DISTINCT ad_id) as total_unique_devices FROM at_poi`;
}

function buildODSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  return `WITH ${atPoiWithDwellCTE(table, minDwell, maxDwell, hourFrom, hourTo)},
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

function buildCatchmentSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  return `WITH ${atPoiWithDwellCTE(table, minDwell, maxDwell, hourFrom, hourTo)},
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
function buildAffinitySQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23): string {
  return `WITH ${atPoiWithDwellCTE(table, minDwell, maxDwell, hourFrom, hourTo)},
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

interface PoiCoordForMobility {
  lat: number;
  lng: number;
  radiusM: number;
}

function buildMobilitySQL(table: string, minDwell = 0, maxDwell = 0, poiCoords?: PoiCoordForMobility[], hourFrom = 0, hourTo = 23): string {
  // Mobility: POI categories visited before/after target POI visit.
  //
  // Strategy (same as mega-job consolidation):
  // 1. Identify visit_time using spatial proximity to POI coords (if available)
  //    or fallback to median timestamp of poi_id pings
  // 2. Find all pings ±2h of visit
  // 3. Geohash-bucket join with Overture POIs (0.01° grid, 3×3 expansion)
  // 4. Filter by distance ≤ 200m, keep closest per (ad_id, date, timing, category)
  // 5. Aggregate by timing (before/after) + category

  const GRID_STEP = 0.01; // ~1.1km geohash grid

  // Build visit_time CTE — spatial when POI coords available
  let visitTimeCTE: string;
  if (poiCoords?.length) {
    // Spatial proximity: identify actual at-POI pings via Haversine distance
    const poiValues = poiCoords
      .map(p => `(${p.lat}, ${p.lng}, ${p.radiusM})`)
      .join(', ');

    const hasDwellFilter = minDwell > 0 || maxDwell > 0;
    const dwellWhereParts: string[] = [];
    if (minDwell > 0) dwellWhereParts.push(`dwell_minutes >= ${minDwell}`);
    if (maxDwell > 0) dwellWhereParts.push(`dwell_minutes <= ${maxDwell}`);
    const dwellCTEs = hasDwellFilter ? `,
    visit_dwell AS (
      SELECT ad_id, date,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_minutes
      FROM at_poi_pings
      GROUP BY ad_id, date
    ),
    dwell_filtered AS (
      SELECT ad_id, date FROM visit_dwell
      WHERE ${dwellWhereParts.join(' AND ')}
    )` : '';

    const visitorSource = hasDwellFilter
      ? `at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date`
      : `at_poi_pings`;
    const visitorPrefix = hasDwellFilter ? 'a.' : '';

    visitTimeCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    ),
    target_pois AS (
      SELECT * FROM (VALUES ${poiValues}) AS t(poi_lat, poi_lng, poi_radius_m)
    ),
    at_poi_pings AS (
      SELECT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM all_pings_raw p
      CROSS JOIN target_pois tp
      WHERE 111320 * SQRT(
        POW(p.lat - tp.poi_lat, 2) +
        POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
      ) <= tp.poi_radius_m
        ${hourFilterSQL(hourFrom, hourTo, 'p.utc_timestamp')}
    )${dwellCTEs},
    target_visits AS (
      SELECT
        ${visitorPrefix}ad_id,
        ${visitorPrefix}date,
        MIN(${visitorPrefix}utc_timestamp) as visit_time
      FROM ${visitorSource}
      GROUP BY ${visitorPrefix}ad_id, ${visitorPrefix}date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng FROM all_pings_raw
    )`;
  } else {
    // Fallback: use poi_ids with median timestamp as visit_time proxy
    const dwellHavingParts: string[] = [];
    if (minDwell > 0) dwellHavingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
    if (maxDwell > 0) dwellHavingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
    const dwellHaving = dwellHavingParts.length > 0 ? `HAVING ${dwellHavingParts.join(' AND ')}` : '';

    visitTimeCTE = `
    target_visits AS (
      SELECT ad_id, date,
        FROM_UNIXTIME(APPROX_PERCENTILE(TO_UNIXTIME(utc_timestamp), 0.5)) as visit_time
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${hourFilterSQL(hourFrom, hourTo)}
      GROUP BY ad_id, date
      ${dwellHaving}
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
    )`;
  }

  return `WITH
    ${visitTimeCTE},
    nearby_pings AS (
      SELECT
        a.ad_id,
        a.date,
        a.utc_timestamp,
        a.lat,
        a.lng,
        CAST(FLOOR(a.lat / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(a.lng / ${GRID_STEP}) AS BIGINT) as lng_bucket,
        CASE WHEN a.utc_timestamp < t.visit_time THEN 'before' ELSE 'after' END as timing
      FROM all_pings a
      INNER JOIN target_visits t ON a.ad_id = t.ad_id AND a.date = t.date
      WHERE ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) <= 120
        AND ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) > 0
    ),
    poi_buckets AS (
      SELECT
        id as poi_id,
        category,
        latitude as poi_lat,
        longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID_STEP}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(longitude / ${GRID_STEP}) AS BIGINT) + dlng as lng_bucket
      FROM ${POI_GMC_TABLE}
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
      WHERE category IS NOT NULL
    ),
    matched AS (
      SELECT
        p.ad_id,
        p.date,
        p.timing,
        b.category,
        111320 * SQRT(
          POW(p.lat - b.poi_lat, 2) +
          POW((p.lng - b.poi_lng) * COS(RADIANS((p.lat + b.poi_lat) / 2)), 2)
        ) as distance_m
      FROM nearby_pings p
      INNER JOIN poi_buckets b
        ON p.lat_bucket = b.lat_bucket
        AND p.lng_bucket = b.lng_bucket
    ),
    closest AS (
      SELECT
        ad_id, date, timing, category, distance_m,
        ROW_NUMBER() OVER (PARTITION BY ad_id, date, timing, category ORDER BY distance_m) as rn
      FROM matched
      WHERE distance_m <= 200
    )
    SELECT
      timing,
      category,
      COUNT(DISTINCT CONCAT(ad_id, '-', date)) as device_days,
      COUNT(*) as hits
    FROM closest
    WHERE rn = 1
    GROUP BY timing, category
    ORDER BY timing, device_days DESC`;
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

/** Haversine distance in km between two points */
function haversineKm(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

async function computeAffinityReport(
  datasetName: string,
  rows: any[],
  coordToZip: Map<string, { zipCode: string; city: string; country: string }>,
  country?: string,
): Promise<{ analyzedAt: string; datasetName: string; byZipCode: AffinityByZip[] }> {
  // ── Step 1: Aggregate raw data by postal code ──────────────────
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

  // ── Step 2: Score "hot" CPs at 85-100 ──────────────────────────
  const zips = Array.from(zipMap.values()).filter(z => z.zipCode !== 'UNKNOWN');
  const maxDwell = Math.max(1, ...zips.map(z => z.dwellCount > 0 ? z.dwellSum / z.dwellCount : 0));
  const maxFreq = Math.max(1, ...zips.map(z => z.freqCount > 0 ? z.freqSum / z.freqCount : 0));

  const hotCps: AffinityByZip[] = zips.map(z => {
    const avgDwell = z.dwellCount > 0 ? z.dwellSum / z.dwellCount : 0;
    const avgFreq = z.freqCount > 0 ? z.freqSum / z.freqCount : 1;
    const rawScore = (avgDwell / maxDwell) * 0.5 + (avgFreq / maxFreq) * 0.5;
    // Scale to 85-100
    const affinityIndex = Math.round(85 + rawScore * 15);

    return {
      zipCode: z.zipCode, city: z.city, country: z.country,
      lat: z.lat, lng: z.lng,
      uniqueDevices: z.uniqueDevices, totalVisitDays: z.totalVisitDays,
      avgDwellMinutes: Math.round(avgDwell * 10) / 10,
      avgFrequency: Math.round(avgFreq * 100) / 100,
      affinityIndex,
    };
  });

  // ── Step 3: Geographic decay — adjacent CPs scored progressively lower ──
  const adjacentCps: AffinityByZip[] = [];

  if (country && hotCps.length > 0) {
    try {
      const fs = await import('fs');
      const path = await import('path');
      const geojsonPath = path.join(process.cwd(), 'data', 'geojson', `${country}.geojson`);

      if (fs.existsSync(geojsonPath)) {
        const geojson = JSON.parse(fs.readFileSync(geojsonPath, 'utf-8'));
        const hotSet = new Set(hotCps.map(h => h.zipCode));

        // Compute centroid for each GeoJSON feature
        for (const feature of geojson.features) {
          const cp = feature.properties?.postal_code || feature.properties?.postcode || '';
          if (!cp || hotSet.has(cp)) continue;

          // Quick centroid from bbox or first coordinate
          let cLat = 0, cLng = 0;
          if (feature.properties?.latitude && feature.properties?.longitude) {
            cLat = parseFloat(feature.properties.latitude);
            cLng = parseFloat(feature.properties.longitude);
          } else if (feature.bbox) {
            cLat = (feature.bbox[1] + feature.bbox[3]) / 2;
            cLng = (feature.bbox[0] + feature.bbox[2]) / 2;
          } else {
            // Extract from geometry
            const coords = feature.geometry?.coordinates;
            if (!coords) continue;
            const flat = feature.geometry.type === 'MultiPolygon'
              ? coords[0][0] : coords[0];
            if (!flat?.length) continue;
            const sumLat = flat.reduce((s: number, c: number[]) => s + c[1], 0);
            const sumLng = flat.reduce((s: number, c: number[]) => s + c[0], 0);
            cLat = sumLat / flat.length;
            cLng = sumLng / flat.length;
          }

          if (!cLat || !cLng) continue;

          // Find distance to nearest hot CP
          let minDist = Infinity;
          for (const hot of hotCps) {
            const d = haversineKm(cLat, cLng, hot.lat, hot.lng);
            if (d < minDist) minDist = d;
          }

          // Decay by distance rings
          let score = 0;
          if (minDist <= 5) {
            score = Math.round(65 + (1 - minDist / 5) * 19);        // 65-84
          } else if (minDist <= 15) {
            score = Math.round(40 + (1 - (minDist - 5) / 10) * 24); // 40-64
          } else if (minDist <= 30) {
            score = Math.round(15 + (1 - (minDist - 15) / 15) * 24); // 15-39
          } else if (minDist <= 50) {
            score = Math.round(1 + (1 - (minDist - 30) / 20) * 13);  // 1-14
          }

          if (score > 0) {
            const city = feature.properties?.city || feature.properties?.estado || '';
            adjacentCps.push({
              zipCode: cp, city, country: country,
              lat: cLat, lng: cLng,
              uniqueDevices: 0, totalVisitDays: 0,
              avgDwellMinutes: 0, avgFrequency: 0,
              affinityIndex: score,
            });
          }
        }
        console.log(`[DS-REPORT] Affinity decay: ${hotCps.length} hot + ${adjacentCps.length} adjacent CPs`);
      }
    } catch (e: any) {
      console.warn(`[DS-REPORT] Affinity decay failed: ${e.message}`);
    }
  }

  // ── Merge hot + adjacent, sort by affinity ─────────────────────
  const allCps = [...hotCps, ...adjacentCps];
  allCps.sort((a, b) => b.affinityIndex - a.affinityIndex);

  return {
    analyzedAt: new Date().toISOString(),
    datasetName,
    byZipCode: allCps,
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

    // Parse request body for dwell interval + hour filters
    let minDwell = 0;
    let maxDwell = 0;
    let hourFrom = 0;
    let hourTo = 23;
    try {
      const body = await request.json();
      if (body?.minDwell) minDwell = parseInt(body.minDwell, 10) || 0;
      if (body?.maxDwell) maxDwell = parseInt(body.maxDwell, 10) || 0;
      if (body?.hourFrom != null) hourFrom = Math.max(0, Math.min(23, parseInt(body.hourFrom, 10) || 0));
      if (body?.hourTo != null) hourTo = Math.max(0, Math.min(23, parseInt(body.hourTo, 10) || 23));
    } catch { /* no body */ }

    // Reset if done, error, or filters changed
    if (state?.phase === 'done' || state?.phase === 'error') state = null;
    if (state && ((state.minDwell ?? 0) !== minDwell || (state.maxDwell ?? 0) !== maxDwell)) {
      console.log(`[DS-REPORT] Dwell filter changed (${state.minDwell ?? 0}-${state.maxDwell ?? 0} → ${minDwell}-${maxDwell}), resetting`);
      state = null;
    }
    if (state && ((state.hourFrom ?? 0) !== hourFrom || (state.hourTo ?? 23) !== hourTo)) {
      console.log(`[DS-REPORT] Hour filter changed (${state.hourFrom ?? 0}-${state.hourTo ?? 23} → ${hourFrom}-${hourTo}), resetting`);
      state = null;
    }

    // ── Phase: start ────────────────────────────────────────────
    if (!state) {
      const dwellLabel = (minDwell > 0 || maxDwell > 0) ? `, dwell=${minDwell}-${maxDwell || '∞'}` : '';
      const hourLabel = (hourFrom > 0 || hourTo < 23) ? `, hours=${hourFrom}-${hourTo}` : '';
      console.log(`[DS-REPORT] Starting reports for ${datasetName} (${dwellLabel}${hourLabel})`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      // Try to load POI coords from the job associated with this dataset
      let poiCoords: PoiCoordForMobility[] | undefined;
      try {
        const { getJob } = await import('@/lib/jobs');
        const { extractPoiCoords } = await import('@/lib/mega-consolidation-queries');
        const index = await getConfig<Record<string, any>>('jobs-index');
        if (index) {
          const jobEntry = Object.entries(index).find(
            ([_, j]: [string, any]) => j.s3DestPath?.replace(/\/$/, '').split('/').pop() === datasetName
          );
          if (jobEntry) {
            const job = await getJob(jobEntry[0]);
            if (job) {
              poiCoords = extractPoiCoords([job]);
              if (poiCoords.length > 0) {
                console.log(`[DS-REPORT] Loaded ${poiCoords.length} POI coords for spatial mobility`);
              } else {
                poiCoords = undefined;
              }
            }
          }
        }
      } catch (e: any) {
        console.warn(`[DS-REPORT] Could not load POI coords: ${e.message}`);
      }

      // Launch all 7 queries in parallel
      const queries: Record<string, string> = {};
      await Promise.all([
        startQueryAsync(buildODSQL(table, minDwell, maxDwell, hourFrom, hourTo)).then(id => { queries.od = id; }).catch(e => console.error('[DS-REPORT] od:', e.message)),
        startQueryAsync(buildHourlySQL(table, minDwell, maxDwell, hourFrom, hourTo)).then(id => { queries.hourly = id; }).catch(e => console.error('[DS-REPORT] hourly:', e.message)),
        startQueryAsync(buildCatchmentSQL(table, minDwell, maxDwell, hourFrom, hourTo)).then(id => { queries.catchment = id; }).catch(e => console.error('[DS-REPORT] catchment:', e.message)),
        startQueryAsync(buildTemporalSQL(table, minDwell, maxDwell, hourFrom, hourTo)).then(id => { queries.temporal = id; }).catch(e => console.error('[DS-REPORT] temporal:', e.message)),
        startQueryAsync(buildTotalDevicesSQL(table, minDwell, maxDwell, hourFrom, hourTo)).then(id => { queries.totalDevices = id; }).catch(e => console.error('[DS-REPORT] totalDevices:', e.message)),
        startQueryAsync(buildMobilitySQL(table, minDwell, maxDwell, poiCoords, hourFrom, hourTo)).then(id => { queries.mobility = id; }).catch(e => console.error('[DS-REPORT] mobility:', e.message)),
        startQueryAsync(buildAffinitySQL(table, minDwell, maxDwell, hourFrom, hourTo)).then(id => { queries.affinity = id; }).catch(e => console.error('[DS-REPORT] affinity:', e.message)),
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

      state = {
        phase: 'polling', queries, datasetSizeGB,
        minDwell: minDwell || undefined,
        maxDwell: maxDwell || undefined,
        hourFrom: hourFrom > 0 ? hourFrom : undefined,
        hourTo: hourTo < 23 ? hourTo : undefined,
      };
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
      const dMin = state.minDwell || 0;
      const dMax = state.maxDwell || 0;
      const hFrom = state.hourFrom ?? 0;
      const hTo = state.hourTo ?? 23;
      const rk = (type: string) => REPORT_KEY(datasetName, type, dMin, dMax, hFrom, hTo);
      const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();

      // Parse hourly
      if (queries.hourly) {
        try {
          const r = await fetchQueryResults(queries.hourly);
          await putConfig(`${rk('hourly')}`, parseConsolidatedHourly(datasetName, r.rows), { compact: true });
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
          await putConfig(`${rk('temporal')}`, report, { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] temporal parse:', e.message); }
      }

      // Parse mobility (POI categories before/after visit)
      if (queries.mobility) {
        try {
          const r = await fetchQueryResults(queries.mobility);
          const beforeMap = new Map<string, { deviceDays: number; hits: number }>();
          const afterMap = new Map<string, { deviceDays: number; hits: number }>();
          for (const row of r.rows) {
            const cat = row.category;
            const dd = parseInt(row.device_days, 10) || 0;
            const h = parseInt(row.hits, 10) || 0;
            const map = row.timing === 'before' ? beforeMap : afterMap;
            const ex = map.get(cat);
            if (ex) { ex.deviceDays += dd; ex.hits += h; }
            else map.set(cat, { deviceDays: dd, hits: h });
          }
          const toArr = (m: Map<string, any>) =>
            Array.from(m.entries())
              .map(([category, v]) => ({ category, ...v }))
              .sort((a, b) => b.deviceDays - a.deviceDays);
          await putConfig(`${rk('mobility')}`, {
            analyzedAt: new Date().toISOString(),
            datasetName,
            before: toArr(beforeMap),
            after: toArr(afterMap),
            categories: toArr(beforeMap), // legacy compat
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
      let detectedCountry: string | undefined;
      if (coordsToGeocode.size > 0) {
        try {
          const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
          detectedCountry = analysis?.country;
          if (detectedCountry) setCountryFilter([detectedCountry]);

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
            const roundKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
            const idx = rKeys.indexOf(roundKey);
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
        await putConfig(`${rk('od')}`, buildODReport(datasetName, odClusters.clusters, coordToZip), { compact: true });
      }
      if (catchmentRows) {
        await putConfig(`${rk('catchment')}`, buildCatchmentReport(datasetName, catchmentRows, coordToZip), { compact: true });
      }

      // Save affinity report
      if (affinityRows) {
        const affinityReport = await computeAffinityReport(datasetName, affinityRows, coordToZip, detectedCountry);
        await putConfig(`${rk('affinity')}`, affinityReport, { compact: true });
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
