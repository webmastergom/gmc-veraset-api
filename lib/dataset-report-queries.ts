/**
 * Single-dataset Athena query builders.
 * Mirrors mega-consolidation-queries.ts but for a single table (no UNION ALL).
 *
 * Queries:
 * - OD (Origin-Destination) with first/last ping
 * - POI activity by hour
 * - Catchment origins (first ping of day) + departure hour
 * - Mobility trends (nearby POI categories ±2h, before/after)
 *
 * CRITICAL: Veraset assigns poi_ids to ALL pings of a visiting device for the
 * entire day — not just pings near the POI. When poiCoords are provided, we use
 * spatial proximity to known POI coordinates for accurate at-POI filtering.
 */

import { getTableName, startQueryAsync, startCTASAsync, tempTableName, ensureTableForDataset } from './athena';
import { type PoiCoord, type DwellFilter } from './mega-consolidation-queries';

const ACCURACY_THRESHOLD = 500;
const COORDINATE_PRECISION = 4; // ~11m
const GRID_STEP = 0.01;         // ~1.1km geohash grid
const POI_TABLE = 'lab_pois_gmc';
// Buffer around POI bounding box to pre-filter pings (degrees, ~2.2km)
const BBOX_BUFFER = 0.02;

/** Build dwell filter CTEs for dataset queries (same pattern as mega). */
function buildDwellCTEs(dwell?: DwellFilter): string {
  if (!dwell || (dwell.minMinutes == null && dwell.maxMinutes == null)) return '';
  const conditions: string[] = [];
  if (dwell.minMinutes != null) conditions.push(`dwell_minutes >= ${dwell.minMinutes}`);
  if (dwell.maxMinutes != null) conditions.push(`dwell_minutes <= ${dwell.maxMinutes}`);
  return `,
    visit_dwell AS (
      SELECT ad_id, date,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_minutes
      FROM at_poi_pings
      GROUP BY ad_id, date
    ),
    dwell_filtered AS (
      SELECT ad_id, date FROM visit_dwell
      WHERE ${conditions.join(' AND ')}
    )`;
}

function hasDwell(dwell?: DwellFilter): boolean {
  return !!(dwell && (dwell.minMinutes != null || dwell.maxMinutes != null));
}

/**
 * Build a SQL VALUES clause for target POI coordinates.
 */
function buildTargetPoisValues(poiCoords: PoiCoord[]): string {
  const rows = poiCoords
    .map((c) => `(${c.lat}, ${c.lng}, ${c.radiusM}.0)`)
    .join(', ');
  return `(VALUES ${rows}) AS t(poi_lat, poi_lng, poi_radius_m)`;
}

/**
 * Compute bounding box WHERE clause from POI coordinates.
 * Pre-filters pings to only those within the POI extent + buffer,
 * eliminating 90%+ of pings before expensive CROSS JOINs.
 */
function buildBboxFilter(poiCoords: PoiCoord[], latCol = 'latitude', lngCol = 'longitude', cast = true): string {
  if (!poiCoords.length) return '';
  const lats = poiCoords.map(c => c.lat);
  const lngs = poiCoords.map(c => c.lng);
  const minLat = Math.min(...lats) - BBOX_BUFFER;
  const maxLat = Math.max(...lats) + BBOX_BUFFER;
  const minLng = Math.min(...lngs) - BBOX_BUFFER;
  const maxLng = Math.max(...lngs) + BBOX_BUFFER;
  if (cast) {
    return `AND TRY_CAST(${latCol} AS DOUBLE) BETWEEN ${minLat} AND ${maxLat}
        AND TRY_CAST(${lngCol} AS DOUBLE) BETWEEN ${minLng} AND ${maxLng}`;
  }
  return `AND ${latCol} BETWEEN ${minLat} AND ${maxLat}
        AND ${lngCol} BETWEEN ${minLng} AND ${maxLng}`;
}

// ── Q1: Origin-Destination ───────────────────────────────────────────

export async function startDatasetODQuery(
  datasetName: string,
  poiCoords?: PoiCoord[],
  poiTableName?: string,
  dwell?: DwellFilter,
): Promise<string> {
  const table = getTableName(datasetName);

  let sql: string;

  if (poiCoords?.length) {
    // ── Spatial proximity approach (correct) ──
    const bbox = buildBboxFilter(poiCoords);
    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        ${bbox}
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    ${buildTargetPoisCTE(poiCoords, poiTableName)},
    at_poi_pings AS (
      SELECT DISTINCT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM all_pings p
      CROSS JOIN target_pois tp
      WHERE ABS(p.lat - tp.poi_lat) < 0.01
        AND ABS(p.lng - tp.poi_lng) < 0.02
        AND 111320 * SQRT(
          POW(p.lat - tp.poi_lat, 2) +
          POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
        ) <= tp.poi_radius_m
    )${buildDwellCTEs(dwell)},
    poi_visits AS (
      SELECT
        ${hasDwell(dwell) ? 'a.' : ''}ad_id,
        ${hasDwell(dwell) ? 'a.' : ''}date,
        MIN(${hasDwell(dwell) ? 'a.' : ''}utc_timestamp) as first_poi_visit
      FROM ${hasDwell(dwell) ? 'at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date' : 'at_poi_pings'}
      GROUP BY ${hasDwell(dwell) ? 'a.' : ''}ad_id, ${hasDwell(dwell) ? 'a.' : ''}date
    ),
    device_day AS (
      SELECT
        p.ad_id,
        p.date,
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
      ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
      ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
      ROUND(dest_lat, ${COORDINATE_PRECISION}) as dest_lat,
      ROUND(dest_lng, ${COORDINATE_PRECISION}) as dest_lng,
      departure_hour,
      poi_arrival_hour,
      COUNT(*) as device_days
    FROM device_day
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY device_days DESC
    LIMIT 100000
    `;
  } else {
    // ── Fallback: poi_ids approach ──
    sql = `
    WITH
    poi_visits AS (
      SELECT
        ad_id,
        date,
        MIN(utc_timestamp) as first_poi_visit
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    device_day AS (
      SELECT
        p.ad_id,
        p.date,
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
      ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
      ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
      ROUND(dest_lat, ${COORDINATE_PRECISION}) as dest_lat,
      ROUND(dest_lng, ${COORDINATE_PRECISION}) as dest_lng,
      departure_hour,
      poi_arrival_hour,
      COUNT(*) as device_days
    FROM device_day
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY device_days DESC
    LIMIT 100000
    `;
  }

  console.log(`[DATASET-OD] Starting OD query for ${datasetName} (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q2: POI Activity by Hour ─────────────────────────────────────────

export async function startDatasetHourlyQuery(
  datasetName: string,
  poiCoords?: PoiCoord[],
  poiTableName?: string,
  dwell?: DwellFilter,
): Promise<string> {
  const table = getTableName(datasetName);

  let sql: string;

  if (poiCoords?.length) {
    // ── Spatial proximity approach (correct) ──
    const bbox = buildBboxFilter(poiCoords);
    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        ${bbox}
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    ${buildTargetPoisCTE(poiCoords, poiTableName)},
    at_poi_pings AS (
      SELECT DISTINCT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM all_pings p
      CROSS JOIN target_pois tp
      WHERE ABS(p.lat - tp.poi_lat) < 0.01
        AND ABS(p.lng - tp.poi_lng) < 0.02
        AND 111320 * SQRT(
          POW(p.lat - tp.poi_lat, 2) +
          POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
        ) <= tp.poi_radius_m
    )
    SELECT
      HOUR(utc_timestamp) as touch_hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM at_poi_pings
    GROUP BY HOUR(utc_timestamp)
    ORDER BY touch_hour
    `;
  } else {
    // ── Fallback: poi_ids approach ──
    sql = `
    SELECT
      HOUR(utc_timestamp) as touch_hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM ${table}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    GROUP BY HOUR(utc_timestamp)
    ORDER BY touch_hour
    `;
  }

  console.log(`[DATASET-HOURLY] Starting hourly query for ${datasetName} (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q3: Catchment Origins (first ping of day) ────────────────────────

export async function startDatasetCatchmentQuery(
  datasetName: string,
  poiCoords?: PoiCoord[],
  poiTableName?: string,
  dwell?: DwellFilter,
): Promise<string> {
  const table = getTableName(datasetName);

  let visitorsCTE: string;
  if (poiCoords?.length) {
    const bbox = buildBboxFilter(poiCoords);
    visitorsCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        ${bbox}
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    ${buildTargetPoisCTE(poiCoords, poiTableName)},
    at_poi_pings AS (
      SELECT DISTINCT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM all_pings_raw p
      CROSS JOIN target_pois tp
      WHERE ABS(p.lat - tp.poi_lat) < 0.01
        AND ABS(p.lng - tp.poi_lng) < 0.02
        AND 111320 * SQRT(
          POW(p.lat - tp.poi_lat, 2) +
          POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
        ) <= tp.poi_radius_m
    ),
    poi_visitors AS (
      SELECT DISTINCT ad_id FROM at_poi_pings
    ),
    valid_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng FROM all_pings_raw
    )`;
  } else {
    visitorsCTE = `
    poi_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    ),
    valid_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    )`;
  }

  const sql = `
    WITH
    ${visitorsCTE},
    first_pings AS (
      SELECT
        v.ad_id,
        vp.date,
        MIN_BY(vp.lat, vp.utc_timestamp) as origin_lat,
        MIN_BY(vp.lng, vp.utc_timestamp) as origin_lng,
        HOUR(MIN(vp.utc_timestamp)) as departure_hour
      FROM valid_pings vp
      INNER JOIN poi_visitors v ON vp.ad_id = v.ad_id
      GROUP BY v.ad_id, vp.date
    )
    SELECT
      ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
      ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
      departure_hour,
      COUNT(*) as device_days
    FROM first_pings
    GROUP BY
      ROUND(origin_lat, ${COORDINATE_PRECISION}),
      ROUND(origin_lng, ${COORDINATE_PRECISION}),
      departure_hour
    ORDER BY device_days DESC
    LIMIT 100000
  `;

  console.log(`[DATASET-CATCHMENT] Starting catchment query for ${datasetName} (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q4: Mobility Trends (±2h nearby POI categories) ──────────────────

export async function startDatasetMobilityQuery(
  datasetName: string,
  poiCoords?: PoiCoord[],
  poiTableName?: string,
  dwell?: DwellFilter,
): Promise<string> {
  const table = getTableName(datasetName);

  let visitTimeCTE: string;
  if (poiCoords?.length) {
    const bbox = buildBboxFilter(poiCoords);
    visitTimeCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        ${bbox}
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    ${buildTargetPoisCTE(poiCoords, poiTableName)},
    at_poi_pings AS (
      SELECT DISTINCT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM all_pings_raw p
      CROSS JOIN target_pois tp
      WHERE ABS(p.lat - tp.poi_lat) < 0.01
        AND ABS(p.lng - tp.poi_lng) < 0.02
        AND 111320 * SQRT(
          POW(p.lat - tp.poi_lat, 2) +
          POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
        ) <= tp.poi_radius_m
    ),
    target_visits AS (
      SELECT
        ad_id,
        date,
        MIN(utc_timestamp) as visit_time
      FROM at_poi_pings
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng FROM all_pings_raw
    )`;
  } else {
    visitTimeCTE = `
    target_visits AS (
      SELECT
        ad_id,
        date,
        MIN(utc_timestamp) as visit_time
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    )`;
  }

  const sql = `
    WITH
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
        name as poi_name,
        category,
        latitude as poi_lat,
        longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID_STEP}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(longitude / ${GRID_STEP}) AS BIGINT) + dlng as lng_bucket
      FROM ${POI_TABLE}
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
        b.poi_name,
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
        ad_id, date, timing, category, poi_name, distance_m,
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
    ORDER BY timing, device_days DESC
  `;

  console.log(`[DATASET-MOBILITY] Starting mobility query for ${datasetName} (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Temporal (daily pings/devices for POI visitors) ─────────────────

/**
 * Daily pings + unique devices of POI-visiting devices.
 * Used for summary stats and temporal trend charts.
 */
export async function startDatasetTemporalQuery(
  datasetName: string,
  poiIds?: string[],
  dwell?: DwellFilter,
): Promise<string> {
  const table = getTableName(datasetName);
  const poiFilter = buildPoiIdFilter(poiIds);

  const sql = `
    SELECT
      date,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM ${table}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      ${poiFilter}
    GROUP BY date
    ORDER BY date
  `;

  console.log(`[DATASET-TEMPORAL] Starting temporal query for ${datasetName} (poiFilter=${!!poiIds?.length})`);
  return await startQueryAsync(sql);
}

// ── Total unique devices (global COUNT DISTINCT) ────────────────────

/**
 * Single-row query: total unique devices that visited any POI.
 * Needed because summing daily COUNT(DISTINCT) gives device-days, not unique devices.
 */
export async function startDatasetTotalDevicesQuery(
  datasetName: string,
  poiIds?: string[],
  dwell?: DwellFilter,
): Promise<string> {
  const table = getTableName(datasetName);
  const poiFilter = buildPoiIdFilter(poiIds);

  const sql = `
    SELECT COUNT(DISTINCT ad_id) as total_unique_devices
    FROM ${table}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      ${poiFilter}
  `;

  console.log(`[DATASET-TOTAL] Starting total devices query for ${datasetName} (poiFilter=${!!poiIds?.length})`);
  return await startQueryAsync(sql);
}

/**
 * Build a poi_id IN (...) filter clause for direct poi_id matching.
 */
function buildPoiIdFilter(poiIds?: string[]): string {
  if (!poiIds?.length) return '';
  const list = poiIds.map((p) => `'${p.replace(/'/g, "''")}'`).join(',');
  return `AND poi_id IN (${list})`;
}

// ── POI coordinates temp table (for large POI sets) ─────────────────

const MAX_VALUES_POI_COUNT = 500;

/**
 * For datasets with many POIs (>500), a VALUES clause exceeds Athena's 256KB
 * query limit. Instead, write coords to S3 as CSV and create an external table.
 * Returns the table name to use in queries.
 */
export async function ensurePoiCoordsTable(
  datasetName: string,
  poiCoords: PoiCoord[],
): Promise<string> {
  const { runQuery } = await import('./athena');
  const { s3Client, BUCKET } = await import('./s3-config');
  const { PutObjectCommand } = await import('@aws-sdk/client-s3');

  const tableName = `poi_coords_${getTableName(datasetName)}`;
  const s3Prefix = `config/poi-coords/${datasetName}`;

  // Write CSV
  const csv = poiCoords
    .map((c) => `${c.lat},${c.lng},${c.radiusM}`)
    .join('\n');

  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${s3Prefix}/coords.csv`,
    Body: csv,
    ContentType: 'text/csv',
  }));

  // Create external table (idempotent)
  const createSql = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} (
      poi_lat DOUBLE,
      poi_lng DOUBLE,
      poi_radius_m DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 's3://${BUCKET}/${s3Prefix}/'
  `;
  await runQuery(createSql);
  console.log(`[POI-TABLE] Created/verified ${tableName} with ${poiCoords.length} POIs`);

  return tableName;
}

/**
 * Decide whether to use VALUES clause or temp table based on POI count.
 * Returns either a VALUES string or a table reference for use in queries.
 */
export function shouldUsePoiTable(poiCoords: PoiCoord[]): boolean {
  return poiCoords.length > MAX_VALUES_POI_COUNT;
}

/**
 * Build the target_pois CTE fragment — either from VALUES or from a table.
 */
export function buildTargetPoisCTE(poiCoords: PoiCoord[], poiTableName?: string): string {
  if (poiTableName) {
    return `target_pois AS (SELECT poi_lat, poi_lng, poi_radius_m FROM ${poiTableName})`;
  }
  return `target_pois AS (SELECT * FROM ${buildTargetPoisValues(poiCoords)})`;
}

// ══════════════════════════════════════════════════════════════════════
// Pre-computed Dwell Bucket Analysis
// ══════════════════════════════════════════════════════════════════════

export const DWELL_BUCKETS = [0, 2, 5, 10, 15, 30, 60, 120, 180];

// ── Dwell CTAS: materialize at-POI pings with dwell_minutes ─────────

/**
 * Creates a temp Parquet table with schema:
 *   (ad_id, date, utc_timestamp, lat, lng, dwell_minutes)
 *
 * Performs the spatial join ONCE and computes dwell_minutes per (ad_id, date).
 * All bucket queries then read from this materialized table.
 */
export async function startDwellCTASQuery(
  datasetName: string,
  poiCoords: PoiCoord[],
  poiTableName?: string,
): Promise<{ queryId: string; tableName: string }> {
  await ensureTableForDataset(datasetName);
  const table = getTableName(datasetName);
  const runId = Date.now().toString(36);
  const tblName = tempTableName(datasetName, `dwell_${runId}`);
  const bbox = buildBboxFilter(poiCoords);

  const selectSql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        ${bbox}
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    ${buildTargetPoisCTE(poiCoords, poiTableName)},
    at_poi_pings AS (
      SELECT DISTINCT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM all_pings p
      CROSS JOIN target_pois tp
      WHERE ABS(p.lat - tp.poi_lat) < 0.01
        AND ABS(p.lng - tp.poi_lng) < 0.02
        AND 111320 * SQRT(
          POW(p.lat - tp.poi_lat, 2) +
          POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
        ) <= tp.poi_radius_m
    ),
    visit_dwell AS (
      SELECT ad_id, date,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_minutes
      FROM at_poi_pings
      GROUP BY ad_id, date
    )
    SELECT a.ad_id, a.date, a.utc_timestamp, a.lat, a.lng, d.dwell_minutes
    FROM at_poi_pings a
    JOIN visit_dwell d ON a.ad_id = d.ad_id AND a.date = d.date
  `;

  console.log(`[DWELL-CTAS] Starting dwell materialization for ${datasetName} → ${tblName}`);
  const queryId = await startCTASAsync(selectSql, tblName);
  return { queryId, tableName: tblName };
}

// ── Helper: dwell filter WHERE clause ───────────────────────────────

function bucketDwellWhere(bucket: number): string {
  return bucket > 0 ? `WHERE dwell_minutes >= ${bucket}` : '';
}

function bucketDwellAnd(bucket: number): string {
  return bucket > 0 ? `AND dwell_minutes >= ${bucket}` : '';
}

// ── BucketOD ────────────────────────────────────────────────────────

/**
 * OD query reading visitors from the temp dwell table but origins/dests from raw.
 */
export async function startBucketODQuery(
  datasetName: string,
  tempTable: string,
  bucket: number,
): Promise<string> {
  const rawTable = getTableName(datasetName);
  const dwellFilter = bucketDwellAnd(bucket);

  const sql = `
    WITH
    poi_visits AS (
      SELECT ad_id, date, MIN(utc_timestamp) as first_poi_visit
      FROM ${tempTable}
      ${bucketDwellWhere(bucket)}
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${rawTable}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
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
    SELECT ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
           ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
           ROUND(dest_lat, ${COORDINATE_PRECISION}) as dest_lat,
           ROUND(dest_lng, ${COORDINATE_PRECISION}) as dest_lng,
           departure_hour, poi_arrival_hour, COUNT(*) as device_days
    FROM device_day GROUP BY 1,2,3,4,5,6 ORDER BY device_days DESC LIMIT 100000
  `;

  console.log(`[BUCKET-OD] Starting OD query for ${datasetName} bucket=${bucket}`);
  return await startQueryAsync(sql);
}

// ── BucketHourly ────────────────────────────────────────────────────

export async function startBucketHourlyQuery(
  datasetName: string,
  tempTable: string,
  bucket: number,
): Promise<string> {
  const sql = `
    SELECT HOUR(utc_timestamp) as touch_hour, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM ${tempTable}
    ${bucketDwellWhere(bucket)}
    GROUP BY HOUR(utc_timestamp) ORDER BY touch_hour
  `;

  console.log(`[BUCKET-HOURLY] Starting hourly query for ${datasetName} bucket=${bucket}`);
  return await startQueryAsync(sql);
}

// ── BucketCatchment ─────────────────────────────────────────────────

export async function startBucketCatchmentQuery(
  datasetName: string,
  tempTable: string,
  bucket: number,
): Promise<string> {
  const rawTable = getTableName(datasetName);

  const sql = `
    WITH
    poi_visitors AS (
      SELECT DISTINCT ad_id FROM ${tempTable}
      ${bucketDwellWhere(bucket)}
    ),
    valid_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${rawTable}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    first_pings AS (
      SELECT v.ad_id, vp.date,
        MIN_BY(vp.lat, vp.utc_timestamp) as origin_lat,
        MIN_BY(vp.lng, vp.utc_timestamp) as origin_lng,
        HOUR(MIN(vp.utc_timestamp)) as departure_hour
      FROM valid_pings vp
      INNER JOIN poi_visitors v ON vp.ad_id = v.ad_id
      GROUP BY v.ad_id, vp.date
    )
    SELECT ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
           ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
           departure_hour, COUNT(*) as device_days
    FROM first_pings GROUP BY 1,2,3 ORDER BY device_days DESC LIMIT 100000
  `;

  console.log(`[BUCKET-CATCHMENT] Starting catchment query for ${datasetName} bucket=${bucket}`);
  return await startQueryAsync(sql);
}

// ── BucketMobility ──────────────────────────────────────────────────

export async function startBucketMobilityQuery(
  datasetName: string,
  tempTable: string,
  bucket: number,
): Promise<string> {
  const rawTable = getTableName(datasetName);

  const sql = `
    WITH
    target_visits AS (
      SELECT ad_id, date, MIN(utc_timestamp) as visit_time
      FROM ${tempTable}
      ${bucketDwellWhere(bucket)}
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${rawTable}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    nearby_pings AS (
      SELECT a.ad_id, a.date, a.utc_timestamp, a.lat, a.lng,
        CAST(FLOOR(a.lat / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(a.lng / ${GRID_STEP}) AS BIGINT) as lng_bucket,
        CASE WHEN a.utc_timestamp < t.visit_time THEN 'before' ELSE 'after' END as timing
      FROM all_pings a
      INNER JOIN target_visits t ON a.ad_id = t.ad_id AND a.date = t.date
      WHERE ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) <= 120
        AND ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) > 0
    ),
    poi_buckets AS (
      SELECT id as poi_id, name as poi_name, category, latitude as poi_lat, longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID_STEP}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(longitude / ${GRID_STEP}) AS BIGINT) + dlng as lng_bucket
      FROM ${POI_TABLE}
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
      WHERE category IS NOT NULL
    ),
    matched AS (
      SELECT p.ad_id, p.date, p.timing, b.category, b.poi_name,
        111320 * SQRT(
          POW(p.lat - b.poi_lat, 2) +
          POW((p.lng - b.poi_lng) * COS(RADIANS((p.lat + b.poi_lat) / 2)), 2)
        ) as distance_m
      FROM nearby_pings p
      INNER JOIN poi_buckets b ON p.lat_bucket = b.lat_bucket AND p.lng_bucket = b.lng_bucket
    ),
    closest AS (
      SELECT ad_id, date, timing, category, poi_name, distance_m,
        ROW_NUMBER() OVER (PARTITION BY ad_id, date, timing, category ORDER BY distance_m) as rn
      FROM matched WHERE distance_m <= 200
    )
    SELECT timing, category, COUNT(DISTINCT CONCAT(ad_id, '-', date)) as device_days, COUNT(*) as hits
    FROM closest WHERE rn = 1 GROUP BY timing, category ORDER BY timing, device_days DESC
  `;

  console.log(`[BUCKET-MOBILITY] Starting mobility query for ${datasetName} bucket=${bucket}`);
  return await startQueryAsync(sql);
}

// ── BucketTemporal ──────────────────────────────────────────────────

export async function startBucketTemporalQuery(
  datasetName: string,
  tempTable: string,
  bucket: number,
): Promise<string> {
  const sql = `
    SELECT date, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM ${tempTable}
    ${bucketDwellWhere(bucket)}
    GROUP BY date ORDER BY date
  `;

  console.log(`[BUCKET-TEMPORAL] Starting temporal query for ${datasetName} bucket=${bucket}`);
  return await startQueryAsync(sql);
}

// ── BucketTotalDevices ──────────────────────────────────────────────

export async function startBucketTotalDevicesQuery(
  datasetName: string,
  tempTable: string,
  bucket: number,
): Promise<string> {
  const sql = `
    SELECT COUNT(DISTINCT ad_id) as total_unique_devices
    FROM ${tempTable}
    ${bucketDwellWhere(bucket)}
  `;

  console.log(`[BUCKET-TOTAL] Starting total devices query for ${datasetName} bucket=${bucket}`);
  return await startQueryAsync(sql);
}
