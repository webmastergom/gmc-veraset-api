/**
 * Mega-job consolidated Athena query builders.
 * Builds UNION ALL queries across multiple sub-job tables for:
 * - Origin-Destination (OD) analysis
 * - POI activity by hour
 * - Mobility trends (nearby POI categories visited ±2h)
 * - Catchment origins (first-ping-of-day for reverse geocoding)
 */

import { getTableName, startQueryAsync } from './athena';
import { type Job } from './jobs';

// ── Helpers ─────────────────────────────────────────────────────────────

const ACCURACY_THRESHOLD = 500;
const COORDINATE_PRECISION = 4; // ~11m resolution
const GRID_STEP = 0.01; // ~1.1km geohash grid for spatial join
const POI_TABLE = 'lab_pois_gmc';

/**
 * Build the UNION ALL across all synced sub-job tables.
 * Returns the SQL fragment (no outer parens).
 */
function buildUnionAll(
  syncedJobs: Job[],
  columns: string,
  whereExtra = '',
): string {
  return syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT ${columns} FROM ${table} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '' ${whereExtra}`;
    })
    .join('\n    UNION ALL\n    ');
}

/**
 * Build optional POI filter clause.
 */
function buildPoiFilter(poiIds?: string[]): string {
  if (!poiIds?.length) return '';
  const list = poiIds.map((p) => `'${p.replace(/'/g, "''")}'`).join(',');
  return `AND poi_id IN (${list})`;
}

// ── Q1: Origin-Destination ──────────────────────────────────────────────

/**
 * Build and start the consolidated OD query across all sub-job tables.
 * Returns Athena queryId for polling.
 *
 * Pattern: UNION ALL → poi_visits → all_pings → device_day_trips
 * Uses MIN_BY for origin (first ping) and MAX_BY for destination (last ping).
 */
export async function startConsolidatedODQuery(
  subJobs: Job[],
  poiIds?: string[],
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for OD query');

  const poiFilter = buildPoiFilter(poiIds);

  // UNION ALL for POI visits (to identify visitors)
  const poiUnion = syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT ad_id, date, utc_timestamp FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
    })
    .join('\n      UNION ALL\n      ');

  // UNION ALL for all pings (for origin/dest inference)
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  const sql = `
    WITH
    poi_visits AS (
      SELECT
        ad_id,
        date,
        MIN(utc_timestamp) as first_poi_visit
      FROM (
        ${poiUnion}
      )
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
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

  console.log(`[MEGA-OD] Starting OD query across ${syncedJobs.length} tables`);
  return await startQueryAsync(sql);
}

// ── Q2: POI Activity by Hour ────────────────────────────────────────────

/**
 * Build and start the consolidated POI activity by hour query.
 * Returns Athena queryId for polling.
 */
export async function startConsolidatedHourlyQuery(
  subJobs: Job[],
  poiIds?: string[],
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for hourly query');

  const poiFilter = buildPoiFilter(poiIds);

  const unionParts = syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT ad_id, utc_timestamp FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
    })
    .join('\n      UNION ALL\n      ');

  const sql = `
    SELECT
      HOUR(utc_timestamp) as touch_hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM (
      ${unionParts}
    )
    GROUP BY HOUR(utc_timestamp)
    ORDER BY touch_hour
  `;

  console.log(`[MEGA-HOURLY] Starting POI hourly query across ${syncedJobs.length} tables`);
  return await startQueryAsync(sql);
}

// ── Q3: Catchment Origins (first-ping-of-day) ──────────────────────────

/**
 * Build and start the catchment origins query (first ping of day per device).
 * Used for reverse geocoding → zip code aggregation.
 * Returns Athena queryId for polling.
 */
export async function startConsolidatedCatchmentQuery(
  subJobs: Job[],
  poiIds?: string[],
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for catchment query');

  const poiFilter = buildPoiFilter(poiIds);

  // POI visitors
  const poiUnion = syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT ad_id FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
    })
    .join('\n      UNION ALL\n      ');

  // All pings with accuracy filter
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  const sql = `
    WITH
    poi_visitors AS (
      SELECT DISTINCT ad_id FROM (
        ${poiUnion}
      )
    ),
    valid_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
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

  console.log(`[MEGA-CATCHMENT] Starting catchment query across ${syncedJobs.length} tables`);
  return await startQueryAsync(sql);
}

// ── Q4: Mobility Trends (nearby POI categories ±2h) ────────────────────

/**
 * Build and start the mobility trends query.
 * Spatial join movement pings (within ±2h of POI visit) with Overture POIs.
 * Uses geohash-bucket pattern (0.01° grid, 3×3 expansion) for efficient matching.
 * Returns Athena queryId for polling.
 */
export async function startConsolidatedMobilityQuery(
  subJobs: Job[],
  poiIds?: string[],
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for mobility query');

  const poiFilter = buildPoiFilter(poiIds);

  // POI visits (to get visit times)
  const poiVisitsUnion = syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT ad_id, date, utc_timestamp FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
    })
    .join('\n      UNION ALL\n      ');

  // All pings (for spatial join with Overture POIs)
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  const sql = `
    WITH
    target_visits AS (
      SELECT
        ad_id,
        date,
        MIN(utc_timestamp) as visit_time
      FROM (
        ${poiVisitsUnion}
      )
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
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

  console.log(`[MEGA-MOBILITY] Starting mobility trends query across ${syncedJobs.length} tables`);
  return await startQueryAsync(sql);
}

// ── Q5: MAIDs (unique device IDs) ──────────────────────────────────────

/**
 * Build and start a query for all unique MAIDs (ad_id) that visited POIs.
 * Returns Athena queryId. The output CSV is used directly for export.
 */
export async function startConsolidatedMAIDsQuery(
  subJobs: Job[],
  poiIds?: string[],
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for MAIDs query');

  const poiFilter = buildPoiFilter(poiIds);

  const unionParts = syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT DISTINCT ad_id FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
    })
    .join('\n      UNION\n      ');

  // UNION (not UNION ALL) to deduplicate across sub-jobs
  const sql = `
    SELECT DISTINCT ad_id
    FROM (
      ${unionParts}
    )
    ORDER BY ad_id
  `;

  console.log(`[MEGA-MAIDS] Starting MAIDs query across ${syncedJobs.length} tables`);
  return await startQueryAsync(sql);
}
