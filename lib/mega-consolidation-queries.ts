/**
 * Mega-job consolidated Athena query builders.
 * Builds UNION ALL queries across multiple sub-job tables for:
 * - Origin-Destination (OD) analysis
 * - POI activity by hour
 * - Mobility trends (nearby POI categories visited ±2h)
 * - Catchment origins (first-ping-of-day for reverse geocoding)
 *
 * CRITICAL: Veraset assigns poi_ids to ALL pings of a visiting device for the
 * entire day — not just pings near the POI. To determine actual at-POI pings,
 * we use spatial proximity to known POI coordinates instead of relying on poi_ids.
 * POI coords come from job.verasetPayload.geo_radius[] or job.externalPois[].
 */

import { getTableName, startQueryAsync } from './athena';
import { type Job } from './jobs';

// ── Helpers ─────────────────────────────────────────────────────────────

const ACCURACY_THRESHOLD = 500;
const COORDINATE_PRECISION = 4; // ~11m resolution
const GRID_STEP = 0.01; // ~1.1km geohash grid for spatial join
const POI_TABLE = 'lab_pois_gmc';

/** POI coordinate with radius for spatial proximity checks. */
export interface PoiCoord {
  lat: number;
  lng: number;
  radiusM: number;
}

/** Dwell time filter for reports (min/max minutes at POI). */
export interface DwellFilter {
  minMinutes?: number;
  maxMinutes?: number;
}

/**
 * Build dwell time filter CTEs.
 * Computes dwell = time between first and last ping at POI per (ad_id, date).
 * Returns SQL fragment with visit_dwell + dwell_filtered CTEs.
 * If no filter provided, returns empty string (no filtering).
 */
function buildDwellFilterCTEs(dwell?: DwellFilter): string {
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

/**
 * Returns the correct visitor CTE name based on whether dwell filter is active.
 * When dwell filter is active, queries should JOIN against dwell_filtered.
 */
function hasDwellFilter(dwell?: DwellFilter): boolean {
  return !!(dwell && (dwell.minMinutes != null || dwell.maxMinutes != null));
}

/**
 * Extract POI coordinates from job metadata.
 * Prefers verasetPayload.geo_radius (has distance_in_meters), falls back to externalPois.
 */
export function extractPoiCoords(jobs: Job[]): PoiCoord[] {
  const coords: PoiCoord[] = [];
  for (const job of jobs) {
    if (job.verasetPayload?.geo_radius?.length) {
      for (const g of job.verasetPayload.geo_radius) {
        coords.push({ lat: g.latitude, lng: g.longitude, radiusM: g.distance_in_meters || 200 });
      }
    } else if (job.externalPois?.length) {
      for (const p of job.externalPois) {
        coords.push({ lat: p.latitude, lng: p.longitude, radiusM: 200 });
      }
    }
  }
  // Deduplicate by rounding to 4 decimals
  const seen = new Set<string>();
  return coords.filter((c) => {
    const key = `${c.lat.toFixed(4)},${c.lng.toFixed(4)}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

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
 * Build optional POI filter clause (for poi_id string matching).
 */
function buildPoiFilter(poiIds?: string[]): string {
  if (!poiIds?.length) return '';
  const list = poiIds.map((p) => `'${p.replace(/'/g, "''")}'`).join(',');
  return `AND poi_id IN (${list})`;
}

/**
 * Build a SQL VALUES clause for target POI coordinates.
 * Returns a CTE fragment like: (VALUES (lat1, lng1, radius1), ...) AS t(poi_lat, poi_lng, poi_radius_m)
 */
function buildTargetPoisValues(poiCoords: PoiCoord[]): string {
  const rows = poiCoords
    .map((c) => `(${c.lat}, ${c.lng}, ${c.radiusM}.0)`)
    .join(', ');
  return `(VALUES ${rows}) AS t(poi_lat, poi_lng, poi_radius_m)`;
}

/**
 * Build the at-POI pings CTE using spatial proximity.
 * Uses a bounding box pre-filter (~0.01° ≈ 1.1km) before Haversine distance calc.
 */
function buildAtPoiPingsCTE(allPingsCte: string, poiCoords: PoiCoord[]): string {
  return `
    target_pois AS (
      SELECT * FROM ${buildTargetPoisValues(poiCoords)}
    ),
    at_poi_pings AS (
      SELECT DISTINCT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM ${allPingsCte} p
      CROSS JOIN target_pois tp
      WHERE ABS(p.lat - tp.poi_lat) < 0.01
        AND ABS(p.lng - tp.poi_lng) < 0.02
        AND 111320 * SQRT(
          POW(p.lat - tp.poi_lat, 2) +
          POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
        ) <= tp.poi_radius_m
    )`;
}

// ── Q1: Origin-Destination ──────────────────────────────────────────────

/**
 * Build and start the consolidated OD query across all sub-job tables.
 * Returns Athena queryId for polling.
 *
 * When poiCoords are provided, uses spatial proximity to determine actual POI visit time.
 * Falls back to poi_ids-based approach when no coordinates available.
 */
export async function startConsolidatedODQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for OD query');

  // All pings with lat/lng (for origin/dest inference)
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  let sql: string;

  if (poiCoords?.length) {
    // ── Spatial proximity approach (correct) ──
    const dwellCTEs = buildDwellFilterCTEs(dwell);
    const useDwell = hasDwellFilter(dwell);
    const poiVisitSource = useDwell
      ? `at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date`
      : `at_poi_pings`;

    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords)}${dwellCTEs},
    poi_visits AS (
      SELECT
        ${useDwell ? 'a.' : ''}ad_id,
        ${useDwell ? 'a.' : ''}date,
        MIN(${useDwell ? 'a.' : ''}utc_timestamp) as first_poi_visit
      FROM ${poiVisitSource}
      GROUP BY ${useDwell ? 'a.' : ''}ad_id, ${useDwell ? 'a.' : ''}date
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
    // ── Fallback: poi_ids approach (legacy, less accurate) ──
    const poiFilter = buildPoiFilter(poiIds);
    const poiUnion = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT ad_id, date, utc_timestamp FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION ALL\n      ');

    sql = `
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
  }

  console.log(`[MEGA-OD] Starting OD query across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q2: POI Activity by Hour ────────────────────────────────────────────

/**
 * Build and start the consolidated POI activity by hour query.
 * When poiCoords provided, only counts pings actually near a POI (spatial proximity).
 * Returns Athena queryId for polling.
 */
export async function startConsolidatedHourlyQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for hourly query');

  let sql: string;

  if (poiCoords?.length) {
    // ── Spatial proximity approach (correct) ──
    const allPingsUnion = buildUnionAll(
      syncedJobs,
      'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
      `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    );

    const dwellCTEs = buildDwellFilterCTEs(dwell);
    const useDwell = hasDwellFilter(dwell);
    const hourlySource = useDwell
      ? `at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date`
      : `at_poi_pings`;

    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords)}${dwellCTEs}
    SELECT
      HOUR(${useDwell ? 'a.' : ''}utc_timestamp) as touch_hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ${useDwell ? 'a.' : ''}ad_id) as devices
    FROM ${hourlySource}
    GROUP BY HOUR(${useDwell ? 'a.' : ''}utc_timestamp)
    ORDER BY touch_hour
    `;
  } else {
    // ── Fallback: poi_ids approach ──
    const poiFilter = buildPoiFilter(poiIds);
    const unionParts = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT ad_id, utc_timestamp FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION ALL\n      ');

    sql = `
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
  }

  console.log(`[MEGA-HOURLY] Starting POI hourly query across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q3: Catchment Origins (first-ping-of-day) ──────────────────────────

/**
 * Build and start the catchment origins query (first ping of day per device).
 * Used for reverse geocoding → zip code aggregation.
 * Returns Athena queryId for polling.
 *
 * Note: Catchment is ✅ correct with poi_ids — it just identifies visitors
 * and gets their first ping of day (origin). But when poiCoords are provided,
 * we use spatial proximity for more accurate visitor identification.
 */
export async function startConsolidatedCatchmentQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for catchment query');

  // All pings with accuracy filter
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  let visitorsCTE: string;
  if (poiCoords?.length) {
    // Use spatial proximity to identify visitors
    const dwellCTEs = buildDwellFilterCTEs(dwell);
    const useDwell = hasDwellFilter(dwell);

    visitorsCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings_raw', poiCoords)}${dwellCTEs},
    poi_visitors AS (
      ${useDwell
        ? `SELECT DISTINCT ad_id FROM dwell_filtered`
        : `SELECT DISTINCT ad_id FROM at_poi_pings`}
    ),
    valid_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng FROM all_pings_raw
    )`;
  } else {
    // Fallback: poi_ids
    const poiFilter = buildPoiFilter(poiIds);
    const poiUnion = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT ad_id FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION ALL\n      ');

    visitorsCTE = `
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

  console.log(`[MEGA-CATCHMENT] Starting catchment query across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q4: Mobility Trends (nearby POI categories ±2h) ────────────────────

/**
 * Build and start the mobility trends query.
 * Spatial join movement pings (within ±2h of POI visit) with Overture POIs.
 * Uses geohash-bucket pattern (0.01° grid, 3×3 expansion) for efficient matching.
 *
 * When poiCoords provided, uses spatial proximity for actual visit_time
 * instead of poi_ids MIN(utc_timestamp) which = first ping of day.
 * Returns Athena queryId for polling.
 */
export async function startConsolidatedMobilityQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for mobility query');

  // All pings (for spatial join with Overture POIs)
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  let visitTimeCTE: string;
  if (poiCoords?.length) {
    // Use spatial proximity for actual visit time
    const dwellCTEs = buildDwellFilterCTEs(dwell);
    const useDwell = hasDwellFilter(dwell);
    const targetVisitSource = useDwell
      ? `at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date`
      : `at_poi_pings`;

    visitTimeCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings_raw', poiCoords)}${dwellCTEs},
    target_visits AS (
      SELECT
        ${useDwell ? 'a.' : ''}ad_id,
        ${useDwell ? 'a.' : ''}date,
        MIN(${useDwell ? 'a.' : ''}utc_timestamp) as visit_time
      FROM ${targetVisitSource}
      GROUP BY ${useDwell ? 'a.' : ''}ad_id, ${useDwell ? 'a.' : ''}date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng FROM all_pings_raw
    )`;
  } else {
    // Fallback: poi_ids
    const poiFilter = buildPoiFilter(poiIds);
    const poiVisitsUnion = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT ad_id, date, utc_timestamp FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION ALL\n      ');

    visitTimeCTE = `
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

  console.log(`[MEGA-MOBILITY] Starting mobility trends query across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q5: Temporal (daily pings / devices) ────────────────────────────────

/**
 * Build and start the consolidated temporal query across all sub-job tables.
 * Returns daily pings, unique devices, and unique POI-visiting devices.
 *
 * Note: This counts pings of POI-visiting devices (identified via poi_ids).
 * This is acceptable even with the poi_ids gotcha since it just counts daily
 * volume of visitors (not claiming pings are at the POI).
 */
export async function startConsolidatedTemporalQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for temporal query');

  const useDwell = hasDwellFilter(dwell);
  let sql: string;

  if (useDwell && poiCoords?.length) {
    // Spatial + dwell filter path
    const allPingsUnion = buildUnionAll(
      syncedJobs,
      'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
      `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    );
    const dwellCTEs = buildDwellFilterCTEs(dwell);

    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords)}${dwellCTEs}
    SELECT
      a.date,
      COUNT(*) as pings,
      COUNT(DISTINCT a.ad_id) as devices
    FROM at_poi_pings a
    INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date
    GROUP BY a.date
    ORDER BY a.date
    `;
  } else {
    // Original poi_ids path (no dwell filter)
    const poiFilter = buildPoiFilter(poiIds);
    const poiPingsUnion = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT date, ad_id FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION ALL\n      ');

    sql = `
    SELECT
      date,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM (
      ${poiPingsUnion}
    )
    GROUP BY date
    ORDER BY date
    `;
  }

  console.log(`[MEGA-TEMPORAL] Starting temporal query across ${syncedJobs.length} tables (dwell=${useDwell})`);
  return await startQueryAsync(sql);
}

// ── Q5b: Total unique devices (global COUNT DISTINCT) ─────────────────

/**
 * Single-row query: total unique devices that visited any POI across all sub-jobs.
 * Needed because summing daily COUNT(DISTINCT) gives device-days, not unique devices.
 */
export async function startConsolidatedTotalDevicesQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for total devices query');

  const useDwell = hasDwellFilter(dwell);
  let sql: string;

  if (useDwell && poiCoords?.length) {
    // Spatial + dwell filter path
    const allPingsUnion = buildUnionAll(
      syncedJobs,
      'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
      `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    );
    const dwellCTEs = buildDwellFilterCTEs(dwell);

    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords)}${dwellCTEs}
    SELECT COUNT(DISTINCT ad_id) as total_unique_devices
    FROM dwell_filtered
    `;
  } else {
    // Original poi_ids path
    const poiFilter = buildPoiFilter(poiIds);
    const unionParts = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT DISTINCT ad_id FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION\n      ');

    sql = `
    SELECT COUNT(*) as total_unique_devices
    FROM (
      ${unionParts}
    )
    `;
  }

  console.log(`[MEGA-TOTAL-DEVICES] Starting total unique devices query across ${syncedJobs.length} tables (dwell=${useDwell})`);
  return await startQueryAsync(sql);
}

// ── Q6: MAIDs (unique device IDs) ──────────────────────────────────────

/**
 * Build and start a query for all unique MAIDs (ad_id) that visited POIs.
 * Returns Athena queryId. The output CSV is used directly for export.
 *
 * Note: Uses poi_ids which is ✅ correct for identifying visitors.
 */
export async function startConsolidatedMAIDsQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for MAIDs query');

  const useDwell = hasDwellFilter(dwell);
  let sql: string;

  if (useDwell && poiCoords?.length) {
    // Spatial + dwell filter path
    const allPingsUnion = buildUnionAll(
      syncedJobs,
      'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
      `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    );
    const dwellCTEs = buildDwellFilterCTEs(dwell);

    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords)}${dwellCTEs}
    SELECT DISTINCT ad_id
    FROM dwell_filtered
    ORDER BY ad_id
    `;
  } else {
    // Original poi_ids path
    const poiFilter = buildPoiFilter(poiIds);
    const unionParts = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT DISTINCT ad_id FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION\n      ');

    sql = `
    SELECT DISTINCT ad_id
    FROM (
      ${unionParts}
    )
    ORDER BY ad_id
    `;
  }

  console.log(`[MEGA-MAIDS] Starting MAIDs query across ${syncedJobs.length} tables (dwell=${useDwell})`);
  return await startQueryAsync(sql);
}

// ── Q7: NSE (ad_id + origin coords for socioeconomic segmentation) ──────

/**
 * Build and start a query that returns each MAID with its origin coordinates.
 * Used for NSE (socioeconomic) segmentation: geocode origins → postal codes → match to NSE brackets.
 * Origin = first ping of day per device (MIN_BY latitude/longitude by timestamp).
 * Coordinates rounded to 1 decimal (~11km) at SQL level to reduce geocoding workload.
 */
export async function startConsolidatedNseQuery(
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for NSE query');

  // All pings with accuracy filter
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  let visitorsCTE: string;
  if (poiCoords?.length) {
    // Spatial proximity path (same as catchment)
    const dwellCTEs = buildDwellFilterCTEs(dwell);
    const useDwell = hasDwellFilter(dwell);

    visitorsCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings_raw', poiCoords)}${dwellCTEs},
    poi_visitors AS (
      ${useDwell
        ? `SELECT DISTINCT ad_id FROM dwell_filtered`
        : `SELECT DISTINCT ad_id FROM at_poi_pings`}
    ),
    valid_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng FROM all_pings_raw
    )`;
  } else {
    // Fallback: poi_ids
    const poiFilter = buildPoiFilter(poiIds);
    const poiUnion = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT ad_id FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION ALL\n      ');

    visitorsCTE = `
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
    )`;
  }

  const sql = `
    WITH
    ${visitorsCTE},
    first_pings AS (
      SELECT
        v.ad_id,
        MIN_BY(vp.lat, vp.utc_timestamp) as origin_lat,
        MIN_BY(vp.lng, vp.utc_timestamp) as origin_lng
      FROM valid_pings vp
      INNER JOIN poi_visitors v ON vp.ad_id = v.ad_id
      GROUP BY v.ad_id
    )
    SELECT DISTINCT ad_id,
      ROUND(origin_lat, 1) as origin_lat,
      ROUND(origin_lng, 1) as origin_lng
    FROM first_pings
    WHERE origin_lat IS NOT NULL
  `;

  console.log(`[MEGA-NSE] Starting NSE query across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length})`);
  return await startQueryAsync(sql);
}

// ── Q8: Affinity data (per-origin visit metrics) ────────────────────────

/**
 * Build and start the affinity data query.
 * Computes per-origin: total visits, unique devices, avg dwell, avg frequency.
 * Results are geocoded server-side to postal codes, then scored 0-100.
 * Requires spatial path (poiCoords) since dwell time computation needs at_poi_pings.
 */
export async function startConsolidatedAffinityQuery(
  subJobs: Job[],
  poiCoords: PoiCoord[],
  dwell?: DwellFilter,
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for affinity query');

  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
  );

  const dwellCTEs = buildDwellFilterCTEs(dwell);
  const useDwell = hasDwellFilter(dwell);

  // visit_metrics: per (ad_id, date) at the POI — dwell + origin coords
  const visitMetricsSource = useDwell
    ? `at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date`
    : `at_poi_pings a`;
  const visitMetricsPrefix = 'a.';

  const sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords)}${dwellCTEs},
    visit_metrics AS (
      SELECT
        ${visitMetricsPrefix}ad_id,
        ${visitMetricsPrefix}date,
        ROUND(DATE_DIFF('second', MIN(${visitMetricsPrefix}utc_timestamp), MAX(${visitMetricsPrefix}utc_timestamp)) / 60.0, 1) as dwell_minutes
      FROM ${visitMetricsSource}
      GROUP BY ${visitMetricsPrefix}ad_id, ${visitMetricsPrefix}date
    ),
    device_origins AS (
      SELECT
        vm.ad_id,
        vm.date,
        vm.dwell_minutes,
        MIN_BY(ap.lat, ap.utc_timestamp) as origin_lat,
        MIN_BY(ap.lng, ap.utc_timestamp) as origin_lng
      FROM visit_metrics vm
      JOIN all_pings ap ON vm.ad_id = ap.ad_id AND vm.date = ap.date
      GROUP BY vm.ad_id, vm.date, vm.dwell_minutes
    )
    SELECT
      ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
      ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
      COUNT(*) as total_visits,
      COUNT(DISTINCT ad_id) as unique_devices,
      AVG(dwell_minutes) as avg_dwell,
      CAST(COUNT(*) AS DOUBLE) / NULLIF(COUNT(DISTINCT ad_id), 0) as avg_frequency
    FROM device_origins
    GROUP BY ROUND(origin_lat, ${COORDINATE_PRECISION}), ROUND(origin_lng, ${COORDINATE_PRECISION})
    ORDER BY total_visits DESC
    LIMIT 100000
  `;

  console.log(`[MEGA-AFFINITY] Starting affinity query across ${syncedJobs.length} tables (dwell=${useDwell})`);
  return await startQueryAsync(sql);
}
