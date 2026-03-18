/**
 * Mobility analysis: POI categories visited before/after target POI.
 *
 * Uses spatial proximity to identify at-POI visits, then finds nearby
 * Overture POI categories visited within ±2 hours (before/after).
 *
 * Mirrors the mega-consolidation mobility query but for a single dataset,
 * with synchronous execution (waits for Athena results).
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';
import type { PoiCoord } from './mega-consolidation-queries';

export interface MobilityCategoryEntry {
  category: string;
  deviceDays: number;
  hits: number;
}

export interface MobilityAnalysisResult {
  analyzedAt: string;
  /** Combined categories (before + after merged) */
  categories: MobilityCategoryEntry[];
  /** Categories visited BEFORE arriving at target POI (up to 2h prior) */
  before: MobilityCategoryEntry[];
  /** Categories visited AFTER leaving target POI (up to 2h after) */
  after: MobilityCategoryEntry[];
}

export interface MobilityFilters {
  poiIds?: string[];
  poiCoords?: PoiCoord[];
}

const ACCURACY_THRESHOLD = 500;
const GRID_STEP = 0.01; // ~1.1km geohash grid
const POI_TABLE = 'lab_pois_gmc';

function buildTargetPoisValues(poiCoords: PoiCoord[]): string {
  const rows = poiCoords
    .map((c) => `(${c.lat}, ${c.lng}, ${c.radiusM}.0)`)
    .join(', ');
  return `(VALUES ${rows}) AS t(poi_lat, poi_lng, poi_radius_m)`;
}

/**
 * Build the mobility SQL query for a single dataset.
 * Identifies at-POI visits, then finds nearby POI categories ±2h.
 */
function buildMobilitySQL(table: string, poiCoords?: PoiCoord[], poiIds?: string[]): string {
  let visitTimeCTE: string;

  if (poiCoords?.length) {
    // Spatial proximity approach (accurate)
    visitTimeCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
    ),
    target_pois AS (SELECT * FROM ${buildTargetPoisValues(poiCoords)}),
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
    // Fallback: poi_ids array approach
    const poiFilter = poiIds?.length
      ? `AND poi_id IN (${poiIds.map((id) => `'${id}'`).join(', ')})`
      : '';
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
        ${poiFilter}
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

  return `
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
}

/**
 * Analyze mobility patterns — what POI categories do visitors go to
 * before and after visiting the target POI(s).
 */
export async function analyzeMobility(
  datasetName: string,
  filters: MobilityFilters = {},
): Promise<MobilityAnalysisResult> {
  console.log(`[MOBILITY] Starting mobility analysis for dataset: ${datasetName}`, {
    hasPoisCoords: !!filters.poiCoords?.length,
    hasPoiIds: !!filters.poiIds?.length,
    timestamp: new Date().toISOString(),
  });

  // Check AWS credentials
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error(
      'AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.',
    );
  }

  // Ensure table exists
  let exists = false;
  try {
    exists = await tableExists(datasetName);
  } catch (error: any) {
    if (
      error.message?.includes('not authorized') ||
      error.message?.includes('Access denied') ||
      error.name === 'AccessDeniedException'
    ) {
      throw new Error(
        `Athena access denied. Please ensure your AWS IAM user has Athena and Glue permissions.\n\nOriginal error: ${error.message}`,
      );
    }
    throw error;
  }

  if (!exists) {
    console.log(`[MOBILITY] Creating table for dataset: ${datasetName}`);
    try {
      await createTableForDataset(datasetName);
    } catch (error: any) {
      if (error.message?.includes('not authorized') || error.message?.includes('Access denied')) {
        throw new Error(`Cannot create Athena table: Access denied.\n\nOriginal error: ${error.message}`);
      }
      throw error;
    }
  } else {
    try {
      await createTableForDataset(datasetName);
    } catch (error: any) {
      if (!error.message?.includes('already exists')) {
        console.warn(`[MOBILITY] Warning checking table schema:`, error.message);
      }
    }
  }

  // Build and run the mobility query synchronously
  const table = getTableName(datasetName);
  const sql = buildMobilitySQL(table, filters.poiCoords, filters.poiIds);

  console.log(`[MOBILITY] Running Athena query for ${datasetName} (spatial=${!!filters.poiCoords?.length})`);
  const result = await runQuery(sql);
  const rows = result.rows;

  console.log(`[MOBILITY] Query returned ${rows.length} rows`);

  // Parse rows into before/after categories
  const all: Array<{ timing: string; category: string; deviceDays: number; hits: number }> = rows.map(
    (row: Record<string, any>) => ({
      timing: String(row.timing || 'after'),
      category: String(row.category || 'UNKNOWN'),
      deviceDays: parseInt(row.device_days, 10) || 0,
      hits: parseInt(row.hits, 10) || 0,
    }),
  );

  const before = all.filter((r) => r.timing === 'before').slice(0, 25);
  const after = all.filter((r) => r.timing === 'after').slice(0, 25);

  // Combined (merge before + after, sum device_days)
  const merged = new Map<string, { deviceDays: number; hits: number }>();
  for (const r of all) {
    const existing = merged.get(r.category);
    if (existing) {
      existing.deviceDays += r.deviceDays;
      existing.hits += r.hits;
    } else {
      merged.set(r.category, { deviceDays: r.deviceDays, hits: r.hits });
    }
  }
  const categories = Array.from(merged.entries())
    .map(([category, v]) => ({ category, ...v }))
    .sort((a, b) => b.deviceDays - a.deviceDays)
    .slice(0, 50);

  return {
    analyzedAt: new Date().toISOString(),
    categories,
    before,
    after,
  };
}
