/**
 * Persona feature CTAS — builds the per-device feature vector that feeds
 * k-means clustering + RFM grid + cross-dataset analysis.
 *
 * Output Parquet table columns (one row per ad_id):
 *   ad_id STRING
 *   total_visits BIGINT
 *   total_dwell_min DOUBLE
 *   recency_days BIGINT
 *   avg_dwell_min DOUBLE
 *   morning_share, midday_share, afternoon_share, evening_share, night_share DOUBLE
 *   weekend_share DOUBLE
 *   friday_evening_share DOUBLE
 *   gyration_km DOUBLE
 *   unique_h3_cells BIGINT
 *   home_zip STRING       -- prefers FULL geo_fields['zipcode'], else NULL
 *   home_region STRING    -- FULL geo_fields['region'], else NULL
 *   gps_share DOUBLE      -- FULL only; 1.0 fallback for BASIC
 *   avg_circle_score DOUBLE
 *   brand_visits_json STRING   -- JSON map "brand → count" (parsed in Node)
 *   brand_loyalty_hhi DOUBLE
 *   tier_high_quality BOOLEAN
 */

import { Job } from './jobs';
import { getTableName } from './athena';
import {
  extractPoiCoords,
  buildAtPoiPingsCTE,
  buildHourFilterClause,
  buildGpsOnlyClause,
  buildCircleScoreClause,
  type PoiCoord,
  type VisitorFilter,
  type DwellFilter,
} from './mega-consolidation-queries';

const ACCURACY_THRESHOLD = 500;

/**
 * Build the feature-vector CTAS query for ONE megajob.
 *
 * Strategy:
 * - UNION ALL over the megajob's sub-job tables (already date-partitioned).
 * - Spatial filter: pings within radius of the megajob's POIs (reuse
 *   buildAtPoiPingsCTE — grid-bucket join, O(N+M)).
 * - Brand mapping: a small `(poi_id, brand)` external table joined per ping
 *   (built by the endpoint before this query runs; passed as `brandTableRef`).
 * - Date envelope: `dateRangeTo` is the megajob's `to_date` — used for
 *   computing `recency_days` deterministically (otherwise it'd depend on
 *   when the query runs).
 *
 * @param ctasTable Output table name (must be unique).
 * @param ctasS3Path S3 location for Parquet output (athena-temp/...)
 * @param syncedJobs Sub-jobs of the megajob.
 * @param poiCoords All POI coords for the megajob.
 * @param brandTableRef Athena table name for the (poi_id, brand) mapping.
 * @param dateRangeTo End date YYYY-MM-DD for recency calc.
 * @param filters Optional source-side filters.
 * @param sourceMegajobId To stamp into the output for cross-dataset analysis.
 */
export function buildFeatureCTAS(args: {
  ctasTable: string;
  ctasS3Path: string;
  syncedJobs: Job[];
  poiCoords: PoiCoord[];
  brandTableRef: string;
  dateRangeTo: string;
  sourceMegajobId: string;
  filters?: VisitorFilter & { dwell?: DwellFilter };
}): string {
  const {
    ctasTable,
    ctasS3Path,
    syncedJobs,
    poiCoords,
    brandTableRef,
    dateRangeTo,
    sourceMegajobId,
    filters,
  } = args;

  const hourClause = buildHourFilterClause(filters);
  const gpsClause = buildGpsOnlyClause(filters);
  const scoreClause = buildCircleScoreClause(filters);

  // Build the all_pings UNION over sub-job tables.
  const allPingsUnion = syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `
        SELECT
          ad_id, date, utc_timestamp,
          TRY_CAST(latitude AS DOUBLE) as lat,
          TRY_CAST(longitude AS DOUBLE) as lng,
          horizontal_accuracy,
          poi_ids,
          TRY(geo_fields['zipcode']) as native_zip,
          TRY(geo_fields['region']) as native_region,
          TRY(geo_fields['h3_res10']) as h3_cell,
          TRY(quality_fields['ping_origin_type']) as ping_origin_type,
          TRY_CAST(quality_fields['ping_circle_score'] AS DOUBLE) as ping_circle_score
        FROM ${table}
        WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
          AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
          AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
          AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
          ${hourClause}
          ${gpsClause}
          ${scoreClause}
      `;
    })
    .join('\n      UNION ALL\n      ');

  const sql = `
    CREATE TABLE ${ctasTable}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='${ctasS3Path}')
    AS
    WITH
    all_pings AS (
      ${allPingsUnion}
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords)},
    -- Per (ad_id, date): one row with first/last ping, dwell, hour bucket, dow
    daily_visits AS (
      SELECT
        ad_id,
        date,
        MIN(utc_timestamp) as first_ping,
        MAX(utc_timestamp) as last_ping,
        DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell_minutes,
        HOUR(MIN(utc_timestamp)) as arrival_hour,
        DAY_OF_WEEK(MIN(utc_timestamp)) as dow
      FROM at_poi_pings
      GROUP BY ad_id, date
    ),
    visitor_ad_ids AS (
      SELECT DISTINCT ad_id FROM at_poi_pings
    ),
    -- Home zipcode: prefer FULL geo_fields['zipcode']; mode of zips in night hours (22-06)
    home_zip_pre AS (
      SELECT
        p.ad_id,
        p.native_zip as zip,
        p.native_region as region,
        COUNT(*) as freq
      FROM all_pings p
      INNER JOIN visitor_ad_ids v ON p.ad_id = v.ad_id
      WHERE p.native_zip IS NOT NULL AND p.native_zip != ''
        AND (HOUR(p.utc_timestamp) >= 22 OR HOUR(p.utc_timestamp) <= 6)
      GROUP BY p.ad_id, p.native_zip, p.native_region
    ),
    home_zip_ranked AS (
      SELECT ad_id, zip, region, freq,
        ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY freq DESC, zip) as rn
      FROM home_zip_pre
    ),
    home_zip AS (
      SELECT ad_id, zip as home_zip, region as home_region
      FROM home_zip_ranked
      WHERE rn = 1
    ),
    -- Brand visits per device: poi_ids array → resolved brand → count
    poi_unnest AS (
      SELECT vp.ad_id, t.poi_id
      FROM all_pings vp
      INNER JOIN visitor_ad_ids v ON vp.ad_id = v.ad_id
      CROSS JOIN UNNEST(COALESCE(vp.poi_ids, ARRAY[])) as t(poi_id)
      WHERE t.poi_id IS NOT NULL AND t.poi_id != ''
    ),
    brand_visits AS (
      SELECT pu.ad_id, b.brand, COUNT(*) as visits
      FROM poi_unnest pu
      INNER JOIN ${brandTableRef} b ON pu.poi_id = b.poi_id
      GROUP BY pu.ad_id, b.brand
    ),
    brand_visits_agg AS (
      SELECT
        ad_id,
        MAP_AGG(brand, visits) as brand_map,
        SUM(visits) as total_brand_visits,
        SUM(visits * visits) as sum_sq
      FROM brand_visits
      GROUP BY ad_id
    ),
    -- Mobility radius (rough): max distance between min/max lat,lng for the device
    mobility AS (
      SELECT
        p.ad_id,
        APPROX_DISTINCT(p.h3_cell) as unique_h3_cells,
        AVG(IF(p.ping_origin_type = 'gps', 1.0, 0.0)) as gps_share,
        AVG(p.ping_circle_score) as avg_circle_score,
        MAX(p.lat) as max_lat,
        MIN(p.lat) as min_lat,
        MAX(p.lng) as max_lng,
        MIN(p.lng) as min_lng
      FROM all_pings p
      INNER JOIN visitor_ad_ids v ON p.ad_id = v.ad_id
      GROUP BY p.ad_id
    ),
    -- Per-device aggregates from daily_visits
    visit_agg AS (
      SELECT
        ad_id,
        COUNT(DISTINCT date) as total_visits,
        SUM(dwell_minutes) as total_dwell_min,
        AVG(dwell_minutes) as avg_dwell_min,
        DATE_DIFF('day', MAX(DATE_PARSE(date, '%Y-%m-%d')), DATE '${dateRangeTo}') as recency_days,
        AVG(IF(arrival_hour >= 5  AND arrival_hour < 11, 1.0, 0.0)) as morning_share,
        AVG(IF(arrival_hour >= 11 AND arrival_hour < 14, 1.0, 0.0)) as midday_share,
        AVG(IF(arrival_hour >= 14 AND arrival_hour < 18, 1.0, 0.0)) as afternoon_share,
        AVG(IF(arrival_hour >= 18 AND arrival_hour < 22, 1.0, 0.0)) as evening_share,
        AVG(IF(arrival_hour >= 22 OR  arrival_hour <  5, 1.0, 0.0)) as night_share,
        AVG(IF(dow IN (6, 7), 1.0, 0.0)) as weekend_share,
        AVG(IF(dow = 5 AND arrival_hour >= 19 AND arrival_hour <= 23, 1.0, 0.0)) as friday_evening_share
      FROM daily_visits
      GROUP BY ad_id
    )
    SELECT
      v.ad_id,
      v.total_visits,
      v.total_dwell_min,
      v.recency_days,
      v.avg_dwell_min,
      v.morning_share,
      v.midday_share,
      v.afternoon_share,
      v.evening_share,
      v.night_share,
      v.weekend_share,
      v.friday_evening_share,
      -- Approx gyration radius: half-diagonal of bounding box (km)
      111.32 * SQRT(
        POW(m.max_lat - m.min_lat, 2) +
        POW((m.max_lng - m.min_lng) * COS(RADIANS((m.max_lat + m.min_lat) / 2)), 2)
      ) / 2 as gyration_km,
      m.unique_h3_cells,
      hz.home_zip,
      hz.home_region,
      COALESCE(m.gps_share, 0.0) as gps_share,
      COALESCE(m.avg_circle_score, 9.99) as avg_circle_score,
      -- brand_visits_json: serialize MAP as VARCHAR (Parquet doesn't accept JSON type).
      -- JSON_FORMAT(CAST(... AS JSON)) returns a VARCHAR like '{"burger_king":12,"mcdonalds":4}'.
      JSON_FORMAT(CAST(b.brand_map AS JSON)) as brand_visits_json,
      CASE WHEN b.total_brand_visits > 0
        THEN CAST(b.sum_sq AS DOUBLE) / (CAST(b.total_brand_visits AS DOUBLE) * b.total_brand_visits)
        ELSE 0.0
      END as brand_loyalty_hhi,
      (COALESCE(m.gps_share, 0.0) > 0.7 AND COALESCE(m.avg_circle_score, 9.99) < 1.0) as tier_high_quality,
      '${sourceMegajobId}' as source_megajob_id
    FROM visit_agg v
    LEFT JOIN mobility m ON v.ad_id = m.ad_id
    LEFT JOIN home_zip hz ON v.ad_id = hz.ad_id
    LEFT JOIN brand_visits_agg b ON v.ad_id = b.ad_id
  `;

  return sql;
}

export { extractPoiCoords };
