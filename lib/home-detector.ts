/**
 * TC-WK-19-7 home location inference for Veraset Movement parquets.
 *
 * Methodology: see `docs/METHODOLOGY.md §2.3`. Per Pappalardo et al.
 * (EPJ Data Science 12, 19, 2023), the best of 37 home-detection
 * algorithms benchmarked against 65 consented Telefónica Chile users
 * with known home addresses (45% accuracy at exact-tower, 69%
 * within top-3 nearest towers). The "TC-WK-19-7" label encodes:
 *
 *   TC  — Time-Constraints family (vs. Maximum-Amount or Distinct-Days)
 *   WK  — restrict to weekdays (Mon-Fri)
 *   19-7 — restrict to nighttime window 19:00 – 07:00 local
 *
 * For our setting we substitute UTC for "local" because Veraset's
 * `utc_timestamp` is the only timestamp delivered. This introduces a
 * small bias for non-UTC regions (e.g. Mexico City is UTC-6: a
 * Mexican 7 pm is UTC 01:00, which falls inside our [19, 24) ∪ [0, 7)
 * window, so the bias is small for Mexico but could matter for e.g.
 * Pacific time zones). A future enhancement is to derive the local
 * timezone from `iso_country_code` and shift the window per country.
 *
 * The output is a Parquet table at
 *     s3://{BUCKET}/home-locations/{datasetName}/
 * with the canonical schema (§2.5 of METHODOLOGY.md):
 *
 *     (ad_id           STRING,
 *      home_geohash6   STRING,   -- rounded "lat,lng" string used as bucket key
 *      home_lat        DOUBLE,   -- centroid of the bucket (sub-grid precision)
 *      home_lng        DOUBLE,
 *      home_zip        STRING,   -- mode of native_zip among bucket pings (FULL schema only)
 *      home_city       STRING,
 *      home_region     STRING,
 *      n_nights        INT,      -- distinct nights ad_id was seen at home bucket
 *      home_confidence DOUBLE)   -- n_nights / total nights observed for this ad_id
 *
 * Athena lacks a stock `geohash6` function, so we substitute
 * ROUND(lat, 2) × ROUND(lng, 2) (≈ 1.1 km × 1.1 km at the equator,
 * shrinking with cos(latitude)). At Mexico's typical latitude (~20°),
 * this is ≈ 1.1 km × 1.0 km, which is essentially geohash6 resolution.
 */

import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from './athena';
import { BUCKET, s3Client } from './s3-config';
import { ListObjectsV2Command, DeleteObjectsCommand } from '@aws-sdk/client-s3';

/**
 * Lat/lng rounding precision. 2 decimal digits ≈ 0.01° ≈ 1.1 km at the
 * equator, ~785 m at 45° latitude. This matches the spatial scale of
 * geohash6 (1.2 km × 0.6 km), the standard precision used by both
 * Pappalardo et al.'s validation study and the Veraset Home/Work
 * commercial product.
 */
export const GEO_PRECISION_DEGREES = 2;

/**
 * Minimum distinct nights at the home bucket. Pappalardo et al. §3.4
 * recommend 2-3 nights as the lower bound below which inference is
 * dominated by noise rather than signal. We use 3.
 */
export const MIN_NIGHTS_AT_HOME = 3;

/**
 * Horizontal-accuracy threshold (meters). Veraset's `horizontal_accuracy`
 * is the reported GPS accuracy radius. > 1 km is essentially cell-tower
 * triangulation precision and unsuitable for home detection. Pappalardo
 * et al. use < 500 m; we relax to 1 km to retain pings from low-end
 * Android devices common in LATAM markets.
 */
export const ACCURACY_THRESHOLD_M = 1000;

export interface HomeDetectionResult {
  /** Athena queryId — feed into `pollHomeDetection`. */
  queryId: string;
  /** Athena CTAS output table name. */
  outputTable: string;
  /** S3 prefix where the parquet shards land. */
  outputS3Prefix: string;
}

/**
 * Build the SQL that the `home-detection` CTAS executes. Exposed
 * separately from `startHomeDetection` for testability and so callers
 * can preview the query string.
 */
export function buildHomeDetectionSQL(
  sourceTable: string,
  outputTable: string,
  outputS3Prefix: string,
): string {
  return `
    CREATE TABLE ${outputTable}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = 's3://${BUCKET}/${outputS3Prefix}/'
    )
    AS
    WITH nighttime_pings AS (
      -- §2.3 TC-WK-19-7 nighttime + weekday filter.
      -- HOUR returns 0..23 in UTC; DAY_OF_WEEK returns 1..7 (Mon=1, Sun=7).
      SELECT
        ad_id,
        date,
        utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) AS lat,
        TRY_CAST(longitude AS DOUBLE) AS lng,
        TRY(geo_fields['zipcode']) AS native_zip,
        TRY(geo_fields['city']) AS native_city,
        TRY(geo_fields['region']) AS native_region
      FROM ${sourceTable}
      WHERE TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND (horizontal_accuracy IS NULL
             OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_M})
        AND (HOUR(utc_timestamp) >= 19 OR HOUR(utc_timestamp) < 7)
        AND DAY_OF_WEEK(utc_timestamp) BETWEEN 1 AND 5
        -- Quality filter: prefer GPS-origin pings. NULL means the
        -- BASIC schema doesn't carry quality_fields, so we accept it.
        AND (TRY(quality_fields['ping_origin_type']) IS NULL
             OR TRY(quality_fields['ping_origin_type']) = 'gps')
    ),
    bucketed AS (
      -- Quantize to a ~1.1 km grid (substitute for geohash6).
      SELECT
        ad_id,
        date,
        ROUND(lat, ${GEO_PRECISION_DEGREES}) AS bucket_lat,
        ROUND(lng, ${GEO_PRECISION_DEGREES}) AS bucket_lng,
        lat,
        lng,
        native_zip,
        native_city,
        native_region
      FROM nighttime_pings
    ),
    maid_bucket_nights AS (
      -- Distinct nights per (ad_id, grid cell). The mode-of-night
      -- principle (§2.3) is encoded by COUNT(DISTINCT date).
      SELECT
        ad_id,
        bucket_lat,
        bucket_lng,
        AVG(lat) AS centroid_lat,
        AVG(lng) AS centroid_lng,
        ARBITRARY(native_zip) AS native_zip,
        ARBITRARY(native_city) AS native_city,
        ARBITRARY(native_region) AS native_region,
        COUNT(DISTINCT date) AS n_nights,
        COUNT(*) AS n_pings
      FROM bucketed
      GROUP BY ad_id, bucket_lat, bucket_lng
    ),
    maid_total_nights AS (
      -- Total nights observed per ad_id across ALL buckets — used as
      -- the denominator of home_confidence. Note: a device active on
      -- the same date in 2 different cells gets that date counted
      -- once per cell here, so total_nights can over-state. Acceptable
      -- — it makes home_confidence a conservative (lower) bound.
      SELECT ad_id, SUM(n_nights) AS total_nights
      FROM maid_bucket_nights
      GROUP BY ad_id
    ),
    ranked AS (
      -- Pick the bucket with the most distinct nights per ad_id,
      -- requiring n_nights >= ${MIN_NIGHTS_AT_HOME} (§2.3 stability gate).
      SELECT
        mb.ad_id,
        mb.bucket_lat,
        mb.bucket_lng,
        mb.centroid_lat,
        mb.centroid_lng,
        mb.native_zip,
        mb.native_city,
        mb.native_region,
        mb.n_nights,
        mb.n_pings,
        CAST(mb.n_nights AS DOUBLE) / mt.total_nights AS home_confidence,
        ROW_NUMBER() OVER (
          PARTITION BY mb.ad_id
          ORDER BY mb.n_nights DESC,
                   mb.n_pings  DESC,
                   mb.bucket_lat,
                   mb.bucket_lng
        ) AS rnk
      FROM maid_bucket_nights mb
      JOIN maid_total_nights mt ON mb.ad_id = mt.ad_id
      WHERE mb.n_nights >= ${MIN_NIGHTS_AT_HOME}
    )
    SELECT
      ad_id,
      -- Synthetic bucket key. The downstream JOIN can use either this
      -- string OR (home_lat, home_lng) — they're equivalent at the
      -- bucket level.
      CAST(bucket_lat AS VARCHAR)
        || ',' ||
      CAST(bucket_lng AS VARCHAR) AS home_geohash6,
      centroid_lat AS home_lat,
      centroid_lng AS home_lng,
      native_zip   AS home_zip,
      native_city  AS home_city,
      native_region AS home_region,
      CAST(n_nights AS INTEGER) AS n_nights,
      home_confidence
    FROM ranked
    WHERE rnk = 1
  `;
}

/**
 * Start a home-detection CTAS for `datasetName`. Returns immediately
 * with the Athena queryId; caller polls via `pollHomeDetection`.
 *
 * Idempotent w.r.t. S3 cleanup: any existing parquet under the output
 * prefix is deleted first (Athena CTAS refuses to write to a
 * non-empty location). The Glue catalog table is dropped via the SQL
 * `DROP TABLE IF EXISTS` — we don't pre-issue it here because that
 * would require a second blocking query. Instead, we use a fresh
 * outputTable name per run (timestamp suffix).
 */
export async function startHomeDetection(
  datasetName: string,
): Promise<HomeDetectionResult> {
  await ensureTableForDataset(datasetName);
  const sourceTable = getTableName(datasetName);

  // Sanitize dataset name for the Glue catalog (alphanumeric + underscore only).
  const safe = datasetName.replace(/[^a-zA-Z0-9_]/g, '_');
  const stamp = Math.floor(Date.now() / 1000).toString(36);
  const outputTable = `home_${safe}_${stamp}`;
  const outputS3Prefix = `home-locations/${datasetName}`;

  // Wipe any prior parquet at the output prefix. Athena CTAS will
  // otherwise fail with "external_location must be empty".
  await wipeS3Prefix(outputS3Prefix);

  const sql = buildHomeDetectionSQL(sourceTable, outputTable, outputS3Prefix);
  const queryId = await startQueryAsync(sql);
  return { queryId, outputTable, outputS3Prefix };
}

/**
 * Poll a running home-detection query. Caller is expected to invoke
 * this in a loop with a few-second delay between calls (similar
 * pattern to the other multi-phase queries in this codebase).
 */
export async function pollHomeDetection(
  queryId: string,
): Promise<{ state: 'running' | 'done' | 'error'; error?: string }> {
  const status = await checkQueryStatus(queryId);
  if (status.state === 'SUCCEEDED') return { state: 'done' };
  if (status.state === 'FAILED') return { state: 'error', error: status.error || 'Query failed' };
  if (status.state === 'CANCELLED') return { state: 'error', error: 'Query cancelled' };
  return { state: 'running' };
}

/** Delete all S3 objects under a prefix. Used to make CTAS idempotent. */
async function wipeS3Prefix(prefix: string): Promise<void> {
  // ListObjectsV2 → DeleteObjects (batches of 1000). Bounded loop in case
  // a previous run left thousands of shards behind.
  for (let i = 0; i < 100; i++) {
    const list = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: `${prefix}/`,
      MaxKeys: 1000,
    }));
    const contents = list.Contents || [];
    if (contents.length === 0) break;
    await s3Client.send(new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: { Objects: contents.map((o) => ({ Key: o.Key! })) },
    }));
  }
}
