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
  runQuery,
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
 * Canonical Glue catalog table name for the home table of `datasetName`.
 *
 * Always `home_{sanitized}` — fixed, no timestamp suffix. This makes
 * the table addressable from any future query (`SELECT … FROM
 * home_{ds} JOIN …`) without needing to look up a per-run name. The
 * trade-off is that recomputing replaces the existing table; we accept
 * this because home location only needs to be computed once per
 * dataset and is overwriteable on demand.
 */
export function homeTableName(datasetName: string): string {
  const safe = datasetName.replace(/[^a-zA-Z0-9_]/g, '_');
  return `home_${safe}`;
}

/** S3 prefix where the home table's parquet shards live. */
export function homeTableS3Prefix(datasetName: string): string {
  return `home-locations/${datasetName}`;
}

/**
 * Returns true if the home table has been computed and is queryable
 * (Glue catalog row exists + at least one parquet shard in S3).
 *
 * We require both checks because a partial state — Glue row but no
 * data, or data but no Glue row — would silently return wrong results.
 */
export async function homeTableExists(datasetName: string): Promise<boolean> {
  const table = homeTableName(datasetName);
  // 1. Cheap probe of the S3 prefix.
  const list = await s3Client.send(new ListObjectsV2Command({
    Bucket: BUCKET,
    Prefix: `${homeTableS3Prefix(datasetName)}/`,
    MaxKeys: 1,
  }));
  if (!list.Contents || list.Contents.length === 0) return false;
  // 2. Glue table is queryable: DESCRIBE is metadata-only and fails
  //    fast if the table is unregistered.
  try {
    await runQuery(`DESCRIBE ${table}`);
    return true;
  } catch {
    return false;
  }
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
        -- Quality: we deliberately do NOT filter by
        -- quality_fields['ping_origin_type']. The previous "GPS only"
        -- filter biased home detection against dense urban residents,
        -- whose indoor pings are WiFi-based — restricting to GPS pings
        -- left only their outdoor activity and pushed inferred "home"
        -- to streets / parks / suburbs (Pappalardo et al. recommend
        -- against single-origin filtering in mixed-density geographies).
        -- We keep the horizontal_accuracy < 1 km bound, which is the
        -- stronger signal of whether a ping is usable for home inference.
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
 * S3 path strategy: Athena's CTAS refuses to write to a non-empty
 * external_location with `HIVE_PATH_ALREADY_EXISTS`, and the check is
 * sensitive to object *presence* regardless of size. When the Vercel
 * IAM lacks `s3:DeleteObject` on `home-locations/{ds}/`, our Reset
 * endpoint falls back to 0-byte soft-deletes — those tombstones still
 * trip the Athena precondition. To stay robust against that we:
 *   1. Drop the Glue table and best-effort wipe the canonical prefix.
 *   2. If anything (incl. 0-byte tombstones) is still there, pivot to
 *      a timestamped sub-prefix `home-locations/{ds}/_{ts}/` for this
 *      run's CTAS output. The new Glue table's LOCATION points there,
 *      so all the downstream JOIN consumers find the fresh data.
 * The canonical prefix is never used to *store* data anymore — only as
 * a parent under which sub-prefixes live. Existing legacy tables that
 * were created directly at `home-locations/{ds}/` keep working because
 * the Glue catalog already stores their exact LOCATION.
 */
export async function startHomeDetection(
  datasetName: string,
): Promise<HomeDetectionResult> {
  await ensureTableForDataset(datasetName);
  const sourceTable = getTableName(datasetName);

  const outputTable = homeTableName(datasetName);
  const rootPrefix = homeTableS3Prefix(datasetName);

  // Step 1: drop the Glue table and try to wipe the canonical prefix.
  await dropHomeTableForCTAS(outputTable);
  await wipeS3Prefix(rootPrefix);

  // Step 2: count whatever survived (size doesn't matter — Athena's
  // HIVE_PATH_ALREADY_EXISTS fires on tombstones too).
  let survivors = 0;
  try {
    const list = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET, Prefix: `${rootPrefix}/`, MaxKeys: 1,
    }));
    survivors = list.Contents?.length || 0;
  } catch {
    survivors = 0;
  }

  // Step 3: pivot to a timestamped sub-prefix if needed.
  let outputS3Prefix = rootPrefix;
  if (survivors > 0) {
    const ts = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 14);
    outputS3Prefix = `${rootPrefix}/_${ts}`;
    console.warn(
      `[HOME-DETECT] ${datasetName}: ${survivors}+ object(s) survived wipe of ` +
      `canonical prefix (likely IAM-locked soft-delete tombstones); ` +
      `redirecting CTAS to s3://${BUCKET}/${outputS3Prefix}/`,
    );
  }

  const sql = buildHomeDetectionSQL(sourceTable, outputTable, outputS3Prefix);
  const queryId = await startQueryAsync(sql);
  return { queryId, outputTable, outputS3Prefix };
}

/**
 * Register an EXTERNAL TABLE pointing at an existing home-table parquet
 * location, without re-running the (expensive) CTAS. Used when the
 * parquet data was already produced — e.g. by a previous timestamped
 * CTAS — and we just need the canonical Glue catalog entry to make it
 * queryable as `home_{ds}`.
 *
 * No-op if the table already exists.
 */
/**
 * Find where the actual home-table parquet shards live for a dataset.
 * Supports two layouts:
 *   (a) Legacy: directly under `home-locations/{ds}/`
 *   (b) Soft-delete-pivoted: under `home-locations/{ds}/_{ts}/`
 * Returns the prefix that should be used as the Athena LOCATION, or
 * null if no sized parquets are found anywhere underneath.
 */
async function findLiveHomePrefix(rootPrefix: string): Promise<string | null> {
  const top = await s3Client.send(new ListObjectsV2Command({
    Bucket: BUCKET,
    Prefix: `${rootPrefix}/`,
    Delimiter: '/',
  }));
  // Layout (a): sized files directly under the root prefix.
  const directSized = (top.Contents || []).filter((o) => (o.Size || 0) > 0);
  if (directSized.length > 0) return rootPrefix;
  // Layout (b): walk sub-prefixes newest-first (lexical order on `_{ts}/`
  // matches chronological order because the timestamp is ISO without
  // separators), return the first one with at least one sized file.
  const subPrefixes = (top.CommonPrefixes || [])
    .map((p) => p.Prefix!)
    .filter((p): p is string => Boolean(p))
    .sort()
    .reverse();
  for (const sub of subPrefixes) {
    const r = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET, Prefix: sub, MaxKeys: 100,
    }));
    if ((r.Contents || []).some((o) => (o.Size || 0) > 0)) {
      return sub.replace(/\/$/, '');
    }
  }
  return null;
}

export async function attachHomeTable(datasetName: string): Promise<void> {
  const table = homeTableName(datasetName);
  const rootPrefix = homeTableS3Prefix(datasetName);

  // Discover where the actual parquet shards live. Two layouts to support:
  //   (a) Legacy: parquets directly under `home-locations/{ds}/`
  //   (b) Soft-delete-pivoted: parquets under `home-locations/{ds}/_{ts}/`
  // We list one level deep, pick direct (sized) files first; if none,
  // fall back to the most-recent sub-prefix that has sized files.
  const prefix = await findLiveHomePrefix(rootPrefix);
  if (!prefix) {
    throw new Error(`attachHomeTable(${datasetName}): no parquet at s3://${BUCKET}/${rootPrefix}/ (root or any sub-prefix)`);
  }
  // Schema must exactly match the CTAS output (see buildHomeDetectionSQL).
  await runQuery(`
    CREATE EXTERNAL TABLE IF NOT EXISTS ${table} (
      ad_id           STRING,
      home_geohash6   STRING,
      home_lat        DOUBLE,
      home_lng        DOUBLE,
      home_zip        STRING,
      home_city       STRING,
      home_region     STRING,
      n_nights        INT,
      home_confidence DOUBLE
    )
    STORED AS PARQUET
    LOCATION 's3://${BUCKET}/${prefix}/'
    TBLPROPERTIES ('parquet.compress'='SNAPPY')
  `);
}

/**
 * Drop the home table for a dataset: removes the Glue catalog entry
 * AND deletes the parquet shards in S3. Used by the "Re-detect homes"
 * admin endpoint so the next analysis click triggers a fresh CTAS
 * with the current home-detection SQL (rather than reusing an old
 * possibly-biased table).
 *
 * Throws with an actionable IAM message if `glue:DeleteTable` is
 * denied — the caller should surface this to the user rather than
 * silently leaving the Glue catalog inconsistent (which then blocks
 * the next CTAS with a confusing "table already exists" error).
 */
export async function dropHomeTable(datasetName: string): Promise<void> {
  const table = homeTableName(datasetName);
  const prefix = homeTableS3Prefix(datasetName);
  await dropHomeTableStrict(table);
  await wipeS3Prefix(prefix);
}

/** Drop a Glue table via Athena DDL. Throws with actionable IAM
 *  guidance if the caller IAM user lacks glue:DeleteTable. Treats
 *  "table not found" as success (DROP IF EXISTS semantics). */
async function dropHomeTableStrict(table: string): Promise<void> {
  try {
    await runQuery(`DROP TABLE IF EXISTS ${table}`);
  } catch (e: any) {
    const msg = e?.message || String(e);
    if (/glue:DeleteTable|AccessDenied/i.test(msg)) {
      throw new Error(
        `Cannot drop Glue table "${table}": the IAM user lacks glue:DeleteTable. ` +
        `Ask your AWS admin to add this action (resource: arn:aws:glue:us-west-2:*:catalog) ` +
        `to the veraset_api IAM policy. Until then, "Re-detect homes" cannot replace ` +
        `an existing home table — but new datasets without a home table still work via ` +
        `the auto-trigger on Analyze.`,
      );
    }
    // Any other failure (network, transient) is also surfaced — silent
    // swallowing was what created the inconsistent state in the first place.
    throw new Error(`DROP TABLE IF EXISTS ${table} failed: ${msg}`);
  }
}

/** Internal helper used during startHomeDetection() for idempotency.
 *  Tolerates the same IAM denial that dropHomeTableStrict surfaces —
 *  during a fresh CTAS we'd rather try and fail at CREATE TABLE than
 *  block before we even start. */
async function dropHomeTableForCTAS(table: string): Promise<void> {
  try {
    await runQuery(`DROP TABLE IF EXISTS ${table}`);
  } catch (e: any) {
    console.warn(`[HOME-DETECTOR] DROP TABLE IF EXISTS ${table} warning (CTAS will fail if table exists):`, e?.message || e);
  }
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
