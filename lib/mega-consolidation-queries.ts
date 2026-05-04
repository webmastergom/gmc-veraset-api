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

import { getTableName, startQueryAsync, startCTASAsync, tempTableName, runQuery, dropTempTable, cleanupTempS3 } from './athena';
import { type Job } from './jobs';
import { PutObjectCommand, ListObjectsV2Command, DeleteObjectsCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from './s3-config';

/**
 * Result returned by every consolidated query: the running queryId and the
 * CTAS table name where Parquet results land. CTAS is used so queries are
 * NOT subject to Athena's 30-minute regular-query timeout — critical when
 * spatial joins involve thousands of POIs (e.g., 25k POI grid).
 *
 * After polling shows the CTAS query SUCCEEDED, downstream phases read
 * results via `SELECT * FROM ctasTable` (or `runQueryViaS3` for big rowsets).
 */
export interface ConsolidatedQueryHandle {
  queryId: string;
  ctasTable: string;
}

/**
 * Wrap a SELECT statement in CTAS and start it asynchronously.
 * Returns both the queryId (for polling) and the ctasTable (for fetching results
 * after the CTAS finishes).
 *
 * `runId` makes table names unique per consolidation run so re-consolidations
 * never collide with leftover tables (Athena would reject CREATE TABLE if it
 * already exists in the catalog).
 */
async function startAsCTAS(
  megaJobId: string,
  runId: string,
  queryName: string,
  selectSql: string,
): Promise<ConsolidatedQueryHandle> {
  const ctasTable = tempTableName(`mc_${queryName}`, `${megaJobId}_${runId}`);
  const queryId = await startCTASAsync(selectSql, ctasTable);
  return { queryId, ctasTable };
}

// ── POI materialization (for queries with > MAX_INLINE_POIS coords) ──────

/**
 * Athena's max query size is ~256 KB. Inlining 25k POIs as VALUES generates
 * a >1 MB SQL string, so we materialize them as an external CSV-backed table
 * once per consolidation run and reference it by name in spatial joins.
 *
 * Threshold of 1000 keeps small jobs on the cheap inline path (~50 KB SQL).
 */
export const MAX_INLINE_POIS = 1000;

const POI_TEMP_PREFIX = 'poi-temp';

/**
 * Build the S3 prefix where the POI CSV for a given run lives.
 * Caller uses this prefix for cleanup at the end of consolidation.
 */
export function poiCoordsTablePrefix(megaJobId: string, runId: string): string {
  return `${POI_TEMP_PREFIX}/${megaJobId}_${runId}`;
}

/**
 * Build the Athena table name that backs the materialized POI CSV.
 */
export function poiCoordsTableName(megaJobId: string, runId: string): string {
  return tempTableName('mc_pois', `${megaJobId}_${runId}`);
}

/**
 * Materialize POI coordinates as an Athena external table backed by a CSV in
 * S3. Returns the table name to use in spatial joins (instead of inline VALUES).
 *
 * Used when poiCoords.length > MAX_INLINE_POIS to avoid Athena's 256 KB SQL limit.
 */
export async function materializePoiCoordsTable(
  megaJobId: string,
  runId: string,
  poiCoords: PoiCoord[],
): Promise<string> {
  const tableName = poiCoordsTableName(megaJobId, runId);
  const s3Prefix = poiCoordsTablePrefix(megaJobId, runId);
  const csvKey = `${s3Prefix}/pois.csv`;

  // Build CSV in memory (header + rows). For 25k POIs this is ~1 MB.
  const lines = ['poi_lat,poi_lng,poi_radius_m'];
  for (const c of poiCoords) {
    lines.push(`${c.lat},${c.lng},${c.radiusM}`);
  }
  const csv = lines.join('\n');

  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: csvKey,
    Body: csv,
    ContentType: 'text/csv',
  }));

  // Drop any leftover table from a previous run with same name (idempotent)
  try { await dropTempTable(tableName); } catch {}

  // Create external table over the CSV. DDL is fast (<5s).
  const ddl = `
    CREATE EXTERNAL TABLE ${tableName} (
      poi_lat DOUBLE,
      poi_lng DOUBLE,
      poi_radius_m DOUBLE
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 's3://${BUCKET}/${s3Prefix}/'
    TBLPROPERTIES ('skip.header.line.count'='1')
  `;

  await runQuery(ddl);
  console.log(`[POI-MATERIALIZE] Created external table ${tableName} with ${poiCoords.length} POIs at s3://${BUCKET}/${s3Prefix}/`);
  return tableName;
}

/**
 * Drop the POI temp table + delete the underlying CSV in S3.
 * Best-effort: failures (e.g., IAM denied for glue:DeleteTable) are logged but not thrown.
 */
export async function dropPoiCoordsTable(tableName: string, s3Prefix: string): Promise<void> {
  await dropTempTable(tableName);

  // Delete CSV objects under the prefix
  try {
    const fullPrefix = `${s3Prefix}/`;
    let continuationToken: string | undefined;
    do {
      const listRes = await s3Client.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: fullPrefix,
        ContinuationToken: continuationToken,
      }));
      const objects = listRes.Contents || [];
      if (objects.length > 0) {
        await s3Client.send(new DeleteObjectsCommand({
          Bucket: BUCKET,
          Delete: { Objects: objects.map(o => ({ Key: o.Key! })), Quiet: true },
        }));
      }
      continuationToken = listRes.IsTruncated ? listRes.NextContinuationToken : undefined;
    } while (continuationToken);
  } catch (e: any) {
    console.warn(`[POI-MATERIALIZE] Failed to delete S3 prefix ${s3Prefix}:`, e.message);
  }
}

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
 * Visitor-level filters that mirror the dataset page controls so megajob
 * consolidation can be scoped the same way: hour-of-day and minimum number
 * of distinct visit-days per ad_id.
 *
 * - hourFrom/hourTo (0..23): only count pings whose HOUR(utc_timestamp) falls
 *   in [hourFrom, hourTo]. When hourFrom > hourTo the window wraps midnight
 *   (e.g. 22..6 = 22:00–06:00). Both undefined / 0..23 = no filter.
 * - minVisits: only count ad_ids whose distinct visit-days at POI(s) meets
 *   this threshold. Applied via an extra `qualified_visitors` CTE so the
 *   downstream metrics naturally exclude one-off bouncers.
 */
export interface VisitorFilter {
  hourFrom?: number;
  hourTo?: number;
  minVisits?: number;
}

/**
 * Build SQL fragment for hour-of-day filter on `utc_timestamp`. Returns
 * a fragment starting with `AND ` (or empty string when no filter applies).
 */
export function buildHourFilterClause(filter?: VisitorFilter, tsCol: string = 'utc_timestamp'): string {
  if (!filter) return '';
  const from = filter.hourFrom ?? 0;
  const to = filter.hourTo ?? 23;
  if (from === 0 && to === 23) return '';
  if (from <= to) {
    return `AND HOUR(${tsCol}) >= ${from} AND HOUR(${tsCol}) <= ${to}`;
  }
  // Wrap-around window (e.g. 22..6)
  return `AND (HOUR(${tsCol}) >= ${from} OR HOUR(${tsCol}) <= ${to})`;
}

/**
 * Returns true when minVisits requires a HAVING / JOIN against a
 * qualified-visitors CTE.
 */
export function hasMinVisitsFilter(filter?: VisitorFilter): boolean {
  return !!(filter && typeof filter.minVisits === 'number' && filter.minVisits > 1);
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
 *
 * `visitorFilter` (optional): when present, appends a hour-of-day predicate
 * to every per-table SELECT so the filter is pushed down to scan time.
 */
function buildUnionAll(
  syncedJobs: Job[],
  columns: string,
  whereExtra = '',
  visitorFilter?: VisitorFilter,
): string {
  const hourClause = buildHourFilterClause(visitorFilter);
  return syncedJobs
    .map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT ${columns} FROM ${table} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '' ${whereExtra} ${hourClause}`;
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
 * Build the at-POI pings CTE using a grid-bucket spatial join.
 *
 * Strategy: bucket both pings and POIs into ~1.1 km grid cells (GRID_STEP=0.01°).
 * For each POI, expand to 9 surrounding cells (3x3) so radius search up to
 * ~1.1 km is covered. Then INNER JOIN on (lat_bucket, lng_bucket) — Athena uses
 * a hash join, which is O(N + M) instead of CROSS JOIN's O(N × M). Haversine
 * distance is only computed on the small set of bucket-matched candidates.
 *
 * For 25k POIs × 12 GB pings, this drops query runtime from ~30 min (timeout)
 * to a few minutes.
 *
 * If `poiTableRef` is provided, target_pois reads from that external table
 * (used when poiCoords > MAX_INLINE_POIS to avoid Athena's SQL size limit).
 * Otherwise falls back to inlining the coords as VALUES.
 */
function buildAtPoiPingsCTE(allPingsCte: string, poiCoords: PoiCoord[], poiTableRef?: string): string {
  const targetPoisSource = poiTableRef
    ? `(SELECT poi_lat, poi_lng, poi_radius_m FROM ${poiTableRef})`
    : buildTargetPoisValues(poiCoords);
  // Prefix internal CTEs with `_atp_` to avoid collisions with caller CTEs
  // (e.g., mobility query has its own `poi_buckets` CTE for the Overture POI join).
  // Only `at_poi_pings` is the public name callers reference.
  return `
    _atp_target_pois AS (
      SELECT
        poi_lat,
        poi_lng,
        poi_radius_m,
        CAST(FLOOR(poi_lat / ${GRID_STEP}) AS BIGINT) as base_lat_bucket,
        CAST(FLOOR(poi_lng / ${GRID_STEP}) AS BIGINT) as base_lng_bucket
      FROM ${targetPoisSource}
    ),
    _atp_poi_buckets AS (
      SELECT poi_lat, poi_lng, poi_radius_m,
        base_lat_bucket + dlat as lat_bucket,
        base_lng_bucket + dlng as lng_bucket
      FROM _atp_target_pois
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
    ),
    _atp_pings_bucketed AS (
      SELECT
        p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng,
        CAST(FLOOR(p.lat / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(p.lng / ${GRID_STEP}) AS BIGINT) as lng_bucket
      FROM ${allPingsCte} p
    ),
    at_poi_pings AS (
      SELECT DISTINCT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM _atp_pings_bucketed p
      INNER JOIN _atp_poi_buckets pb
        ON p.lat_bucket = pb.lat_bucket
        AND p.lng_bucket = pb.lng_bucket
      WHERE 111320 * SQRT(
          POW(p.lat - pb.poi_lat, 2) +
          POW((p.lng - pb.poi_lng) * COS(RADIANS((p.lat + pb.poi_lat) / 2)), 2)
        ) <= pb.poi_radius_m
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
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for OD query');

  // All pings with lat/lng (for origin/dest inference)
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs},
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

  console.log(`[MEGA-OD] Starting OD query (CTAS) across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length}, poiTableRef=${poiTableRef ?? 'inline'})`);
  return await startAsCTAS(megaJobId, runId, 'od', sql);
}

// ── Q2: POI Activity by Hour ────────────────────────────────────────────

/**
 * Build and start the consolidated POI activity by hour query.
 * When poiCoords provided, only counts pings actually near a POI (spatial proximity).
 * Returns Athena queryId for polling.
 */
export async function startConsolidatedHourlyQuery(
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for hourly query');

  let sql: string;

  if (poiCoords?.length) {
    // ── Spatial proximity approach (correct) ──
    const allPingsUnion = buildUnionAll(
      syncedJobs,
      'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
      `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
      visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs}
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

  console.log(`[MEGA-HOURLY] Starting POI hourly query (CTAS) across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length}, poiTableRef=${poiTableRef ?? 'inline'})`);
  return await startAsCTAS(megaJobId, runId, 'hourly', sql);
}

// ── Q2b: POI Activity by Day-of-Week × Hour ────────────────────────────

/**
 * Build and start the consolidated day-of-week × hour heatmap query.
 * Returns 7×24 cells (DAY_OF_WEEK 1=Mon..7=Sun + HOUR 0..23) with pings + devices.
 * Mirrors the hourly query shape but adds the DAY_OF_WEEK group key.
 */
export async function startConsolidatedDayHourQuery(
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for dayhour query');

  let sql: string;

  if (poiCoords?.length) {
    const allPingsUnion = buildUnionAll(
      syncedJobs,
      'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
      `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
      visitorFilter,
    );

    const dwellCTEs = buildDwellFilterCTEs(dwell);
    const useDwell = hasDwellFilter(dwell);
    const dhSource = useDwell
      ? `at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date`
      : `at_poi_pings`;
    const aPrefix = useDwell ? 'a.' : '';

    sql = `
    WITH
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng
      FROM (
        ${allPingsUnion}
      )
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs}
    SELECT
      DAY_OF_WEEK(${aPrefix}utc_timestamp) as dow,
      HOUR(${aPrefix}utc_timestamp) as hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ${aPrefix}ad_id) as devices
    FROM ${dhSource}
    GROUP BY DAY_OF_WEEK(${aPrefix}utc_timestamp), HOUR(${aPrefix}utc_timestamp)
    ORDER BY dow, hour
    `;
  } else {
    // Fallback: poi_ids approach
    const poiFilter = buildPoiFilter(poiIds);
    const unionParts = syncedJobs
      .map((job) => {
        const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
        return `SELECT ad_id, utc_timestamp FROM ${table} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != '' ${poiFilter}`;
      })
      .join('\n      UNION ALL\n      ');

    sql = `
    SELECT
      DAY_OF_WEEK(utc_timestamp) as dow,
      HOUR(utc_timestamp) as hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM (
      ${unionParts}
    )
    GROUP BY DAY_OF_WEEK(utc_timestamp), HOUR(utc_timestamp)
    ORDER BY dow, hour
    `;
  }

  console.log(`[MEGA-DAYHOUR] Starting day-hour heatmap query (CTAS) across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length}, poiTableRef=${poiTableRef ?? 'inline'})`);
  return await startAsCTAS(megaJobId, runId, 'dayhour', sql);
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
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for catchment query');

  // All pings with accuracy filter
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings_raw', poiCoords, poiTableRef)}${dwellCTEs},
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

  // Pick exactly ONE home location per device — the rounded origin coord
  // with the most distinct visit-days (ties broken by the lower-bounded coord
  // for determinism). This guarantees every POI visitor is counted exactly
  // once, so the catchment totals match the visitor population from visits/
  // temporal/OD reports. The previous implementation HAD `HAVING COUNT(DISTINCT
  // date) >= 3`, which silently dropped any visitor whose top home didn't
  // accumulate 3+ days — for the GDL megajob that filtered out 87% of
  // visitors and made catchment look like a different population.
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
    ),
    device_homes_agg AS (
      SELECT ad_id,
        ROUND(origin_lat, ${COORDINATE_PRECISION}) as home_lat,
        ROUND(origin_lng, ${COORDINATE_PRECISION}) as home_lng,
        COUNT(DISTINCT date) as days_at_loc
      FROM first_pings
      GROUP BY ad_id,
        ROUND(origin_lat, ${COORDINATE_PRECISION}),
        ROUND(origin_lng, ${COORDINATE_PRECISION})
    ),
    device_homes AS (
      SELECT ad_id, home_lat, home_lng, days_at_loc
      FROM (
        SELECT ad_id, home_lat, home_lng, days_at_loc,
          ROW_NUMBER() OVER (
            PARTITION BY ad_id
            ORDER BY days_at_loc DESC, home_lat, home_lng
          ) as rn
        FROM device_homes_agg
      )
      WHERE rn = 1
    )
    SELECT
      home_lat as origin_lat,
      home_lng as origin_lng,
      0 as departure_hour,
      COUNT(*) as device_days
    FROM device_homes
    GROUP BY home_lat, home_lng
    ORDER BY device_days DESC
    LIMIT 100000
  `;

  console.log(`[MEGA-CATCHMENT] Starting catchment query (CTAS) across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length}, poiTableRef=${poiTableRef ?? 'inline'})`);
  return await startAsCTAS(megaJobId, runId, 'catchment', sql);
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
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for mobility query');

  // All pings (for spatial join with Overture POIs)
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings_raw', poiCoords, poiTableRef)}${dwellCTEs},
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
    -- Pre-aggregate pings per (ad_id, date, timing, lat_bucket, lng_bucket)
    -- before the spatial join. For dense data (every-minute pings during
    -- ±2h around a visit, ~240 pings per device-day), this collapses to
    -- maybe 5-20 unique buckets per device-day — drops the JOIN input by
    -- ~2 orders of magnitude. AVG(lat/lng) keeps the distance filter
    -- accurate enough to determine which POI bucket the device was in
    -- (sub-bucket precision doesn't matter for category-level mobility).
    nearby_pings AS (
      SELECT
        a.ad_id,
        a.date,
        CASE WHEN a.utc_timestamp < t.visit_time THEN 'before' ELSE 'after' END as timing,
        CAST(FLOOR(a.lat / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(a.lng / ${GRID_STEP}) AS BIGINT) as lng_bucket,
        AVG(a.lat) as lat,
        AVG(a.lng) as lng
      FROM all_pings a
      INNER JOIN target_visits t ON a.ad_id = t.ad_id AND a.date = t.date
      WHERE ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) <= 120
        AND ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) > 0
      GROUP BY
        a.ad_id, a.date,
        CASE WHEN a.utc_timestamp < t.visit_time THEN 'before' ELSE 'after' END,
        CAST(FLOOR(a.lat / ${GRID_STEP}) AS BIGINT),
        CAST(FLOOR(a.lng / ${GRID_STEP}) AS BIGINT)
    ),
    -- Restrict the global Overture POI catalog (lab_pois_gmc has POIs from many
    -- countries) to a bbox around the target POIs + 0.5° buffer (~55 km, enough
    -- for ±120 min of travel). For a country-scale POI grid this naturally
    -- limits to that country, dropping the candidate set from millions to
    -- thousands.
    ${poiCoords?.length ? `
    mob_poi_bounds AS (
      SELECT
        MIN(poi_lat) - 0.5 as min_lat,
        MAX(poi_lat) + 0.5 as max_lat,
        MIN(poi_lng) - 0.5 as min_lng,
        MAX(poi_lng) + 0.5 as max_lng
      FROM _atp_target_pois
    ),
    mob_poi_filtered AS (
      SELECT p.id, p.name, p.category, p.latitude, p.longitude
      FROM ${POI_TABLE} p
      CROSS JOIN mob_poi_bounds b
      WHERE p.category IS NOT NULL
        AND p.latitude  BETWEEN b.min_lat AND b.max_lat
        AND p.longitude BETWEEN b.min_lng AND b.max_lng
    ),` : `
    mob_poi_filtered AS (
      SELECT id, name, category, latitude, longitude
      FROM ${POI_TABLE}
      WHERE category IS NOT NULL
    ),`}
    poi_buckets AS (
      SELECT
        id as poi_id,
        name as poi_name,
        category,
        latitude as poi_lat,
        longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID_STEP}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(longitude / ${GRID_STEP}) AS BIGINT) + dlng as lng_bucket
      FROM mob_poi_filtered
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
    ),
    -- Match nearby pings to POIs via grid bucket join + inline distance filter.
    -- DISTINCT collapses repeated matches per (ad_id, date, timing, category)
    -- before the final GROUP BY — this avoids the expensive ROW_NUMBER window
    -- function over millions of rows that the previous version used to pick
    -- the "closest POI" per match (the closest-POI info was never surfaced
    -- in the UI, so picking it was wasted work).
    matched AS (
      SELECT DISTINCT
        p.ad_id,
        p.date,
        p.timing,
        b.category
      FROM nearby_pings p
      INNER JOIN poi_buckets b
        ON p.lat_bucket = b.lat_bucket
        AND p.lng_bucket = b.lng_bucket
      WHERE 111320 * SQRT(
          POW(p.lat - b.poi_lat, 2) +
          POW((p.lng - b.poi_lng) * COS(RADIANS((p.lat + b.poi_lat) / 2)), 2)
        ) <= 200
    )
    SELECT
      timing,
      category,
      COUNT(DISTINCT CONCAT(ad_id, '-', date)) as device_days,
      COUNT(*) as hits  -- one hit per (device, day, timing, category); UI ignores this field
    FROM matched
    GROUP BY timing, category
    ORDER BY timing, device_days DESC
  `;

  console.log(`[MEGA-MOBILITY] Starting mobility trends query (CTAS) across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length}, poiTableRef=${poiTableRef ?? 'inline'})`);
  return await startAsCTAS(megaJobId, runId, 'mobility', sql);
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
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
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
      visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs}
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

  console.log(`[MEGA-TEMPORAL] Starting temporal query (CTAS) across ${syncedJobs.length} tables (dwell=${useDwell}, poiTableRef=${poiTableRef ?? 'none'})`);
  return await startAsCTAS(megaJobId, runId, 'temporal', sql);
}

// ── Q5b: Total unique devices (global COUNT DISTINCT) ─────────────────

/**
 * Single-row query: total unique devices that visited any POI across all sub-jobs.
 * Needed because summing daily COUNT(DISTINCT) gives device-days, not unique devices.
 */
export async function startConsolidatedTotalDevicesQuery(
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
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
      visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs}
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

  console.log(`[MEGA-TOTAL-DEVICES] Starting total unique devices query (CTAS) across ${syncedJobs.length} tables (dwell=${useDwell}, poiTableRef=${poiTableRef ?? 'none'})`);
  return await startAsCTAS(megaJobId, runId, 'total_devices', sql);
}

// ── Q6: MAIDs (unique device IDs) ──────────────────────────────────────

/**
 * Build and start a query for all unique MAIDs (ad_id) that visited POIs.
 * Returns Athena queryId. The output CSV is used directly for export.
 *
 * Note: Uses poi_ids which is ✅ correct for identifying visitors.
 */
export async function startConsolidatedMAIDsQuery(
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
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
      visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs}
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

  console.log(`[MEGA-MAIDS] Starting MAIDs query (CTAS) across ${syncedJobs.length} tables (dwell=${useDwell}, poiTableRef=${poiTableRef ?? 'none'})`);
  return await startAsCTAS(megaJobId, runId, 'maids', sql);
}

// ── Q7: NSE (ad_id + origin coords for socioeconomic segmentation) ──────

/**
 * Build and start a query that returns each MAID with its origin coordinates.
 * Used for NSE (socioeconomic) segmentation: geocode origins → postal codes → match to NSE brackets.
 * Origin = first ping of day per device (MIN_BY latitude/longitude by timestamp).
 * Coordinates rounded to 1 decimal (~11km) at SQL level to reduce geocoding workload.
 */
export async function startConsolidatedNseQuery(
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiIds?: string[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for NSE query');

  // All pings with accuracy filter
  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings_raw', poiCoords, poiTableRef)}${dwellCTEs},
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

  // One row per device. ARRAY_AGG'ing ad_ids per coord exceeds Athena's
  // 32 MB per-cell limit for dense urban coords (e.g. downtown San Salvador
  // can have 100k+ devices in one ~11km bucket). The geocoding phase aggregates
  // by coord in-memory after streaming the CSV, which is cheap.
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

  console.log(`[MEGA-NSE] Starting NSE query (CTAS) across ${syncedJobs.length} tables (spatial=${!!poiCoords?.length}, poiTableRef=${poiTableRef ?? 'inline'})`);
  return await startAsCTAS(megaJobId, runId, 'nse', sql);
}

// ── Q8: Affinity data (per-origin visit metrics) ────────────────────────

/**
 * Build and start the affinity data query.
 * Computes per-origin: total visits, unique devices, avg dwell, avg frequency.
 * Results are geocoded server-side to postal codes, then scored 0-100.
 * Requires spatial path (poiCoords) since dwell time computation needs at_poi_pings.
 */
export async function startConsolidatedAffinityQuery(
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiCoords: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs for affinity query');

  const allPingsUnion = buildUnionAll(
    syncedJobs,
    'ad_id, date, utc_timestamp, TRY_CAST(latitude AS DOUBLE) as lat, TRY_CAST(longitude AS DOUBLE) as lng, horizontal_accuracy',
    `AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})`,
    visitorFilter,
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
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs},
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

  console.log(`[MEGA-AFFINITY] Starting affinity query (CTAS) across ${syncedJobs.length} tables (dwell=${useDwell}, poiTableRef=${poiTableRef ?? 'inline'})`);
  return await startAsCTAS(megaJobId, runId, 'affinity', sql);
}
