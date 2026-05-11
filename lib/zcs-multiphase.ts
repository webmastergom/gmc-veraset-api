/**
 * Zip Code Signals — multi-phase state machine.
 *
 * Replaces the single-invoke SSE pipeline in
 * /api/zip-code-signals/analyze/stream with a poll-based state machine
 * that fits each phase in well under 60s, making the pipeline
 * plan-agnostic (works on Vercel Hobby's 300s cap, Pro's 800s, anywhere).
 *
 * Phase flow (FULL schema):
 *   starting → prepare_table → launch_queries → polling_queries
 *     → aggregate_full → done
 *
 * Phase flow (BASIC schema):
 *   starting → prepare_table → launch_queries → polling_queries
 *     → pass1_basic → geocoding → pass2_basic → done
 *
 * Each phase persists its outputs to S3 (state file + optionally a side
 * file for big intermediate data like the coords map). The endpoint loops
 * advances per call until the wall budget runs out or polling has to
 * wait, then returns to let the frontend poll again.
 */

import { CopyObjectCommand, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import {
  startQueryAsync,
  checkQueryStatus,
  runQuery,
  fetchQueryResultsViaS3,
  streamQueryResultsViaS3,
  ensureTableForDataset,
  getTableName,
} from './athena';
import { s3Client, BUCKET, getConfig, putConfig } from './s3-config';
import { batchReverseGeocode, setCountryFilter } from './reverse-geocode';
import { localTimestamp, tzForCountry } from './timezones';
import type {
  PostalMaidFilters,
  PostalMaidResult,
  PostalMaidDevice,
  ZipSignature,
  RegionSummary,
} from './postal-maid-types';

// ── Constants ───────────────────────────────────────────────────────
const ACCURACY_THRESHOLD_METERS = 500;
// 1-decimal degree ≈ 11km cells. Matches the geocoder's own rounding
// (batchReverseGeocode does Math.round(lat * 10)/10 internally), so the
// final postal-code assignment precision is 1-decimal regardless.
// Outputting 4-decimal coords in the SQL was wasted precision that
// blew up the intermediate coord maps — for France 50k (24M MAIDs,
// dense urban), 4-decimal produced ~3M unique cells (~450 MB in
// memory), which got close to the Vercel 1 GB cap during pass1. At
// 1-decimal we get ~5-10K unique cells nationwide — sub-megabyte.
// Final accuracy is identical because the geocoder operated at 1dec
// in both cases.
const COORDINATE_PRECISION = 1;

// ── State machine ─────────────────────────────────────────────────────

export type ZcsPhase =
  | 'starting'
  | 'prepare_table'
  | 'launch_queries'
  | 'polling_queries'
  | 'aggregate_full'   // FULL: stream devices+h3 → build signatures
  | 'aggregate_basic'  // BASIC (new): stream pre-matched (ad_id, zip, days) → done
  // Legacy 3-phase BASIC path (pass1+geocode+pass2). Kept for runs that
  // started before the JOIN-in-Athena rewrite landed; we auto-promote
  // their state to aggregate_basic on next poll.
  | 'pass1_basic'
  | 'geocoding'
  | 'pass2_basic'
  | 'done'
  | 'error';

export interface ZcsRunConfig {
  /** Single-dataset mode. */
  datasetName?: string;
  /** Megajob mode (preferred for big multi-dataset runs). */
  megaJobId?: string;
  country: string;
  postalCodes: string[];
  dateFrom?: string;
  dateTo?: string;
}

export interface ZcsState {
  phase: ZcsPhase;
  runId: string;
  config: ZcsRunConfig;
  /** Display label — megajob name or dataset folder name. */
  sourceLabel: string;
  /** Mode flag — controls FULL vs BASIC branches. */
  isFull?: boolean;
  /** Athena external table name for the country's geocode cache
   *  (BASIC path only — used to JOIN postal codes into the origins
   *  query so Node never sees unmatched rows). Set by ensureGeocodeTable.
   *  When null, the BASIC path can't push the match to Athena and would
   *  have to fall back to the legacy 3-phase pass1/geocode/pass2 flow. */
  geocodeTable?: string | null;
  /** Human-readable schema-decision source (for activity log). */
  schemaSource?: string;
  /** Effective date range after fallback to catalog/sub-jobs. */
  effectiveDateFrom?: string;
  effectiveDateTo?: string;
  /** SQL fragment "AND date >= '…' AND date <= '…'" — empty if no dates. */
  dateWhere?: string;
  /** Total POI visitors in the dataset/megajob — denominator for coverage %. */
  totalDevicesInDataset?: number;
  /** Athena table name for the megajob's consolidated MAIDs CSV
   *  (megajob mode only). For single-dataset mode this stays undefined
   *  and the analyzer scans poi_ids directly. */
  maidsTableName?: string;
  /** UNION-ALL FROM expression over sub-job tables (megajob mode). */
  tableExpr?: string;
  /** Athena table for single-dataset mode. */
  singleTable?: string;
  /** Normalized requested postal codes (after normalizePostalForCountry). */
  normalizedPostals?: string[];

  // ── Query phase outputs ─────────────────────────────────────────
  /** FULL path: devices + h3 query IDs. BASIC path: origins query ID. */
  queryIds?: Record<string, string>;
  /** Wall-clock when queries were launched (for elapsed display). */
  queriesStartedAt?: number;

  // ── Intermediate file pointers ──────────────────────────────────
  /** BASIC pass 1: S3 key for the JSON-serialized coord map. */
  coordsKey?: string;
  /** BASIC pass 1 result: stats only (not the map). */
  pass1Stats?: { totalRows: number; uniqueCoords: number; totalDeviceDays: number };
  /** BASIC geocoding: S3 key for the JSON-serialized coord→postal map. */
  geocodeKey?: string;

  // ── Final output ─────────────────────────────────────────────────
  /** S3 key for the final PostalMaidResult JSON. */
  resultKey?: string;

  // ── UX ──────────────────────────────────────────────────────────
  subProgress?: { label: string; percent: number; details?: string };
  error?: string;
  updatedAt: string;
}

// ── S3 keys ─────────────────────────────────────────────────────────

export function stateKey(runId: string): string {
  return `zcs-state/${runId}`;
}
function coordsS3Key(runId: string): string {
  return `zcs-coords/${runId}.json`;
}
function geocodeS3Key(runId: string): string {
  return `zcs-geocode/${runId}.json`;
}
function resultS3Key(runId: string): string {
  // CRITICAL: this key is consumed by /api/zip-code-signals/spill, which
  // validates `key.startsWith('postal-maid-spill/')` and reads via
  // getConfig() (which prepends `config/` and appends `.json`). So:
  //   - The key MUST start with 'postal-maid-spill/' or the endpoint
  //     rejects it with 400 "Invalid key".
  //   - The result MUST be written with putConfig() (not raw PutObject)
  //     so the path matches what getConfig() expects.
  // See phaseAggregateFull / phasePass2Basic where putConfig is used.
  return `postal-maid-spill/zcs-${runId}`;
}

// ── Helpers ─────────────────────────────────────────────────────────

function normalizePostalForCountry(country: string, raw: string): string {
  const cc = country.toUpperCase();
  let s = raw.trim().toUpperCase().replace(/\s+/g, '');
  if (/^[A-Z]{2}-/.test(s)) s = s.slice(3);
  if (cc === 'MX') {
    const digits = s.replace(/\D/g, '');
    if (digits.length >= 1 && digits.length <= 5) return digits.padStart(5, '0');
  }
  return s;
}

function buildDateWhere(from?: string, to?: string): string {
  const conds: string[] = [];
  if (from) conds.push(`date >= '${from}'`);
  if (to) conds.push(`date <= '${to}'`);
  return conds.length ? `AND ${conds.join(' AND ')}` : '';
}

async function writeJson(key: string, data: any): Promise<void> {
  await s3Client.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: JSON.stringify(data),
      ContentType: 'application/json',
    }),
  );
}

async function readJson<T>(key: string): Promise<T | null> {
  try {
    const r = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
    const txt = await r.Body?.transformToString();
    return txt ? (JSON.parse(txt) as T) : null;
  } catch (e: any) {
    // Differentiate "file doesn't exist yet" from "S3 broke" — only the first
    // is a normal flow (caller asked too early). Other errors should surface
    // so the caller can decide whether to retry or abort.
    if (e?.name === 'NoSuchKey' || e?.$metadata?.httpStatusCode === 404) {
      return null;
    }
    console.error(`[ZCS] readJson(${key}) error: ${e?.message || e}`);
    throw e;
  }
}

/**
 * Ensure the country's geocode cache CSV is available to Athena as an
 * external table. Pushes the reverse-geocode step DOWN into Athena: the
 * origins query joins on this table and filters by target zipcodes
 * upfront, so we never have to stream millions of unmatched rows to
 * Node (which previously took 20+ minutes for France 50k).
 *
 * The cache CSV at config/geocode-cache/{CC}.csv has rows:
 *   lat_key,lng_key,zipcode
 * where lat_key = Math.round(lat * 10) and lng_key = Math.round(lng * 10).
 * 1-decimal degree (~11 km) precision — matches the resolution of
 * batchReverseGeocode's fast path.
 *
 * The Athena table is created at a stable per-country location so
 * concurrent runs share it. Idempotent: re-creating is a no-op.
 */
async function ensureGeocodeTable(country: string): Promise<string | null> {
  const cc = country.toUpperCase();
  const srcKey = `config/geocode-cache/${cc}.csv`;

  // Verify the source CSV exists in S3.
  try {
    await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: srcKey }));
  } catch (e: any) {
    if (e?.name === 'NoSuchKey' || e?.$metadata?.httpStatusCode === 404) {
      console.warn(`[ZCS] No geocode cache at ${srcKey} for ${cc} — BASIC path will fall back to Node-side geocoding`);
      return null;
    }
    throw e;
  }

  // Copy to a fresh Athena-friendly prefix (must be a folder containing
  // just the CSV — Athena's external-table LOCATION can't point at a
  // single file). Use a stable per-country path so the table is shared
  // across runs of the same country.
  const athenaPrefix = `athena-temp/zcs-geocode/${cc.toLowerCase()}/`;
  const athenaKey = `${athenaPrefix}data.csv`;
  try {
    await s3Client.send(
      new CopyObjectCommand({
        Bucket: BUCKET,
        Key: athenaKey,
        CopySource: `${BUCKET}/${srcKey}`,
      }),
    );
  } catch (e: any) {
    // If the destination already exists, S3 lets us overwrite. If
    // CopySource is missing or auth fails, surface.
    if (!/AccessDenied/i.test(e?.message || '')) {
      // Only re-throw on real errors. Athena reads will tell us if the
      // path is bad anyway.
      console.warn(`[ZCS] geocode CSV copy warn: ${e?.message}`);
    }
  }

  // Idempotent DDL — IF NOT EXISTS so concurrent runs don't fight.
  const tableName = `zcs_geocode_${cc.toLowerCase()}`;
  await runQuery(`
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} (
      lat_key INT, lng_key INT, zipcode STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    STORED AS TEXTFILE
    LOCATION 's3://${BUCKET}/${athenaPrefix}'
  `);
  return tableName;
}

// ── Phase: starting ─────────────────────────────────────────────────

export async function phaseStarting(state: ZcsState): Promise<ZcsState> {
  const { config } = state;
  if (!config.datasetName && !config.megaJobId) {
    throw new Error('Either datasetName or megaJobId required');
  }
  if (config.datasetName && config.megaJobId) {
    throw new Error('Provide either datasetName or megaJobId, not both');
  }
  if (!config.country || config.country.length !== 2) {
    throw new Error('country must be a 2-letter ISO code');
  }
  if (!config.postalCodes?.length) {
    throw new Error('At least one postal code required');
  }
  const normalizedPostals = config.postalCodes
    .map((p) => normalizePostalForCountry(config.country, p))
    .filter((p) => p.length > 0); // drop empties after normalization
  if (normalizedPostals.length === 0) {
    throw new Error(
      'All postal codes were empty after normalization. Check the input format.',
    );
  }
  return {
    ...state,
    phase: 'prepare_table',
    normalizedPostals,
    subProgress: {
      label: 'Validated inputs',
      percent: 3,
      details: `${normalizedPostals.length} postal codes · country=${config.country}`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase: prepare_table ────────────────────────────────────────────
//
// For megajob mode: load megajob, materialize MAIDs CSV as Athena
// external table, ensure sub-job tables, decide FULL vs BASIC schema,
// derive date range from sourceScope or sub-jobs.
//
// For single-dataset mode: ensure dataset table, sniff FULL schema,
// derive date range from job.dateRange.

export async function phasePrepareTable(state: ZcsState): Promise<ZcsState> {
  const { config, runId } = state;

  if (config.megaJobId) {
    return await prepareMegaJob(state);
  }
  // Single-dataset mode
  return await prepareSingleDataset(state);
}

async function prepareMegaJob(state: ZcsState): Promise<ZcsState> {
  const { config, runId } = state;
  const { getMegaJob } = await import('./mega-jobs');
  const { getJob } = await import('./jobs');

  const megaJob = await getMegaJob(config.megaJobId!);
  if (!megaJob) throw new Error(`Megajob ${config.megaJobId} not found`);
  const maidsCsvKey = megaJob.consolidatedReports?.maids;
  if (!maidsCsvKey) {
    throw new Error(
      'This megajob has not been consolidated yet. Run consolidation first ' +
        '(it produces the MAIDs CSV that ZCS reuses).',
    );
  }
  if (!megaJob.subJobIds?.length) throw new Error(`Megajob has no sub-jobs`);
  const subJobs = (await Promise.all(megaJob.subJobIds.map((j) => getJob(j))))
    .filter((j): j is NonNullable<typeof j> => j !== null && j.status === 'SUCCESS' && !!j.syncedAt);
  if (subJobs.length === 0) throw new Error('Megajob has no synced sub-jobs');

  const datasetNames = subJobs
    .map((j) => j.s3DestPath?.replace(/\/$/, '').split('/').pop()!)
    .filter(Boolean);
  // Parallel ensureTableForDataset — saves 10-15s of preamble.
  await Promise.all(datasetNames.map((ds) => ensureTableForDataset(ds)));
  const tableNames = datasetNames.map((ds) => getTableName(ds));

  // Schema decision (declared → sourceScope hint → sniff)
  const declaredSchemas = subJobs.map((j: any) => j?.schema || '').filter(Boolean);
  const declaredFull = declaredSchemas.length > 0 && declaredSchemas.every((s) => s === 'FULL' || s === 'ENHANCED');
  const declaredBasic = declaredSchemas.length > 0 && declaredSchemas.every((s) => s === 'BASIC');
  const megaJobScopeSchema = megaJob.sourceScope?.schema;
  const megaJobSaysFull = megaJobScopeSchema === 'FULL' || megaJobScopeSchema === 'ENHANCED';
  let isFull = declaredFull || (declaredSchemas.length === 0 && megaJobSaysFull);
  if (!isFull && !declaredBasic && !megaJobSaysFull) {
    // Sniff sub-job tables in parallel — sequential was eating 50s+ for
    // megajobs with many sub-jobs. We resolve as soon as ANY sniff returns
    // a non-null zipcode (Promise.race-ish via a rejection trick); if all
    // resolve null, isFull stays false → BASIC path.
    const sniffPromises = tableNames.map(async (t) => {
      const sql = `SELECT geo_fields['zipcode'] AS z FROM ${t} WHERE geo_fields['zipcode'] IS NOT NULL AND geo_fields['zipcode'] != '' LIMIT 1`;
      try {
        const r = await runQuery(sql);
        return r.rows.length > 0 && !!r.rows[0]?.z;
      } catch {
        return false;
      }
    });
    const results = await Promise.all(sniffPromises);
    if (results.some((r) => r === true)) isFull = true;
  }
  const schemaSource = declaredFull
    ? 'FULL (sub-jobs declared)'
    : declaredBasic
      ? 'BASIC (sub-jobs declared)'
      : megaJobSaysFull && isFull
        ? `FULL (megajob.sourceScope.schema=${megaJobScopeSchema})`
        : isFull
          ? 'FULL (sniffed)'
          : 'BASIC (sniffed/fallback)';

  // Date range fallback (user → sourceScope → sub-jobs)
  let catalogFrom = megaJob.sourceScope?.dateRange?.from;
  let catalogTo = megaJob.sourceScope?.dateRange?.to;
  if (!catalogFrom || !catalogTo) {
    for (const sj of subJobs as any[]) {
      const r = sj?.dateRange;
      if (r?.from && (!catalogFrom || r.from < catalogFrom)) catalogFrom = r.from;
      if (r?.to && (!catalogTo || r.to > catalogTo)) catalogTo = r.to;
    }
  }
  const effectiveFrom = config.dateFrom || catalogFrom;
  const effectiveTo = config.dateTo || catalogTo;
  const dateWhere = buildDateWhere(effectiveFrom, effectiveTo);

  // Materialize MAIDs CSV as Athena external table
  const ts = Date.now();
  const safeId = config.megaJobId!.replace(/[^a-z0-9_]/gi, '_').slice(0, 32);
  const maidsTableName = `zcs_maids_${safeId}_${ts}`;
  const maidsPrefix = `athena-temp/zcs-maids/${safeId}/${ts}/`;
  const maidsObjectKey = `${maidsPrefix}data.csv`;
  await s3Client.send(
    new CopyObjectCommand({
      Bucket: BUCKET,
      Key: maidsObjectKey,
      CopySource: `${BUCKET}/${maidsCsvKey}`,
    }),
  );
  await runQuery(`
    CREATE EXTERNAL TABLE IF NOT EXISTS ${maidsTableName} (ad_id STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    STORED AS TEXTFILE
    LOCATION 's3://${BUCKET}/${maidsPrefix}'
    TBLPROPERTIES ('skip.header.line.count' = '1')
  `);

  // Count for coverage stat (small, fast — 1-3s)
  const totalRes = await runQuery(`SELECT COUNT(*) as n FROM ${maidsTableName}`);
  const totalDevicesInDataset = parseInt(String(totalRes.rows[0]?.n)) || 0;
  if (totalDevicesInDataset === 0) {
    // Early-exit: empty megajob → write empty result and finish
    const result = buildEmptyResult(state.sourceLabel, config, 0);
    // Use putConfig (not writeJson) so the file lands at the path the
  // spill endpoint reads via getConfig — see comment on resultS3Key.
  await putConfig(resultS3Key(runId), result, { compact: true });
    return {
      ...state,
      phase: 'done',
      resultKey: resultS3Key(runId),
      subProgress: { label: 'Megajob has no MAIDs', percent: 100 },
      updatedAt: new Date().toISOString(),
    };
  }

  // Build the UNION ALL FROM expression
  const fullColumns = 'ad_id, date, utc_timestamp, geo_fields, quality_fields, latitude, longitude, horizontal_accuracy';
  const basicColumns = 'ad_id, date, utc_timestamp, latitude, longitude, horizontal_accuracy';
  const tableExpr = `(\n      ${tableNames
    .map((t) => `SELECT ${isFull ? fullColumns : basicColumns} FROM ${t}`)
    .join('\n      UNION ALL\n      ')}\n    )`;

  // For BASIC schema, ensure the country's geocode-cache table is set up
  // in Athena so the origins query can JOIN + filter zipcodes server-side.
  // Skip for FULL (already has geo_fields[zipcode] in the parquets).
  const geocodeTable = isFull ? null : await ensureGeocodeTable(state.config.country);

  return {
    ...state,
    phase: 'launch_queries',
    // Upgrade the opaque "megajob:<id>" label set by the POST handler to
    // the human-readable megajob name. Frontend / result.dataset surfaces
    // this everywhere — without the upgrade the user sees a UUID instead
    // of "France Grid 50k April 2026".
    sourceLabel: megaJob.name || state.sourceLabel,
    isFull,
    schemaSource,
    effectiveDateFrom: effectiveFrom,
    effectiveDateTo: effectiveTo,
    dateWhere,
    totalDevicesInDataset,
    maidsTableName,
    tableExpr,
    geocodeTable,
    subProgress: {
      label: `Schema: ${schemaSource} · ${isFull ? 'FAST PATH' : 'BASIC path'}`,
      percent: 12,
      details: `Date range: ${effectiveFrom || '∞'} → ${effectiveTo || '∞'} · ${totalDevicesInDataset.toLocaleString()} MAIDs in dataset`,
    },
    updatedAt: new Date().toISOString(),
  };
}

async function prepareSingleDataset(state: ZcsState): Promise<ZcsState> {
  const { config, runId } = state;
  const tableName = getTableName(config.datasetName!);
  await ensureTableForDataset(config.datasetName!);

  // Date range from job catalog (single-dataset)
  let effectiveFrom = config.dateFrom;
  let effectiveTo = config.dateTo;
  if (!effectiveFrom || !effectiveTo) {
    try {
      const { getAllJobsSummary } = await import('./jobs');
      const jobs = await getAllJobsSummary().catch(() => []);
      const matching = jobs.find((j: any) => {
        const folderId = j.s3DestPath?.replace('s3://', '').replace(/.*\//, '').replace(/\/$/, '') || '';
        return folderId === config.datasetName;
      });
      const r = (matching as any)?.dateRange;
      if (r?.from && !effectiveFrom) effectiveFrom = r.from;
      if (r?.to && !effectiveTo) effectiveTo = r.to;
    } catch {
      /* ignore */
    }
  }
  const dateWhere = buildDateWhere(effectiveFrom, effectiveTo);

  // Run sniff + total-count in parallel — they don't depend on each other
  // and each is a single Athena call (~2-5s). Sequential was a free 5s of
  // dead time in the preamble.
  const [sniffRes, totalRes] = await Promise.all([
    runQuery(`
      SELECT geo_fields['zipcode'] AS z FROM ${tableName}
      WHERE geo_fields['zipcode'] IS NOT NULL AND geo_fields['zipcode'] != ''
      LIMIT 1
    `).catch(() => ({ rows: [] as Record<string, string>[] })),
    runQuery(`
      SELECT COUNT(DISTINCT ad_id) as total_devices
      FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${dateWhere}
    `),
  ]);
  const isFull = sniffRes.rows.length > 0 && !!sniffRes.rows[0]?.z;
  const schemaSource = isFull ? 'FULL (sniffed)' : 'BASIC (sniffed/fallback)';
  const totalDevicesInDataset = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  if (totalDevicesInDataset === 0) {
    const result = buildEmptyResult(state.sourceLabel, config, 0);
    // Use putConfig (not writeJson) so the file lands at the path the
  // spill endpoint reads via getConfig — see comment on resultS3Key.
  await putConfig(resultS3Key(runId), result, { compact: true });
    return {
      ...state,
      phase: 'done',
      resultKey: resultS3Key(runId),
      subProgress: { label: 'Dataset has no POI visitors', percent: 100 },
      updatedAt: new Date().toISOString(),
    };
  }

  // BASIC: prep geocode-cache table for the Athena-side zip JOIN.
  const geocodeTable = isFull ? null : await ensureGeocodeTable(state.config.country);

  return {
    ...state,
    phase: 'launch_queries',
    isFull,
    schemaSource,
    effectiveDateFrom: effectiveFrom,
    effectiveDateTo: effectiveTo,
    dateWhere,
    totalDevicesInDataset,
    singleTable: tableName,
    geocodeTable,
    subProgress: {
      label: `Schema: ${schemaSource} · ${isFull ? 'FAST PATH' : 'BASIC path'}`,
      percent: 12,
      details: `Date range: ${effectiveFrom || '∞'} → ${effectiveTo || '∞'} · ${totalDevicesInDataset.toLocaleString()} POI visitors`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase: launch_queries ───────────────────────────────────────────

export async function phaseLaunchQueries(state: ZcsState): Promise<ZcsState> {
  const { isFull, normalizedPostals, dateWhere = '', maidsTableName, tableExpr, singleTable } = state;
  const localTz = tzForCountry(state.config.country);
  const lts = (col: string) => localTimestamp(col, localTz);
  const requestedSql = (normalizedPostals || []).map((c) => `'${c.replace(/'/g, "''")}'`).join(', ');

  // poi_visitors CTE varies by mode:
  //   - megajob: SELECT ad_id FROM <maids table>
  //   - single-dataset: SELECT DISTINCT ad_id FROM ... CROSS JOIN UNNEST(poi_ids)
  const poiVisitorsCte = maidsTableName
    ? `SELECT ad_id FROM ${maidsTableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''`
    : `SELECT DISTINCT ad_id
       FROM ${singleTable}
       CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
       WHERE poi_id IS NOT NULL AND poi_id != ''
         AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
         ${dateWhere}`;
  const fromExpr = tableExpr || singleTable;

  const queryIds: Record<string, string> = {};

  if (isFull) {
    // FULL: one query per (zip metadata + device-level) + h3 hotspots
    const devicesQuery = `
      WITH
      poi_visitors AS (${poiVisitorsCte}),
      valid_pings AS (
        SELECT
          t.ad_id, t.date, t.utc_timestamp,
          TRY(t.geo_fields['zipcode']) as zip,
          TRY(t.geo_fields['region']) as region,
          TRY(t.geo_fields['city']) as city,
          TRY(t.geo_fields['h3_res10']) as h3,
          TRY_CAST(t.latitude AS DOUBLE) as lat,
          TRY_CAST(t.longitude AS DOUBLE) as lng,
          IF(TRY(t.quality_fields['ping_origin_type']) = 'gps', 1.0, 0.0) as is_gps,
          TRY_CAST(t.quality_fields['ping_circle_score'] AS DOUBLE) as circle_score
        FROM ${fromExpr} t
        INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
        WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
          AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
          AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
          AND TRY(t.geo_fields['zipcode']) IS NOT NULL
          AND TRY(t.geo_fields['zipcode']) != ''
          ${dateWhere}
      ),
      first_ping_per_day AS (
        SELECT
          ad_id, date,
          MIN_BY(zip, utc_timestamp) as zip,
          MIN_BY(region, utc_timestamp) as region,
          MIN_BY(city, utc_timestamp) as city,
          MIN_BY(h3, utc_timestamp) as h3,
          MIN_BY(lat, utc_timestamp) as lat,
          MIN_BY(lng, utc_timestamp) as lng,
          HOUR(${lts('MIN(utc_timestamp)')}) as origin_hour,
          DAY_OF_WEEK(${lts('MIN(utc_timestamp)')}) as origin_dow,
          MAX(IF(HOUR(${lts('utc_timestamp')}) >= 22 OR HOUR(${lts('utc_timestamp')}) <= 6, 1, 0)) as has_overnight,
          AVG(is_gps) as gps_share,
          AVG(circle_score) as avg_circle
        FROM valid_pings
        GROUP BY ad_id, date
      ),
      matched AS (SELECT * FROM first_ping_per_day WHERE zip IN (${requestedSql}))
      SELECT
        ad_id, zip,
        ANY_VALUE(region) as region, ANY_VALUE(city) as city,
        COUNT(*) as device_days,
        MAX(has_overnight) as has_overnight,
        AVG(gps_share) as avg_gps,
        AVG(avg_circle) as avg_circle,
        AVG(lat) as avg_lat, AVG(lng) as avg_lng,
        COUNT_IF(origin_hour BETWEEN 5 AND 10) as morning_dd,
        COUNT_IF(origin_hour BETWEEN 11 AND 13) as midday_dd,
        COUNT_IF(origin_hour BETWEEN 14 AND 17) as afternoon_dd,
        COUNT_IF(origin_hour BETWEEN 18 AND 21) as evening_dd,
        COUNT_IF(origin_hour >= 22 OR origin_hour <= 4) as night_dd,
        COUNT_IF(origin_dow IN (6, 7)) as weekend_dd
      FROM matched
      GROUP BY ad_id, zip
    `;

    const h3Query = `
      WITH
      poi_visitors AS (${poiVisitorsCte}),
      valid_pings AS (
        SELECT
          t.ad_id, t.date, t.utc_timestamp,
          TRY(t.geo_fields['zipcode']) as zip,
          TRY(t.geo_fields['h3_res10']) as h3,
          TRY_CAST(t.latitude AS DOUBLE) as lat,
          TRY_CAST(t.longitude AS DOUBLE) as lng
        FROM ${fromExpr} t
        INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
        WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
          AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
          AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
          AND TRY(t.geo_fields['zipcode']) IN (${requestedSql})
          AND TRY(t.geo_fields['h3_res10']) IS NOT NULL
          ${dateWhere}
      ),
      first_ping_per_day AS (
        SELECT ad_id, date,
          MIN_BY(zip, utc_timestamp) as zip,
          MIN_BY(h3, utc_timestamp) as h3,
          MIN_BY(lat, utc_timestamp) as lat,
          MIN_BY(lng, utc_timestamp) as lng
        FROM valid_pings GROUP BY ad_id, date
      )
      SELECT zip, h3,
        COUNT(DISTINCT ad_id) as devices,
        COUNT(*) as pings,
        AVG(lat) as lat, AVG(lng) as lng
      FROM first_ping_per_day
      GROUP BY zip, h3
      HAVING COUNT(*) >= 2
    `;

    queryIds['devices'] = await startQueryAsync(devicesQuery);
    queryIds['h3'] = await startQueryAsync(h3Query);
  } else {
    // BASIC: origins query JOINs the country's geocode cache to translate
    // first-ping lat/lng directly into a zipcode at SQL time, then filters
    // by the user's target ZIPs upfront. Output is ONLY matched rows
    // (ad_id, zipcode, device_days) — for France 50k typically a few
    // hundred K rows instead of the 40M unfiltered rows the old design
    // streamed to Node. Pass1/geocoding/pass2 collapse into a single
    // small stream + accumulate in `aggregate_basic`.
    //
    // Fallback: if no geocode cache exists for the country (geocodeTable
    // is null), we error out clearly — Node-side geocoding for 24M+
    // devices is not feasible within Vercel budget. The user can either
    // pre-build the cache via scripts/generate-geocode-cache or upload
    // their own.
    if (!state.geocodeTable) {
      throw new Error(
        `BASIC path requires a geocode cache for ${state.config.country}. ` +
          'Build it via scripts/generate-geocode-cache.ts or upload to ' +
          `s3://${BUCKET}/config/geocode-cache/${state.config.country}.csv`,
      );
    }
    const originsQuery = `
      WITH
      poi_visitors AS (${poiVisitorsCte}),
      valid_pings AS (
        SELECT
          t.ad_id, t.date, t.utc_timestamp,
          TRY_CAST(t.latitude AS DOUBLE) as lat,
          TRY_CAST(t.longitude AS DOUBLE) as lng
        FROM ${fromExpr} t
        INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
        WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
          AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
          AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
          ${dateWhere}
      ),
      first_pings AS (
        SELECT ad_id, date,
          MIN_BY(lat, utc_timestamp) as origin_lat,
          MIN_BY(lng, utc_timestamp) as origin_lng
        FROM valid_pings GROUP BY ad_id, date
      ),
      first_pings_with_keys AS (
        SELECT
          ad_id,
          CAST(ROUND(origin_lat * 10) AS INTEGER) as lat_key,
          CAST(ROUND(origin_lng * 10) AS INTEGER) as lng_key
        FROM first_pings
        WHERE origin_lat IS NOT NULL AND origin_lng IS NOT NULL
      ),
      with_zip AS (
        SELECT f.ad_id, g.zipcode
        FROM first_pings_with_keys f
        INNER JOIN ${state.geocodeTable} g
          -- OpenCSVSerde stores ALL columns as STRING regardless of the
          -- DDL declaration, so we cast g.lat_key/lng_key to INTEGER for
          -- the JOIN. Without the cast Athena does a string compare and
          -- "488" != 488, so zero rows would match.
          ON CAST(g.lat_key AS INTEGER) = f.lat_key
          AND CAST(g.lng_key AS INTEGER) = f.lng_key
        WHERE g.zipcode IN (${requestedSql})
      )
      SELECT
        ad_id,
        zipcode,
        COUNT(*) as device_days
      FROM with_zip
      GROUP BY ad_id, zipcode
    `;
    queryIds['origins'] = await startQueryAsync(originsQuery);
  }

  return {
    ...state,
    phase: 'polling_queries',
    queryIds,
    queriesStartedAt: Date.now(),
    subProgress: {
      label: `Launched ${Object.keys(queryIds).length} Athena ${state.isFull ? 'FAST-path' : 'BASIC'} query/queries`,
      percent: 18,
      details: `query IDs: ${Object.values(queryIds).map((q) => q.slice(0, 8)).join(', ')}`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase: polling_queries ──────────────────────────────────────────

export async function phasePollingQueries(state: ZcsState): Promise<ZcsState> {
  if (!state.queryIds) throw new Error('No queryIds in polling phase');
  const entries = Object.entries(state.queryIds);
  const statuses = await Promise.all(entries.map(([, q]) => checkQueryStatus(q)));
  const elapsed = state.queriesStartedAt
    ? Math.round((Date.now() - state.queriesStartedAt) / 1000)
    : 0;
  const totalScannedGB = statuses.reduce(
    (s, x) => s + ((x.statistics?.dataScannedBytes || 0) / 1e9),
    0,
  );
  // Critical-vs-optional: 'devices' (FULL) and 'origins' (BASIC) are
  // critical — failure aborts the run. 'h3' is optional enrichment
  // (sub-zip hotspot map); the analyzer original tolerated failure
  // here so we preserve that. The aggregate_full phase's h3 stream is
  // already wrapped in try/catch so a missing h3 CSV is handled.
  const OPTIONAL_QUERIES = new Set(['h3']);
  const criticalFailed = statuses.findIndex((s, i) => {
    const label = entries[i][0];
    return (s.state === 'FAILED' || s.state === 'CANCELLED') && !OPTIONAL_QUERIES.has(label);
  });
  if (criticalFailed !== -1) {
    const [label] = entries[criticalFailed];
    throw new Error(
      `Athena query "${label}" ${statuses[criticalFailed].state}: ${statuses[criticalFailed].error || 'unknown'}`,
    );
  }

  // For our purposes a query is "done" if SUCCEEDED, or if it's an
  // OPTIONAL query that FAILED — both let us proceed.
  const isQueryDone = (s: any, label: string) =>
    s.state === 'SUCCEEDED' ||
    ((s.state === 'FAILED' || s.state === 'CANCELLED') && OPTIONAL_QUERIES.has(label));
  const doneFlags = statuses.map((s, i) => isQueryDone(s, entries[i][0]));
  const doneCount = doneFlags.filter(Boolean).length;
  const allDone = doneCount === entries.length;

  const subProgress = {
    label: allDone
      ? `Athena queries done · ${elapsed}s · ${totalScannedGB.toFixed(1)} GB scanned`
      : `Athena: ${doneCount}/${entries.length} query/queries done · ${elapsed}s · ${totalScannedGB.toFixed(1)} GB scanned`,
    percent: 18 + Math.round((doneCount / entries.length) * 30),
    details: entries
      .map(([k], i) => {
        const s = statuses[i];
        const icon =
          s.state === 'SUCCEEDED'
            ? '✅'
            : (s.state === 'FAILED' || s.state === 'CANCELLED') && OPTIONAL_QUERIES.has(k)
              ? '⚠️'
              : '⏳';
        return `${icon} ${k}`;
      })
      .join('  '),
  };

  if (!allDone) {
    return { ...state, subProgress, updatedAt: new Date().toISOString() };
  }

  // All critical done — advance based on schema. BASIC path now goes
  // straight to aggregate_basic (single small stream of pre-matched rows)
  // instead of the legacy pass1+geocoding+pass2 trio.
  return {
    ...state,
    phase: state.isFull ? 'aggregate_full' : 'aggregate_basic',
    subProgress,
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase: aggregate_full (FULL path) ───────────────────────────────

export async function phaseAggregateFull(state: ZcsState): Promise<ZcsState> {
  const { runId, normalizedPostals } = state;
  if (!state.queryIds?.devices || !state.queryIds?.h3) {
    throw new Error('Missing devices/h3 query IDs');
  }

  // Stream devices CSV
  type DeviceRow = {
    deviceDays: number;
    postalCodes: Set<string>;
    region: string | null;
    city: string | null;
    hasOvernight: boolean;
    avgGps: number;
    avgCircle: number;
    samples: number;
  };
  type ZipAcc = {
    zip: string;
    region: Map<string, number>;
    city: Map<string, number>;
    deviceDays: number;
    deviceSet: Set<string>;
    persistence: { onceOnly: number; casual: number; regular: number; resident: number };
    hourBuckets: { morning: number; midday: number; afternoon: number; evening: number; night: number };
    weekendDD: number;
    overnightDays: number;
    sumGps: number;
    sumCircle: number;
    sumLat: number;
    sumLng: number;
    rows: number;
  };

  const deviceMap = new Map<string, DeviceRow>();
  const zipMap = new Map<string, ZipAcc>();
  const newZipAcc = (zip: string): ZipAcc => ({
    zip,
    region: new Map(),
    city: new Map(),
    deviceDays: 0,
    deviceSet: new Set(),
    persistence: { onceOnly: 0, casual: 0, regular: 0, resident: 0 },
    hourBuckets: { morning: 0, midday: 0, afternoon: 0, evening: 0, night: 0 },
    weekendDD: 0,
    overnightDays: 0,
    sumGps: 0,
    sumCircle: 0,
    sumLat: 0,
    sumLng: 0,
    rows: 0,
  });
  let totalDeviceDays = 0;

  await streamQueryResultsViaS3(state.queryIds.devices, (row) => {
    const adId = String(row.ad_id || '');
    const zip = String(row.zip || '');
    if (!adId || !zip) return;
    const dd = parseInt(String(row.device_days)) || 0;
    const region = (row.region || '').trim() || null;
    const city = (row.city || '').trim() || null;
    const hasOvernight = String(row.has_overnight) === '1' || String(row.has_overnight).toLowerCase() === 'true';
    const avgGps = parseFloat(String(row.avg_gps)) || 0;
    const avgCircle = parseFloat(String(row.avg_circle)) || 0;
    const lat = parseFloat(String(row.avg_lat)) || 0;
    const lng = parseFloat(String(row.avg_lng)) || 0;
    const m_dd = parseInt(String(row.morning_dd)) || 0;
    const mi_dd = parseInt(String(row.midday_dd)) || 0;
    const a_dd = parseInt(String(row.afternoon_dd)) || 0;
    const e_dd = parseInt(String(row.evening_dd)) || 0;
    const n_dd = parseInt(String(row.night_dd)) || 0;
    const w_dd = parseInt(String(row.weekend_dd)) || 0;
    totalDeviceDays += dd;

    let dev = deviceMap.get(adId);
    if (!dev) {
      dev = {
        deviceDays: 0, postalCodes: new Set(),
        region: null, city: null,
        hasOvernight: false, avgGps: 0, avgCircle: 0, samples: 0,
      };
      deviceMap.set(adId, dev);
    }
    dev.deviceDays += dd;
    dev.postalCodes.add(zip);
    if (region && !dev.region) dev.region = region;
    if (city && !dev.city) dev.city = city;
    dev.hasOvernight = dev.hasOvernight || hasOvernight;
    const w = dd;
    dev.avgGps = (dev.avgGps * dev.samples + avgGps * w) / (dev.samples + w || 1);
    dev.avgCircle = (dev.avgCircle * dev.samples + avgCircle * w) / (dev.samples + w || 1);
    dev.samples += w;

    let z = zipMap.get(zip);
    if (!z) { z = newZipAcc(zip); zipMap.set(zip, z); }
    if (region) z.region.set(region, (z.region.get(region) || 0) + dd);
    if (city) z.city.set(city, (z.city.get(city) || 0) + dd);
    z.deviceDays += dd;
    z.deviceSet.add(adId);
    z.hourBuckets.morning += m_dd;
    z.hourBuckets.midday += mi_dd;
    z.hourBuckets.afternoon += a_dd;
    z.hourBuckets.evening += e_dd;
    z.hourBuckets.night += n_dd;
    z.weekendDD += w_dd;
    if (hasOvernight) z.overnightDays += dd;
    z.sumGps += avgGps * dd;
    z.sumCircle += avgCircle * dd;
    z.sumLat += lat * dd;
    z.sumLng += lng * dd;
    z.rows += 1;
    if (dd === 1) z.persistence.onceOnly++;
    else if (dd <= 7) z.persistence.casual++;
    else if (dd <= 30) z.persistence.regular++;
    else z.persistence.resident++;
  });

  // H3 hotspots (small, may fail without aborting)
  const h3PerZip = new Map<string, Array<{ h3: string; lat: number; lng: number; devices: number; pings: number }>>();
  try {
    await streamQueryResultsViaS3(state.queryIds.h3, (row) => {
      const zip = String(row.zip || '');
      if (!zip) return;
      const arr = h3PerZip.get(zip) || [];
      arr.push({
        h3: String(row.h3),
        lat: parseFloat(String(row.lat)) || 0,
        lng: parseFloat(String(row.lng)) || 0,
        devices: parseInt(String(row.devices)) || 0,
        pings: parseInt(String(row.pings)) || 0,
      });
      h3PerZip.set(zip, arr);
    });
  } catch (e: any) {
    console.warn(`[ZCS-POLL ${runId}] h3 stream error: ${e?.message} — skipping hotspots`);
  }

  // Build the PostalMaidResult
  const devices: PostalMaidDevice[] = Array.from(deviceMap.entries())
    .map(([adId, d]) => ({
      adId,
      deviceDays: d.deviceDays,
      postalCodes: Array.from(d.postalCodes),
      region: d.region ?? undefined,
      city: d.city ?? undefined,
      qualityTier: deriveQualityTier(d.avgGps, d.avgCircle),
      overnightPresence: d.hasOvernight,
    }))
    .sort((a, b) => b.deviceDays - a.deviceDays);

  const postalCodeBreakdown = Array.from(zipMap.entries())
    .map(([zip, z]) => ({
      postalCode: zip,
      devices: z.deviceSet.size,
      deviceDays: z.deviceDays,
    }))
    .sort((a, b) => b.devices - a.devices);
  // Ensure all requested ZIPs appear, even if empty
  const seen = new Set(postalCodeBreakdown.map((p) => p.postalCode));
  for (const pc of normalizedPostals || []) {
    if (!seen.has(pc)) postalCodeBreakdown.push({ postalCode: pc, devices: 0, deviceDays: 0 });
  }

  const zipSignatures: ZipSignature[] = Array.from(zipMap.values()).map((z) => {
    const total = z.deviceDays || 1;
    const buckets = z.hourBuckets;
    const max = Math.max(buckets.morning, buckets.midday, buckets.afternoon, buckets.evening, buckets.night) || 1;
    const peakKey = (['morning', 'midday', 'afternoon', 'evening', 'night'] as const).find(
      (k) => buckets[k] === max,
    ) || 'midday';
    const persistTotal =
      z.persistence.onceOnly + z.persistence.casual + z.persistence.regular + z.persistence.resident || 1;
    const gpsAvg = z.sumGps / total;
    const circleAvg = z.sumCircle / total;
    const qualityTier: 'high' | 'mixed' | 'low' =
      gpsAvg > 0.7 && circleAvg < 1 ? 'high' : gpsAvg > 0.4 ? 'mixed' : 'low';
    return {
      postalCode: z.zip,
      region: pickTopMapKey(z.region),
      topCities: topNKeys(z.city, 3).map(([city, devices]) => ({ city, devices })),
      devices: z.deviceSet.size,
      deviceDays: z.deviceDays,
      hourBuckets: { ...buckets },
      peakHourBucket: peakKey,
      weekendShare: z.weekendDD / total,
      overnightShare: z.overnightDays / total,
      qualityTier,
      gpsShare: gpsAvg,
      avgCircleScore: circleAvg,
      persistence: { ...z.persistence },
      centroid: { lat: z.sumLat / total, lng: z.sumLng / total },
      topH3Cells: (h3PerZip.get(z.zip) || []).sort((a, b) => b.devices - a.devices).slice(0, 5),
    };
  });

  // Region rollup
  const regionTotals = new Map<string, { devices: number; zips: Set<string> }>();
  for (const z of zipMap.values()) {
    const top = pickTopMapKey(z.region) || 'UNKNOWN';
    const r = regionTotals.get(top) || { devices: 0, zips: new Set() };
    r.devices += z.deviceSet.size;
    r.zips.add(z.zip);
    regionTotals.set(top, r);
  }
  const totalDevicesMatched = devices.length;
  const regionSummary: RegionSummary[] = Array.from(regionTotals.entries())
    .map(([region, v]) => ({
      region,
      devices: v.devices,
      zips: v.zips.size,
      shareOfTotal: totalDevicesMatched > 0 ? v.devices / totalDevicesMatched : 0,
    }))
    .sort((a, b) => b.devices - a.devices);

  // Quality histogram
  const qualityHistogram = { high: 0, medium: 0, low: 0 };
  for (const d of devices) {
    if (d.qualityTier === 'high') qualityHistogram.high++;
    else if (d.qualityTier === 'low') qualityHistogram.low++;
    else qualityHistogram.medium++;
  }

  const top = postalCodeBreakdown[0];
  const result: PostalMaidResult = {
    dataset: state.sourceLabel,
    analyzedAt: new Date().toISOString(),
    filters: {
      postalCodes: state.config.postalCodes,
      country: state.config.country,
      dateFrom: state.effectiveDateFrom,
      dateTo: state.effectiveDateTo,
      megaJobId: state.config.megaJobId,
    },
    methodology: {
      approach: 'first_ping_per_day_geo_fields',
      description: 'FULL-schema fast path: filters geo_fields[zipcode] directly in SQL',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
      fastPath: true,
    },
    coverage: {
      totalDevicesInDataset: state.totalDevicesInDataset || 0,
      totalDeviceDays,
      devicesMatchedToPostalCodes: devices.length,
      matchedDeviceDays: devices.reduce((s, d) => s + d.deviceDays, 0),
      postalCodesRequested: normalizedPostals?.length || 0,
      postalCodesWithDevices: postalCodeBreakdown.filter((p) => p.devices > 0).length,
    },
    summary: {
      totalMaids: devices.length,
      topPostalCode: top?.postalCode || null,
      topPostalCodeDevices: top?.devices || 0,
    },
    devices,
    postalCodeBreakdown,
    fullSchema: {
      detectedAt: new Date().toISOString(),
      zipSignatures,
      regionSummary,
      qualityHistogram,
    },
  };

  // Use putConfig (not writeJson) so the file lands at the path the
  // spill endpoint reads via getConfig — see comment on resultS3Key.
  await putConfig(resultS3Key(runId), result, { compact: true });

  return {
    ...state,
    phase: 'done',
    resultKey: resultS3Key(runId),
    subProgress: {
      label: `Done · ${devices.length.toLocaleString()} MAIDs matched`,
      percent: 100,
      details: `${postalCodeBreakdown.filter((p) => p.devices > 0).length}/${normalizedPostals?.length || 0} postal codes with data`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase: aggregate_basic (BASIC path, JOIN-in-Athena rewrite) ─────
//
// The BASIC origins query now JOINs the country's geocode cache + filters
// by target zipcodes inside Athena, so the result CSV contains ONLY
// matched (ad_id, zipcode, device_days) rows — typically a few hundred K
// rows for a country-scale megajob (down from ~40M unfiltered rows).
//
// This phase streams that small CSV in one pass, building deviceMap +
// postalBreakdown directly. Replaces the old pass1_basic + geocoding +
// pass2_basic trio, which together took 20+ minutes locally because
// they had to stream + parse the unfiltered 40M-row CSV twice and
// Node-side reverse-geocode 18K unique cells.

export async function phaseAggregateBasic(state: ZcsState): Promise<ZcsState> {
  const { runId } = state;
  if (!state.queryIds?.origins) throw new Error('Missing origins query ID');

  const requestedSet = new Set(state.normalizedPostals || []);
  const deviceMap = new Map<string, { deviceDays: number; postalCodes: Set<string> }>();
  const postalBreakdown = new Map<string, { devices: Set<string>; deviceDays: number }>();
  for (const pc of requestedSet) {
    postalBreakdown.set(pc, { devices: new Set(), deviceDays: 0 });
  }

  let totalRows = 0;
  await streamQueryResultsViaS3(state.queryIds.origins, (row) => {
    totalRows++;
    const adId = String(row.ad_id || '');
    const zip = String(row.zipcode || '');
    const days = parseInt(String(row.device_days)) || 0;
    if (!adId || !zip) return;
    // The Athena query already filtered to target zipcodes, but we
    // re-check defensively (cheap O(1) Set membership).
    if (!requestedSet.has(zip)) return;
    const ex = deviceMap.get(adId);
    if (ex) {
      ex.deviceDays += days;
      ex.postalCodes.add(zip);
    } else {
      deviceMap.set(adId, { deviceDays: days, postalCodes: new Set([zip]) });
    }
    const b = postalBreakdown.get(zip)!;
    b.devices.add(adId);
    b.deviceDays += days;
  });

  const devices: PostalMaidDevice[] = Array.from(deviceMap.entries())
    .map(([adId, d]) => ({
      adId,
      deviceDays: d.deviceDays,
      postalCodes: Array.from(d.postalCodes),
    }))
    .sort((a, b) => b.deviceDays - a.deviceDays);
  const postalCodeBreakdown = Array.from(postalBreakdown.entries())
    .map(([postalCode, data]) => ({
      postalCode,
      devices: data.devices.size,
      deviceDays: data.deviceDays,
    }))
    .sort((a, b) => b.devices - a.devices);

  const top = postalCodeBreakdown[0];
  const matchedDeviceDays = devices.reduce((s, d) => s + d.deviceDays, 0);
  const result: PostalMaidResult = {
    dataset: state.sourceLabel,
    analyzedAt: new Date().toISOString(),
    filters: {
      postalCodes: state.config.postalCodes,
      country: state.config.country,
      dateFrom: state.effectiveDateFrom,
      dateTo: state.effectiveDateTo,
      megaJobId: state.config.megaJobId,
    },
    methodology: {
      approach: 'first_ping_per_day_reverse_geocoded',
      description:
        'BASIC schema: lat/lng of first ping per day, reverse-geocoded ' +
        'to postal code via an Athena-side JOIN against the country ' +
        'geocode cache. Matching done server-side; only matched ' +
        '(ad_id, postal) rows leave Athena.',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
    },
    coverage: {
      totalDevicesInDataset: state.totalDevicesInDataset || 0,
      // totalDeviceDays for matched devices only — we don't know the
      // global total without scanning the unfiltered output, which is
      // the whole point of this rewrite. matchedDeviceDays is the only
      // honest number we have for the BASIC path.
      totalDeviceDays: matchedDeviceDays,
      devicesMatchedToPostalCodes: devices.length,
      matchedDeviceDays,
      postalCodesRequested: state.normalizedPostals?.length || 0,
      postalCodesWithDevices: postalCodeBreakdown.filter((p) => p.devices > 0).length,
    },
    summary: {
      totalMaids: devices.length,
      topPostalCode: top?.postalCode || null,
      topPostalCodeDevices: top?.devices || 0,
    },
    devices,
    postalCodeBreakdown,
  };
  await putConfig(resultS3Key(runId), result, { compact: true });

  return {
    ...state,
    phase: 'done',
    resultKey: resultS3Key(runId),
    subProgress: {
      label: `Done · ${devices.length.toLocaleString()} MAIDs matched (${totalRows.toLocaleString()} rows from Athena)`,
      percent: 100,
      details: `${postalCodeBreakdown.filter((p) => p.devices > 0).length}/${state.normalizedPostals?.length || 0} postal codes with data · single-stream aggregate`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase: pass1_basic (BASIC path) ─────────────────────────────────

export async function phasePass1Basic(state: ZcsState): Promise<ZcsState> {
  const { runId } = state;
  if (!state.queryIds?.origins) throw new Error('Missing origins query ID');
  const coordMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
  let totalRows = 0;
  let totalDeviceDays = 0;
  await streamQueryResultsViaS3(state.queryIds.origins, (row) => {
    totalRows++;
    const lat = parseFloat(String(row.origin_lat));
    const lng = parseFloat(String(row.origin_lng));
    const days = parseInt(String(row.device_days)) || 0;
    if (isNaN(lat) || isNaN(lng)) return;
    totalDeviceDays += days;
    const key = `${lat.toFixed(COORDINATE_PRECISION)},${lng.toFixed(COORDINATE_PRECISION)}`;
    const ex = coordMap.get(key);
    if (ex) ex.deviceCount += days;
    else coordMap.set(key, { lat, lng, deviceCount: days });
  });

  // Persist the coord map as JSON in S3 (small — at most ~50K coords)
  const coordsArray = Array.from(coordMap.entries()).map(([k, v]) => [k, v.lat, v.lng, v.deviceCount]);
  await writeJson(coordsS3Key(runId), coordsArray);

  return {
    ...state,
    phase: 'geocoding',
    coordsKey: coordsS3Key(runId),
    pass1Stats: { totalRows, uniqueCoords: coordMap.size, totalDeviceDays },
    subProgress: {
      label: `Pass 1 done · ${coordMap.size.toLocaleString()} unique coords from ${totalRows.toLocaleString()} rows`,
      percent: 60,
      details: 'Now reverse-geocoding…',
    },
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase: geocoding (BASIC path) ───────────────────────────────────

export async function phaseGeocoding(state: ZcsState): Promise<ZcsState> {
  if (!state.coordsKey) throw new Error('No coordsKey for geocoding');
  const coordsArr = await readJson<[string, number, number, number][]>(state.coordsKey);
  if (!coordsArr) throw new Error('coords blob missing in S3');

  setCountryFilter([state.config.country.toUpperCase()]);
  try {
    // Since COORDINATE_PRECISION=1 the SQL already emits coords at the
    // geocoder's resolution. We feed them directly to batchReverseGeocode
    // and build coordToPostal[1dec_key] → postal. Pass2 looks up at the
    // same 1-decimal precision. Simpler + smaller than the legacy two-
    // step (4dec dedup → 1dec geocode → 4dec→1dec lookup table).
    const points = coordsArr.map(([, lat, lng, count]) => ({
      lat,
      lng,
      deviceCount: count,
    }));
    const classified = await batchReverseGeocode(points);

    const requestedSet = new Set(state.normalizedPostals || []);
    const coordToPostal: Record<string, string> = {};
    let matched = 0;
    let postalResolved = 0;
    for (let i = 0; i < coordsArr.length && i < classified.length; i++) {
      const c = classified[i];
      if (c.type !== 'geojson_local' && c.type !== 'nominatim_match') continue;
      postalResolved++;
      const postal = normalizePostalForCountry(state.config.country, c.postcode);
      if (postal && requestedSet.has(postal)) {
        const [key] = coordsArr[i];
        coordToPostal[key] = postal;
        matched++;
      }
    }
    await writeJson(geocodeS3Key(state.runId), coordToPostal);

    return {
      ...state,
      phase: 'pass2_basic',
      geocodeKey: geocodeS3Key(state.runId),
      subProgress: {
        label: `Geocoded ${classified.length.toLocaleString()} unique coords · ${matched.toLocaleString()} match target postal codes`,
        percent: 75,
        details: `${postalResolved} postal-resolved · ${matched}/${coordsArr.length} match`,
      },
      updatedAt: new Date().toISOString(),
    };
  } finally {
    setCountryFilter(null);
  }
}

// ── Phase: pass2_basic (BASIC path) ─────────────────────────────────

export async function phasePass2Basic(state: ZcsState): Promise<ZcsState> {
  const { runId } = state;
  if (!state.queryIds?.origins || !state.geocodeKey) {
    throw new Error('Missing prerequisites for pass2_basic');
  }
  const coordToPostal = await readJson<Record<string, string>>(state.geocodeKey);
  if (!coordToPostal) throw new Error('geocode blob missing');

  const requestedSet = new Set(state.normalizedPostals || []);
  const deviceMap = new Map<string, { deviceDays: number; postalCodes: Set<string> }>();
  const postalBreakdown = new Map<string, { devices: Set<string>; deviceDays: number }>();
  for (const pc of requestedSet) {
    postalBreakdown.set(pc, { devices: new Set(), deviceDays: 0 });
  }

  let matchedRows = 0;
  await streamQueryResultsViaS3(state.queryIds.origins, (row) => {
    const adId = String(row.ad_id || '');
    const lat = parseFloat(String(row.origin_lat));
    const lng = parseFloat(String(row.origin_lng));
    const deviceDays = parseInt(String(row.device_days)) || 0;
    if (!adId || isNaN(lat) || isNaN(lng)) return;
    const key = `${lat.toFixed(COORDINATE_PRECISION)},${lng.toFixed(COORDINATE_PRECISION)}`;
    const postal = coordToPostal[key];
    if (!postal || !requestedSet.has(postal)) return;
    matchedRows++;
    const ex = deviceMap.get(adId);
    if (ex) {
      ex.deviceDays += deviceDays;
      ex.postalCodes.add(postal);
    } else {
      deviceMap.set(adId, { deviceDays, postalCodes: new Set([postal]) });
    }
    const b = postalBreakdown.get(postal)!;
    b.devices.add(adId);
    b.deviceDays += deviceDays;
  });

  const devices: PostalMaidDevice[] = Array.from(deviceMap.entries())
    .map(([adId, d]) => ({
      adId,
      deviceDays: d.deviceDays,
      postalCodes: Array.from(d.postalCodes),
    }))
    .sort((a, b) => b.deviceDays - a.deviceDays);
  const postalCodeBreakdown = Array.from(postalBreakdown.entries())
    .map(([postalCode, data]) => ({
      postalCode,
      devices: data.devices.size,
      deviceDays: data.deviceDays,
    }))
    .sort((a, b) => b.devices - a.devices);

  const top = postalCodeBreakdown[0];
  const matchedDeviceDays = devices.reduce((s, d) => s + d.deviceDays, 0);
  const pass1 = state.pass1Stats || { totalRows: 0, uniqueCoords: 0, totalDeviceDays: 0 };
  const result: PostalMaidResult = {
    dataset: state.sourceLabel,
    analyzedAt: new Date().toISOString(),
    filters: {
      postalCodes: state.config.postalCodes,
      country: state.config.country,
      dateFrom: state.effectiveDateFrom,
      dateTo: state.effectiveDateTo,
      megaJobId: state.config.megaJobId,
    },
    methodology: {
      approach: 'first_ping_per_day_reverse_geocoded',
      description: 'BASIC schema: lat/lng of first ping per day reverse-geocoded to postal code (local GeoJSON).',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
    },
    coverage: {
      totalDevicesInDataset: state.totalDevicesInDataset || 0,
      totalDeviceDays: pass1.totalDeviceDays,
      devicesMatchedToPostalCodes: devices.length,
      matchedDeviceDays,
      postalCodesRequested: state.normalizedPostals?.length || 0,
      postalCodesWithDevices: postalCodeBreakdown.filter((p) => p.devices > 0).length,
    },
    summary: {
      totalMaids: devices.length,
      topPostalCode: top?.postalCode || null,
      topPostalCodeDevices: top?.devices || 0,
    },
    devices,
    postalCodeBreakdown,
  };
  // Use putConfig (not writeJson) so the file lands at the path the
  // spill endpoint reads via getConfig — see comment on resultS3Key.
  await putConfig(resultS3Key(runId), result, { compact: true });

  return {
    ...state,
    phase: 'done',
    resultKey: resultS3Key(runId),
    subProgress: {
      label: `Done · ${devices.length.toLocaleString()} MAIDs matched (${matchedRows.toLocaleString()} matched device-day rows)`,
      percent: 100,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ── Misc helpers ────────────────────────────────────────────────────

function buildEmptyResult(label: string, filters: ZcsRunConfig, totalDevicesInDataset: number): PostalMaidResult {
  return {
    dataset: label,
    analyzedAt: new Date().toISOString(),
    filters: {
      postalCodes: filters.postalCodes,
      country: filters.country,
      dateFrom: filters.dateFrom,
      dateTo: filters.dateTo,
      megaJobId: filters.megaJobId,
    },
    methodology: {
      approach: 'first_ping_per_day_geo_fields',
      description: 'No matches',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
    },
    coverage: {
      totalDevicesInDataset,
      totalDeviceDays: 0,
      devicesMatchedToPostalCodes: 0,
      matchedDeviceDays: 0,
      postalCodesRequested: filters.postalCodes.length,
      postalCodesWithDevices: 0,
    },
    summary: { totalMaids: 0, topPostalCode: null, topPostalCodeDevices: 0 },
    devices: [],
    postalCodeBreakdown: filters.postalCodes.map((pc) => ({ postalCode: pc, devices: 0, deviceDays: 0 })),
  };
}

function deriveQualityTier(gps: number, circle: number): 'high' | 'medium' | 'low' {
  if (gps > 0.7 && circle < 1) return 'high';
  if (gps > 0.4) return 'medium';
  return 'low';
}

function pickTopMapKey(m: Map<string, number>): string | null {
  let bestKey: string | null = null;
  let bestVal = -Infinity;
  for (const [k, v] of m.entries()) {
    if (v > bestVal) { bestVal = v; bestKey = k; }
  }
  return bestKey;
}

function topNKeys<K>(m: Map<K, number>, n: number): Array<[K, number]> {
  return Array.from(m.entries()).sort((a, b) => b[1] - a[1]).slice(0, n);
}

// ── Result accessor ─────────────────────────────────────────────────

export async function readResult(runId: string): Promise<PostalMaidResult | null> {
  // Use getConfig (matches the putConfig write side + the spill endpoint
  // read side) so all three readers see the same bytes.
  return await getConfig<PostalMaidResult>(resultS3Key(runId));
}
