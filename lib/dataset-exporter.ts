/**
 * Dataset export using AWS Athena
 */

import { runQuery, createTableForDataset, tableExists, getTableName, startQueryAndWait, startQueryAsync, checkQueryStatus, startCTASAsync, dropTempTable, fetchQueryResults } from './athena';
import { PutObjectCommand, CopyObjectCommand, HeadObjectCommand, GetObjectCommand, CreateMultipartUploadCommand, UploadPartCopyCommand, CompleteMultipartUploadCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { s3Client, BUCKET } from './s3-config';
import { getZipcode, ensureCountriesLoaded } from './reverse-geocode';
import { Readable, PassThrough } from 'stream';
import { createInterface } from 'readline';

const EXPORT_MAX_ROWS = 500000;

export interface ExportFilters {
  minDwellTime?: number | null;  // seconds
  minPings?: number | null;
  dateFrom?: string;
  dateTo?: string;
  poiIds?: string[];
  /** 'maids' = filename maids-*.csv; otherwise full-*.csv */
  format?: 'full' | 'maids';
}

export interface ExportResult {
  success: boolean;
  deviceCount: number;
  totalDevices: number;
  filters: ExportFilters;
  s3Path: string;
  downloadUrl: string;
  createdAt: string;
}

function sanitizeDate(d: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) {
    throw new Error(`Invalid date format: ${d}. Expected YYYY-MM-DD.`);
  }
  return d;
}

function validateFilters(filters: ExportFilters): void {
  if (filters.minDwellTime != null) {
    const val = Number(filters.minDwellTime);
    if (!Number.isFinite(val) || val < 0) throw new Error('Invalid minDwellTime');
  }
  if (filters.minPings != null) {
    const val = Number(filters.minPings);
    if (!Number.isInteger(val) || val < 0) throw new Error('Invalid minPings');
  }
}

/**
 * Export devices matching filters using Athena
 */
export async function exportDevices(
  datasetName: string,
  filters: ExportFilters = {}
): Promise<ExportResult> {
  validateFilters(filters);

  const tableName = getTableName(datasetName);

  // Ensure table exists
  if (!(await tableExists(datasetName))) {
    await createTableForDataset(datasetName);
  }

  // Date conditions as fragments (no WHERE - integrate with AND)
  const dateConditions: string[] = [];
  if (filters.dateFrom) {
    dateConditions.push(`date >= '${sanitizeDate(filters.dateFrom)}'`);
  }
  if (filters.dateTo) {
    dateConditions.push(`date <= '${sanitizeDate(filters.dateTo)}'`);
  }

  // POI filter: always require visitors to POIs
  let poiFilter = '';
  if (filters.poiIds?.length) {
    const poiList = filters.poiIds.map((p) => {
      if (!/^[\w\-. ]+$/.test(p)) {
        throw new Error(`Invalid POI ID format: ${p}`);
      }
      const escaped = p.replace(/'/g, "''");
      return `'${escaped}'`;
    }).join(',');
    poiFilter = `AND poi_id IN (${poiList})`;
  }

  // UNNEST poi_ids: every ping is counted for ALL POIs it belongs to.
  // We use a CTE to flatten the array once, then filter/aggregate from it.
  const basePoiFilter = `poi_id IS NOT NULL AND poi_id != ''${poiFilter ? ` ${poiFilter}` : ''}`;
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  // HAVING conditions for dwell time / ping count
  const havingConditions: string[] = [];
  if (filters.minDwellTime != null) {
    const dwellExpr = "date_diff('second', MIN(utc_timestamp), MAX(utc_timestamp))";
    havingConditions.push(
      filters.minDwellTime === 0
        ? `${dwellExpr} > 0`
        : `${dwellExpr} >= ${Number(filters.minDwellTime)}`
    );
  }
  if (filters.minPings != null) {
    havingConditions.push(`COUNT(*) >= ${Number(filters.minPings)}`);
  }

  let result: { columns: string[]; rows: Record<string, any>[] };
  let csvContent: string;
  const suffix = filters.format === 'maids' ? 'maids' : 'full';

  if (filters.format === 'maids') {
    // MAIDs format: only ad_ids
    let sql: string;
    if (!filters.minDwellTime && !filters.minPings) {
      sql = `
        SELECT DISTINCT ad_id
        FROM ${tableName}
        CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
        WHERE ${basePoiFilter}
        ${dateWhere}
      `;
    } else {
      sql = `
        WITH unnested AS (
          SELECT ad_id, poi_id, utc_timestamp
          FROM ${tableName}
          CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
          WHERE ${basePoiFilter}
          ${dateWhere}
        ),
        device_stats AS (
          SELECT
            ad_id,
            poi_id,
            COUNT(*) as ping_count,
            date_diff('second', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell_seconds
          FROM unnested
          GROUP BY ad_id, poi_id
          ${havingConditions.length ? `HAVING ${havingConditions.join(' AND ')}` : ''}
        )
        SELECT DISTINCT ad_id
        FROM device_stats
      `;
    }

    result = await runQuery(sql);
    console.log(`[EXPORT] Query returned ${result.rows.length} unique ad_ids`);

    if (result.rows.length > EXPORT_MAX_ROWS) {
      console.warn(`[EXPORT] Truncating export to ${EXPORT_MAX_ROWS} rows (original: ${result.rows.length})`);
      result.rows = result.rows.slice(0, EXPORT_MAX_ROWS);
    }

    csvContent = 'ad_id\n' + result.rows.map((r) => r.ad_id).join('\n');
  } else {
    // Full export: enriched data per device-POI (UNNEST for all POI assignments)
    const fullSql = `
      WITH unnested AS (
        SELECT ad_id, poi_id, utc_timestamp, latitude, longitude
        FROM ${tableName}
        CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
        WHERE ${basePoiFilter}
        ${dateWhere}
      )
      SELECT
        ad_id,
        poi_id,
        COUNT(*) as ping_count,
        date_diff('second', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell_seconds,
        MIN(utc_timestamp) as first_seen,
        MAX(utc_timestamp) as last_seen,
        ROUND(APPROX_PERCENTILE(TRY_CAST(latitude AS DOUBLE), 0.5), 5) as median_lat,
        ROUND(APPROX_PERCENTILE(TRY_CAST(longitude AS DOUBLE), 0.5), 5) as median_lng
      FROM unnested
      GROUP BY ad_id, poi_id
      ${havingConditions.length ? `HAVING ${havingConditions.join(' AND ')}` : ''}
      ORDER BY 1, 5
    `;
    result = await runQuery(fullSql);
    console.log(`[EXPORT] Full query returned ${result.rows.length} rows`);

    if (result.rows.length > EXPORT_MAX_ROWS) {
      console.warn(`[EXPORT] Truncating export to ${EXPORT_MAX_ROWS} rows (original: ${result.rows.length})`);
      result.rows = result.rows.slice(0, EXPORT_MAX_ROWS);
    }

    const headers = ['ad_id', 'poi_id', 'ping_count', 'dwell_seconds', 'first_seen', 'last_seen', 'median_lat', 'median_lng'];
    csvContent = headers.join(',') + '\n' +
      result.rows.map((r) => headers.map((h) => r[h] ?? '').join(',')).join('\n');
  }

  const deviceCount = result.rows.length;

  // Total devices: only visitors to POIs (UNNEST for complete coverage)
  const totalSql = `
    SELECT COUNT(DISTINCT ad_id) as total
    FROM ${tableName}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE ${basePoiFilter}
    ${dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : ''}
  `;
  const totalRes = await runQuery(totalSql);
  const totalDevices = parseInt(String(totalRes.rows[0]?.total)) || 0;

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const fileName = `${datasetName}-${suffix}-${timestamp}.csv`;
  const exportKey = `exports/${fileName}`;

  // TODO: Configure S3 Lifecycle Rule for prefix "exports/" with 7-day expiration
  // aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET --lifecycle-configuration '{
  //   "Rules": [{"ID": "ExpireExports", "Prefix": "exports/", "Status": "Enabled",
  //     "Expiration": {"Days": 7}}]
  // }'

  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: exportKey,
    Body: csvContent,
    ContentType: 'text/csv',
    Metadata: {
      'export-dataset': datasetName,
      'export-created': new Date().toISOString(),
    },
    Expires: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
  }));

  return {
    success: true,
    deviceCount,
    totalDevices,
    filters,
    s3Path: `s3://${BUCKET}/${exportKey}`,
    downloadUrl: `/api/datasets/${datasetName}/export/download?file=${fileName}`,
    createdAt: new Date().toISOString(),
  };
}

export interface ActivationResult {
  success: boolean;
  deviceCount: number;
  devicesWithZipcode: number;
  s3Path: string;
  folderName: string;
  createdAt: string;
}

export type ActivationProgressCallback = (step: string, percent: number, message: string) => void;

/**
 * Sanitize a job name for use as an S3 folder name.
 */
function sanitizeFolderName(name: string): string {
  return name
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^a-zA-Z0-9\-_]/g, '')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

/** Detect ISO country code from job/dataset name. Returns 2-letter code or undefined. */
function detectCountryFromName(name: string): string | undefined {
  const n = name.toLowerCase();
  const map: Record<string, string> = {
    germany: 'DE', deutschland: 'DE',
    france: 'FR', francia: 'FR',
    spain: 'ES', españa: 'ES', espana: 'ES',
    mexico: 'MX', méxico: 'MX',
    'united kingdom': 'UK', uk: 'UK',
    'costa rica': 'CR',
    guatemala: 'GT',
    ecuador: 'EC',
    colombia: 'CO',
    brazil: 'BR', brasil: 'BR',
    argentina: 'AR',
    chile: 'CL',
    peru: 'PE', perú: 'PE',
    italy: 'IT', italia: 'IT',
    portugal: 'PT',
    netherlands: 'NL',
    belgium: 'BE',
    switzerland: 'CH', suiza: 'CH',
    austria: 'AT',
    poland: 'PL', polonia: 'PL',
  };
  for (const [keyword, code] of Object.entries(map)) {
    if (n.includes(keyword)) return code;
  }
  return undefined;
}

/**
 * Export all MAIDs from a dataset to staging/{handle}.csv for Karlsgate.
 *
 * Flow:
 * 1. Query A: all distinct MAIDs (fast).
 * 2. Query B: per-device home location via nighttime pings (reuses catchment pattern).
 * 3. Download both results, reverse geocode home locations → zip codes.
 * 4. Build CSV: maid,country,zipcode — upload to staging.
 *
 * Queries A and B run in parallel for speed.
 */
export async function activateDevices(
  datasetName: string,
  jobName: string,
  countryCode?: string,
  onProgress?: ActivationProgressCallback
): Promise<ActivationResult> {
  const tableName = getTableName(datasetName);
  const progress = onProgress || (() => {});

  progress('table', 5, 'Verificando tabla Athena...');
  if (!(await tableExists(datasetName))) {
    progress('table', 8, 'Creando tabla Athena...');
    await createTableForDataset(datasetName);
  }

  // 1. Run both queries in parallel
  progress('queries', 10, 'Ejecutando queries Athena (MAIDs + home locations)...');

  const maidsSql = `SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND ad_id != ''`;

  const homeSql = `
    WITH night_pings AS (
      SELECT ad_id,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng,
        utc_timestamp
      FROM ${tableName}
      WHERE (HOUR(utc_timestamp) >= 20 OR HOUR(utc_timestamp) < 4)
        AND CARDINALITY(poi_ids) = 0
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < 200)
        AND ad_id IS NOT NULL AND ad_id != ''
    )
    SELECT ad_id,
      ROUND(APPROX_PERCENTILE(lat, 0.5), 4) as home_lat,
      ROUND(APPROX_PERCENTILE(lng, 0.5), 4) as home_lng
    FROM night_pings
    GROUP BY ad_id
    HAVING COUNT(*) >= 3
      AND COUNT(DISTINCT DATE(utc_timestamp)) >= 2
  `;

  console.log(`[ACTIVATE] ${datasetName}: running MAIDs + home location queries in parallel...`);
  let queryElapsed = 0;
  const onQueryPoll = (elapsed: number, state: string) => {
    queryElapsed = elapsed;
    // Send progress updates every ~10s so the SSE stream stays alive
    progress('queries', Math.min(10 + Math.floor(elapsed / 5), 38), `Queries Athena en progreso (${state}, ${elapsed}s)...`);
  };
  const [maidsResult, homeResult] = await Promise.all([
    startQueryAndWait(maidsSql, onQueryPoll),
    startQueryAndWait(homeSql, onQueryPoll),
  ]);
  progress('queries', 40, `Queries completadas en ${queryElapsed}s. Descargando resultados...`);

  // 2. Stream home locations CSV and build lookup map
  const homeResponse = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: homeResult.outputCsvKey,
  }));

  const homeMap = new Map<string, { lat: number; lng: number }>();
  const homeRl = createInterface({ input: homeResponse.Body as Readable, crlfDelay: Infinity });
  let homeHeader = true;
  for await (const line of homeRl) {
    if (homeHeader) { homeHeader = false; continue; }
    const trimmed = line.trim();
    if (!trimmed) continue;
    const parts = trimmed.replace(/"/g, '').split(',');
    const adId = parts[0];
    const lat = parseFloat(parts[1]);
    const lng = parseFloat(parts[2]);
    if (adId && !isNaN(lat) && !isNaN(lng)) {
      homeMap.set(adId, { lat, lng });
    }
  }
  progress('geocode', 50, `${homeMap.size.toLocaleString()} devices con home location. Geocodificando...`);

  // 3. Reverse geocode unique home locations
  const uniqueLocations = new Map<string, { lat: number; lng: number }>();
  homeMap.forEach(({ lat, lng }) => {
    const key = `${lat},${lng}`;
    if (!uniqueLocations.has(key)) {
      uniqueLocations.set(key, { lat, lng });
    }
  });

  const points: Array<{ lat: number; lng: number }> = [];
  uniqueLocations.forEach(v => points.push(v));
  progress('geocode', 55, `Cargando GeoJSON para ${points.length.toLocaleString()} ubicaciones...`);
  await ensureCountriesLoaded(points);

  const zipLookup = new Map<string, string>();
  for (const { lat, lng } of points) {
    const result = getZipcode(lat, lng);
    const key = `${lat},${lng}`;
    zipLookup.set(key, result?.postcode || 'UNKNOWN');
  }
  progress('geocode', 70, `Geocodificadas ${zipLookup.size.toLocaleString()} ubicaciones`);

  // 4. Stream MAIDs CSV from S3 → build final CSV → stream upload to S3
  //    (avoids loading the entire MAIDs file into memory for large datasets)
  progress('build_csv', 72, 'Procesando MAIDs y subiendo CSV (streaming)...');

  const handle = sanitizeFolderName(jobName) || datasetName;
  const csvKey = `staging/${handle}.csv`;
  const specKey = `staging/${handle}.csv.spec.yml`;

  const maidsResponse = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: maidsResult.outputCsvKey,
  }));

  const country = countryCode || '';
  let deviceCount = 0;
  let devicesWithZipcode = 0;

  // PassThrough stream pipes processed lines directly to S3 multipart upload
  const outputStream = new PassThrough();
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET,
      Key: csvKey,
      Body: outputStream,
      ContentType: 'text/csv',
    },
    // 5 MB parts, 4 concurrent uploads
    partSize: 5 * 1024 * 1024,
    queueSize: 4,
  });

  outputStream.write('maid,country,zipcode\n');

  // Stream input line-by-line from S3 → transform → write to output
  const rl = createInterface({ input: maidsResponse.Body as Readable, crlfDelay: Infinity });
  let isHeader = true;
  for await (const line of rl) {
    if (isHeader) { isHeader = false; continue; }
    const adId = line.trim().replace(/"/g, '');
    if (!adId) continue;

    const home = homeMap.get(adId);
    let zipcode = 'UNKNOWN';
    if (home) {
      zipcode = zipLookup.get(`${home.lat},${home.lng}`) || 'UNKNOWN';
      if (zipcode !== 'UNKNOWN') devicesWithZipcode++;
    }
    outputStream.write(`${adId},${country},${zipcode}\n`);
    deviceCount++;

    // Send progress updates every 1M rows to keep connection alive
    if (deviceCount % 1_000_000 === 0) {
      const pct = Math.min(72 + Math.floor((deviceCount / 1_000_000) * 0.5), 84);
      progress('build_csv', pct, `Procesando MAIDs: ${deviceCount.toLocaleString()}...`);
    }
  }

  outputStream.end();
  await upload.done();

  progress('build_csv', 85, `CSV subido: ${deviceCount.toLocaleString()} MAIDs, ${devicesWithZipcode.toLocaleString()} con zip code`);

  progress('upload', 95, 'Subiendo spec file...');
  const specContent = 'identifiers:\n  - name: maid\n    type: uuid-maid\nattributes:\n  - country\n  - zipcode\n';
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: specKey,
    Body: specContent,
    ContentType: 'text/yaml',
  }));

  progress('done', 100, `Listo: ${deviceCount.toLocaleString()} MAIDs activados`);
  console.log(`[ACTIVATE] Uploaded to s3://${BUCKET}/${csvKey} + spec (country=${country})`);

  return {
    success: true,
    deviceCount,
    devicesWithZipcode,
    s3Path: `s3://${BUCKET}/${csvKey}`,
    folderName: handle,
    createdAt: new Date().toISOString(),
  };
}

// ── Multi-phase activation (works within 60s Vercel Hobby limit) ─────

export interface ActivationState {
  status: 'starting' | 'queries_running' | 'geocoding' | 'join_running' | 'finalizing' | 'completed' | 'error';
  datasetName: string;
  jobName: string;
  countryCode: string;
  handle: string;
  /** Unique run ID for temp table names */
  runId?: string;
  /** Athena query ID for DISTINCT rounded home locations */
  uniqueLocsQueryId?: string;
  /** Athena CTAS query ID for home locations table */
  homeCTASQueryId?: string;
  /** Temp table name for home locations (Parquet) */
  homeTableName?: string;
  /** Temp table name for geocode lookup (CSV) */
  geoTableName?: string;
  /** Athena query ID for the final JOIN */
  joinQueryId?: string;
  /** Athena query ID for device count */
  countQueryId?: string;
  progress: { step: string; percent: number; message: string };
  result?: ActivationResult;
  error?: string;
  startedAt: string;
  updatedAt: string;
}

const ACTIVATION_CONFIG_PREFIX = 'config/activations';

async function getActivationState(datasetName: string): Promise<ActivationState | null> {
  try {
    const res = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: `${ACTIVATION_CONFIG_PREFIX}/${datasetName}.json`,
    }));
    const body = await res.Body?.transformToString();
    return body ? JSON.parse(body) : null;
  } catch {
    return null;
  }
}

async function saveActivationState(state: ActivationState): Promise<void> {
  state.updatedAt = new Date().toISOString();
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${ACTIVATION_CONFIG_PREFIX}/${state.datasetName}.json`,
    Body: JSON.stringify(state),
    ContentType: 'application/json',
  }));
}

/**
 * Start or advance a multi-phase Karlsgate activation.
 * Each call completes within ~50s (safe for 60s Vercel timeout).
 *
 * Architecture:
 * - Phase 1: Start Athena queries (unique locations + home CTAS)
 * - Phase 2: Poll queries
 * - Phase 3: Download small unique-locations CSV, geocode, create lookup table, start JOIN
 * - Phase 4: Poll JOIN query
 * - Phase 5: Copy Athena result to staging, upload spec
 *
 * The key insight: NEVER download large CSVs (MAIDs 86M rows, home 25M rows).
 * Only download the small unique-locations CSV (~50K-200K rows).
 * Athena does the heavy JOIN and produces the final CSV.
 */
export async function activateDevicesMultiPhase(
  datasetName: string,
  jobName: string,
  countryCode?: string,
): Promise<ActivationState> {
  let state = await getActivationState(datasetName);

  // If there's a completed/error state from a previous run, start fresh
  if (state && (state.status === 'completed' || state.status === 'error')) {
    state = null;
  }

  // If state has old format (no runId), start fresh
  if (state && !state.runId) {
    state = null;
  }

  if (!state) {
    // ── Phase 1: Start Athena queries ──────────────────────────
    const tableName = getTableName(datasetName);
    if (!(await tableExists(datasetName))) {
      await createTableForDataset(datasetName);
    }

    const runId = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
    const handle = sanitizeFolderName(jobName) || datasetName;
    const homeTableName = `temp_home_${runId}`;
    const geoTableName = `temp_geo_${runId}`;

    // Drop temp tables if they somehow exist from a previous failed run
    await Promise.all([
      dropTempTable(homeTableName).catch(() => {}),
      dropTempTable(geoTableName).catch(() => {}),
    ]);

    // CTE shared by both queries
    // NOTE: We do NOT filter by CARDINALITY(poi_ids) = 0 because cohort datasets
    // (geofenced) have poi_ids on EVERY ping, so that filter would eliminate all rows.
    // Nighttime pings near POIs are still valid for home estimation in urban areas.
    const nightPingsCTE = `
      night_pings AS (
        SELECT ad_id,
          TRY_CAST(latitude AS DOUBLE) as lat,
          TRY_CAST(longitude AS DOUBLE) as lng,
          utc_timestamp
        FROM ${tableName}
        WHERE (HOUR(utc_timestamp) >= 20 OR HOUR(utc_timestamp) < 4)
          AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
          AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
          AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < 200)
          AND ad_id IS NOT NULL AND ad_id != ''
      )
    `;

    // Query 1: DISTINCT rounded home locations (small result — ~50K-200K rows)
    // Uses integer keys (lat*1000, lng*1000) to avoid floating point issues in JOINs
    const uniqueLocsSql = `
      WITH ${nightPingsCTE},
      homes AS (
        SELECT
          CAST(ROUND(APPROX_PERCENTILE(lat, 0.5), 3) * 1000 AS BIGINT) as lat_key,
          CAST(ROUND(APPROX_PERCENTILE(lng, 0.5), 3) * 1000 AS BIGINT) as lng_key
        FROM night_pings
        GROUP BY ad_id
        HAVING COUNT(*) >= 3 AND COUNT(DISTINCT DATE(utc_timestamp)) >= 2
      )
      SELECT DISTINCT lat_key, lng_key FROM homes
    `;

    // Query 2: CTAS — materializes home locations as a Parquet table for efficient JOIN
    const homeCTASSelect = `
      WITH ${nightPingsCTE}
      SELECT ad_id,
        CAST(ROUND(APPROX_PERCENTILE(lat, 0.5), 3) * 1000 AS BIGINT) as lat_key,
        CAST(ROUND(APPROX_PERCENTILE(lng, 0.5), 3) * 1000 AS BIGINT) as lng_key
      FROM night_pings
      GROUP BY ad_id
      HAVING COUNT(*) >= 3 AND COUNT(DISTINCT DATE(utc_timestamp)) >= 2
    `;

    const [uniqueLocsQueryId, homeCTASQueryId] = await Promise.all([
      startQueryAsync(uniqueLocsSql),
      startCTASAsync(homeCTASSelect, homeTableName),
    ]);

    // Resolve country code: explicit param > name-based detection > empty
    const resolvedCountry = countryCode || detectCountryFromName(jobName) || '';

    state = {
      status: 'queries_running',
      datasetName,
      jobName,
      countryCode: resolvedCountry,
      handle,
      runId,
      uniqueLocsQueryId,
      homeCTASQueryId,
      homeTableName,
      geoTableName,
      progress: { step: 'queries', percent: 10, message: 'Queries Athena iniciadas (locations + home CTAS)...' },
      startedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await saveActivationState(state);
    console.log(`[ACTIVATE] ${datasetName}: started unique_locs(${uniqueLocsQueryId}) + home_ctas(${homeCTASQueryId})`);
    return state;
  }

  // ── Phase 2: Poll Athena queries ──────────────────────────
  if (state.status === 'queries_running') {
    const [locsStatus, homeStatus] = await Promise.all([
      checkQueryStatus(state.uniqueLocsQueryId!),
      checkQueryStatus(state.homeCTASQueryId!),
    ]);

    if (locsStatus.state === 'FAILED') {
      state.status = 'error';
      state.error = `Unique locations query failed: ${locsStatus.error}`;
      state.progress = { step: 'error', percent: 0, message: state.error };
      await saveActivationState(state);
      return state;
    }
    if (homeStatus.state === 'FAILED') {
      state.status = 'error';
      state.error = `Home CTAS query failed: ${homeStatus.error}`;
      state.progress = { step: 'error', percent: 0, message: state.error };
      await saveActivationState(state);
      return state;
    }

    const locsDone = locsStatus.state === 'SUCCEEDED';
    const homeDone = homeStatus.state === 'SUCCEEDED';

    if (locsDone && homeDone) {
      state.status = 'geocoding';
      state.progress = { step: 'queries', percent: 30, message: 'Queries completadas. Preparando geocodificación...' };
      await saveActivationState(state);
      console.log(`[ACTIVATE] ${state.datasetName}: both queries completed`);
      return state; // Don't fall through — geocoding phase does heavy work
    } else {
      const statusMsg = `Locations: ${locsStatus.state}, Home: ${homeStatus.state}`;
      state.progress = { step: 'queries', percent: 20, message: `Esperando queries... (${statusMsg})` };
      await saveActivationState(state);
      return state;
    }
  }

  // ── Phase 3: Geocode unique locations + start Athena JOIN ──────
  if (state.status === 'geocoding') {
    // Download unique locations CSV (SMALL — ~50K-200K rows, a few MB)
    const locsOutputKey = `athena-results/${state.uniqueLocsQueryId}.csv`;
    const locsResponse = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: locsOutputKey,
    }));
    const locsCsv = await locsResponse.Body?.transformToString() || '';

    // Parse lat_key, lng_key → actual coordinates
    const locations: { latKey: number; lngKey: number; lat: number; lng: number }[] = [];
    const locsLines = locsCsv.split('\n');
    for (let i = 1; i < locsLines.length; i++) {
      const line = locsLines[i].trim().replace(/"/g, '');
      if (!line) continue;
      const [latKeyStr, lngKeyStr] = line.split(',');
      const latKey = parseInt(latKeyStr);
      const lngKey = parseInt(lngKeyStr);
      if (!isNaN(latKey) && !isNaN(lngKey)) {
        locations.push({ latKey, lngKey, lat: latKey / 1000, lng: lngKey / 1000 });
      }
    }

    console.log(`[ACTIVATE] ${state.datasetName}: ${locations.length} unique locations to geocode`);
    state.progress = {
      step: 'geocode',
      percent: 35,
      message: `${locations.length.toLocaleString()} ubicaciones únicas. Geocodificando...`,
    };

    // Load GeoJSON for needed countries
    const points = locations.map(l => ({ lat: l.lat, lng: l.lng }));
    await ensureCountriesLoaded(points);

    // Geocode each unique location (fast: ~10-100μs per lookup with spatial index)
    const geoLines: string[] = [];
    for (const loc of locations) {
      const result = getZipcode(loc.lat, loc.lng);
      if (result?.postcode) {
        geoLines.push(`${loc.latKey},${loc.lngKey},${result.postcode}`);
      }
    }

    console.log(`[ACTIVATE] ${state.datasetName}: geocoded ${geoLines.length}/${locations.length} locations`);
    state.progress = {
      step: 'geocode',
      percent: 50,
      message: `Geocodificadas ${geoLines.length.toLocaleString()} de ${locations.length.toLocaleString()} ubicaciones. Iniciando JOIN...`,
    };

    // Upload geocode lookup CSV (NO HEADER — external table reads positionally)
    const geoDir = `athena-temp/${state.geoTableName}`;
    const geoCsvContent = geoLines.length > 0 ? geoLines.join('\n') + '\n' : '0,0,EMPTY\n';
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${geoDir}/data.csv`,
      Body: geoCsvContent,
      ContentType: 'text/csv',
    }));

    // Create external table for geocode lookup
    const createGeoTableSql = `
      CREATE EXTERNAL TABLE ${state.geoTableName} (
        lat_key BIGINT,
        lng_key BIGINT,
        zipcode STRING
      )
      ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
      STORED AS TEXTFILE
      LOCATION 's3://${BUCKET}/${geoDir}/'
    `;
    await runQuery(createGeoTableSql);
    console.log(`[ACTIVATE] ${state.datasetName}: created geo table ${state.geoTableName} (${geoLines.length} entries)`);

    // Start the final JOIN query — Athena produces the CSV (no Node.js download needed!)
    const tableName = getTableName(datasetName);
    const joinSql = `
      WITH all_devices AS (
        SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND ad_id != ''
      )
      SELECT
        d.ad_id as maid,
        '${state.countryCode}' as country,
        COALESCE(g.zipcode, 'UNKNOWN') as zipcode
      FROM all_devices d
      LEFT JOIN ${state.homeTableName} h ON d.ad_id = h.ad_id
      LEFT JOIN ${state.geoTableName} g ON h.lat_key = g.lat_key AND h.lng_key = g.lng_key
    `;

    // Count query — runs in parallel, gets device/zipcode counts
    const countSql = `
      WITH all_devices AS (
        SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND ad_id != ''
      )
      SELECT
        COUNT(*) as total_devices,
        COUNT(g.zipcode) as devices_with_zip
      FROM all_devices d
      LEFT JOIN ${state.homeTableName} h ON d.ad_id = h.ad_id
      LEFT JOIN ${state.geoTableName} g ON h.lat_key = g.lat_key AND h.lng_key = g.lng_key
    `;

    const [joinQueryId, countQueryId] = await Promise.all([
      startQueryAsync(joinSql),
      startQueryAsync(countSql),
    ]);

    state.joinQueryId = joinQueryId;
    state.countQueryId = countQueryId;
    state.status = 'join_running';
    state.progress = { step: 'join', percent: 55, message: 'JOIN query en progreso en Athena...' };
    await saveActivationState(state);
    console.log(`[ACTIVATE] ${state.datasetName}: started join(${joinQueryId}) + count(${countQueryId})`);
    return state;
  }

  // ── Phase 4: Poll JOIN + count queries ──────────────────────
  if (state.status === 'join_running') {
    const [joinStatus, countStatus] = await Promise.all([
      checkQueryStatus(state.joinQueryId!),
      checkQueryStatus(state.countQueryId!),
    ]);

    if (joinStatus.state === 'FAILED') {
      state.status = 'error';
      state.error = `Join query failed: ${joinStatus.error}`;
      state.progress = { step: 'error', percent: 0, message: state.error };
      await saveActivationState(state);
      return state;
    }
    // Count query failure is non-critical
    if (countStatus.state === 'FAILED') {
      console.warn(`[ACTIVATE] Count query failed (non-critical): ${countStatus.error}`);
    }

    const joinDone = joinStatus.state === 'SUCCEEDED';
    const countDone = countStatus.state === 'SUCCEEDED' || countStatus.state === 'FAILED';

    if (joinDone && countDone) {
      state.status = 'finalizing';
      state.progress = { step: 'finalizing', percent: 80, message: 'Copiando resultado a staging...' };
      await saveActivationState(state);
      // Fall through to finalizing
    } else {
      const statusMsg = `Join: ${joinStatus.state}, Count: ${countStatus.state}`;
      state.progress = { step: 'join', percent: 65, message: `Esperando queries... (${statusMsg})` };
      await saveActivationState(state);
      return state;
    }
  }

  // ── Phase 5: Copy result to staging + cleanup ────────────────
  if (state.status === 'finalizing') {
    const csvKey = `staging/${state.handle}.csv`;
    const specKey = `staging/${state.handle}.csv.spec.yml`;
    const athenaOutputKey = `athena-results/${state.joinQueryId}.csv`;

    // Get device counts from count query
    let deviceCount = 0;
    let devicesWithZipcode = 0;
    try {
      const countResult = await fetchQueryResults(state.countQueryId!);
      const row = countResult.rows[0];
      if (row) {
        deviceCount = Number(row.total_devices) || 0;
        devicesWithZipcode = Number(row.devices_with_zip) || 0;
      }
    } catch (e: any) {
      console.warn(`[ACTIVATE] Failed to fetch count results: ${e.message}`);
    }

    // Copy Athena result CSV to staging (server-side, no download!)
    const head = await s3Client.send(new HeadObjectCommand({
      Bucket: BUCKET,
      Key: athenaOutputKey,
    }));
    const fileSize = head.ContentLength || 0;
    console.log(`[ACTIVATE] Athena result size: ${(fileSize / (1024 * 1024)).toFixed(1)} MB`);

    if (fileSize <= 100 * 1024 * 1024) {
      // Under 100MB — simple CopyObject (fast enough to complete in seconds)
      await s3Client.send(new CopyObjectCommand({
        Bucket: BUCKET,
        CopySource: `${BUCKET}/${athenaOutputKey}`,
        Key: csvKey,
        ContentType: 'text/csv',
      }));
    } else {
      // Over 100MB — use concurrent multipart copy (CopyObject takes ~96s for 4GB!)
      console.log(`[ACTIVATE] Using concurrent multipart copy for ${(fileSize / (1024 * 1024)).toFixed(0)} MB file...`);
      await multipartCopyS3(BUCKET, athenaOutputKey, csvKey);
    }

    // Upload spec
    const specContent = 'identifiers:\n  - name: maid\n    type: uuid-maid\nattributes:\n  - country\n  - zipcode\n';
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: specKey,
      Body: specContent,
      ContentType: 'text/yaml',
    }));

    // Cleanup temp Athena tables (best-effort, fire-and-forget)
    Promise.all([
      dropTempTable(state.homeTableName!).catch(() => {}),
      dropTempTable(state.geoTableName!).catch(() => {}),
    ]).catch(() => {});

    const result: ActivationResult = {
      success: true,
      deviceCount,
      devicesWithZipcode,
      s3Path: `s3://${BUCKET}/${csvKey}`,
      folderName: state.handle,
      createdAt: new Date().toISOString(),
    };

    state.status = 'completed';
    state.result = result;
    state.progress = {
      step: 'done',
      percent: 100,
      message: `${deviceCount.toLocaleString()} MAIDs activados (${devicesWithZipcode.toLocaleString()} con zip code)`,
    };
    await saveActivationState(state);
    console.log(`[ACTIVATE] ${state.datasetName}: completed! ${deviceCount} MAIDs → ${csvKey}`);
    return state;
  }

  return state;
}

/**
 * Server-side multipart copy for large S3 objects.
 * Uses concurrent UploadPartCopy — no data is downloaded to the function.
 * CopyObject takes ~96s for 4GB (exceeds 60s Vercel limit), but concurrent
 * multipart copy with 10 parallel 100MB parts completes in ~15-20s.
 */
async function multipartCopyS3(bucket: string, sourceKey: string, destKey: string): Promise<void> {
  const head = await s3Client.send(new HeadObjectCommand({ Bucket: bucket, Key: sourceKey }));
  const fileSize = head.ContentLength || 0;
  const partSize = 100 * 1024 * 1024; // 100 MB parts
  const concurrency = 10; // 10 concurrent copy requests

  const { UploadId } = await s3Client.send(new CreateMultipartUploadCommand({
    Bucket: bucket,
    Key: destKey,
    ContentType: 'text/csv',
  }));

  try {
    const totalParts = Math.ceil(fileSize / partSize);
    const parts: { ETag: string; PartNumber: number }[] = new Array(totalParts);

    // Copy parts in concurrent batches
    for (let batch = 0; batch < totalParts; batch += concurrency) {
      const batchEnd = Math.min(batch + concurrency, totalParts);
      const promises: Promise<void>[] = [];

      for (let i = batch; i < batchEnd; i++) {
        const offset = i * partSize;
        const end = Math.min(offset + partSize - 1, fileSize - 1);
        const partNumber = i + 1;

        promises.push(
          s3Client.send(new UploadPartCopyCommand({
            Bucket: bucket,
            Key: destKey,
            CopySource: `${bucket}/${sourceKey}`,
            UploadId,
            PartNumber: partNumber,
            CopySourceRange: `bytes=${offset}-${end}`,
          })).then(res => {
            parts[i] = { ETag: res.CopyPartResult?.ETag || '', PartNumber: partNumber };
          })
        );
      }

      await Promise.all(promises);
    }

    await s3Client.send(new CompleteMultipartUploadCommand({
      Bucket: bucket,
      Key: destKey,
      UploadId,
      MultipartUpload: { Parts: parts },
    }));

    console.log(`[ACTIVATE] Multipart copy complete: ${totalParts} parts (${concurrency} concurrent), ${(fileSize / (1024 * 1024)).toFixed(0)} MB`);
  } catch (err) {
    // Abort the multipart upload on error to avoid orphaned parts
    try {
      const { AbortMultipartUploadCommand } = await import('@aws-sdk/client-s3');
      await s3Client.send(new AbortMultipartUploadCommand({ Bucket: bucket, Key: destKey, UploadId }));
    } catch { /* ignore abort errors */ }
    throw err;
  }
}

/**
 * Reset activation state (e.g., to retry after error).
 */
export async function resetActivationState(datasetName: string): Promise<void> {
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${ACTIVATION_CONFIG_PREFIX}/${datasetName}.json`,
      Body: '',
    }));
  } catch { /* ignore */ }
}
