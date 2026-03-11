/**
 * Dataset export using AWS Athena
 */

import { runQuery, createTableForDataset, tableExists, getTableName, startQueryAndWait, startQueryAsync, checkQueryStatus } from './athena';
import { PutObjectCommand, CopyObjectCommand, HeadObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
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
  status: 'starting' | 'queries_running' | 'geocoding' | 'building_csv' | 'completed' | 'error';
  datasetName: string;
  jobName: string;
  countryCode: string;
  handle: string;
  maidsQueryId?: string;
  homeQueryId?: string;
  /** S3 key where geocoded lookup (ad_id,zipcode) is stored */
  geocodedKey?: string;
  /** S3 key for the final join query output */
  joinQueryId?: string;
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
 * Frontend calls this repeatedly (polling) until status === 'completed' or 'error'.
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

  if (!state) {
    // ── Phase 1: Start Athena queries ──────────────────────────
    const tableName = getTableName(datasetName);
    if (!(await tableExists(datasetName))) {
      await createTableForDataset(datasetName);
    }

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

    const [maidsQueryId, homeQueryId] = await Promise.all([
      startQueryAsync(maidsSql),
      startQueryAsync(homeSql),
    ]);

    const handle = sanitizeFolderName(jobName) || datasetName;
    state = {
      status: 'queries_running',
      datasetName,
      jobName,
      countryCode: countryCode || '',
      handle,
      maidsQueryId,
      homeQueryId,
      progress: { step: 'queries', percent: 10, message: 'Queries Athena iniciadas...' },
      startedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await saveActivationState(state);
    console.log(`[ACTIVATE] ${datasetName}: started queries ${maidsQueryId} + ${homeQueryId}`);
    return state;
  }

  // ── Phase 2: Poll Athena queries ──────────────────────────
  if (state.status === 'queries_running') {
    const [maidsStatus, homeStatus] = await Promise.all([
      checkQueryStatus(state.maidsQueryId!),
      checkQueryStatus(state.homeQueryId!),
    ]);

    if (maidsStatus.state === 'FAILED') {
      state.status = 'error';
      state.error = `MAIDs query failed: ${maidsStatus.error}`;
      state.progress = { step: 'error', percent: 0, message: state.error };
      await saveActivationState(state);
      return state;
    }
    if (homeStatus.state === 'FAILED') {
      state.status = 'error';
      state.error = `Home locations query failed: ${homeStatus.error}`;
      state.progress = { step: 'error', percent: 0, message: state.error };
      await saveActivationState(state);
      return state;
    }

    const maidsDone = maidsStatus.state === 'SUCCEEDED';
    const homeDone = homeStatus.state === 'SUCCEEDED';

    if (maidsDone && homeDone) {
      state.status = 'geocoding';
      state.progress = { step: 'queries', percent: 40, message: 'Queries completadas. Geocodificando...' };
      await saveActivationState(state);
      console.log(`[ACTIVATE] ${state.datasetName}: both queries completed`);
      // Fall through to geocoding phase (still have time in this invocation)
    } else {
      const statusMsg = `MAIDs: ${maidsStatus.state}, Home: ${homeStatus.state}`;
      state.progress = { step: 'queries', percent: 20, message: `Esperando queries... (${statusMsg})` };
      await saveActivationState(state);
      return state;
    }
  }

  // ── Phase 3: Geocode home locations ──────────────────────────
  if (state.status === 'geocoding') {
    const homeOutputKey = `athena-results/${state.homeQueryId}.csv`;
    const homeResponse = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: homeOutputKey,
    }));

    // Stream home CSV → build homeMap
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

    state.progress = { step: 'geocode', percent: 50, message: `${homeMap.size.toLocaleString()} devices con home. Geocodificando...` };

    // Get unique locations
    const uniqueLocations = new Map<string, { lat: number; lng: number }>();
    homeMap.forEach(({ lat, lng }) => {
      const key = `${lat},${lng}`;
      if (!uniqueLocations.has(key)) uniqueLocations.set(key, { lat, lng });
    });

    const points: Array<{ lat: number; lng: number }> = [];
    uniqueLocations.forEach(v => points.push(v));
    await ensureCountriesLoaded(points);

    const zipLookup = new Map<string, string>();
    for (const { lat, lng } of points) {
      const result = getZipcode(lat, lng);
      zipLookup.set(`${lat},${lng}`, result?.postcode || 'UNKNOWN');
    }

    // Build geocoded CSV (ad_id,zipcode) and upload to S3
    const geocodedLines: string[] = ['ad_id,zipcode'];
    let devicesWithZipcode = 0;
    homeMap.forEach(({ lat, lng }, adId) => {
      const zip = zipLookup.get(`${lat},${lng}`) || 'UNKNOWN';
      if (zip !== 'UNKNOWN') devicesWithZipcode++;
      geocodedLines.push(`${adId},${zip}`);
    });

    const geocodedKey = `staging/.temp/${state.handle}-geocoded.csv`;
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: geocodedKey,
      Body: geocodedLines.join('\n'),
      ContentType: 'text/csv',
    }));

    state.geocodedKey = geocodedKey;
    state.status = 'building_csv';
    state.progress = {
      step: 'geocode',
      percent: 70,
      message: `Geocodificadas ${zipLookup.size.toLocaleString()} ubicaciones (${devicesWithZipcode.toLocaleString()} con zip)`,
    };
    await saveActivationState(state);
    console.log(`[ACTIVATE] ${state.datasetName}: geocoded ${zipLookup.size} locations, uploaded ${geocodedKey}`);
    return state;
  }

  // ── Phase 4: Stream MAIDs + geocoded lookup → final CSV → S3 ─────
  if (state.status === 'building_csv') {
    state.progress = { step: 'build_csv', percent: 72, message: 'Construyendo CSV final (streaming)...' };

    // Load geocoded lookup into memory (it's the filtered subset, manageable)
    const geoResponse = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: state.geocodedKey!,
    }));
    const geoCsv = await geoResponse.Body?.transformToString() || '';
    const geoMap = new Map<string, string>();
    const geoLines = geoCsv.split('\n');
    for (let i = 1; i < geoLines.length; i++) {
      const line = geoLines[i].trim();
      if (!line) continue;
      const [adId, zip] = line.split(',');
      if (adId) geoMap.set(adId, zip || 'UNKNOWN');
    }

    // Stream MAIDs from S3 → join with geoMap → stream upload to S3
    const maidsOutputKey = `athena-results/${state.maidsQueryId}.csv`;
    const maidsResponse = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: maidsOutputKey,
    }));

    const csvKey = `staging/${state.handle}.csv`;
    const specKey = `staging/${state.handle}.csv.spec.yml`;

    const outputStream = new PassThrough();
    const upload = new Upload({
      client: s3Client,
      params: { Bucket: BUCKET, Key: csvKey, Body: outputStream, ContentType: 'text/csv' },
      partSize: 5 * 1024 * 1024,
      queueSize: 4,
    });

    outputStream.write('maid,country,zipcode\n');

    const rl = createInterface({ input: maidsResponse.Body as Readable, crlfDelay: Infinity });
    let isHeader = true;
    let deviceCount = 0;
    let devicesWithZipcode = 0;
    for await (const line of rl) {
      if (isHeader) { isHeader = false; continue; }
      const adId = line.trim().replace(/"/g, '');
      if (!adId) continue;
      const zip = geoMap.get(adId) || 'UNKNOWN';
      if (zip !== 'UNKNOWN') devicesWithZipcode++;
      outputStream.write(`${adId},${state.countryCode},${zip}\n`);
      deviceCount++;
    }

    outputStream.end();
    await upload.done();

    // Upload spec
    const specContent = 'identifiers:\n  - name: maid\n    type: uuid-maid\nattributes:\n  - country\n  - zipcode\n';
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: specKey,
      Body: specContent,
      ContentType: 'text/yaml',
    }));

    // Clean up temp geocoded file
    try {
      await s3Client.send(new PutObjectCommand({ Bucket: BUCKET, Key: state.geocodedKey!, Body: '' }));
    } catch { /* ignore cleanup errors */ }

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
