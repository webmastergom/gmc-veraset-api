/**
 * Dataset export using AWS Athena
 */

import { runQuery, createTableForDataset, tableExists, getTableName, startQueryAndWait } from './athena';
import { PutObjectCommand, CopyObjectCommand, HeadObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from './s3-config';
import { getZipcode, ensureCountriesLoaded } from './reverse-geocode';

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

  // 2. Download home locations CSV and build lookup map
  const homeResponse = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: homeResult.outputCsvKey,
  }));
  const homeCsv = await homeResponse.Body?.transformToString() || '';

  const homeMap = new Map<string, { lat: number; lng: number }>();
  const homeLines = homeCsv.split('\n');
  for (let i = 1; i < homeLines.length; i++) {
    const line = homeLines[i].trim();
    if (!line) continue;
    const parts = line.replace(/"/g, '').split(',');
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

  // 4. Download MAIDs CSV and build final CSV with maid,country,zipcode
  progress('build_csv', 72, 'Descargando lista de MAIDs...');
  const maidsResponse = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: maidsResult.outputCsvKey,
  }));
  const maidsCsv = await maidsResponse.Body?.transformToString() || '';

  const country = countryCode || '';
  const csvLines: string[] = ['maid,country,zipcode'];
  const maidsLines = maidsCsv.split('\n');
  let devicesWithZipcode = 0;
  for (let i = 1; i < maidsLines.length; i++) {
    const line = maidsLines[i].trim();
    if (!line) continue;
    const adId = line.replace(/"/g, '');
    if (!adId) continue;

    const home = homeMap.get(adId);
    let zipcode = 'UNKNOWN';
    if (home) {
      zipcode = zipLookup.get(`${home.lat},${home.lng}`) || 'UNKNOWN';
      if (zipcode !== 'UNKNOWN') devicesWithZipcode++;
    }
    csvLines.push(`${adId},${country},${zipcode}`);
  }

  const deviceCount = csvLines.length - 1;
  progress('build_csv', 80, `CSV listo: ${deviceCount.toLocaleString()} MAIDs, ${devicesWithZipcode.toLocaleString()} con zip code`);

  // 5. Upload CSV + spec to staging
  const handle = sanitizeFolderName(jobName) || datasetName;
  const csvKey = `staging/${handle}.csv`;
  const specKey = `staging/${handle}.csv.spec.yml`;

  progress('upload', 85, 'Subiendo CSV a staging...');
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: csvKey,
    Body: csvLines.join('\n'),
    ContentType: 'text/csv',
  }));

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
