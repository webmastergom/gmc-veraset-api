/**
 * Dataset export using AWS Athena
 */

import { runQuery, createTableForDataset, tableExists, getTableName, startQueryAndWait } from './athena';
import { PutObjectCommand, CopyObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from './s3-config';

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
  s3Path: string;
  folderName: string;
  createdAt: string;
}

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
 * Export all MAIDs from a dataset to staging/activations/{folderName}/maids.csv.
 *
 * Instead of loading millions of rows into memory, this:
 * 1. Runs a COUNT query for the device count (returns 1 row).
 * 2. Runs a SELECT DISTINCT query and waits for Athena to finish.
 * 3. Copies the Athena result CSV directly to the activations folder via S3 CopyObject.
 *
 * This works for datasets of any size without OOM or timeout issues.
 */
export async function activateDevices(
  datasetName: string,
  jobName: string
): Promise<ActivationResult> {
  const tableName = getTableName(datasetName);

  if (!(await tableExists(datasetName))) {
    await createTableForDataset(datasetName);
  }

  // 1. Get device count (1 row result, fast)
  const countSql = `SELECT COUNT(DISTINCT ad_id) as cnt FROM ${tableName} WHERE ad_id IS NOT NULL AND ad_id != ''`;
  const countResult = await runQuery(countSql);
  const deviceCount = parseInt(String(countResult.rows[0]?.cnt)) || 0;
  console.log(`[ACTIVATE] ${datasetName}: ${deviceCount} distinct MAIDs`);

  // 2. Run the DISTINCT query — wait for completion but don't fetch rows
  const distinctSql = `SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND ad_id != ''`;
  const { outputCsvKey } = await startQueryAndWait(distinctSql);

  // 3. Copy the Athena result CSV directly to the activations folder
  const folderName = sanitizeFolderName(jobName) || datasetName;
  const activationKey = `staging/activations/${folderName}/maids.csv`;

  await s3Client.send(new CopyObjectCommand({
    Bucket: BUCKET,
    CopySource: `${BUCKET}/${outputCsvKey}`,
    Key: activationKey,
    ContentType: 'text/csv',
    MetadataDirective: 'REPLACE',
    Metadata: {
      'activation-dataset': datasetName,
      'activation-job-name': jobName,
      'activation-maid-count': String(deviceCount),
      'activation-created': new Date().toISOString(),
    },
  }));

  console.log(`[ACTIVATE] Copied ${deviceCount} MAIDs to s3://${BUCKET}/${activationKey}`);

  return {
    success: true,
    deviceCount,
    s3Path: `s3://${BUCKET}/${activationKey}`,
    folderName,
    createdAt: new Date().toISOString(),
  };
}
