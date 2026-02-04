/**
 * Dataset export using AWS Athena
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';
import { S3Client, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';

const s3 = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

export interface ExportFilters {
  minDwellTime?: number | null;  // seconds
  minPings?: number | null;
  dateFrom?: string;
  dateTo?: string;
  poiIds?: string[];
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

/**
 * Export devices matching filters using Athena
 */
export async function exportDevices(
  datasetName: string,
  filters: ExportFilters = {}
): Promise<ExportResult> {
  const tableName = getTableName(datasetName);

  // Ensure table exists
  if (!(await tableExists(datasetName))) {
    await createTableForDataset(datasetName);
  }

  // Build date filter
  const dateConditions: string[] = [];
  if (filters.dateFrom) {
    dateConditions.push(`date >= '${filters.dateFrom}'`);
  }
  if (filters.dateTo) {
    dateConditions.push(`date <= '${filters.dateTo}'`);
  }
  const dateWhere = dateConditions.length ? `WHERE ${dateConditions.join(' AND ')}` : '';

  // Build POI filter
  let poiFilter = '';
  if (filters.poiIds?.length) {
    const poiList = filters.poiIds.map(p => {
      const escaped = p.replace(/'/g, "''");
      return `'${escaped}'`;
    }).join(',');
    poiFilter = `AND poi_ids[1] IN (${poiList})`;
  }

  // Build HAVING clause for dwell/ping filters
  const havingConditions: string[] = [];
  
  // If minDwellTime is null, include all devices
  // If minDwellTime is 0, require some dwell (dwell_seconds > 0)
  // If minDwellTime > 0, require at least that many seconds
  if (filters.minDwellTime !== null && filters.minDwellTime !== undefined) {
    if (filters.minDwellTime === 0) {
      havingConditions.push('dwell_seconds > 0');
    } else {
      havingConditions.push(`dwell_seconds >= ${filters.minDwellTime}`);
    }
  }
  
  if (filters.minPings !== null && filters.minPings !== undefined) {
    havingConditions.push(`ping_count >= ${filters.minPings}`);
  }
  
  const havingClause = havingConditions.length ? `HAVING ${havingConditions.join(' AND ')}` : '';

  // Query to get qualifying devices
  let sql: string;
  
  if (filters.minDwellTime === null && filters.minPings === null) {
    // Simple query for all devices
    sql = `
      SELECT DISTINCT ad_id
      FROM ${tableName}
      ${dateWhere}
      ${poiFilter ? `AND poi_ids[1] IS NOT NULL ${poiFilter}` : ''}
    `;
  } else {
    // Need to calculate dwell time
    sql = `
      WITH device_stats AS (
        SELECT 
          ad_id,
          poi_ids[1] as poi_id,
          COUNT(*) as ping_count,
          date_diff('second', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell_seconds
        FROM ${tableName}
        ${dateWhere}
        ${poiFilter ? `AND poi_ids[1] IS NOT NULL ${poiFilter}` : 'WHERE poi_ids[1] IS NOT NULL'}
        GROUP BY ad_id, poi_ids[1]
        ${havingClause}
      )
      SELECT DISTINCT ad_id
      FROM device_stats
    `;
  }

  const result = await runQuery(sql);
  const deviceCount = result.rows.length;

  // Get total devices for comparison
  const totalRes = await runQuery(`
    SELECT COUNT(DISTINCT ad_id) as total
    FROM ${tableName}
    ${dateWhere}
  `);
  const totalDevices = parseInt(String(totalRes.rows[0]?.total)) || 0;

  // Export to CSV in S3
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const fileName = `${datasetName}-export-${timestamp}.csv`;
  const exportKey = `exports/${fileName}`;

  // Create CSV content
  const csvContent = 'ad_id\n' + result.rows.map(r => r.ad_id).join('\n');

  // Upload to S3
  const { PutObjectCommand } = await import('@aws-sdk/client-s3');
  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: exportKey,
    Body: csvContent,
    ContentType: 'text/csv',
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
