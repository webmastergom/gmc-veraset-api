/**
 * Dataset analysis using AWS Athena
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';

export interface AnalysisFilters {
  dateFrom?: string;
  dateTo?: string;
  poiIds?: string[];
}

export interface AnalysisResult {
  dataset: string;
  analyzedAt: string;
  filters: AnalysisFilters;
  summary: {
    totalPings: number;
    uniqueDevices: number;
    uniquePois: number;
    dateRange: { from: string; to: string };
    daysAnalyzed: number;
  };
  deviceTypes: Record<string, number>;
  dailyActivity: Array<{ date: string; pings: number; devices: number }>;
  dwellDistribution: Record<string, number>;
  topPois: Array<{ poiId: string; pings: number; devices: number }>;
}

/**
 * Analyze a dataset with optional filters
 */
export async function analyzeDataset(
  datasetName: string,
  filters: AnalysisFilters = {}
): Promise<AnalysisResult> {
  const tableName = getTableName(datasetName);

  // Check if AWS credentials are configured
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error(
      'AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.'
    );
  }

  // Ensure table exists
  let tableExistsResult = false;
  try {
    tableExistsResult = await tableExists(datasetName);
  } catch (error: any) {
    // If it's an access denied error, provide helpful instructions
    if (error.message?.includes('not authorized') || 
        error.message?.includes('Access denied') ||
        error.name === 'AccessDeniedException') {
      throw new Error(
        `Athena access denied. Please add the following IAM permissions to your AWS user:\n\n` +
        `1. Athena: StartQueryExecution, GetQueryExecution, GetQueryResults\n` +
        `2. Glue: GetDatabase, CreateTable, GetTable, BatchCreatePartition\n` +
        `3. S3: GetObject, PutObject, ListBucket (on garritz-veraset-data-us-west-2)\n\n` +
        `See ATHENA_SETUP.md for detailed instructions.\n\n` +
        `Original error: ${error.message}`
      );
    }
    throw error;
  }

  if (!tableExistsResult) {
    console.log(`Creating table for dataset: ${datasetName}`);
    try {
      await createTableForDataset(datasetName);
    } catch (error: any) {
      if (error.message?.includes('not authorized') || 
          error.message?.includes('Access denied')) {
        throw new Error(
          `Cannot create Athena table: Access denied. Please ensure your IAM user has Glue CreateTable permissions.\n\n` +
          `See ATHENA_SETUP.md for setup instructions.\n\n` +
          `Original error: ${error.message}`
        );
      }
      throw error;
    }
  }

  // Build WHERE clause - use DATE(utc_timestamp) to filter by actual data dates
  const conditions: string[] = [];
  if (filters.dateFrom) {
    conditions.push(`DATE(utc_timestamp) >= DATE '${filters.dateFrom}'`);
  }
  if (filters.dateTo) {
    conditions.push(`DATE(utc_timestamp) <= DATE '${filters.dateTo}'`);
  }
  if (filters.poiIds?.length) {
    const poiList = filters.poiIds.map(p => {
      const escaped = p.replace(/'/g, "''");
      return `'${escaped}'`;
    }).join(',');
    conditions.push(`poi_ids[1] IN (${poiList})`);
  }
  const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  
  // Ensure WHERE clause is properly formatted (no double WHERE)
  const whereClauseSafe = whereClause;

  // First, verify we're reading all partitions
  console.log(`🔍 Analyzing dataset: ${datasetName} (table: ${tableName})`);
  
  // Check partition count and sample data - this helps verify all partitions are loaded
  // Note: Querying partition column directly can sometimes cause issues in Athena
  // We'll skip this check if it fails and continue with the analysis
  try {
    const partitionCheck = await runQuery(`
      SELECT 
        date as partition_date, 
        COUNT(*) as row_count, 
        COUNT(DISTINCT ad_id) as unique_devices
      FROM ${tableName}
      GROUP BY date
      ORDER BY date
      LIMIT 20
    `);
    console.log(`📅 Found partitions in table (showing first 20):`);
    partitionCheck.rows.forEach((r: any) => {
      console.log(`   - ${r.partition_date}: ${parseInt(String(r.row_count)).toLocaleString()} rows, ${parseInt(String(r.unique_devices)).toLocaleString()} unique devices`);
    });
  } catch (err: any) {
    console.warn('Could not check partitions (this is OK, continuing with analysis):', err.message || err);
    // Don't throw - this is just for logging, analysis can continue
  }

  // Run queries in parallel (with error handling per query)
  let summaryRes, deviceTypesRes, dailyRes, dwellRes, topPoisRes;
  
  console.log(`📊 Running 5 analysis queries in parallel for ${datasetName}...`);
  
  // Helper to wrap queries with error handling and logging
  const runQueryWithName = async (name: string, sql: string) => {
    try {
      console.log(`   🔄 Starting query: ${name}`);
      const result = await runQuery(sql);
      console.log(`   ✅ Completed query: ${name} (${result.rows.length} rows)`);
      return result;
    } catch (error: any) {
      console.error(`   ❌ Failed query: ${name}`);
      console.error(`   Error: ${error.message}`);
      throw new Error(`${name} query failed: ${error.message}`);
    }
  };
  
  try {
    [summaryRes, deviceTypesRes, dailyRes, dwellRes, topPoisRes] = await Promise.all([
    // Summary stats - use DATE(utc_timestamp) to ensure all parquet files are integrated
    // Also count from both partition date and utc_timestamp to verify consistency
    runQueryWithName('Summary', `
      SELECT 
        COUNT(*) as total_pings,
        COUNT(DISTINCT ad_id) as unique_devices,
        COUNT(DISTINCT poi_ids[1]) as unique_pois,
        MIN(DATE(utc_timestamp)) as min_date,
        MAX(DATE(utc_timestamp)) as max_date,
        COUNT(DISTINCT DATE(utc_timestamp)) as days_analyzed,
        COUNT(DISTINCT date) as partition_count
      FROM ${tableName}
      ${whereClause}
    `),

    // Device types
    runQueryWithName('Device Types', `
      SELECT id_type, COUNT(DISTINCT ad_id) as count
      FROM ${tableName}
      ${whereClause}
      GROUP BY id_type
    `),

    // Daily activity - extract date from utc_timestamp and aggregate ALL parquet files by day
    // This ensures all data across all partitions is summed per day
    runQueryWithName('Daily Activity', `
      SELECT 
        date_format(DATE(utc_timestamp), '%Y-%m-%d') as date,
        COUNT(*) as pings,
        COUNT(DISTINCT ad_id) as devices
      FROM ${tableName}
      ${whereClause ? whereClause + ' AND utc_timestamp IS NOT NULL' : 'WHERE utc_timestamp IS NOT NULL'}
      GROUP BY DATE(utc_timestamp)
      ORDER BY DATE(utc_timestamp) ASC
    `),

    // Dwell distribution - use date_diff with correct Presto/Athena syntax
    runQueryWithName('Dwell Distribution', `
      WITH device_dwell AS (
        SELECT 
          ad_id,
          poi_ids[1] as poi_id,
          date_diff('second', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell_seconds
        FROM ${tableName}
        ${whereClause ? whereClause + ' AND poi_ids[1] IS NOT NULL' : 'WHERE poi_ids[1] IS NOT NULL'}
        GROUP BY ad_id, poi_ids[1]
        HAVING MAX(utc_timestamp) > MIN(utc_timestamp)
      ),
      dwell_ranges AS (
        SELECT
          CASE
            WHEN dwell_seconds < 30 THEN '0-30s'
            WHEN dwell_seconds < 60 THEN '30s-1m'
            WHEN dwell_seconds < 300 THEN '1-5m'
            WHEN dwell_seconds < 900 THEN '5-15m'
            WHEN dwell_seconds < 3600 THEN '15-60m'
            ELSE '60m+'
          END as dwell_range
        FROM device_dwell
      )
      SELECT
        dwell_range,
        COUNT(*) as count
      FROM dwell_ranges
      GROUP BY dwell_range
      ORDER BY 
        CASE dwell_range
          WHEN '0-30s' THEN 1
          WHEN '30s-1m' THEN 2
          WHEN '1-5m' THEN 3
          WHEN '5-15m' THEN 4
          WHEN '15-60m' THEN 5
          WHEN '60m+' THEN 6
          ELSE 7
        END
    `),

    // Top POIs
    runQueryWithName('Top POIs', `
      SELECT 
        poi_ids[1] as poi_id,
        COUNT(*) as pings,
        COUNT(DISTINCT ad_id) as devices
      FROM ${tableName}
      ${whereClause ? whereClause + ' AND poi_ids[1] IS NOT NULL' : 'WHERE poi_ids[1] IS NOT NULL'}
      GROUP BY poi_ids[1]
      ORDER BY COUNT(DISTINCT ad_id) DESC
      LIMIT 20
    `),
    ]);
  } catch (error: any) {
    console.error('❌ Error running analysis queries:', error);
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);
    // Re-throw with more context
    throw new Error(`Analysis query failed: ${error.message || 'Unknown error'}`);
  }

  // Parse results
  const summary = summaryRes.rows[0] || {};
  
  // Log summary for debugging
  console.log(`📊 Analysis summary for ${datasetName}:`);
  console.log(`   Total pings: ${parseInt(String(summary.total_pings)).toLocaleString()}`);
  console.log(`   Unique devices: ${parseInt(String(summary.unique_devices)).toLocaleString()}`);
  console.log(`   Unique POIs: ${parseInt(String(summary.unique_pois)).toLocaleString()}`);
  console.log(`   Date range: ${summary.min_date} to ${summary.max_date}`);
  console.log(`   Days analyzed: ${summary.days_analyzed}`);
  console.log(`   Partitions found: ${summary.partition_count || 'N/A'}`);

  const deviceTypes: Record<string, number> = {};
  deviceTypesRes.rows.forEach(r => {
    if (r.id_type) {
      deviceTypes[r.id_type] = parseInt(String(r.count)) || 0;
    }
  });
  
  console.log(`📱 Device types:`, deviceTypes);
  console.log(`📅 Daily activity records: ${dailyRes.rows.length}`);

  // Process daily activity - ensure dates are properly formatted
  const dailyActivity = dailyRes.rows
    .map(r => {
      // Get date string from Athena result
      let dateStr = String(r.date || '').trim();
      
      // Normalize date format to YYYY-MM-DD
      if (!dateStr) {
        return null;
      }
      
      // Handle different formats from Athena
      if (dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
        // Already in YYYY-MM-DD format - perfect!
        dateStr = dateStr;
      } else if (dateStr.includes('T')) {
        // ISO format: "2026-01-05T00:00:00.000Z" -> "2026-01-05"
        dateStr = dateStr.split('T')[0];
      } else {
        // Try to parse and reformat
        try {
          const date = new Date(dateStr);
          if (!isNaN(date.getTime())) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            dateStr = `${year}-${month}-${day}`;
          } else {
            console.warn(`Invalid date format: ${dateStr}`);
            return null;
          }
        } catch (e) {
          console.warn(`Error parsing date: ${dateStr}`, e);
          return null;
        }
      }
      
      return {
        date: dateStr,
        pings: parseInt(String(r.pings)) || 0,
        devices: parseInt(String(r.devices)) || 0,
      };
    })
    .filter((item): item is { date: string; pings: number; devices: number } => item !== null)
    .sort((a, b) => a.date.localeCompare(b.date)); // Ensure sorted by date

  const dwellDistribution: Record<string, number> = {
    '0-30s': 0,
    '30s-1m': 0,
    '1-5m': 0,
    '5-15m': 0,
    '15-60m': 0,
    '60m+': 0,
  };
  dwellRes.rows.forEach(r => {
    if (r.dwell_range) {
      dwellDistribution[r.dwell_range] = parseInt(String(r.count)) || 0;
    }
  });

  const topPois = topPoisRes.rows.map(r => ({
    poiId: String(r.poi_id || ''),
    pings: parseInt(String(r.pings)) || 0,
    devices: parseInt(String(r.devices)) || 0,
  }));

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    summary: {
      totalPings: parseInt(String(summary.total_pings)) || 0,
      uniqueDevices: parseInt(String(summary.unique_devices)) || 0,
      uniquePois: parseInt(String(summary.unique_pois)) || 0,
      dateRange: {
        from: String(summary.min_date || ''),
        to: String(summary.max_date || ''),
      },
      daysAnalyzed: parseInt(String(summary.days_analyzed)) || 0,
    },
    deviceTypes,
    dailyActivity,
    dwellDistribution,
    topPois,
  };
}
