/**
 * AWS Athena client for querying Parquet files in S3
 */

import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  QueryExecutionState,
} from '@aws-sdk/client-athena';
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';

const athena = new AthenaClient({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const DATABASE = 'veraset';
const OUTPUT_LOCATION = process.env.ATHENA_OUTPUT_LOCATION || 
  `s3://${process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2'}/athena-results/`;
const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

export interface QueryResult {
  columns: string[];
  rows: Record<string, any>[];
}

/**
 * Run an Athena SQL query and return results
 */
export async function runQuery(sql: string): Promise<QueryResult> {
  try {
    // Check if AWS credentials are configured
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      throw new Error('AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.');
    }

    // Start query
    const startRes = await athena.send(new StartQueryExecutionCommand({
      QueryString: sql,
      QueryExecutionContext: { Database: DATABASE },
      ResultConfiguration: { OutputLocation: OUTPUT_LOCATION },
    }));

    const queryId = startRes.QueryExecutionId;
    if (!queryId) {
      throw new Error('No query execution ID returned from Athena');
    }

    console.log(`🚀 Started Athena query: ${queryId}`);
    console.log(`   Query preview: ${sql.substring(0, 200)}${sql.length > 200 ? '...' : ''}`);

    // Poll for completion
    let state: QueryExecutionState = 'QUEUED';
    let attempts = 0;
    const maxAttempts = 600; // 5 minutes max (500ms * 600) - Athena queries can take time
    const logInterval = 20; // Log every 10 seconds

    while ((state === 'QUEUED' || state === 'RUNNING') && attempts < maxAttempts) {
      await new Promise(r => setTimeout(r, 500));
      attempts++;

      const statusRes = await athena.send(new GetQueryExecutionCommand({
        QueryExecutionId: queryId,
      }));

      state = statusRes.QueryExecution?.Status?.State as QueryExecutionState;
      
      // Log progress periodically
      if (attempts % logInterval === 0) {
        const elapsedSeconds = Math.floor(attempts * 0.5);
        console.log(`   ⏳ Query ${queryId}: ${state} (${elapsedSeconds}s elapsed)`);
        
        // If stuck in QUEUED for too long, log warning
        if (state === 'QUEUED' && elapsedSeconds > 30) {
          console.warn(`   ⚠️ Query ${queryId} has been QUEUED for ${elapsedSeconds}s. This may indicate Athena is overloaded or there are permission issues.`);
        }
      }

      if (state === 'FAILED') {
        const reason = statusRes.QueryExecution?.Status?.StateChangeReason || 'Query failed';
        const queryString = statusRes.QueryExecution?.Query || '';
        const statistics = statusRes.QueryExecution?.Statistics;
        console.error(`❌ Athena query ${queryId} failed:`);
        console.error(`   Reason: ${reason}`);
        console.error(`   Query: ${queryString.substring(0, 500)}${queryString.length > 500 ? '...' : ''}`);
        if (statistics) {
          console.error(`   Statistics:`, JSON.stringify(statistics, null, 2));
        }
        throw new Error(`Athena query failed: ${reason}`);
      }

      if (state === 'CANCELLED') {
        console.error(`❌ Athena query ${queryId} was cancelled`);
        throw new Error('Athena query was cancelled');
      }
    }

    if (state !== 'SUCCEEDED') {
      const elapsedSeconds = Math.floor(attempts * 0.5);
      const errorMsg = `Query ${queryId} did not complete within ${elapsedSeconds}s. Final state: ${state}`;
      console.error(`❌ ${errorMsg}`);
      console.error(`   Query: ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
      console.error(`   You can check the query status in AWS Console: https://console.aws.amazon.com/athena/home?region=${process.env.AWS_REGION || 'us-west-2'}#query-history`);
      throw new Error(errorMsg);
    }

    console.log(`✅ Athena query ${queryId} completed successfully (${Math.floor(attempts * 0.5)}s)`);

    // Get results
    const resultsRes = await athena.send(new GetQueryResultsCommand({
      QueryExecutionId: queryId,
    }));

    const rows = resultsRes.ResultSet?.Rows || [];
    if (rows.length === 0) {
      return { columns: [], rows: [] };
    }

    // First row is headers
    const columns = rows[0].Data?.map(d => d.VarCharValue || '') || [];

    // Remaining rows are data
    const data = rows.slice(1).map(row => {
      const obj: Record<string, any> = {};
      row.Data?.forEach((d, i) => {
        const value = d.VarCharValue;
        const columnName = columns[i];
        
        // Keep dates as strings (they come formatted from Athena)
        if (columnName === 'date' && value) {
          obj[columnName] = value;
        } else if (value !== null && value !== undefined) {
          // Try to parse numbers
          const numValue = parseFloat(value);
          obj[columnName] = isNaN(numValue) ? value : numValue;
        } else {
          obj[columnName] = null;
        }
      });
      return obj;
    });

    return { columns, rows: data };
  } catch (error: any) {
    console.error('Athena query error:', error);
    
    // Provide more helpful error messages
    if (error.name === 'InvalidRequestException') {
      if (error.message?.includes('Database') || error.message?.includes('not found')) {
        throw new Error(`Athena database '${DATABASE}' not found. Please create it in AWS Glue first.`);
      }
      if (error.message?.includes('does not exist')) {
        throw new Error(`Table does not exist. Error: ${error.message}`);
      }
    }
    
    if (error.name === 'AccessDeniedException') {
      throw new Error(`Access denied to Athena. Please check IAM permissions. Error: ${error.message}`);
    }
    
    throw error;
  }
}

/**
 * Discover partitions from S3 by listing objects
 */
async function discoverPartitionsFromS3(datasetName: string): Promise<string[]> {
  const prefix = `${datasetName}/`;
  const partitions = new Set<string>();
  
  try {
    console.log(`🔍 Discovering partitions for ${datasetName} from S3 prefix: ${prefix}`);
    let continuationToken: string | undefined;
    let pageCount = 0;
    
    do {
      pageCount++;
      const listRes = await s3Client.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        Delimiter: '/',
        MaxKeys: 1000, // Get more results per page
      }));
      
      // Extract partition dates from common prefixes
      if (listRes.CommonPrefixes) {
        console.log(`   Page ${pageCount}: Found ${listRes.CommonPrefixes.length} common prefixes`);
        for (const prefixObj of listRes.CommonPrefixes) {
          const prefixPath = prefixObj.Prefix || '';
          const match = prefixPath.match(/date=(\d{4}-\d{2}-\d{2})/);
          if (match) {
            partitions.add(match[1]);
          }
        }
      }
      
      // Also check individual objects for date patterns (in case Delimiter didn't catch them)
      if (listRes.Contents) {
        console.log(`   Page ${pageCount}: Found ${listRes.Contents.length} objects`);
        for (const obj of listRes.Contents) {
          const key = obj.Key || '';
          const match = key.match(/date=(\d{4}-\d{2}-\d{2})/);
          if (match) {
            partitions.add(match[1]);
          }
        }
      }
      
      continuationToken = listRes.NextContinuationToken;
      if (continuationToken) {
        console.log(`   Page ${pageCount}: More results available, continuing...`);
      }
    } while (continuationToken);
    
    const sortedPartitions = Array.from(partitions).sort();
    console.log(`✅ Discovered ${sortedPartitions.length} partitions: ${sortedPartitions.slice(0, 5).join(', ')}${sortedPartitions.length > 5 ? '...' : ''}`);
    
    return sortedPartitions;
  } catch (error: any) {
    console.error(`❌ Error discovering partitions from S3:`, error.message || error);
    return [];
  }
}

/**
 * Add partitions manually to ensure all data is accessible
 */
async function addPartitionsManually(tableName: string, datasetName: string, partitions: string[]): Promise<void> {
  if (partitions.length === 0) {
    console.log(`No partitions found in S3 for ${datasetName}`);
    return;
  }
  
  console.log(`📦 Found ${partitions.length} partitions in S3, adding to table...`);
  
  // Add partitions in batches (Athena has limits)
  const batchSize = 50;
  for (let i = 0; i < partitions.length; i += batchSize) {
    const batch = partitions.slice(i, i + batchSize);
    const partitionSpecs = batch.map(date => 
      `PARTITION (date='${date}') LOCATION 's3://${BUCKET}/${datasetName}/date=${date}/'`
    ).join(', ');
    
    try {
      await runQuery(`ALTER TABLE ${tableName} ADD IF NOT EXISTS ${partitionSpecs}`);
      console.log(`✅ Added ${batch.length} partitions (${i + 1}-${Math.min(i + batch.length, partitions.length)}/${partitions.length})`);
    } catch (error: any) {
      // Ignore "partition already exists" errors
      if (!error.message?.includes('already exists')) {
        console.warn(`Warning adding partitions batch ${i / batchSize + 1}:`, error.message);
      }
    }
  }
}

/**
 * Create an external table for a dataset in Athena/Glue
 * Ensures all partitions are loaded so all Parquet files are accessible
 */
export async function createTableForDataset(datasetName: string): Promise<void> {
  const tableName = getTableName(datasetName);

  // First, discover partitions from S3
  console.log(`🔍 Discovering partitions for ${datasetName} from S3...`);
  const s3Partitions = await discoverPartitionsFromS3(datasetName);
  console.log(`📅 Found ${s3Partitions.length} partitions in S3:`, s3Partitions.slice(0, 10).join(', '), s3Partitions.length > 10 ? '...' : '');

  const sql = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} (
      ad_id STRING,
      utc_timestamp TIMESTAMP,
      horizontal_accuracy DOUBLE,
      id_type STRING,
      ip_address STRING,
      latitude DOUBLE,
      longitude DOUBLE,
      iso_country_code STRING,
      poi_ids ARRAY<STRING>
    )
    PARTITIONED BY (date STRING)
    STORED AS PARQUET
    LOCATION 's3://${BUCKET}/${datasetName}/'
  `;

  try {
    await runQuery(sql);
    console.log(`✅ Created/verified table: ${tableName}`);
    
    // Try MSCK REPAIR first (fastest if it works)
    try {
      await runQuery(`MSCK REPAIR TABLE ${tableName}`);
      console.log(`✅ Repaired partitions with MSCK REPAIR`);
    } catch (repairError: any) {
      console.warn(`MSCK REPAIR failed:`, repairError.message);
      // Fall back to manual partition addition
      if (s3Partitions.length > 0) {
        await addPartitionsManually(tableName, datasetName, s3Partitions);
      }
    }
    
    // Verify partitions are accessible by checking row count
    try {
      const countRes = await runQuery(`SELECT COUNT(*) as total FROM ${tableName} LIMIT 1`);
      const totalRows = countRes.rows[0]?.total || 0;
      console.log(`📊 Table ${tableName} has ${totalRows.toLocaleString()} total rows`);
      
      // Also check partition count
      const partitionRes = await runQuery(`SELECT COUNT(DISTINCT date) as partition_count FROM ${tableName}`);
      const partitionCount = partitionRes.rows[0]?.partition_count || 0;
      console.log(`📅 Table ${tableName} has ${partitionCount} partitions loaded`);
      
      if (totalRows === 0) {
        console.warn(`⚠️  Warning: Table ${tableName} appears empty. Expected ${s3Partitions.length} partitions from S3.`);
      } else if (partitionCount < s3Partitions.length) {
        console.warn(`⚠️  Warning: Only ${partitionCount}/${s3Partitions.length} partitions loaded. Trying manual addition...`);
        await addPartitionsManually(tableName, datasetName, s3Partitions);
      }
    } catch (countError) {
      console.warn(`Could not verify row count:`, countError);
    }
  } catch (error: any) {
    // If table exists with different schema, try to repair partitions
    if (error.message?.includes('already exists')) {
      console.log(`Table ${tableName} already exists, repairing partitions...`);
      try {
        await runQuery(`MSCK REPAIR TABLE ${tableName}`);
        console.log(`✅ Repaired partitions for ${tableName}`);
      } catch (repairError) {
        console.warn(`MSCK REPAIR failed, trying manual partition addition...`);
        if (s3Partitions.length > 0) {
          await addPartitionsManually(tableName, datasetName, s3Partitions);
        }
      }
    } else {
      throw error;
    }
  }
}

/**
 * Check if a table exists in Athena
 */
export async function tableExists(datasetName: string): Promise<boolean> {
  const tableName = getTableName(datasetName);
  try {
    await runQuery(`DESCRIBE ${tableName}`);
    return true;
  } catch (error: any) {
    // If it's a "table not found" error, return false
    if (error.message?.includes('does not exist') || 
        error.message?.includes('Table not found') ||
        error.message?.includes('Table veraset.') ||
        error.name === 'InvalidRequestException') {
      return false;
    }
    // For access/permission errors, re-throw with helpful message
    if (error.message?.includes('not authorized') || 
        error.message?.includes('Access denied') ||
        error.name === 'AccessDeniedException') {
      throw error; // Will be caught and handled with better message
    }
    // For other errors, re-throw
    throw error;
  }
}

/**
 * Get table name for a dataset
 * Adds 'ds_' prefix to ensure table name starts with a letter (required by SQL)
 */
export function getTableName(datasetName: string): string {
  const sanitized = datasetName.replace(/-/g, '_').replace(/[^a-z0-9_]/gi, '_');
  // SQL table names cannot start with a number, so add prefix if needed
  return /^[0-9]/.test(sanitized) ? `ds_${sanitized}` : sanitized;
}
