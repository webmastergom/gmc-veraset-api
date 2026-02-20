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
import { S3Client, ListObjectsV2Command, type ListObjectsV2CommandOutput } from '@aws-sdk/client-s3';

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

/** Columns that must always remain as strings (never coerced to number) */
const STRING_COLUMNS = new Set([
  'ad_id', 'poi_id', 'id_type', 'ip_address', 'iso_country_code',
  'date', 'first_seen', 'last_seen', 'utc_timestamp',
  'horizontal_accuracy', 'latitude', 'longitude',
  'partition', 'result', 'col_name', 'data_type', 'name', 'type', 'comment',
]);

/**
 * Parse an Athena result row into a keyed object.
 *
 * IMPORTANT: Only converts to number when the ENTIRE string is a valid number.
 * parseFloat("38f7a2b1") returns 38 (not NaN!), which would silently corrupt
 * UUIDs and hex-prefixed strings. We use a strict regex test instead.
 */
function parseRow(row: any, columns: string[]): Record<string, any> {
  const obj: Record<string, any> = {};
  row.Data?.forEach((d: any, i: number) => {
    const value = d.VarCharValue;
    const columnName = columns[i];
    if (value === null || value === undefined) {
      obj[columnName] = null;
    } else if (STRING_COLUMNS.has(columnName)) {
      obj[columnName] = value;
    } else if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(value)) {
      // Only convert if the ENTIRE value is a valid number (integer, decimal, or scientific notation)
      obj[columnName] = parseFloat(value);
    } else {
      obj[columnName] = value;
    }
  });
  return obj;
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
    const maxAttempts = 1800; // 15 minutes max (500ms * 1800) - lab queries scan billions of rows
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
        
        // Check if this is an "already exists" error - these are expected and should not be logged as errors
        const isAlreadyExistsError = 
          reason?.includes('already exists') || 
          reason?.includes('Partition already exists') ||
          reason?.includes('already exist') ||
          reason?.includes('AlreadyExistsException') ||
          reason?.includes('AlreadyExists');
        
        if (!isAlreadyExistsError) {
          // Only log real errors
          console.error(`❌ Athena query ${queryId} failed:`);
          console.error(`   Reason: ${reason}`);
          console.error(`   Query: ${queryString.substring(0, 500)}${queryString.length > 500 ? '...' : ''}`);
          if (statistics) {
            console.error(`   Statistics:`, JSON.stringify(statistics, null, 2));
          }
        }
        
        // Always throw the error so callers can handle it, but with a cleaner message for "already exists"
        if (isAlreadyExistsError) {
          throw new Error(`Partition already exists: ${reason}`);
        } else {
          throw new Error(`Athena query failed: ${reason}`);
        }
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

    // Get results with pagination (Athena returns max 1000 rows per page)
    const allDataRows: Record<string, any>[] = [];
    let nextToken: string | undefined;
    let columns: string[] = [];
    let isFirstPage = true;

    do {
      const resultsRes = await athena.send(new GetQueryResultsCommand({
        QueryExecutionId: queryId,
        NextToken: nextToken,
        MaxResults: 1000,
      }));

      const rows = resultsRes.ResultSet?.Rows || [];

      if (isFirstPage) {
        if (rows.length === 0) return { columns: [], rows: [] };
        columns = rows[0].Data?.map((d: any) => d.VarCharValue || '') || [];
        for (let i = 1; i < rows.length; i++) {
          allDataRows.push(parseRow(rows[i], columns));
        }
        isFirstPage = false;
      } else {
        for (const row of rows) {
          allDataRows.push(parseRow(row, columns));
        }
      }

      nextToken = resultsRes.NextToken;
    } while (nextToken);

    console.log(`✅ Athena query ${queryId}: ${allDataRows.length} rows returned (paginated)`);
    return { columns, rows: allDataRows };
  } catch (error: any) {
    // Don't log "already exists" errors - they're expected when adding partitions
    const isAlreadyExistsError =
      error.message?.includes('already exists') ||
      error.message?.includes('Partition already exists') ||
      error.message?.includes('already exist') ||
      error.message?.includes('AlreadyExistsException') ||
      error.message?.includes('AlreadyExists');
    if (!isAlreadyExistsError) {
      console.error('Athena query error:', error);
    }

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

// ── Async Query Functions (fire-and-forget + status check) ───────────────

/**
 * Start an Athena query and return the QueryExecutionId immediately.
 * Does NOT wait for the query to finish — use checkQueryStatus() to poll.
 */
export async function startQueryAsync(sql: string): Promise<string> {
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials not configured.');
  }

  const startRes = await athena.send(new StartQueryExecutionCommand({
    QueryString: sql,
    QueryExecutionContext: { Database: DATABASE },
    ResultConfiguration: { OutputLocation: OUTPUT_LOCATION },
  }));

  const queryId = startRes.QueryExecutionId;
  if (!queryId) throw new Error('No query execution ID returned from Athena');

  console.log(`[ATHENA-ASYNC] Started query: ${queryId}`);
  console.log(`   Query preview: ${sql.substring(0, 200)}${sql.length > 200 ? '...' : ''}`);
  return queryId;
}

/**
 * Check the status of an Athena query without blocking.
 * Returns the current state, output location, and any error.
 */
export async function checkQueryStatus(queryId: string): Promise<{
  state: 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED';
  outputLocation?: string;
  error?: string;
  statistics?: { dataScannedBytes?: number; engineExecutionTimeMs?: number };
}> {
  const statusRes = await athena.send(new GetQueryExecutionCommand({
    QueryExecutionId: queryId,
  }));

  const execution = statusRes.QueryExecution;
  const state = (execution?.Status?.State || 'QUEUED') as any;
  const stats = execution?.Statistics;

  return {
    state,
    outputLocation: execution?.ResultConfiguration?.OutputLocation,
    error: state === 'FAILED'
      ? execution?.Status?.StateChangeReason || 'Query failed'
      : undefined,
    statistics: stats ? {
      dataScannedBytes: Number(stats.DataScannedInBytes) || undefined,
      engineExecutionTimeMs: Number(stats.EngineExecutionTimeInMillis) || undefined,
    } : undefined,
  };
}

/**
 * Fetch paginated results from a completed Athena query.
 * The query must be in SUCCEEDED state — call checkQueryStatus() first.
 */
export async function fetchQueryResults(queryId: string): Promise<QueryResult> {
  const allDataRows: Record<string, any>[] = [];
  let nextToken: string | undefined;
  let columns: string[] = [];
  let isFirstPage = true;

  do {
    const resultsRes = await athena.send(new GetQueryResultsCommand({
      QueryExecutionId: queryId,
      NextToken: nextToken,
      MaxResults: 1000,
    }));

    const rows = resultsRes.ResultSet?.Rows || [];

    if (isFirstPage) {
      if (rows.length === 0) return { columns: [], rows: [] };
      columns = rows[0].Data?.map((d: any) => d.VarCharValue || '') || [];
      for (let i = 1; i < rows.length; i++) {
        allDataRows.push(parseRow(rows[i], columns));
      }
      isFirstPage = false;
    } else {
      for (const row of rows) {
        allDataRows.push(parseRow(row, columns));
      }
    }

    nextToken = resultsRes.NextToken;
  } while (nextToken);

  console.log(`[ATHENA-ASYNC] Fetched ${allDataRows.length} rows for query ${queryId}`);
  return { columns, rows: allDataRows };
}

// ── CTAS (Create Table As Select) Functions ──────────────────────────────

/**
 * Generate a sanitized temp table name from a base + runId.
 * Athena table names must start with a letter and use only [a-z0-9_].
 */
export function tempTableName(base: string, runId: string): string {
  const safe = runId.replace(/-/g, '_');
  return `temp_${base}_${safe}`;
}

/**
 * Start a CTAS query that materializes a SELECT result as Parquet in S3.
 * Returns QueryExecutionId immediately — poll with checkQueryStatus().
 *
 * The result is a new Athena table backed by Parquet files at:
 *   s3://{BUCKET}/athena-temp/{tableName}/
 */
export async function startCTASAsync(
  selectSql: string,
  tableName: string,
): Promise<string> {
  const s3Location = `s3://${BUCKET}/athena-temp/${tableName}/`;

  const ctasSql = `
    CREATE TABLE ${tableName}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = '${s3Location}'
    )
    AS ${selectSql}
  `;

  return startQueryAsync(ctasSql);
}

/**
 * Drop a temporary Athena table (Glue catalog entry only — data stays in S3).
 * Fire-and-forget safe.
 */
export async function dropTempTable(tableName: string): Promise<void> {
  try {
    await runQuery(`DROP TABLE IF EXISTS ${tableName}`);
    console.log(`[ATHENA] Dropped temp table: ${tableName}`);
  } catch (e: any) {
    console.warn(`[ATHENA] Failed to drop temp table ${tableName}:`, e.message);
  }
}

/**
 * Clean up S3 objects created by a CTAS query under athena-temp/{prefix}/.
 * Uses batched DeleteObjects for efficiency.
 */
export async function cleanupTempS3(prefix: string): Promise<void> {
  const { ListObjectsV2Command, DeleteObjectsCommand } = await import('@aws-sdk/client-s3');
  const fullPrefix = `athena-temp/${prefix}/`;

  try {
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
          Delete: {
            Objects: objects.map(o => ({ Key: o.Key! })),
            Quiet: true,
          },
        }));
      }

      continuationToken = listRes.IsTruncated ? listRes.NextContinuationToken : undefined;
    } while (continuationToken);

    console.log(`[ATHENA] Cleaned up S3: ${fullPrefix}`);
  } catch (e: any) {
    console.warn(`[ATHENA] Failed to cleanup S3 ${fullPrefix}:`, e.message);
  }
}

// ── Partition Discovery ─────────────────────────────────────────────────

/**
 * Discover partitions from S3 by listing objects
 * This function ensures ALL partitions are discovered using a dual-pass strategy:
 * 1. CommonPrefixes (fast) - gets partition folders
 * 2. Individual objects scan (thorough) - catches any partitions missed by CommonPrefixes
 * 
 * This dual-pass approach guarantees no partitions are omitted, even in edge cases.
 */
export async function discoverPartitionsFromS3(datasetName: string): Promise<string[]> {
  const prefix = `${datasetName}/`;
  const partitions = new Set<string>();
  
  try {
    console.log(`🔍 [PARTITION DISCOVERY] Starting dual-pass discovery for ${datasetName} from S3 prefix: ${prefix}`);
    
    // PASS 1: Use Delimiter to get common prefixes (most efficient for partition discovery)
    let continuationToken: string | undefined;
    let pageCount = 0;
    let totalPrefixesFound = 0;
    
    console.log(`   [PASS 1] Scanning common prefixes...`);
    do {
      pageCount++;
      const listRes = await s3Client.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        Delimiter: '/',
        MaxKeys: 1000, // Max allowed by S3 API
        ContinuationToken: continuationToken,
      }));
      
      // Extract partition dates from common prefixes
      if (listRes.CommonPrefixes) {
        const prefixesInPage = listRes.CommonPrefixes.length;
        totalPrefixesFound += prefixesInPage;
        for (const prefixObj of listRes.CommonPrefixes) {
          const prefixPath = prefixObj.Prefix || '';
          const match = prefixPath.match(/date=(\d{4}-\d{2}-\d{2})/);
          if (match && match[1]) {
            // Validate date format
            const dateStr = match[1];
            if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
              partitions.add(dateStr);
            }
          }
        }
        
        if (pageCount % 5 === 0 || continuationToken) {
          console.log(`   [PASS 1] Page ${pageCount}: ${prefixesInPage} prefixes, ${partitions.size} unique partitions found`);
        }
      }
      
      continuationToken = listRes.NextContinuationToken;
    } while (continuationToken);
    
    const pass1Count = partitions.size;
    console.log(`   [PASS 1] Complete: Found ${pass1Count} partitions via CommonPrefixes`);
    
    // PASS 2: Scan individual objects to catch any partitions missed by CommonPrefixes
    // This is critical for edge cases where Delimiter might not return all partitions
    console.log(`   [PASS 2] Scanning individual objects for completeness...`);
    continuationToken = undefined;
    pageCount = 0;
    let objectsScanned = 0;
    let newPartitionsFound = 0;
    
    do {
      pageCount++;
      const listRes: ListObjectsV2CommandOutput = await s3Client.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        MaxKeys: 1000, // Max allowed by S3 API
        ContinuationToken: continuationToken,
      }));
      
      if (listRes.Contents) {
        const objectsInPage = listRes.Contents.length;
        objectsScanned += objectsInPage;
        
        for (const obj of listRes.Contents) {
          const key = obj.Key || '';
          // Skip directory placeholders
          if (key.endsWith('/')) continue;
          
          const match = key.match(/date=(\d{4}-\d{2}-\d{2})/);
          if (match && match[1]) {
            const dateStr = match[1];
            // Validate date format
            if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
              const wasNew = !partitions.has(dateStr);
              partitions.add(dateStr);
              if (wasNew) {
                newPartitionsFound++;
              }
            }
          }
        }
        
        if (pageCount % 10 === 0) {
          console.log(`   [PASS 2] Scanned ${objectsScanned} objects, ${partitions.size} total partitions (${newPartitionsFound} new from this pass)`);
        }
      }
      
      continuationToken = listRes.NextContinuationToken;
    } while (continuationToken);
    
    console.log(`   [PASS 2] Complete: Scanned ${objectsScanned} objects, found ${newPartitionsFound} additional partitions`);
    
    const sortedPartitions = Array.from(partitions).sort();
    const totalPartitions = sortedPartitions.length;
    
    console.log(`✅ [PARTITION DISCOVERY] Total: ${totalPartitions} unique partitions discovered`);
    if (totalPartitions > 0) {
      console.log(`   Date range: ${sortedPartitions[0]} to ${sortedPartitions[totalPartitions - 1]}`);
      if (totalPartitions <= 10) {
        console.log(`   All partitions: ${sortedPartitions.join(', ')}`);
      } else {
        console.log(`   First 5: ${sortedPartitions.slice(0, 5).join(', ')}`);
        console.log(`   Last 5: ${sortedPartitions.slice(-5).join(', ')}`);
      }
      
      // Validate partition continuity (warn about gaps)
      if (totalPartitions > 1) {
        const gaps: string[] = [];
        for (let i = 0; i < totalPartitions - 1; i++) {
          const current = new Date(sortedPartitions[i] + 'T00:00:00Z');
          const next = new Date(sortedPartitions[i + 1] + 'T00:00:00Z');
          const diffDays = Math.floor((next.getTime() - current.getTime()) / (1000 * 60 * 60 * 24));
          if (diffDays > 1) {
            gaps.push(`${sortedPartitions[i]} → ${sortedPartitions[i + 1]} (${diffDays - 1} day gap)`);
          }
        }
        if (gaps.length > 0) {
          console.warn(`   ⚠️  Found ${gaps.length} gap(s) in partition dates:`, gaps.slice(0, 5));
        }
      }
    } else {
      console.warn(`   ⚠️  No partitions found in S3 prefix: ${prefix}`);
    }
    
    return sortedPartitions;
  } catch (error: any) {
    const errorMsg = error.message || String(error);
    console.error(`❌ [PARTITION DISCOVERY] Error discovering partitions from S3:`, errorMsg);
    console.error(`   Dataset: ${datasetName}, Prefix: ${prefix}`);
    
    // Provide actionable error message
    if (error.name === 'NoSuchBucket' || errorMsg.includes('NoSuchBucket')) {
      throw new Error(`S3 bucket '${BUCKET}' not found. Please verify AWS credentials and bucket name.`);
    }
    if (error.name === 'AccessDenied' || errorMsg.includes('Access Denied')) {
      throw new Error(`Access denied to S3 bucket '${BUCKET}'. Please check IAM permissions for S3 ListObjects.`);
    }
    
    throw new Error(`Failed to discover partitions from S3: ${errorMsg}`);
  }
}

/**
 * Get partitions registered in Glue catalog (metadata), not from table data.
 * Uses $partitions metadata table or SHOW PARTITIONS - more reliable than SELECT DISTINCT.
 */
export async function getPartitionsFromCatalog(tableName: string): Promise<string[]> {
  try {
    // $partitions returns catalog metadata - partitions registered in Glue
    const res = await runQuery(`SELECT * FROM "${tableName}$partitions" ORDER BY date`);
    const partitions = res.rows.map((r: any) => {
      const val = r.date ?? r.partition_date ?? r.Date ?? Object.values(r)[0];
      return String(val ?? '').trim();
    }).filter((s: string) => s && s.match(/^\d{4}-\d{2}-\d{2}$/));
    return partitions;
  } catch (err: any) {
    // Fallback: SHOW PARTITIONS returns "date=2026-01-13" format per row
    try {
      const showRes = await runQuery(`SHOW PARTITIONS ${tableName}`);
      const partitionSpecs = showRes.rows.map((r: any) => {
        const val = r.partition ?? r.result ?? Object.values(r).find((v: any) => typeof v === 'string');
        return String(val ?? '').trim();
      });
      return partitionSpecs
        .map((spec: string) => spec.match(/date=([^\s/]+)/)?.[1])
        .filter((d: string | undefined): d is string => !!d && /^\d{4}-\d{2}-\d{2}$/.test(d));
    } catch {
      return [];
    }
  }
}

/**
 * Add partitions manually to ensure all data is accessible
 */
export async function addPartitionsManually(
  tableName: string, 
  datasetName: string, 
  partitions: string[],
  progressTracker?: any
): Promise<void> {
  if (partitions.length === 0) {
    return;
  }
  
  // Get partitions from Glue catalog (metadata) - more reliable than SELECT DISTINCT
  let existingPartitions: Set<string> = new Set();
  try {
    const catalogPartitions = await getPartitionsFromCatalog(tableName);
    existingPartitions = new Set(catalogPartitions);
  } catch (err: any) {
    if (!err.message?.includes('does not exist') && !err.message?.includes('Entity Not Found')) {
      console.warn('Could not check existing partitions, will attempt to add all');
    }
  }
  
  // Filter out partitions that already exist
  const missingPartitions = partitions.filter(p => !existingPartitions.has(p));
  
  if (missingPartitions.length === 0) {
    // All partitions already exist - nothing to do
    return;
  }
  
  console.log(`📦 Adding ${missingPartitions.length} missing partitions (${partitions.length - missingPartitions.length} already exist)...`);
  
  let successCount = 0;
  let failCount = 0;
  
  // Add partitions one by one (more reliable than batches)
  for (let i = 0; i < missingPartitions.length; i++) {
    const date = missingPartitions[i];
    
    // Update progress
    const progress = 30 + Math.floor((i / missingPartitions.length) * 10);
    progressTracker?.update('adding_partitions', progress, `Agregando partición ${i + 1}/${missingPartitions.length}: ${date}...`);
    
    try {
      await runQuery(`ALTER TABLE ${tableName} ADD PARTITION (date='${date}') LOCATION 's3://${BUCKET}/${datasetName}/date=${date}/'`);
      successCount++;
    } catch (error: any) {
      // Check if it's an "already exists" error - this can happen due to race conditions
      const isAlreadyExistsError = 
        error.message?.includes('already exists') || 
        error.message?.includes('Partition already exists') ||
        error.message?.includes('already exist') ||
        error.message?.includes('AlreadyExistsException') ||
        error.message?.includes('AlreadyExists');
      
      if (isAlreadyExistsError) {
        // Partition was added by another process or race condition - count as success
        successCount++;
        // Silently ignore - this is expected behavior
      } else {
        // Real error - log it
        failCount++;
        console.error(`❌ Failed to add partition ${date}:`, error.message);
      }
    }
    
    // Small delay to avoid overwhelming Athena
    if (i < missingPartitions.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  
  if (successCount > 0) {
    console.log(`✅ Successfully processed ${successCount} partitions (${missingPartitions.length - successCount} already existed)`);
  }
  
  if (failCount > 0) {
    console.warn(`⚠️  ${failCount} partitions could not be added. These days may not be included in the analysis.`);
  }
}

/**
 * Create an external table for a dataset in Athena/Glue
 * Ensures all partitions are loaded so all Parquet files are accessible
 */
export async function createTableForDataset(datasetName: string, progressTracker?: any): Promise<void> {
  const tableName = getTableName(datasetName);

  // First, discover partitions from S3
  console.log(`🔍 Discovering partitions for ${datasetName} from S3...`);
  const s3Partitions = await discoverPartitionsFromS3(datasetName);
  console.log(`📅 Found ${s3Partitions.length} partitions in S3:`, s3Partitions.slice(0, 10).join(', '), s3Partitions.length > 10 ? '...' : '');

  // Use STRING for latitude/longitude to handle both DOUBLE and BINARY types in Parquet files
  // We'll CAST to DOUBLE in queries where needed
  const sql = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} (
      ad_id STRING,
      utc_timestamp TIMESTAMP,
      horizontal_accuracy STRING,
      id_type STRING,
      ip_address STRING,
      latitude STRING,
      longitude STRING,
      iso_country_code STRING,
      poi_ids ARRAY<STRING>
    )
    PARTITIONED BY (date STRING)
    STORED AS PARQUET
    LOCATION 's3://${BUCKET}/${datasetName}/'
    TBLPROPERTIES (
      'projection.enabled'='false',
      'parquet.compress'='GZIP'
    )
  `;

  // Check if table exists and verify schema
  const tableExistsResult = await tableExists(datasetName);
  
  if (tableExistsResult) {
    // Table exists - check schema compatibility
    console.log(`🔍 Table ${tableName} exists, checking schema...`);
    try {
      const describeRes = await runQuery(`DESCRIBE ${tableName}`);
      const columns = describeRes.rows.map((r: any) => ({
        name: (r.col_name || r.name || '').trim(),
        type: (r.data_type || r.type || '').toLowerCase().trim()
      }));
      
      const latCol = columns.find((c: any) => c.name === 'latitude');
      const lngCol = columns.find((c: any) => c.name === 'longitude');
      
      console.log(`📊 Current schema - latitude: ${latCol?.type}, longitude: ${lngCol?.type}`);
      
      // If latitude/longitude are DOUBLE, we need to recreate the table
      if ((latCol && latCol.type === 'double') || (lngCol && lngCol.type === 'double')) {
        console.log(`⚠️  Table ${tableName} has old schema (DOUBLE for lat/lng). Recreating with new schema (STRING)...`);
        await runQuery(`DROP TABLE IF EXISTS ${tableName}`);
        console.log(`✅ Dropped old table ${tableName}`);
        
        // Recreate with new schema
        await runQuery(sql);
        console.log(`✅ Recreated table ${tableName} with new schema`);
        
        // Repair partitions
        try {
          await runQuery(`MSCK REPAIR TABLE ${tableName}`);
          console.log(`✅ Repaired partitions with MSCK REPAIR`);
        } catch (repairError: any) {
          console.warn(`MSCK REPAIR failed:`, repairError.message);
          if (s3Partitions.length > 0) {
            await addPartitionsManually(tableName, datasetName, s3Partitions, progressTracker);
          }
        }
        return; // Exit early after recreating
      } else {
        console.log(`✅ Table ${tableName} schema is compatible (STRING for lat/lng)`);
      }
    } catch (describeError: any) {
      console.warn(`Could not describe table ${tableName}:`, describeError.message);
      // Continue to try creating/repairing
    }
  }

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
        await addPartitionsManually(tableName, datasetName, s3Partitions, progressTracker);
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
        await addPartitionsManually(tableName, datasetName, s3Partitions, progressTracker);
      }
    } catch (countError) {
      console.warn(`Could not verify row count:`, countError);
    }
  } catch (error: any) {
    // If table exists with different schema, drop and recreate it
    if (error.message?.includes('already exists')) {
      console.log(`Table ${tableName} already exists, checking schema compatibility...`);
      
      // Try to check if the table has the old schema (DOUBLE for lat/lng)
      // If so, drop and recreate with new schema (STRING for lat/lng)
      try {
        const describeRes = await runQuery(`DESCRIBE ${tableName}`);
        const columns = describeRes.rows.map((r: any) => ({
          name: r.col_name || r.name,
          type: r.data_type || r.type
        }));
        
        const latCol = columns.find((c: any) => c.name === 'latitude');
        const lngCol = columns.find((c: any) => c.name === 'longitude');
        
        // If latitude/longitude are DOUBLE, we need to recreate the table
        if (latCol && latCol.type === 'double' || lngCol && lngCol.type === 'double') {
          console.log(`⚠️  Table ${tableName} has old schema (DOUBLE for lat/lng). Recreating with new schema (STRING)...`);
          await runQuery(`DROP TABLE IF EXISTS ${tableName}`);
          console.log(`✅ Dropped old table ${tableName}`);
          
          // Recreate with new schema
          await runQuery(sql);
          console.log(`✅ Recreated table ${tableName} with new schema`);
          
          // Repair partitions
          try {
            await runQuery(`MSCK REPAIR TABLE ${tableName}`);
            console.log(`✅ Repaired partitions with MSCK REPAIR`);
          } catch (repairError: any) {
            console.warn(`MSCK REPAIR failed:`, repairError.message);
            if (s3Partitions.length > 0) {
              await addPartitionsManually(tableName, datasetName, s3Partitions, progressTracker);
            }
          }
        } else {
          // Schema is compatible, just repair partitions
          console.log(`✅ Table ${tableName} schema is compatible, repairing partitions...`);
          try {
            await runQuery(`MSCK REPAIR TABLE ${tableName}`);
            console.log(`✅ Repaired partitions for ${tableName}`);
          } catch (repairError: any) {
            console.warn(`MSCK REPAIR failed:`, repairError.message);
            if (s3Partitions.length > 0) {
              await addPartitionsManually(tableName, datasetName, s3Partitions, progressTracker);
            }
          }
        }
      } catch (describeError: any) {
        // If we can't describe the table, just try to repair partitions
        console.warn(`Could not describe table ${tableName}, trying to repair partitions:`, describeError.message);
        try {
          await runQuery(`MSCK REPAIR TABLE ${tableName}`);
          console.log(`✅ Repaired partitions for ${tableName}`);
        } catch (repairError: any) {
          console.warn(`MSCK REPAIR failed:`, repairError.message);
          if (s3Partitions.length > 0) {
            await addPartitionsManually(tableName, datasetName, s3Partitions, progressTracker);
          }
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
 * Lightweight table check: if the table already exists and has data, skip the
 * expensive partition discovery + MSCK REPAIR.  Only falls back to the full
 * createTableForDataset when the table doesn't exist or is empty.
 *
 * This is ~100× faster than createTableForDataset for repeat runs.
 */
export async function ensureTableForDataset(datasetName: string): Promise<void> {
  const tableName = getTableName(datasetName);
  const exists = await tableExists(datasetName);
  if (exists) {
    // Quick sanity check — does the table have any rows?
    try {
      const probe = await runQuery(`SELECT 1 FROM ${tableName} LIMIT 1`);
      if (probe.rows.length > 0) {
        console.log(`✅ Table ${tableName} exists and has data — skipping full rebuild`);
        return; // Table is usable
      }
      console.log(`⚠️  Table ${tableName} exists but is empty — running full rebuild`);
    } catch {
      console.log(`⚠️  Table ${tableName} exists but probe failed — running full rebuild`);
    }
  }
  // Full creation with partition discovery
  await createTableForDataset(datasetName);
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
