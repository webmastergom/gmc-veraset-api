import {
  S3Client,
  ListObjectsV2Command,
  CopyObjectCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
  maxAttempts: 3,
});

/** Max keys per ListObjectsV2 request (S3 limit). */
const LIST_PAGE_SIZE = 1000;

export interface S3ObjectInfo {
  Key: string;
  Size: number;
}

/**
 * Normalize S3 key prefix: no leading slash, ensure single trailing slash for directory-style prefix.
 */
export function normalizePrefix(prefix: string): string {
  const trimmed = prefix.replace(/^\/+/, '').trim();
  if (!trimmed) return '';
  return trimmed.endsWith('/') ? trimmed : `${trimmed}/`;
}

/**
 * List ALL objects under a prefix with full pagination.
 * Supports abort signal for cancellation during long listings (100K+ objects).
 */
export async function listS3Objects(
  bucket: string,
  prefix: string,
  signal?: AbortSignal
): Promise<S3ObjectInfo[]> {
  const normalizedPrefix = normalizePrefix(prefix);
  const allObjects: S3ObjectInfo[] = [];
  let continuationToken: string | undefined;
  let pageCount = 0;

  do {
    if (signal?.aborted) {
      throw new Error('Listing aborted');
    }

    const command = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: normalizedPrefix,
      MaxKeys: LIST_PAGE_SIZE,
      ContinuationToken: continuationToken,
    });

    const response = await s3Client.send(command);

    if (response.Contents) {
      for (const obj of response.Contents) {
        const key = obj.Key;
        if (!key || key.endsWith('/')) continue; // skip directory placeholders
        const size = typeof obj.Size === 'number' ? obj.Size : 0;
        allObjects.push({ Key: key, Size: size });
      }
    }

    pageCount++;
    continuationToken = response.NextContinuationToken;

    const isTruncated = response.IsTruncated === true;
    if (isTruncated && !continuationToken) {
      console.warn(
        `[S3 LIST] IsTruncated=true but no NextContinuationToken for bucket=${bucket} prefix=${normalizedPrefix} page=${pageCount}`
      );
    }
  } while (continuationToken);

  return allObjects;
}

/**
 * Count objects and optionally collect date partitions in a single pass.
 * Memory-efficient: only accumulates a counter + optional Set of dates.
 * Use this instead of listS3Objects when you don't need the full object list.
 */
export async function countS3ObjectsByPrefix(
  bucket: string,
  prefix: string,
  options?: { collectDates?: boolean }
): Promise<{ count: number; dates?: string[] }> {
  const normalizedPrefix = normalizePrefix(prefix);
  let count = 0;
  let continuationToken: string | undefined;
  const dateSet = options?.collectDates ? new Set<string>() : undefined;
  const datePattern = /date=(\d{4}-\d{2}-\d{2})/;

  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: normalizedPrefix,
      MaxKeys: LIST_PAGE_SIZE,
      ContinuationToken: continuationToken,
    }));

    if (response.Contents) {
      for (const obj of response.Contents) {
        if (!obj.Key || obj.Key.endsWith('/')) continue;
        count++;
        if (dateSet) {
          const m = obj.Key.match(datePattern);
          if (m?.[1]) dateSet.add(m[1]);
        }
      }
    }
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return { count, dates: dateSet ? Array.from(dateSet).sort() : undefined };
}

/**
 * List destination keys as a Set (memory-efficient for incremental diff).
 * Returns only keys, not sizes — halves memory vs listS3Objects.
 */
export async function listS3KeySet(
  bucket: string,
  prefix: string,
  signal?: AbortSignal
): Promise<Set<string>> {
  const normalizedPrefix = normalizePrefix(prefix);
  const keys = new Set<string>();
  let continuationToken: string | undefined;

  do {
    if (signal?.aborted) {
      throw new Error('Listing aborted');
    }

    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: normalizedPrefix,
      MaxKeys: LIST_PAGE_SIZE,
      ContinuationToken: continuationToken,
    }));

    if (response.Contents) {
      for (const obj of response.Contents) {
        if (obj.Key && !obj.Key.endsWith('/')) keys.add(obj.Key);
      }
    }
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return keys;
}

/** Hard ceiling on concurrent S3 copy operations to prevent throttling (503 SlowDown). */
const MAX_CONCURRENT_COPIES = 25;

/**
 * Run async operations with a concurrency limit (semaphore pattern).
 * Unlike Promise.allSettled with full batches, this maintains a sliding window
 * of at most `limit` in-flight operations.
 */
async function runWithConcurrency<T>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<void>,
  signal?: AbortSignal
): Promise<void> {
  let nextIndex = 0;
  const worker = async () => {
    while (nextIndex < items.length) {
      if (signal?.aborted) return;
      const i = nextIndex++;
      await fn(items[i], i);
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, () => worker())
  );
}

/**
 * Copy a single object with retries and exponential backoff.
 */
async function copyS3ObjectWithRetry(
  sourceBucket: string,
  sourceKey: string,
  destBucket: string,
  destKey: string,
  retries: number = 3
): Promise<void> {
  const copySource = encodeURIComponent(`${sourceBucket}/${sourceKey}`);
  const command = new CopyObjectCommand({
    CopySource: copySource,
    Bucket: destBucket,
    Key: destKey,
  });

  let lastError: Error | null = null;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await s3Client.send(command);
      return;
    } catch (error: any) {
      lastError = error;
      const code = error.$metadata?.httpStatusCode;
      if (code && code >= 400 && code < 500 && code !== 429) {
        throw error;
      }
      if (attempt < retries - 1) {
        const delay = Math.min(1000 * Math.pow(2, attempt), 10000);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw lastError || new Error('Copy failed after retries');
}

export async function copyS3Object(
  sourceBucket: string,
  sourceKey: string,
  destBucket: string,
  destKey: string
): Promise<void> {
  return copyS3ObjectWithRetry(sourceBucket, sourceKey, destBucket, destKey);
}

function calculateBatchSize(objects: Array<{ size?: number }>): number {
  if (objects.length === 0) return 100;
  const avg =
    objects.reduce((s, o) => s + (o.size || 0), 0) / objects.length;
  const avgMB = avg / (1024 * 1024);
  if (avgMB < 1) return 200;
  if (avgMB < 10) return 100;
  if (avgMB < 100) return 50;
  return 25;
}

export interface CopyItem {
  sourceBucket: string;
  sourceKey: string;
  destBucket: string;
  destKey: string;
  size?: number;
}

export interface CopyBatchResult {
  copied: number;
  failed: number;
  totalBytes: number;
  errors: string[];
  failedKeys: string[];
}

/**
 * Copy objects with a hard concurrency limit (25 simultaneous copies).
 * Uses a sliding-window semaphore instead of fixed batches — ensures S3 is never
 * overwhelmed while keeping the pipeline full.
 * Second pass: retry all failed items once; errors from both passes are preserved.
 * If signal is provided and aborted, returns partial result without throwing.
 */
export async function copyS3ObjectsBatch(
  copies: CopyItem[],
  onProgress?: (copied: number, total: number, bytes: number, currentKey?: string) => void,
  signal?: AbortSignal
): Promise<CopyBatchResult> {
  let copied = 0;
  let totalBytes = 0;
  const errors: string[] = [];
  const failedKeys: string[] = [];
  const failedItems: CopyItem[] = [];

  // First pass: copy all items with concurrency limit
  await runWithConcurrency(copies, MAX_CONCURRENT_COPIES, async (item) => {
    if (signal?.aborted) return;
    try {
      await copyS3ObjectWithRetry(item.sourceBucket, item.sourceKey, item.destBucket, item.destKey);
      copied++;
      totalBytes += item.size || 0;
      onProgress?.(copied, copies.length, totalBytes, item.sourceKey);
    } catch (error: any) {
      failedItems.push(item);
      failedKeys.push(item.sourceKey);
      errors.push(`${item.sourceKey}: ${error?.message || 'Unknown error'}`);
      onProgress?.(copied, copies.length, totalBytes, item.sourceKey);
    }
  }, signal);

  if (signal?.aborted) {
    console.log('[SYNC] Copy aborted by signal');
  }

  // Second pass: retry failures once with same concurrency limit
  if (failedItems.length > 0 && !signal?.aborted) {
    console.log(`[SYNC] Retrying ${failedItems.length} failed items...`);
    const firstPassErrors = [...errors];
    const firstPassFailedKeys = [...failedKeys];
    const retryFailedKeys: string[] = [];
    const retryErrors: string[] = [];

    await runWithConcurrency(failedItems, MAX_CONCURRENT_COPIES, async (item) => {
      if (signal?.aborted) return;
      try {
        await copyS3ObjectWithRetry(item.sourceBucket, item.sourceKey, item.destBucket, item.destKey);
        copied++;
        totalBytes += item.size || 0;
        onProgress?.(copied, copies.length, totalBytes, item.sourceKey);
      } catch (error: any) {
        retryFailedKeys.push(item.sourceKey);
        retryErrors.push(`${item.sourceKey}: ${error?.message || 'Unknown error'}`);
      }
    }, signal);

    // Consolidate: final failed = keys that still failed on retry
    failedKeys.length = 0;
    failedKeys.push(...retryFailedKeys);
    errors.length = 0;
    for (const key of retryFailedKeys) {
      const firstIdx = firstPassFailedKeys.indexOf(key);
      if (firstIdx >= 0 && firstPassErrors[firstIdx]) {
        errors.push(firstPassErrors[firstIdx]);
      }
      const retryIdx = retryFailedKeys.indexOf(key);
      if (retryIdx >= 0 && retryErrors[retryIdx]) {
        errors.push(retryErrors[retryIdx]);
      }
    }
  }

  return {
    copied,
    failed: failedKeys.length,
    totalBytes,
    errors,
    failedKeys,
  };
}

/**
 * List destination prefix and return count (for verification after sync).
 * @deprecated Use countS3ObjectsByPrefix for memory-efficient counting.
 */
export async function countS3Objects(
  bucket: string,
  prefix: string
): Promise<number> {
  const result = await countS3ObjectsByPrefix(bucket, prefix);
  return result.count;
}

export async function getObjectMetadata(bucket: string, key: string) {
  return s3Client.send(
    new HeadObjectCommand({
      Bucket: bucket,
      Key: key,
    })
  );
}

/** Get object ETag for verification (checksum). Returns null if head fails. */
export async function getObjectETag(
  bucket: string,
  key: string
): Promise<string | null> {
  try {
    const meta = await s3Client.send(
      new HeadObjectCommand({ Bucket: bucket, Key: key })
    );
    return meta.ETag?.replace(/"/g, '') ?? null;
  } catch {
    return null;
  }
}

export function parseS3Path(path: string): { bucket: string; key: string } {
  const match = path.match(/^s3:\/\/([^/]+)\/(.*)$/);
  if (!match) {
    throw new Error(`Invalid S3 path: ${path}`);
  }
  const key = match[2].replace(/^\/+/, '') || '';
  return {
    bucket: match[1],
    key: key.endsWith('/') ? key : (key ? `${key}/` : ''),
  };
}

export async function listDatasets(): Promise<string[]> {
  const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
  const response = await s3Client.send(
    new ListObjectsV2Command({
      Bucket: BUCKET,
      Delimiter: '/',
    })
  );
  return (response.CommonPrefixes || [])
    .map((p) => (p.Prefix || '').replace(/\/$/, ''))
    .filter(Boolean);
}

/**
 * Extract unique dates from S3 object keys (partitioned by date=YYYY-MM-DD).
 * Returns sorted array of date strings (YYYY-MM-DD format).
 * 
 * Handles multiple partition formats:
 * - date=YYYY-MM-DD/file.parquet
 * - prefix/date=YYYY-MM-DD/file.parquet
 * - Any path containing date=YYYY-MM-DD
 */
export function extractUniqueDatesFromKeys(keys: string[]): string[] {
  const dateSet = new Set<string>();
  // Match date=YYYY-MM-DD pattern anywhere in the key
  const datePattern = /date=(\d{4}-\d{2}-\d{2})/;
  
  for (const key of keys) {
    if (!key || typeof key !== 'string') continue;
    
    const match = key.match(datePattern);
    if (match && match[1]) {
      const dateStr = match[1];
      // Validate date format (YYYY-MM-DD)
      if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        // Validate it's a real date
        const date = new Date(dateStr + 'T00:00:00Z');
        if (!isNaN(date.getTime()) && date.toISOString().startsWith(dateStr)) {
          dateSet.add(dateStr);
        }
      }
    }
  }
  
  return Array.from(dateSet).sort();
}

/**
 * Calculate the number of days between two dates (inclusive).
 * Both fromDate and toDate are included in the count.
 * 
 * @param fromDate - Start date (YYYY-MM-DD format)
 * @param toDate - End date (YYYY-MM-DD format)
 * @returns Number of days (inclusive)
 */
export function calculateDaysInclusive(fromDate: string, toDate: string): number {
  try {
    const from = new Date(fromDate + 'T00:00:00Z');
    const to = new Date(toDate + 'T00:00:00Z');
    
    if (isNaN(from.getTime()) || isNaN(to.getTime())) {
      throw new Error(`Invalid date format: fromDate=${fromDate}, toDate=${toDate}`);
    }
    
    if (from > to) {
      throw new Error(`fromDate (${fromDate}) must be <= toDate (${toDate})`);
    }
    
    // Calculate difference in milliseconds, convert to days, add 1 for inclusive count
    const diffMs = to.getTime() - from.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    return diffDays + 1; // +1 because both dates are inclusive
  } catch (error) {
    console.error('[calculateDaysInclusive] Error:', error);
    throw error;
  }
}
