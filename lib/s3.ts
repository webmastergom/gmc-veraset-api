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
 * TRIPLE CHECK: (1) MaxKeys 1000, (2) paginate while IsTruncated or NextContinuationToken, (3) only stop when no more pages.
 * Does not omit files: every page is requested until AWS returns no continuation.
 */
export async function listS3Objects(
  bucket: string,
  prefix: string
): Promise<S3ObjectInfo[]> {
  const normalizedPrefix = normalizePrefix(prefix);
  const allObjects: S3ObjectInfo[] = [];
  let continuationToken: string | undefined;
  let pageCount = 0;

  do {
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

    // Double-check: if we got a full page, there might be more (S3 returns token when truncated)
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
 * Copy objects in parallel batches with retries.
 * Second pass: retry all failed items once; errors from both passes are preserved (not cleared).
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
  const batchSize = calculateBatchSize(copies);

  const runBatch = async (
    items: CopyItem[],
    outErrors: string[],
    outFailedKeys: string[]
  ): Promise<{ ok: CopyItem[]; fail: CopyItem[] }> => {
    const results = await Promise.allSettled(
      items.map(async (item) => {
        await copyS3ObjectWithRetry(
          item.sourceBucket,
          item.sourceKey,
          item.destBucket,
          item.destKey
        );
        return item;
      })
    );
    const ok: CopyItem[] = [];
    const fail: CopyItem[] = [];
    results.forEach((r, j) => {
      const item = items[j];
      if (r.status === 'fulfilled') {
        ok.push(item);
      } else {
        fail.push(item);
        const msg = r.reason?.message || 'Unknown error';
        outErrors.push(`${item.sourceKey}: ${msg}`);
        outFailedKeys.push(item.sourceKey);
      }
    });
    return { ok, fail };
  };

  // First pass: all items in batches
  let pending = [...copies];
  while (pending.length > 0) {
    if (signal?.aborted) {
      console.log('[SYNC] Copy aborted by signal');
      break;
    }
    const batch = pending.slice(0, batchSize);
    pending = pending.slice(batchSize);
    const { ok, fail } = await runBatch(batch, errors, failedKeys);
    for (const item of ok) {
      copied++;
      totalBytes += item.size || 0;
      if (onProgress) {
        onProgress(copied, copies.length, totalBytes, item.sourceKey);
      }
    }
    for (const item of fail) {
      if (onProgress) {
        onProgress(copied, copies.length, totalBytes, item.sourceKey);
      }
    }
  }

  // Second pass: retry failed items once; use separate arrays then consolidate (preserve full error history)
  const keysToRetry = new Set(failedKeys);
  if (keysToRetry.size > 0 && !signal?.aborted) {
    const firstPassErrors = [...errors];
    const firstPassFailedKeys = [...failedKeys];
    const retryErrors: string[] = [];
    const retryFailedKeys: string[] = [];
    const toRetry = copies.filter((c) => keysToRetry.has(c.sourceKey));
    const { ok, fail } = await runBatch(toRetry, retryErrors, retryFailedKeys);
    for (const item of ok) {
      copied++;
      totalBytes += item.size || 0;
      if (onProgress) {
        onProgress(copied, copies.length, totalBytes, item.sourceKey);
      }
    }
    // Consolidate: final failed = keys that failed on retry; errors = first-pass + retry messages for those keys
    const finalFailedKeys = fail.map((f) => f.sourceKey);
    failedKeys.length = 0;
    failedKeys.push(...finalFailedKeys);
    errors.length = 0;
    finalFailedKeys.forEach((key) => {
      const firstIdx = firstPassFailedKeys.indexOf(key);
      if (firstIdx >= 0 && firstPassErrors[firstIdx]) {
        errors.push(firstPassErrors[firstIdx]);
      }
      const retryIdx = retryFailedKeys.indexOf(key);
      if (retryIdx >= 0 && retryErrors[retryIdx]) {
        errors.push(retryErrors[retryIdx]);
      }
    });
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
 */
export async function countS3Objects(
  bucket: string,
  prefix: string
): Promise<number> {
  const objects = await listS3Objects(bucket, prefix);
  return objects.length;
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
