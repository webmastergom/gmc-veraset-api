import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';

// ── In-memory cache for S3 config reads ─────────────────────────────────
// Avoids repeated S3 round-trips for data that rarely changes.
// Each entry has a TTL; writes invalidate the cache for that key.

const CONFIG_CACHE = new Map<string, { data: any; expiresAt: number }>();
const CONFIG_CACHE_TTL_MS = 60_000; // 60 seconds

function getCached<T>(key: string): T | undefined {
  const entry = CONFIG_CACHE.get(key);
  if (entry && Date.now() < entry.expiresAt) {
    return entry.data as T;
  }
  CONFIG_CACHE.delete(key);
  return undefined;
}

function setCache<T>(key: string, data: T): void {
  CONFIG_CACHE.set(key, { data, expiresAt: Date.now() + CONFIG_CACHE_TTL_MS });
}

export function invalidateCache(key: string): void {
  CONFIG_CACHE.delete(key);
}

// ── S3 client ───────────────────────────────────────────────────────────
// Created eagerly but wrapped in try-catch so a bad env var doesn't crash
// the module import and cause Vercel to return an HTML 500 page.

let s3ClientInstance: S3Client;
try {
  s3ClientInstance = new S3Client({
    region: process.env.AWS_REGION || 'us-west-2',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
    requestChecksumCalculation: 'WHEN_REQUIRED',
    responseChecksumValidation: 'WHEN_REQUIRED',
  });
} catch (err) {
  console.error('[S3-CONFIG] Failed to create S3Client:', err);
  // Create a bare client so imports don't crash — calls will fail at runtime
  // with a clear error instead of an import-time crash.
  s3ClientInstance = new S3Client({ region: 'us-west-2' });
}

export const s3Client = s3ClientInstance;

export const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/** S3 config for health checks */
export async function getS3Config(): Promise<{ bucket: string; region: string } | null> {
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    return null;
  }
  return {
    bucket: BUCKET,
    region: process.env.AWS_REGION || 'us-west-2',
  };
}

/**
 * Check if config file exists in S3
 */
export async function configExists(key: string): Promise<boolean> {
  // If we have it cached, it exists
  if (getCached(key) !== undefined) return true;

  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      return false;
    }

    await s3Client.send(new HeadObjectCommand({
      Bucket: BUCKET,
      Key: `config/${key}.json`,
    }));
    return true;
  } catch (error: any) {
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      return false;
    }
    // Other errors (permissions, network) - assume doesn't exist
    return false;
  }
}

/**
 * Read a JSON config file from S3 with proper error handling.
 * Results are cached in memory for 60s to avoid repeated S3 round-trips.
 */
export async function getConfig<T>(key: string): Promise<T | null> {
  // Check in-memory cache first
  const cached = getCached<T>(key);
  if (cached !== undefined) return cached;

  try {
    // Check if AWS credentials are configured
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      console.warn(`AWS credentials not configured. Returning null for config/${key}.json`);
      return null;
    }

    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: `config/${key}.json`,
    });

    const response = await s3Client.send(command);
    const body = await response.Body?.transformToString();

    if (!body) {
      return null;
    }

    const parsed = JSON.parse(body) as T;
    setCache(key, parsed);
    return parsed;
  } catch (error: any) {
    // File doesn't exist
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      console.warn(`Config file not found: config/${key}.json`);
      return null;
    }

    // Never throw for config reads — a transient S3 error (timeout, throttle,
    // credential rotation) should not crash the entire route handler.
    // Return null and let the caller decide how to handle (e.g., show cached/seed data).
    console.error(`Error reading config/${key}.json:`, error.message || error);
    return null;
  }
}

/**
 * Write a JSON config file to S3 with validation.
 * Invalidates the in-memory cache for this key.
 */
export async function putConfig<T>(key: string, data: T, options?: { compact?: boolean }): Promise<void> {
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      throw new Error('AWS credentials not configured');
    }

    const json = options?.compact ? JSON.stringify(data) : JSON.stringify(data, null, 2);

    const command = new PutObjectCommand({
      Bucket: BUCKET,
      Key: `config/${key}.json`,
      Body: json,
      ContentType: 'application/json',
    });

    await s3Client.send(command);

    // Invalidate cache and store fresh data
    invalidateCache(key);
    setCache(key, data);

    console.log(`✅ Saved config/${key}.json (${(json.length/1024).toFixed(0)}KB${options?.compact ? ' compact' : ''})`);
  } catch (error: any) {
    console.error(`❌ Error saving config/${key}.json:`, error.message || error);
    throw error;
  }
}

/**
 * Initialize config file if it doesn't exist.
 * Uses cache to avoid the HeadObject + GetObject double round-trip.
 */
export async function initConfigIfNeeded<T>(key: string, defaultData: T): Promise<T> {
  // Fast path: cached data available
  const cached = getCached<T>(key);
  if (cached !== undefined) return cached;

  const exists = await configExists(key);

  if (!exists) {
    console.log(`📝 Initializing config/${key}.json with defaults`);
    try {
      await putConfig(key, defaultData);
      return defaultData;
    } catch (error) {
      console.warn(`Could not initialize config/${key}.json, using in-memory default`);
      return defaultData;
    }
  }

  const existing = await getConfig<T>(key);
  return existing || defaultData;
}

/**
 * Update a JSON config file (read, modify, write)
 */
export async function updateConfig<T extends Record<string, any>>(
  key: string,
  updater: (data: T | null) => T
): Promise<T> {
  const existing = await getConfig<T>(key);
  const updated = updater(existing);
  await putConfig(key, updated);
  return updated;
}

/**
 * List all objects in a prefix
 */
export async function listObjects(prefix: string): Promise<string[]> {
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      return [];
    }

    const { ListObjectsV2Command } = await import('@aws-sdk/client-s3');
    const command = new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
    });

    const response = await s3Client.send(command);
    return (response.Contents || []).map(obj => obj.Key || '').filter(Boolean);
  } catch (error) {
    console.warn(`Error listing objects with prefix ${prefix}:`, error);
    return [];
  }
}

// NOTE: getPOICollection and putPOICollection have been moved to ./poi-storage.ts
// to avoid pulling fs/path into every route that imports s3-config.
// Import them from '@/lib/poi-storage' instead.
