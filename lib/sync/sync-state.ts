/**
 * Per-job sync state stored in a separate S3 file (config/sync-state/{jobId}.json).
 *
 * During sync, progress writes go to this small file (~2-5KB) instead of the
 * full jobs.json (~500KB+). This eliminates:
 * - Slow read-modify-write cycles on a large file every 5 seconds
 * - Risk of corrupting other jobs' data during concurrent writes
 * - High S3 bandwidth from repeatedly transferring the full file
 *
 * On sync completion, the final state is merged back into jobs.json (single write).
 */

import { s3Client, BUCKET } from '@/lib/s3-config';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import type { SyncProgress } from '@/lib/sync-types';

const STATE_PREFIX = 'config/sync-state/';

export interface SyncState {
  objectCount: number;
  totalBytes: number;
  expectedObjectCount: number;
  expectedTotalBytes: number;
  syncProgress: SyncProgress;
  syncStartedAt?: string;
  syncedAt?: string;
  syncCancelledAt?: string;
  errorMessage?: string;
}

/**
 * Read per-job sync state. Returns null if no state file exists (job never synced
 * with this system, or sync hasn't started yet).
 */
export async function getSyncState(jobId: string): Promise<SyncState | null> {
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: `${STATE_PREFIX}${jobId}.json`,
    }));
    const body = await response.Body?.transformToString();
    return body ? JSON.parse(body) : null;
  } catch (error: any) {
    // NoSuchKey = file doesn't exist yet (normal for first sync)
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      return null;
    }
    console.warn(`[SYNC-STATE] Failed to read sync state for ${jobId}:`, error.message);
    return null;
  }
}

/**
 * Write per-job sync state. This is a small file (~2-5KB) that gets written
 * every 5 seconds during sync, replacing the previous expensive writes to jobs.json.
 */
export async function putSyncState(jobId: string, state: SyncState): Promise<void> {
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${STATE_PREFIX}${jobId}.json`,
      Body: JSON.stringify(state),
      ContentType: 'application/json',
    }));
  } catch (error: any) {
    console.warn(`[SYNC-STATE] Failed to write sync state for ${jobId}:`, error.message);
    throw error;
  }
}
