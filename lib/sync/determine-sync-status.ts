/**
 * Pure function: compute canonical sync status from job. No side effects (no repair).
 */

import type { Job } from '@/lib/jobs';
import type { SyncStatusResponse } from '@/lib/sync-types';

export function determineSyncStatus(job: Job): SyncStatusResponse {
  if (!job.s3SourcePath) {
    return {
      status: 'not_started',
      message: 'Job has no source path',
      progress: 0,
      total: 0,
      totalBytes: 0,
      copied: 0,
      copiedBytes: 0,
    };
  }

  if (!job.s3DestPath) {
    return {
      status: 'not_started',
      message: 'Sync not started',
      progress: 0,
      total: 0,
      totalBytes: 0,
      copied: 0,
      copiedBytes: 0,
    };
  }

  const copied = job.objectCount ?? 0;
  const copiedBytes = job.totalBytes ?? 0;
  let totalObjects = job.expectedObjectCount ?? 0;
  let totalBytes = job.expectedTotalBytes ?? 0;

  if (totalObjects === 0 && copied > 0) {
    totalObjects = copied;
    totalBytes = copiedBytes;
  }

  const progress = totalObjects > 0 ? Math.round((copied / totalObjects) * 100) : 0;

  if (job.syncCancelledAt) {
    return {
      status: 'cancelled',
      message: 'Sync stopped by user',
      progress,
      total: totalObjects,
      totalBytes,
      copied,
      copiedBytes,
      syncProgress: job.syncProgress ?? null,
    };
  }

  const isComplete = !!job.syncedAt;
  const looksComplete = totalObjects > 0 && copied >= totalObjects;

  if (isComplete || looksComplete) {
    return {
      status: 'completed',
      message: 'Sync completed',
      progress: 100,
      total: totalObjects || copied,
      totalBytes: totalBytes || copiedBytes,
      copied: copied || totalObjects || 0,
      copiedBytes: copiedBytes || totalBytes || 0,
      syncProgress: job.syncProgress ?? null,
    };
  }

  if (copied > 0 || totalObjects > 0) {
    return {
      status: 'syncing',
      message: `Syncing... ${copied}/${totalObjects} objects`,
      progress,
      total: totalObjects,
      totalBytes,
      copied,
      copiedBytes,
      syncProgress: job.syncProgress ?? null,
    };
  }

  return {
    status: 'not_started',
    message: 'Sync not started',
    progress: 0,
    total: totalObjects,
    totalBytes,
    copied: 0,
    copiedBytes: 0,
  };
}
