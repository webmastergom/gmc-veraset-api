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

  let copied = job.objectCount ?? 0;
  let copiedBytes = job.totalBytes ?? 0;
  let totalObjects = job.expectedObjectCount ?? 0;
  let totalBytes = job.expectedTotalBytes ?? 0;

  // Derive overall progress from dayProgress when available.
  // The dayProgress is always at least as fresh as objectCount (they are
  // written in the same flush), and is often more granular because the SSE
  // stream may read the job between flushes — dayProgress is updated in-memory
  // per file while objectCount only updates every 5 s via flushProgress.
  if (job.syncProgress?.dayProgress) {
    const days = Object.values(job.syncProgress.dayProgress);
    const sumCopied = days.reduce((s, d) => s + (d?.copiedFiles ?? 0), 0);
    const sumCopiedBytes = days.reduce((s, d) => s + (d?.copiedBytes ?? 0), 0);
    const sumTotal = days.reduce((s, d) => s + (d?.totalFiles ?? 0), 0);
    const sumTotalBytes = days.reduce((s, d) => s + (d?.totalBytes ?? 0), 0);
    // Use dayProgress-derived values when they show more progress
    if (sumCopied > copied) copied = sumCopied;
    if (sumCopiedBytes > copiedBytes) copiedBytes = sumCopiedBytes;
    if (sumTotal > 0 && totalObjects === 0) totalObjects = sumTotal;
    if (sumTotalBytes > 0 && totalBytes === 0) totalBytes = sumTotalBytes;
  }

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

  // Error: errorMessage is set and sync is NOT complete (no syncedAt)
  // This catches: copy failures, verification failures, orchestrator exceptions
  if (job.errorMessage && !job.syncedAt) {
    return {
      status: 'error',
      message: job.errorMessage,
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

  // Detect stalled syncs: the Vercel function died without writing final state.
  // Scenarios:
  //   1. syncLock is set but very old (function died without releasing lock)
  //   2. syncLock is null (lock released/expired) AND syncStartedAt is set AND
  //      no syncedAt/errorMessage — function finished but never wrote completion
  //   3. syncLock is null, syncStartedAt is set, and it's been >12 min since start
  //      (maxDuration=10min + 2min buffer)
  const lockAge = job.syncLock ? Date.now() - new Date(job.syncLock).getTime() : 0;
  const isLockStale = job.syncLock ? lockAge > 10 * 60 * 1000 : false; // 10 min = maxDuration

  // Check if sync was started but function is no longer running
  const syncStartAge = job.syncStartedAt ? Date.now() - new Date(job.syncStartedAt).getTime() : 0;
  const isFunctionDead = !job.syncLock && job.syncStartedAt && syncStartAge > 12 * 60 * 1000; // 12 min

  // Check lastUpdated from syncProgress — if no update in >12 min, function is dead
  const lastProgressUpdate = job.syncProgress?.lastUpdated
    ? Date.now() - new Date(job.syncProgress.lastUpdated).getTime()
    : 0;
  const isProgressStale = job.syncProgress?.lastUpdated && lastProgressUpdate > 12 * 60 * 1000;

  const isStalled = isLockStale || isFunctionDead || isProgressStale;

  if (isStalled && copied < totalObjects && totalObjects > 0) {
    const stalledMinutes = Math.max(
      isLockStale ? Math.round(lockAge / 60000) : 0,
      isFunctionDead ? Math.round(syncStartAge / 60000) : 0,
      isProgressStale ? Math.round(lastProgressUpdate / 60000) : 0
    );
    return {
      status: 'error',
      message: `Sync appears stalled (no activity for ${stalledMinutes} min). Use "Resync" to continue.`,
      progress,
      total: totalObjects,
      totalBytes,
      copied,
      copiedBytes,
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
