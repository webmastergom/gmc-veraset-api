/**
 * Compute canonical sync status from job + optional per-job sync state.
 * The per-job state file is fresher during active syncs (updated every 5s vs jobs.json).
 *
 * CONCURRENCY NOTE: During active syncs, progress is tracked in per-job
 * sync-state files (config/sync-state/{jobId}.json). When syncState is
 * present, its values are preferred over job fields to avoid stale data
 * from the large jobs.json (which may lag behind).
 */

import type { Job } from '@/lib/jobs';
import type { SyncStatusResponse } from '@/lib/sync-types';
import type { SyncState } from './sync-state';

export function determineSyncStatus(job: Job, syncState?: SyncState | null): SyncStatusResponse {
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

  // Prefer per-job syncState (updated every 5s) over job fields (stale in jobs.json).
  // Only fall back to job fields when no syncState exists.
  let copied = syncState ? (syncState.objectCount ?? 0) : (job.objectCount ?? 0);
  let copiedBytes = syncState ? (syncState.totalBytes ?? 0) : (job.totalBytes ?? 0);
  let totalObjects = syncState?.expectedObjectCount ?? job.expectedObjectCount ?? 0;
  let totalBytes = syncState?.expectedTotalBytes ?? job.expectedTotalBytes ?? 0;

  // Use the freshest syncProgress (per-job state or job)
  const syncProgress = syncState?.syncProgress ?? job.syncProgress;

  // Derive overall progress from dayProgress when available.
  // dayProgress is authoritative for total counts when present.
  if (syncProgress?.dayProgress) {
    const days = Object.values(syncProgress.dayProgress);
    const sumCopied = days.reduce((s, d) => s + (d?.copiedFiles ?? 0), 0);
    const sumCopiedBytes = days.reduce((s, d) => s + (d?.copiedBytes ?? 0), 0);
    const sumTotal = days.reduce((s, d) => s + (d?.totalFiles ?? 0), 0);
    const sumTotalBytes = days.reduce((s, d) => s + (d?.totalBytes ?? 0), 0);
    if (sumCopied > copied) copied = sumCopied;
    if (sumCopiedBytes > copiedBytes) copiedBytes = sumCopiedBytes;
    // dayProgress sums are authoritative when available (not just fallback for 0)
    if (sumTotal > 0) totalObjects = sumTotal;
    if (sumTotalBytes > 0) totalBytes = sumTotalBytes;
  }

  // REMOVED: The old fallback `totalObjects = copied` when totalObjects === 0
  // incorrectly marked partial syncs as complete. Without an explicitly set
  // expectedObjectCount, progress shows 0% — which is better than a false 100%.

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
      syncProgress: syncProgress ?? null,
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
      syncProgress: syncProgress ?? null,
    };
  }

  const isComplete = !!job.syncedAt;
  // Only trust "looks complete" when expectedObjectCount was explicitly set
  // (by initializeSync at sync start). Prevents partial syncs from being
  // reported as complete when expectedObjectCount was never initialized.
  const hasExplicitExpected = !!(syncState?.expectedObjectCount || job.expectedObjectCount);
  const looksComplete = hasExplicitExpected && totalObjects > 0 && copied >= totalObjects;

  if (isComplete || looksComplete) {
    return {
      status: 'completed',
      message: 'Sync completed',
      progress: 100,
      total: totalObjects || copied,
      totalBytes: totalBytes || copiedBytes,
      copied: copied || totalObjects || 0,
      copiedBytes: copiedBytes || totalBytes || 0,
      syncProgress: syncProgress ?? null,
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

  // Check lastUpdated from syncProgress (per-job state is fresher) — if no update in >12 min, function is dead
  const lastUpdated = syncProgress?.lastUpdated ?? job.syncProgress?.lastUpdated;
  const lastProgressUpdate = lastUpdated
    ? Date.now() - new Date(lastUpdated).getTime()
    : 0;
  const isProgressStale = lastUpdated && lastProgressUpdate > 12 * 60 * 1000;

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
      syncProgress: syncProgress ?? null,
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
      syncProgress: syncProgress ?? null,
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
