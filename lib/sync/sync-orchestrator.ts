/**
 * Sync orchestrator: list → copy → verify.
 * Checks abort signal between batches; records partial state on cancel.
 *
 * Progress writes are serialized: only one updateJob at a time, at most every 5s.
 * This prevents S3 race conditions (concurrent read-modify-write on jobs.json).
 */

import { getJob, updateJob, initializeSync, releaseSyncLock } from '@/lib/jobs';
import {
  listS3Objects,
  listS3KeySet,
  copyS3ObjectsBatch,
  normalizePrefix,
  countS3ObjectsByPrefix,
  extractUniqueDatesFromKeys,
  calculateDaysInclusive,
} from '@/lib/s3';
import type { CopyItem } from '@/lib/s3';
import type { S3PathParsed } from '@/lib/sync-types';
import { registerAbortController, unregisterAbortController } from './sync-abort-registry';
import {
  buildKeyToSizeMap,
  buildKeyToDateMap,
  buildInitialDayProgress,
  applyProgressUpdate,
  buildSyncProgressPayload,
} from './sync-progress-tracker';
import { verifyEtags } from './sync-verifier';
import { getSyncState, putSyncState } from './sync-state';
import type { SyncState } from './sync-state';

/** Min interval between progress writes to S3 (avoids race conditions). */
const PROGRESS_WRITE_INTERVAL_MS = 5_000;

export interface SyncOrchestratorParams {
  jobId: string;
  sourcePath: S3PathParsed;
  destPathParsed: S3PathParsed;
  destPath: string; // original s3:// form
}

export async function runSync(params: SyncOrchestratorParams): Promise<void> {
  const { jobId, sourcePath, destPathParsed, destPath } = params;
  const syncStartTime = Date.now();
  const controller = registerAbortController(jobId);

  try {
    console.log(`[SYNC] Starting sync for job ${jobId} (orchestrator)...`);

    // ---------- Phase 1: List source (abort-aware) ----------
    const listStart = Date.now();
    const sourceObjects = await listS3Objects(sourcePath.bucket, sourcePath.key, controller.signal);
    const listDuration = ((Date.now() - listStart) / 1000).toFixed(2);
    console.log(`[SYNC] CHECK 1 - Listed ${sourceObjects.length} source objects in ${listDuration}s`);

    if (sourceObjects.length === 0) {
      const sourcePathStr = `s3://${sourcePath.bucket}/${sourcePath.key}`;
      const errorMsg = `No objects found at source path: ${sourcePathStr}. The job may not have completed processing yet, or the source path may be incorrect.`;
      console.warn(`[SYNC] ⚠️  ${errorMsg}`);
      await updateJob(jobId, {
        s3DestPath: destPath,
        expectedObjectCount: 0,
        expectedTotalBytes: 0,
        errorMessage: errorMsg,
      });
      return;
    }

    const totalBytes = sourceObjects.reduce((s, o) => s + o.Size, 0);
    const totalObjects = sourceObjects.length;
    console.log(`[SYNC] Total to copy: ${totalObjects} objects, ${(totalBytes / 1024 / 1024 / 1024).toFixed(2)} GB`);

    // ---------- Incremental sync: skip files already in destination ----------
    const sourcePrefix = normalizePrefix(sourcePath.key);
    const destPrefix = normalizePrefix(destPathParsed.key);
    const allCopies: CopyItem[] = sourceObjects.map((obj) => {
      const relative = sourcePrefix ? obj.Key.replace(sourcePrefix, '') : obj.Key;
      const destKey = destPrefix + relative.replace(/^\/+/, '');
      return {
        sourceBucket: sourcePath.bucket,
        sourceKey: obj.Key,
        destBucket: destPathParsed.bucket,
        destKey,
        size: obj.Size,
      };
    });

    // Memory-efficient: only load dest keys as a Set (no sizes needed for diff)
    const destKeySet = await listS3KeySet(destPathParsed.bucket, destPathParsed.key, controller.signal);
    const alreadyCopied = allCopies.filter((c) => destKeySet.has(c.destKey));
    const copies = allCopies.filter((c) => !destKeySet.has(c.destKey));
    const alreadyCopiedBytes = alreadyCopied.reduce((s, c) => s + (c.size ?? 0), 0);

    console.log(
      `[SYNC] Incremental: ${alreadyCopied.length}/${totalObjects} already in dest (${(alreadyCopiedBytes / 1024 / 1024 / 1024).toFixed(2)} GB), ${copies.length} remaining to copy`
    );

    await initializeSync(jobId, destPath, totalObjects, totalBytes);

    const sourceKeys = sourceObjects.map((o) => o.Key);
    const keyToSize = buildKeyToSizeMap(allCopies);
    const keyToDate = buildKeyToDateMap(sourceKeys);
    const dayProgress = buildInitialDayProgress(sourceKeys, allCopies);
    const sortedDates = Object.keys(dayProgress).sort();
    console.log(`[SYNC] Grouped ${totalObjects} objects into ${sortedDates.length} date partitions`);

    // Pre-populate dayProgress with already-copied files
    for (const item of alreadyCopied) {
      const date = keyToDate.get(item.sourceKey);
      if (date && dayProgress[date]) {
        dayProgress[date].copiedFiles++;
        dayProgress[date].copiedBytes += item.size ?? 0;
        if (dayProgress[date].copiedFiles >= dayProgress[date].totalFiles) {
          dayProgress[date].status = 'completed';
        } else {
          dayProgress[date].status = 'copying';
        }
      }
    }

    // Write initial state to per-job sync file (fast, small writes)
    const initialSyncState: SyncState = {
      objectCount: alreadyCopied.length,
      totalBytes: alreadyCopiedBytes,
      expectedObjectCount: totalObjects,
      expectedTotalBytes: totalBytes,
      syncStartedAt: new Date().toISOString(),
      syncProgress: {
        dayProgress,
        lastUpdated: new Date().toISOString(),
      },
    };
    await putSyncState(jobId, initialSyncState);

    // Also update jobs.json with initial counts (for backward compat)
    await updateJob(jobId, {
      objectCount: alreadyCopied.length,
      totalBytes: alreadyCopiedBytes,
      syncProgress: {
        dayProgress,
        lastUpdated: new Date().toISOString(),
      },
    });

    // ---------- Serialized progress writer ----------
    // Writes to per-job sync-state file (2-5KB) instead of full jobs.json (500KB+).
    // Only one write in flight at a time; latest snapshot overwrites pending.
    let currentDayIndex = 0;
    let currentFileInDay = 0;
    let lastWriteTime = Date.now();
    let writeInFlight = false;
    let pendingWrite: SyncState | null = null;

    /** Timeout for a single progress write — if S3 hangs, don't block forever. */
    const WRITE_TIMEOUT_MS = 30_000;

    const flushProgress = async (state: SyncState) => {
      if (writeInFlight) {
        pendingWrite = state; // overwrite — only latest matters
        return;
      }
      writeInFlight = true;
      lastWriteTime = Date.now();
      try {
        await Promise.race([
          putSyncState(jobId, state),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Progress write timed out after 30s')), WRITE_TIMEOUT_MS)
          ),
        ]);
      } catch (e) {
        console.warn('[SYNC] Progress write failed:', e instanceof Error ? e.message : e);
      }
      writeInFlight = false;
      // If a newer snapshot was queued while we were writing, flush it now
      if (pendingWrite) {
        const next = pendingWrite;
        pendingWrite = null;
        await flushProgress(next);
      }
    };

    // Offset counters: already-copied files count toward total progress
    const copiedOffset = alreadyCopied.length;
    const bytesOffset = alreadyCopiedBytes;

    const result = await copyS3ObjectsBatch(
      copies,
      (batchCopied, batchTotal, batchBytes, currentKey) => {
        // Overall progress includes already-copied + newly copied
        const overallCopied = copiedOffset + batchCopied;
        const overallBytes = bytesOffset + batchBytes;

        const { currentDayIndex: dayIdx, currentFileInDay: fileInDay, sortedDates: dates } = applyProgressUpdate(
          dayProgress,
          keyToDate,
          keyToSize,
          { copied: batchCopied, total: batchTotal, bytes: batchBytes, currentKey }
        );
        currentDayIndex = dayIdx;
        currentFileInDay = fileInDay;

        const now = Date.now();
        const shouldWrite = now - lastWriteTime >= PROGRESS_WRITE_INTERVAL_MS || batchCopied === batchTotal;
        if (shouldWrite) {
          const currentDay = dates[currentDayIndex] ?? null;
          const totalInDay = currentDay ? dayProgress[currentDay]?.totalFiles : undefined;
          const pct = ((overallCopied / totalObjects) * 100).toFixed(1);
          const elapsed = (Date.now() - syncStartTime) / 1000;
          const speed = elapsed > 0 ? (batchBytes / elapsed / 1024 / 1024).toFixed(2) : '0';
          console.log(
            `[SYNC] Progress: ${pct}% (${overallCopied}/${totalObjects}), ${(overallBytes / 1024 / 1024 / 1024).toFixed(2)} GB @ ${speed} MB/s`
          );
          // Write to per-job state file (fast, ~2-5KB)
          flushProgress({
            objectCount: overallCopied,
            totalBytes: overallBytes,
            expectedObjectCount: totalObjects,
            expectedTotalBytes: totalBytes,
            syncStartedAt: initialSyncState.syncStartedAt,
            syncProgress: buildSyncProgressPayload(
              dayProgress,
              currentDay,
              currentFileInDay,
              totalInDay
            ),
          });
        }
      },
      controller.signal
    );

    // Wait for any in-flight progress write to finish before proceeding
    while (writeInFlight || pendingWrite) {
      await new Promise((r) => setTimeout(r, 100));
    }

    const totalCopied = copiedOffset + result.copied;
    const totalCopiedBytes = bytesOffset + result.totalBytes;
    console.log(`[SYNC] CHECK 2 - Copy done: ${result.copied} new + ${copiedOffset} already existed = ${totalCopied} total, ${result.failed} failed`);

    for (const failedKey of result.failedKeys) {
      if (keyToDate.has(failedKey)) {
        const date = keyToDate.get(failedKey)!;
        const dayProg = dayProgress[date];
        if (dayProg) {
          dayProg.failedFiles++;
          const err = result.errors.find((e) => e.includes(failedKey)) || 'Unknown error';
          dayProg.errors = dayProg.errors ?? [];
          dayProg.errors.push({ file: failedKey.split('/').pop() || failedKey, error: err });
          dayProg.status = 'failed';
        }
      }
    }
    for (const date of sortedDates) {
      const dayProg = dayProgress[date];
      if (dayProg && dayProg.copiedFiles === dayProg.totalFiles && dayProg.failedFiles === 0) {
        dayProg.status = 'completed';
      }
    }

    if (controller.signal.aborted) {
      console.log(`[SYNC] Sync cancelled by user; partial state: ${totalCopied}/${totalObjects}`);
      await updateJob(jobId, {
        objectCount: totalCopied,
        totalBytes: totalCopiedBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        syncCancelledAt: new Date().toISOString(),
      });
      return;
    }

    if (result.failed > 0) {
      const pct = ((result.failed / totalObjects) * 100).toFixed(1);
      console.error(`[SYNC] ❌ Copy failed for ${result.failed}/${totalObjects} files (${pct}%)`);
      await updateJob(jobId, {
        objectCount: totalCopied,
        totalBytes: totalCopiedBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        errorMessage: `Sync incomplete: ${result.failed} of ${totalObjects} files failed to copy. First error: ${result.errors[0] ?? 'unknown'}`,
      });
      return;
    }

    // ---------- Phase 3: Verification (single pass for count + dates) ----------
    // One paginated pass over destination: gets count + date partitions. No full array in memory.
    const verifyResult = await countS3ObjectsByPrefix(
      destPathParsed.bucket,
      destPathParsed.key,
      { collectDates: true }
    );

    const destCount = verifyResult.count;
    const countMatch = destCount === totalObjects;
    const destPartitionDates = verifyResult.dates!;

    if (!countMatch) {
      const diff = Math.abs(destCount - totalObjects);
      console.error(`[SYNC] CHECK 3 - Verification failed: dest=${destCount}, source=${totalObjects}, diff=${diff}`);
      await updateJob(jobId, {
        objectCount: totalCopied,
        totalBytes: totalCopiedBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        errorMessage: `Verification failed: destination has ${destCount} objects, expected ${totalObjects} from source.`,
      });
      return;
    }
    console.log(`[SYNC] CHECK 3 - ✅ Verified: destination has ${destCount} objects (matches source)`);

    // ETag sampling (parallel HEAD requests)
    const etagResult = await verifyEtags(allCopies, { etagSampleRatio: 0.08 });

    if (!etagResult.integrityPassed) {
      console.error(`[SYNC] CHECK 3b - Integrity failed: ${etagResult.etagMismatches}/${etagResult.sampleSize} files have ETag mismatch`);
      await updateJob(jobId, {
        objectCount: totalCopied,
        totalBytes: totalCopiedBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        errorMessage: `Integrity verification failed: ${etagResult.etagMismatches} of ${etagResult.sampleSize} sampled files have corrupted data. ${etagResult.etagErrors.slice(0, 3).join('; ')}${etagResult.etagErrors.length > 3 ? '...' : ''}`,
      });
      return;
    }

    // ---------- Phase 4: Partition verification ----------
    const sourcePartitionDates = extractUniqueDatesFromKeys(sourceKeys);

    const missingInDest = sourcePartitionDates.filter((d) => !destPartitionDates.includes(d));
    if (missingInDest.length > 0) {
      const errorMsg = `Date partition verification failed: ${missingInDest.length} date partitions present in source but missing in destination: ${missingInDest.slice(0, 10).join(', ')}${missingInDest.length > 10 ? '...' : ''}`;
      console.error(`[SYNC] CHECK 4 - ❌ ${errorMsg}`);
      await updateJob(jobId, {
        objectCount: totalCopied,
        totalBytes: totalCopiedBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        errorMessage: errorMsg,
      });
      return;
    }

    const job = await getJob(jobId);
    let verasetMissingDays = 0;
    if (job?.verasetPayload?.date_range?.from_date && job?.verasetPayload?.date_range?.to_date && sourcePartitionDates.length > 0) {
      const requestedFrom = job.verasetPayload.date_range.from_date;
      const requestedTo = job.verasetPayload.date_range.to_date;
      const requestedDays = calculateDaysInclusive(requestedFrom, requestedTo);
      verasetMissingDays = requestedDays - sourcePartitionDates.length;
      await updateJob(jobId, {
        actualDateRange: {
          from: sourcePartitionDates[0],
          to: sourcePartitionDates[sourcePartitionDates.length - 1],
          days: sourcePartitionDates.length,
        },
        dateRangeDiscrepancy: {
          requestedDays,
          actualDays: sourcePartitionDates.length,
          missingDays: verasetMissingDays,
        },
      });
      if (verasetMissingDays > 0) {
        console.warn(`[SYNC] CHECK 4b - ⚠️ Veraset delivered ${sourcePartitionDates.length}/${requestedDays} days (${verasetMissingDays} days not delivered by vendor — this is NOT a sync error)`);
      }
    }

    console.log(`[SYNC] CHECK 4 - ✅ All ${destPartitionDates.length} source date partitions verified in destination`);

    for (const date of sortedDates) {
      const dayProg = dayProgress[date];
      if (dayProg) dayProg.status = 'completed';
    }

    const verificationResult = {
      countMatch,
      sourceCount: totalObjects,
      destCount,
      integrityPassed: etagResult.integrityPassed,
      etagSampleSize: etagResult.sampleSize,
      etagMismatches: etagResult.etagMismatches,
      multipartSkipped: etagResult.multipartSkipped,
      sourcePartitionDates,
      destPartitionDates,
      missingPartitionsInDest: missingInDest,
      verasetMissingDays,
      timestamp: new Date().toISOString(),
    };

    // Final merge to jobs.json (single write with all completion data)
    const finalUpdate = {
      s3DestPath: destPath,
      objectCount: totalCopied,
      totalBytes: totalCopiedBytes,
      expectedObjectCount: totalObjects,
      expectedTotalBytes: totalBytes,
      verificationResult,
      syncedAt: new Date().toISOString(),
      syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
    };
    await updateJob(jobId, finalUpdate);

    // Also update per-job state to reflect completion
    await putSyncState(jobId, {
      objectCount: totalCopied,
      totalBytes: totalCopiedBytes,
      expectedObjectCount: totalObjects,
      expectedTotalBytes: totalBytes,
      syncedAt: finalUpdate.syncedAt,
      syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
    });

    const totalDuration = ((Date.now() - syncStartTime) / 1000).toFixed(1);
    console.log(`[SYNC] Completed job ${jobId}: ${totalCopied} files (${result.copied} new + ${copiedOffset} already existed) in ${totalDuration}s`);
  } catch (err: unknown) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    console.error(`[SYNC] ❌ Error syncing job ${jobId}:`, errorMsg);
    await updateJob(jobId, { errorMessage: `Sync failed: ${errorMsg}` });
  } finally {
    unregisterAbortController(jobId);
    await releaseSyncLock(jobId);
  }
}
