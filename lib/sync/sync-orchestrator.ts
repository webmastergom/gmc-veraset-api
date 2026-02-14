/**
 * Sync orchestrator: list → copy → verify. Uses S3BatchCopier, SyncProgressTracker, SyncVerifier.
 * Checks abort signal (from registry) between batches; records partial state on cancel.
 *
 * Progress writes are serialized: only one updateJob at a time, at most every 5s.
 * This prevents S3 race conditions (concurrent read-modify-write on jobs.json).
 */

import { getJob, updateJob, initializeSync, releaseSyncLock } from '@/lib/jobs';
import {
  listS3Objects,
  copyS3ObjectsBatch,
  normalizePrefix,
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
import { verifySync } from './sync-verifier';

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

    const listStart = Date.now();
    const sourceObjects = await listS3Objects(sourcePath.bucket, sourcePath.key);
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

    await initializeSync(jobId, destPath, totalObjects, totalBytes);

    const sourcePrefix = normalizePrefix(sourcePath.key);
    const destPrefix = normalizePrefix(destPathParsed.key);
    const copies: CopyItem[] = sourceObjects.map((obj) => {
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

    const sourceKeys = sourceObjects.map((o) => o.Key);
    const keyToSize = buildKeyToSizeMap(copies);
    const keyToDate = buildKeyToDateMap(sourceKeys);
    const dayProgress = buildInitialDayProgress(sourceKeys, copies);
    const sortedDates = Object.keys(dayProgress).sort();
    console.log(`[SYNC] Grouped ${totalObjects} objects into ${sortedDates.length} date partitions`);

    await updateJob(jobId, {
      syncProgress: {
        dayProgress,
        lastUpdated: new Date().toISOString(),
      },
    });

    // ---------- Serialized progress writer ----------
    // Only one S3 write in flight at a time. If a write is pending, the latest
    // snapshot is queued and written when the current write completes.
    let currentDayIndex = 0;
    let currentFileInDay = 0;
    let lastWriteTime = Date.now();
    let writeInFlight = false;
    let pendingWrite: Record<string, unknown> | null = null;

    const flushProgress = async (payload: Record<string, unknown>) => {
      if (writeInFlight) {
        pendingWrite = payload; // overwrite — only latest matters
        return;
      }
      writeInFlight = true;
      lastWriteTime = Date.now();
      try {
        await updateJob(jobId, payload as any);
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

    const result = await copyS3ObjectsBatch(
      copies,
      (copied, total, bytes, currentKey) => {
        const { currentDayIndex: dayIdx, currentFileInDay: fileInDay, sortedDates: dates } = applyProgressUpdate(
          dayProgress,
          keyToDate,
          keyToSize,
          { copied, total, bytes, currentKey }
        );
        currentDayIndex = dayIdx;
        currentFileInDay = fileInDay;

        const now = Date.now();
        const shouldWrite = now - lastWriteTime >= PROGRESS_WRITE_INTERVAL_MS || copied === total;
        if (shouldWrite) {
          const currentDay = dates[currentDayIndex] ?? null;
          const totalInDay = currentDay ? dayProgress[currentDay]?.totalFiles : undefined;
          const pct = ((copied / total) * 100).toFixed(1);
          const elapsed = (Date.now() - syncStartTime) / 1000;
          const speed = elapsed > 0 ? (bytes / elapsed / 1024 / 1024).toFixed(2) : '0';
          console.log(
            `[SYNC] Progress: ${pct}% (${copied}/${total}), ${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB @ ${speed} MB/s`
          );
          // Fire-and-forget but serialized — won't pile up
          flushProgress({
            objectCount: copied,
            totalBytes: bytes,
            expectedObjectCount: totalObjects,
            expectedTotalBytes: totalBytes,
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

    console.log(`[SYNC] CHECK 2 - Copy done: ${result.copied} ok, ${result.failed} failed`);

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
      console.log(`[SYNC] Sync cancelled by user; partial state: ${result.copied}/${totalObjects}`);
      await updateJob(jobId, {
        objectCount: result.copied,
        totalBytes: result.totalBytes,
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
        objectCount: result.copied,
        totalBytes: result.totalBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        errorMessage: `Sync incomplete: ${result.failed} of ${totalObjects} files failed to copy. First error: ${result.errors[0] ?? 'unknown'}`,
      });
      return;
    }

    const verification = await verifySync(
      destPathParsed,
      totalObjects,
      copies,
      { etagSampleRatio: 0.08 }
    );

    if (!verification.countMatch) {
      const diff = Math.abs(verification.destCount - totalObjects);
      console.error(`[SYNC] CHECK 3 - Verification failed: dest=${verification.destCount}, source=${totalObjects}, diff=${diff}`);
      await updateJob(jobId, {
        objectCount: result.copied,
        totalBytes: result.totalBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        errorMessage: `Verification failed: destination has ${verification.destCount} objects, expected ${totalObjects} from source.`,
      });
      return;
    }
    console.log(`[SYNC] CHECK 3 - ✅ Verified: destination has ${verification.destCount} objects (matches source)`);

    if (!verification.integrityPassed) {
      console.error(`[SYNC] CHECK 3b - Integrity failed: ${verification.etagMismatches}/${verification.sampleSize} files have ETag mismatch`);
      await updateJob(jobId, {
        objectCount: result.copied,
        totalBytes: result.totalBytes,
        expectedObjectCount: totalObjects,
        expectedTotalBytes: totalBytes,
        syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
        errorMessage: `Integrity verification failed: ${verification.etagMismatches} of ${verification.sampleSize} sampled files have corrupted data. ${verification.etagErrors.slice(0, 3).join('; ')}${verification.etagErrors.length > 3 ? '...' : ''}`,
      });
      return;
    }

    const sourcePartitionDates = extractUniqueDatesFromKeys(sourceKeys);
    const destObjects = await listS3Objects(destPathParsed.bucket, destPathParsed.key);
    const destKeys = destObjects.map((o) => o.Key);
    const destPartitionDates = extractUniqueDatesFromKeys(destKeys);

    const missingInDest = sourcePartitionDates.filter((d) => !destPartitionDates.includes(d));
    if (missingInDest.length > 0) {
      const errorMsg = `Date partition verification failed: ${missingInDest.length} date partitions present in source but missing in destination: ${missingInDest.slice(0, 10).join(', ')}${missingInDest.length > 10 ? '...' : ''}`;
      console.error(`[SYNC] CHECK 4 - ❌ ${errorMsg}`);
      await updateJob(jobId, {
        objectCount: result.copied,
        totalBytes: result.totalBytes,
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
      countMatch: verification.countMatch,
      sourceCount: totalObjects,
      destCount: verification.destCount,
      integrityPassed: verification.integrityPassed,
      etagSampleSize: verification.sampleSize,
      etagMismatches: verification.etagMismatches,
      multipartSkipped: verification.multipartSkipped,
      sourcePartitionDates,
      destPartitionDates,
      missingPartitionsInDest: missingInDest,
      verasetMissingDays,
      timestamp: new Date().toISOString(),
    };

    const finalUpdate = {
      s3DestPath: destPath,
      objectCount: result.copied,
      totalBytes: result.totalBytes,
      expectedObjectCount: totalObjects,
      expectedTotalBytes: totalBytes,
      verificationResult,
      syncedAt: new Date().toISOString(),
      syncProgress: { dayProgress, lastUpdated: new Date().toISOString() },
    };
    await updateJob(jobId, finalUpdate);
    const totalDuration = ((Date.now() - syncStartTime) / 1000).toFixed(1);
    console.log(`[SYNC] Completed job ${jobId}: ${result.copied} files in ${totalDuration}s`);
  } catch (err: unknown) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    console.error(`[SYNC] ❌ Error syncing job ${jobId}:`, errorMsg);
    await updateJob(jobId, { errorMessage: `Sync failed: ${errorMsg}` });
  } finally {
    unregisterAbortController(jobId);
    await releaseSyncLock(jobId);
  }
}
