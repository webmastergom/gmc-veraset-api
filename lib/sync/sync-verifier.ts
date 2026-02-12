/**
 * Post-sync verification: destination count vs source (independent) and ETag sample for corruption detection.
 * S3 CopyObject for objects <5GB preserves ETag; multipart (>5GB) ETags contain '-' and are not comparable.
 */

import { countS3Objects, getObjectETag } from '@/lib/s3';
import type { S3PathParsed, SyncVerificationResult } from '@/lib/sync-types';
import type { CopyItem } from '@/lib/s3';

const DEFAULT_ETAG_SAMPLE_RATIO = 0.08; // 8% of files

/** Multipart ETags contain '-' — not comparable between source and dest copy. */
function isMultipart(etag: string): boolean {
  return etag.includes('-');
}

export interface SyncVerificationOptions {
  /** Min fraction of files to sample for ETag check (0.05–0.10). */
  etagSampleRatio?: number;
}

/**
 * Verify destination: (1) count matches source (independent source of truth), (2) ETag sample for corruption.
 * Only single-part ETags are compared; multipart objects are skipped.
 */
export async function verifySync(
  destPath: S3PathParsed,
  sourceObjectCount: number,
  copies: CopyItem[],
  options: SyncVerificationOptions = {}
): Promise<SyncVerificationResult> {
  const ratio = Math.min(0.10, Math.max(0.05, options.etagSampleRatio ?? DEFAULT_ETAG_SAMPLE_RATIO));
  const destCount = await countS3Objects(destPath.bucket, destPath.key);
  const countMatch = destCount === sourceObjectCount;

  let sampleSize = 0;
  let etagMismatches = 0;
  let multipartSkipped = 0;
  const etagErrors: string[] = [];

  if (copies.length > 0 && countMatch) {
    sampleSize = Math.max(1, Math.min(Math.ceil(copies.length * ratio), 50));
    const step = Math.max(1, Math.floor(copies.length / sampleSize));
    const indices: number[] = [];
    for (let i = 0; i < sampleSize; i++) {
      indices.push(Math.min(i * step, copies.length - 1));
    }
    for (const i of indices) {
      const item = copies[i];
      const [srcEtag, destEtag] = await Promise.all([
        getObjectETag(item.sourceBucket, item.sourceKey),
        getObjectETag(item.destBucket, item.destKey),
      ]);
      if (srcEtag == null || destEtag == null) continue;
      if (isMultipart(srcEtag) || isMultipart(destEtag)) {
        multipartSkipped++;
        continue;
      }
      if (srcEtag !== destEtag) {
        etagMismatches++;
        etagErrors.push(`${item.sourceKey}: ETag mismatch (source ${srcEtag} vs dest ${destEtag})`);
      }
    }
    if (etagMismatches > 0) {
      console.warn(`[SYNC] Verification: ${etagMismatches}/${sampleSize - multipartSkipped} sampled single-part files had ETag mismatch`);
    }
  }

  return {
    destCount,
    countMatch,
    sampleSize,
    etagMismatches,
    etagErrors,
    multipartSkipped,
    integrityPassed: etagMismatches === 0,
  };
}
