/**
 * Post-sync verification: ETag sample for corruption detection.
 * S3 CopyObject for objects <5GB preserves ETag; multipart (>5GB) ETags contain '-' and are not comparable.
 *
 * Count + partition verification moved to orchestrator using countS3ObjectsByPrefix (single pass).
 */

import { getObjectETag } from '@/lib/s3';
import type { CopyItem } from '@/lib/s3';

const DEFAULT_ETAG_SAMPLE_RATIO = 0.08; // 8% of files

/** Multipart ETags contain '-' — not comparable between source and dest copy. */
function isMultipart(etag: string): boolean {
  return etag.includes('-');
}

export interface EtagVerificationOptions {
  /** Fraction of files to sample for ETag check (clamped to 0.05–0.10). */
  etagSampleRatio?: number;
}

export interface EtagVerificationResult {
  sampleSize: number;
  etagMismatches: number;
  etagErrors: string[];
  multipartSkipped: number;
  integrityPassed: boolean;
}

/**
 * Verify data integrity by sampling ETags (parallel HEAD requests).
 * Only single-part ETags are compared; multipart objects are skipped.
 */
export async function verifyEtags(
  copies: CopyItem[],
  options: EtagVerificationOptions = {}
): Promise<EtagVerificationResult> {
  const ratio = Math.min(0.10, Math.max(0.05, options.etagSampleRatio ?? DEFAULT_ETAG_SAMPLE_RATIO));

  if (copies.length === 0) {
    return { sampleSize: 0, etagMismatches: 0, etagErrors: [], multipartSkipped: 0, integrityPassed: true };
  }

  const sampleSize = Math.max(1, Math.min(Math.ceil(copies.length * ratio), 50));
  const step = Math.max(1, Math.floor(copies.length / sampleSize));
  const indices: number[] = [];
  for (let i = 0; i < sampleSize; i++) {
    indices.push(Math.min(i * step, copies.length - 1));
  }

  // Parallel HEAD requests — 50 concurrent requests is well within S3 limits
  const results = await Promise.all(
    indices.map(async (i) => {
      const item = copies[i];
      const [srcEtag, destEtag] = await Promise.all([
        getObjectETag(item.sourceBucket, item.sourceKey),
        getObjectETag(item.destBucket, item.destKey),
      ]);
      return { item, srcEtag, destEtag };
    })
  );

  let etagMismatches = 0;
  let multipartSkipped = 0;
  const etagErrors: string[] = [];

  for (const { item, srcEtag, destEtag } of results) {
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

  return {
    sampleSize,
    etagMismatches,
    etagErrors,
    multipartSkipped,
    integrityPassed: etagMismatches === 0,
  };
}
