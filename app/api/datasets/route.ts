import { NextResponse } from 'next/server';
import { getAllJobs, Job } from '@/lib/jobs';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60;

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

// ── Server-side cache (60s TTL) ──────────────────────────────────────
let cachedDatasets: any[] | null = null;
let cacheTimestamp = 0;
const CACHE_TTL_MS = 60_000;

/**
 * GET /api/datasets
 *
 * Fast listing using jobs.json as the single source of truth.
 * No per-folder S3 ListObjects calls — just 1 S3 read for jobs.json.
 * Scales to 1000+ datasets without timeout.
 */
export async function GET() {
  try {
    // Return cached if fresh
    if (cachedDatasets && Date.now() - cacheTimestamp < CACHE_TTL_MS) {
      return NextResponse.json({ datasets: cachedDatasets });
    }

    const jobs = await getAllJobs().catch((e) => {
      console.error('[DATASETS] getAllJobs failed:', e.message);
      return [] as Job[];
    });

    // Map jobs to dataset objects — only synced jobs with data in S3
    const datasets = jobs
      .filter((job) => job.s3DestPath && job.syncedAt)
      .map((job) => {
        const folderName = job.s3DestPath!.replace('s3://', '').replace(`${BUCKET}/`, '').replace(/\/$/, '').split('/').pop() || job.jobId;
        return {
          id: folderName,
          name: job.name,
          jobId: job.jobId,
          type: job.type || 'pings',
          poiCount: job.poiCount ?? null,
          external: job.external ?? false,
          objectCount: job.objectCount ?? 0,
          totalBytes: job.totalBytes ?? 0,
          dateRange: job.actualDateRange
            ? { from: job.actualDateRange.from, to: job.actualDateRange.to }
            : job.dateRange ?? null,
          lastModified: job.syncedAt || job.createdAt,
          syncedAt: job.syncedAt ?? null,
          country: job.country ?? null,
          dateRangeDiscrepancy: job.dateRangeDiscrepancy ?? null,
          verasetPayload: job.verasetPayload ?? null,
          actualDateRange: job.actualDateRange ?? null,
        };
      });

    // Sort by sync date descending
    datasets.sort((a, b) => {
      const dateA = a.syncedAt || a.lastModified || '0000';
      const dateB = b.syncedAt || b.lastModified || '0000';
      return dateB.localeCompare(dateA);
    });

    // Cache
    cachedDatasets = datasets;
    cacheTimestamp = Date.now();

    return NextResponse.json({ datasets });
  } catch (error: any) {
    console.error('GET /api/datasets error:', error);
    return NextResponse.json(
      { error: 'Failed to list datasets', details: error.message },
      { status: 500 }
    );
  }
}
