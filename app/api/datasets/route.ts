import { NextResponse } from 'next/server';
import { getAllJobsSummary, Job, getJob } from '@/lib/jobs';
import { getAllMegaJobs, getMegaJob } from '@/lib/mega-jobs';
import { megaJobNameToDatasetId } from '@/lib/athena';

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
 * Also includes mega-job integrated datasets (Athena VIEWs).
 * No per-folder S3 ListObjects calls — just 1 S3 read for jobs.json + mega-jobs index.
 * Scales to 1000+ datasets without timeout.
 */
export async function GET() {
  try {
    // Return cached if fresh
    if (cachedDatasets && Date.now() - cacheTimestamp < CACHE_TTL_MS) {
      return NextResponse.json({ datasets: cachedDatasets });
    }

    const [jobs, megaJobs] = await Promise.all([
      getAllJobsSummary().catch((e) => {
        console.error('[DATASETS] getAllJobsSummary failed:', e.message);
        return [] as Partial<Job>[];
      }),
      getAllMegaJobs().catch((e) => {
        console.error('[DATASETS] getAllMegaJobs failed:', e.message);
        return [] as any[];
      }),
    ]);

    // Map jobs to dataset objects — only synced jobs with data in S3
    const datasets: any[] = jobs
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

    // ── Add mega-job integrated datasets (VIEWs) ──────────────────────
    // Only include mega-jobs that have at least some synced sub-jobs
    for (const mj of megaJobs) {
      if (!mj.subJobIds?.length) continue;
      // Only show mega-datasets for mega-jobs that are running, consolidating, or completed
      if (!['running', 'consolidating', 'completed', 'partial'].includes(mj.status)) continue;

      const megaDatasetId = megaJobNameToDatasetId(mj.name || mj.megaJobId);
      if (!megaDatasetId) continue;

      // Collect summary info from sub-jobs (lightweight — uses index data)
      let totalBytes = 0;
      let totalObjects = 0;
      let syncedCount = 0;

      // Load full mega-job to get sub-job details for date range
      const fullMj = await getMegaJob(mj.megaJobId);
      const subJobDetails = fullMj
        ? await Promise.all(fullMj.subJobIds.map(id => getJob(id).catch(() => null)))
        : [];

      let minDate: string | null = null;
      let maxDate: string | null = null;

      for (const sj of subJobDetails) {
        if (!sj || sj.status !== 'SUCCESS' || !sj.syncedAt) continue;
        syncedCount++;
        totalBytes += sj.totalBytes ?? 0;
        totalObjects += sj.objectCount ?? 0;
        const dr = sj.actualDateRange || sj.dateRange;
        if (dr) {
          const from = 'from' in dr ? dr.from : (dr as any).from_date;
          const to = 'to' in dr ? dr.to : (dr as any).to_date;
          if (from && (!minDate || from < minDate)) minDate = from;
          if (to && (!maxDate || to > maxDate)) maxDate = to;
        }
      }

      // Skip if no synced sub-jobs
      if (syncedCount === 0) continue;

      // Inherit country from sub-jobs (first one that has it set)
      const inheritedCountry = subJobDetails.find(sj => sj?.country)?.country
        || (mj as any).country
        || null;

      datasets.push({
        id: megaDatasetId,
        name: `${mj.name || mj.megaJobId} (integrated)`,
        megaJobId: mj.megaJobId,
        type: 'mega',
        isMegaDataset: true,
        subJobCount: mj.subJobIds.length,
        syncedSubJobs: syncedCount,
        poiCount: null,
        external: false,
        objectCount: totalObjects,
        totalBytes,
        dateRange: minDate && maxDate ? { from: minDate, to: maxDate } : null,
        lastModified: mj.updatedAt || mj.createdAt,
        syncedAt: mj.updatedAt || mj.createdAt,
        country: inheritedCountry,
        dateRangeDiscrepancy: null,
        verasetPayload: null,
        actualDateRange: minDate && maxDate ? { from: minDate, to: maxDate, days: 0 } : null,
      });
    }

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
