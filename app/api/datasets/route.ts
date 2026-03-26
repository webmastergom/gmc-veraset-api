import { NextResponse } from 'next/server';
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { getAllJobs, Job } from '@/lib/jobs';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

const s3 = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
const LIST_PAGE_SIZE = 1000;
const SYSTEM_FOLDERS = new Set(['config', 'exports', 'pois', 'athena-results', 'staging', 'MAIDs']);
const CONCURRENCY = 25;

// ── Server-side cache (60s TTL) ──────────────────────────────────────
let cachedDatasets: any[] | null = null;
let cacheTimestamp = 0;
const CACHE_TTL_MS = 60_000;

/** Process a batch of promises with concurrency limit */
async function batchProcess<T>(items: T[], fn: (item: T) => Promise<any>, concurrency: number): Promise<any[]> {
  const results: any[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.all(batch.map(fn));
    results.push(...batchResults);
  }
  return results;
}

export async function GET() {
  try {
    // Return cached if fresh
    if (cachedDatasets && Date.now() - cacheTimestamp < CACHE_TTL_MS) {
      return NextResponse.json({ datasets: cachedDatasets });
    }

    let jobs: Job[] = [];
    const [jobsResult, listRes] = await Promise.all([
      getAllJobs().catch((e) => { console.error('[DATASETS] getAllJobs failed:', e.message); return [] as Job[]; }),
      s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Delimiter: '/' })),
    ]);
    jobs = jobsResult;

    const folderToJob = new Map<string, Job>();
    for (const job of jobs) {
      if (job.s3DestPath) {
        const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
        const folderName = s3Path.split('/').filter(Boolean).pop();
        if (folderName) folderToJob.set(folderName, job);
      }
    }

    const folders = (listRes.CommonPrefixes || [])
      .map((p) => p.Prefix?.replace('/', ''))
      .filter((f): f is string => !!f && !SYSTEM_FOLDERS.has(f));

    // Parallel S3 detail calls (25 concurrent)
    const datasets = (await batchProcess(folders, async (folderName) => {
      const job = folderToJob.get(folderName);
      try {
        const detailRes = await s3.send(new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: `${folderName}/`,
          MaxKeys: LIST_PAGE_SIZE,
        }));
        const objects = detailRes.Contents || [];
        const parquetFiles = objects.filter((o) => o.Key?.endsWith('.parquet'));
        const dates = [...new Set(
          objects.map((o) => o.Key?.match(/date=(\d{4}-\d{2}-\d{2})/)?.[1]).filter(Boolean)
        )].sort() as string[];
        const totalBytes = objects.reduce((sum, o) => sum + (Number(o.Size) || 0), 0);

        return {
          id: folderName,
          name: job?.name || folderName,
          jobId: job?.jobId ?? null,
          type: job?.type || 'pings',
          poiCount: job?.poiCount ?? null,
          external: job?.external ?? false,
          objectCount: parquetFiles.length,
          totalBytes,
          dateRange: dates.length
            ? { from: dates[0], to: dates[dates.length - 1] }
            : job?.dateRange ?? null,
          lastModified: job?.syncedAt || job?.createdAt,
          syncedAt: job?.syncedAt ?? null,
          dateRangeDiscrepancy: job?.dateRangeDiscrepancy ?? null,
          verasetPayload: job?.verasetPayload ?? null,
          actualDateRange: job?.actualDateRange ?? null,
        };
      } catch {
        if (!job) return null;
        return {
          id: folderName,
          name: job.name,
          jobId: job.jobId,
          type: job.type,
          poiCount: job.poiCount,
          external: job.external ?? false,
          objectCount: job.objectCount ?? 0,
          totalBytes: job.totalBytes ?? 0,
          dateRange: job.dateRange,
          lastModified: job.syncedAt || job.createdAt,
          syncedAt: job.syncedAt ?? null,
          dateRangeDiscrepancy: job.dateRangeDiscrepancy ?? null,
          verasetPayload: job.verasetPayload ?? null,
          actualDateRange: job.actualDateRange ?? null,
        };
      }
    }, CONCURRENCY)).filter(Boolean);

    // Sort by sync date descending
    datasets.sort((a: any, b: any) => {
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
