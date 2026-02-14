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

/** One page per folder (max 1000) so list loads fast; use Audit for exact comparison. */
const LIST_PAGE_SIZE = 1000;

export async function GET() {
  try {
    const jobs = await getAllJobs();
    const folderToJob = new Map<string, Job>();
    for (const job of jobs) {
      if (job.s3DestPath) {
        const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
        const folderName = s3Path.split('/').filter(Boolean).pop();
        if (folderName) folderToJob.set(folderName, job);
      }
    }

    const listRes = await s3.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Delimiter: '/',
    }));

    const datasets: any[] = [];
    const systemFolders = ['config', 'exports', 'pois', 'athena-results'];

    for (const prefix of listRes.CommonPrefixes || []) {
      const folderName = prefix.Prefix?.replace('/', '');
      if (!folderName || systemFolders.includes(folderName)) continue;

      const job = folderToJob.get(folderName);

      try {
        const detailRes = await s3.send(new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: `${folderName}/`,
          MaxKeys: LIST_PAGE_SIZE,
        }));
        const objects = detailRes.Contents || [];
        const parquetFiles = objects.filter((o) => o.Key && o.Key.endsWith('.parquet'));
        const dates = [...new Set(
          objects.map((o) => o.Key?.match(/date=(\d{4}-\d{2}-\d{2})/)?.[1]).filter(Boolean)
        )].sort() as string[];
        const totalBytes = objects.reduce((sum, o) => sum + (Number(o.Size) || 0), 0);

        datasets.push({
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
        });
      } catch (error) {
        console.warn(`Error listing objects for ${folderName}:`, error);
        if (job) {
          datasets.push({
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
        });
        }
      }
    }

    // Sort by sync date descending (most recently synced first)
    datasets.sort((a, b) => {
      const dateA = a.syncedAt || a.lastModified || '0000';
      const dateB = b.syncedAt || b.lastModified || '0000';
      return dateB.localeCompare(dateA);
    });

    return NextResponse.json({ datasets });

  } catch (error: any) {
    console.error('GET /api/datasets error:', error);
    return NextResponse.json(
      { error: 'Failed to list datasets', details: error.message },
      { status: 500 }
    );
  }
}
