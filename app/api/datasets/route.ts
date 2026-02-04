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

export async function GET() {
  try {
    // Get all jobs to create lookup map: folder name -> job
    const jobs = await getAllJobs();
    const folderToJob = new Map<string, Job>();
    
    for (const job of jobs) {
      if (job.s3DestPath) {
        // Extract folder name from path like "s3://bucket/folder-name/" or "s3://bucket/folder-name"
        const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
        const folderName = s3Path.split('/').filter(Boolean).pop();
        if (folderName) {
          folderToJob.set(folderName, job);
        }
      }
    }

    // List all S3 folders (datasets)
    const res = await s3.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Delimiter: '/'
    }));

    const datasets: any[] = [];
    const systemFolders = ['config', 'exports', 'pois', 'athena-results'];

    for (const prefix of res.CommonPrefixes || []) {
      const folderName = prefix.Prefix?.replace('/', '');
      
      // Skip system folders
      if (!folderName || systemFolders.includes(folderName)) {
        continue;
      }

      // Look up job info for display name and metadata
      const job = folderToJob.get(folderName);

      // Get dataset details from S3
      try {
        const detailRes = await s3.send(new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: `${folderName}/`
        }));

        const objects = detailRes.Contents || [];
        const parquetFiles = objects.filter(o => o.Key?.endsWith('.parquet'));

        // Extract date range from partition folders
        const dates = objects
          .map(o => o.Key?.match(/date=(\d{4}-\d{2}-\d{2})/)?.[1])
          .filter(Boolean)
          .sort();

        datasets.push({
          id: folderName,
          name: job?.name || folderName, // Use job name if available, otherwise folder name
          jobId: job?.jobId || null,
          type: job?.type || 'pings',
          poiCount: job?.poiCount || null,
          external: job?.external || false,
          objectCount: parquetFiles.length,
          totalBytes: objects.reduce((sum, o) => sum + (o.Size || 0), 0),
          dateRange: dates.length 
            ? { from: dates[0], to: dates[dates.length - 1] } 
            : job?.dateRange || null,
          lastModified: objects[0]?.LastModified?.toISOString() || job?.syncedAt || job?.createdAt,
        });
      } catch (error) {
        console.warn(`Error listing objects for ${folderName}:`, error);
        // Still include dataset with job metadata if available
        if (job) {
          datasets.push({
            id: folderName,
            name: job.name,
            jobId: job.jobId,
            type: job.type,
            poiCount: job.poiCount,
            external: job.external || false,
            objectCount: job.objectCount || 0,
            totalBytes: job.totalBytes || 0,
            dateRange: job.dateRange,
            lastModified: job.syncedAt || job.createdAt,
          });
        }
      }
    }

    // Sort by date descending (newest first)
    datasets.sort((a, b) => {
      const dateA = a.dateRange?.to || a.lastModified || '0000';
      const dateB = b.dateRange?.to || b.lastModified || '0000';
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
