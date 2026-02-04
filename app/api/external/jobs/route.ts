import { NextRequest, NextResponse } from 'next/server';
import { getAllJobs } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/external/jobs
 * List all available jobs/datasets for external clients
 */
export async function GET(request: NextRequest) {
  try {
    // Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // Get all jobs
    const allJobs = await getAllJobs();

    // Filter successful jobs with valid s3DestPath
    const availableJobs = allJobs
      .filter(job => job.status === 'SUCCESS' && job.s3DestPath)
      .map(job => {
        // Extract dataset name from s3DestPath
        // e.g., "s3://bucket/spain-nicotine-full-jan/" -> "spain-nicotine-full-jan"
        const s3Path = job.s3DestPath!.replace('s3://', '').replace(`${BUCKET}/`, '');
        const datasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');

        return {
          datasetName,
          jobName: job.name,
          jobId: job.jobId,
          dateRange: job.dateRange,
          status: job.status,
          poiCount: job.poiCount,
        };
      });

    return NextResponse.json({ jobs: availableJobs });
  } catch (error: any) {
    console.error('GET /api/external/jobs error:', error);
    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message },
      { status: 500 }
    );
  }
}
