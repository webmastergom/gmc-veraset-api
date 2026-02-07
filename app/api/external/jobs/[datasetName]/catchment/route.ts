import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzeResidentialZipcodes } from '@/lib/dataset-analyzer-residential';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/external/jobs/[datasetName]/catchment
 * Get residential zipcode catchment analysis for a job.
 * The datasetName param here is actually a jobId.
 */
export async function GET(
  request: NextRequest,
  { params }: { params: { datasetName: string } }
) {
  const jobId = params.datasetName;

  try {
    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // 2. Find job and verify status
    const job = await getJob(jobId);
    if (!job) {
      return NextResponse.json(
        { error: 'Not Found', message: `Job '${jobId}' not found.` },
        { status: 404 }
      );
    }

    if (job.status !== 'SUCCESS') {
      return NextResponse.json(
        {
          error: 'Job Not Ready',
          message: `Job status is '${job.status}'. Catchment analysis is only available for completed jobs.`,
          status: job.status,
        },
        { status: 409 }
      );
    }

    // 3. Get dataset name from s3DestPath
    if (!job.s3DestPath) {
      return NextResponse.json(
        { error: 'Job data not synced yet. Please try again later.' },
        { status: 409 }
      );
    }

    const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
    const datasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');

    // 4. Check for cached result
    const cacheKey = `catchment-${jobId}`;
    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        logger.log(`Serving cached catchment for job ${jobId}`);
        return NextResponse.json(cached);
      }
    } catch {
      // No cache, continue to compute
    }

    // 5. Run residential analysis
    logger.log(`Computing catchment for job ${jobId}, dataset: ${datasetName}`);

    const result = await analyzeResidentialZipcodes(datasetName, {});

    // 6. Build response
    const response = {
      job_id: jobId,
      analyzed_at: result.analyzedAt,
      summary: {
        total_devices_analyzed: result.summary.totalDevicesInDataset,
        devices_with_home_location: result.summary.devicesWithHomeLocation,
        devices_matched_to_zipcode: result.summary.devicesMatchedToZipcode,
        total_zipcodes: result.summary.totalZipcodes,
        top_zipcode: result.summary.topZipcode,
        top_city: result.summary.topCity,
      },
      zipcodes: result.zipcodes,
    };

    // 7. Cache result in S3
    try {
      await putConfig(cacheKey, response);
      logger.log(`Cached catchment for job ${jobId}`);
    } catch (err: any) {
      logger.warn(`Failed to cache catchment for job ${jobId}`, { error: err.message });
    }

    return NextResponse.json(response);

  } catch (error: any) {
    logger.error(`GET /api/external/jobs/${jobId}/catchment error:`, error);

    if (error.message?.includes('Access Denied') || error.message?.includes('AccessDeniedException')) {
      return NextResponse.json(
        { error: 'Dataset not accessible', message: 'The job data may not be synced yet.' },
        { status: 409 }
      );
    }

    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message },
      { status: 500 }
    );
  }
}
