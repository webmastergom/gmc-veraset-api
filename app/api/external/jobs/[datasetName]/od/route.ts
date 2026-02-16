import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzeOriginDestination } from '@/lib/dataset-analyzer-od';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300; // 5 minutes for Athena queries + reverse geocoding

/**
 * GET /api/external/jobs/[datasetName]/od
 * Get origin-destination analysis for a job.
 *
 * For each device that visited a POI on a given day:
 * - Origin = first GPS ping of the day (where they came from)
 * - Destination = last GPS ping of the day (where they went after)
 * Results are reverse geocoded and aggregated by zipcode.
 *
 * The datasetName param here is actually a jobId.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ datasetName: string }> }
): Promise<NextResponse> {
  let jobId: string | undefined;

  try {
    const params = await context.params;
    jobId = params.datasetName;

    console.log(`[OD] GET /api/external/jobs/${jobId}/od`);

    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      console.error(`[OD] Unauthorized: ${auth.error}`);
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    console.log(`[OD] API key validated for key: ${auth.keyId}`);

    // 2. Find job and verify status
    let job;
    try {
      job = await getJob(jobId);
    } catch (error: any) {
      console.error(`[OD] Error fetching job ${jobId}:`, error.message);
      return NextResponse.json(
        { error: 'Internal Server Error', message: 'Failed to fetch job' },
        { status: 500 }
      );
    }

    if (!job) {
      console.error(`[OD] Job not found: ${jobId}`);
      return NextResponse.json(
        { error: 'Not Found', message: `Job '${jobId}' not found.` },
        { status: 404 }
      );
    }

    console.log(`[OD] Job found: ${jobId}, status: ${job.status}`);

    if (job.status !== 'SUCCESS') {
      return NextResponse.json(
        {
          error: 'Job Not Ready',
          message: `Job status is '${job.status}'. OD analysis is only available for completed jobs.`,
          status: job.status,
        },
        { status: 409 }
      );
    }

    // 3. Get dataset name from s3DestPath
    if (!job.s3DestPath) {
      console.error(`[OD] Job ${jobId} has no s3DestPath`);
      return NextResponse.json(
        { error: 'Job data not synced yet. Please try again later.' },
        { status: 409 }
      );
    }

    const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
    const datasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');

    console.log(`[OD] Dataset name extracted: ${datasetName} from path: ${job.s3DestPath}`);

    // 4. Check for cached result
    const OD_VERSION = 'v1';
    const cacheKey = `od-${OD_VERSION}-${jobId}`;
    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        console.log(`[OD] Serving cached result for job ${jobId}`);
        logger.log(`Serving cached OD analysis for job ${jobId}`);
        return NextResponse.json(cached, {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'X-Cache': 'HIT',
          },
        });
      }
    } catch (error: any) {
      console.log(`[OD] No cache found (this is OK): ${error.message}`);
    }

    // 5. Run OD analysis
    console.log(`[OD] Computing OD analysis for job ${jobId}, dataset: ${datasetName}`);
    logger.log(`Computing OD analysis for job ${jobId}, dataset: ${datasetName}`);

    let result;
    try {
      result = await analyzeOriginDestination(datasetName, {});
    } catch (error: any) {
      console.error(`[OD ERROR] Analysis failed:`, error.message);
      logger.error(`OD analysis failed for job ${jobId}`, { error: error.message });

      if (error.message?.includes('Access Denied') ||
          error.message?.includes('AccessDeniedException') ||
          error.message?.includes('not authorized')) {
        return NextResponse.json(
          {
            error: 'Dataset not accessible',
            message: 'The job data may not be synced yet or AWS permissions are insufficient.',
            details: error.message,
          },
          { status: 409 }
        );
      }

      if (error.message?.includes('AWS credentials not configured')) {
        return NextResponse.json(
          {
            error: 'Configuration Error',
            message: 'AWS credentials not configured.',
          },
          { status: 503 }
        );
      }

      return NextResponse.json(
        {
          error: 'Internal Server Error',
          message: 'Failed to compute OD analysis',
          details: error.message,
        },
        { status: 500 }
      );
    }

    // 6. Build response
    const response = {
      job_id: jobId,
      analyzed_at: result.analyzedAt,
      methodology: result.methodology,
      coverage: result.coverage,
      summary: {
        total_device_days: result.summary.totalDeviceDays,
        top_origin_zipcode: result.summary.topOriginZipcode,
        top_origin_city: result.summary.topOriginCity,
        top_destination_zipcode: result.summary.topDestinationZipcode,
        top_destination_city: result.summary.topDestinationCity,
      },
      origins: result.origins,
      destinations: result.destinations,
      temporal_patterns: result.temporalPatterns,
    };

    console.log(`[OD] Analysis completed successfully`, {
      totalDeviceDays: response.summary.total_device_days,
      originZipcodes: response.origins.length,
      destinationZipcodes: response.destinations.length,
    });

    // 7. Cache result in S3
    try {
      await putConfig(cacheKey, response);
      console.log(`[OD] Cached result for job ${jobId}`);
      logger.log(`Cached OD analysis for job ${jobId}`);
    } catch (err: any) {
      console.warn(`[OD] Failed to cache result: ${err.message}`);
      logger.warn(`Failed to cache OD analysis for job ${jobId}`, { error: err.message });
    }

    return NextResponse.json(response, {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'X-Cache': 'MISS',
      },
    });

  } catch (error: any) {
    const errorJobId = jobId || 'unknown';
    console.error(`[OD ERROR] GET /api/external/jobs/${errorJobId}/od:`, {
      error: error.message,
      stack: error.stack,
      name: error.name,
    });

    logger.error(`GET /api/external/jobs/${errorJobId}/od error:`, error);

    return NextResponse.json(
      {
        error: 'Internal Server Error',
        message: error.message || 'An unexpected error occurred',
      },
      {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }
}
