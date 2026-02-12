import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzeResidentialZipcodes } from '@/lib/dataset-analyzer-residential';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300; // 5 minutes for Athena queries + reverse geocoding

/**
 * GET /api/external/jobs/[datasetName]/catchment
 * Get residential zipcode catchment analysis for a job.
 * 
 * This endpoint:
 * 1. Validates API key
 * 2. Finds the job and verifies it's completed
 * 3. Extracts dataset name from job's S3 path
 * 4. Runs residential zipcode analysis (cached if available)
 * 5. Returns catchment data showing where visitors come from
 * 
 * The datasetName param here is actually a jobId.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ datasetName: string }> | { datasetName: string } }
): Promise<NextResponse> {
  let jobId: string | undefined;
  
  try {
    // Handle params - Next.js 14+ may pass params as Promise
    let params: { datasetName: string };
    if (context.params instanceof Promise) {
      params = await context.params;
    } else {
      params = context.params;
    }
    jobId = params.datasetName;

    console.log(`[CATCHMENT] GET /api/external/jobs/${jobId}/catchment`);

    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      console.error(`[CATCHMENT] Unauthorized: ${auth.error}`);
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    console.log(`[CATCHMENT] API key validated for key: ${auth.keyId}`);

    // 2. Find job and verify status
    let job;
    try {
      job = await getJob(jobId);
    } catch (error: any) {
      console.error(`[CATCHMENT] Error fetching job ${jobId}:`, error.message);
      return NextResponse.json(
        { error: 'Internal Server Error', message: 'Failed to fetch job' },
        { status: 500 }
      );
    }

    if (!job) {
      console.error(`[CATCHMENT] Job not found: ${jobId}`);
      return NextResponse.json(
        { error: 'Not Found', message: `Job '${jobId}' not found.` },
        { status: 404 }
      );
    }

    console.log(`[CATCHMENT] Job found: ${jobId}, status: ${job.status}`);

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
      console.error(`[CATCHMENT] Job ${jobId} has no s3DestPath`);
      return NextResponse.json(
        { error: 'Job data not synced yet. Please try again later.' },
        { status: 409 }
      );
    }

    const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
    const datasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');
    
    console.log(`[CATCHMENT] Dataset name extracted: ${datasetName} from path: ${job.s3DestPath}`);

    // 4. Check for cached result (v3 = Spain bbox, foreign vs unmatched_domestic, nominatim truncated)
    const CATCHMENT_VERSION = 'v3';
    const cacheKey = `catchment-${CATCHMENT_VERSION}-${jobId}`;
    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        console.log(`[CATCHMENT] Serving cached result for job ${jobId}`);
        logger.log(`Serving cached catchment for job ${jobId}`);
        return NextResponse.json(cached, {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'X-Cache': 'HIT',
          },
        });
      }
    } catch (error: any) {
      console.log(`[CATCHMENT] No cache found (this is OK): ${error.message}`);
      // No cache, continue to compute
    }

    // 5. Run residential analysis
    console.log(`[CATCHMENT] Computing catchment for job ${jobId}, dataset: ${datasetName}`);
    logger.log(`Computing catchment for job ${jobId}, dataset: ${datasetName}`);

    let result;
    try {
      result = await analyzeResidentialZipcodes(datasetName, {});
    } catch (error: any) {
      console.error(`[CATCHMENT ERROR] Analysis failed:`, error.message);
      logger.error(`Catchment analysis failed for job ${jobId}`, { error: error.message });

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
            message: 'AWS credentials not configured. Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.',
          },
          { status: 503 }
        );
      }

      return NextResponse.json(
        { 
          error: 'Internal Server Error', 
          message: 'Failed to compute catchment analysis',
          details: error.message,
        },
        { status: 500 }
      );
    }

    // 6. Build response — backward compatible, new fields additive
    const response = {
      job_id: jobId,
      analyzed_at: result.analyzedAt,
      methodology: result.methodology,
      coverage: result.coverage,
      summary: {
        total_devices_analyzed: result.coverage.totalDevicesVisitedPois,
        devices_with_home_location: result.coverage.devicesWithHomeEstimate,
        devices_matched_to_zipcode: result.coverage.devicesMatchedToZipcode,
        devices_foreign_origin: result.coverage.devicesForeignOrigin,
        total_zipcodes: result.summary.totalZipcodes,
        top_zipcode: result.summary.topZipcode,
        top_city: result.summary.topCity,
        classification_rate: result.coverage.classificationRatePercent,
      },
      zipcodes: result.zipcodes,
    };

    console.log(`[CATCHMENT] Analysis completed successfully`, {
      totalDevices: response.summary.total_devices_analyzed,
      matchedDevices: response.summary.devices_matched_to_zipcode,
      zipcodes: response.summary.total_zipcodes,
    });

    // 7. Cache result in S3
    try {
      await putConfig(cacheKey, response);
      console.log(`[CATCHMENT] Cached result for job ${jobId}`);
      logger.log(`Cached catchment for job ${jobId}`);
    } catch (err: any) {
      console.warn(`[CATCHMENT] Failed to cache result: ${err.message}`);
      logger.warn(`Failed to cache catchment for job ${jobId}`, { error: err.message });
      // Continue even if caching fails
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
    console.error(`[CATCHMENT ERROR] GET /api/external/jobs/${errorJobId}/catchment:`, {
      error: error.message,
      stack: error.stack,
      name: error.name,
    });
    
    logger.error(`GET /api/external/jobs/${errorJobId}/catchment error:`, error);

    return NextResponse.json(
      { 
        error: 'Internal Server Error', 
        message: error.message || 'An unexpected error occurred',
      },
      { 
        status: 500,
        headers: {
          'Content-Type': 'application/json',
        }
      }
    );
  }
}
