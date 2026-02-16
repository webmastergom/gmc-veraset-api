import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzeOrigins } from '@/lib/dataset-analyzer-od';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/external/jobs/[datasetName]/catchment
 * Catchment analysis: origin of visitors by postal code.
 * Uses OD methodology: first ping of each device-day = origin, reverse geocoded.
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

    console.log(`[CATCHMENT-OD] GET /api/external/jobs/${jobId}/catchment`);

    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      console.error(`[CATCHMENT-OD] Unauthorized: ${auth.error}`);
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // 2. Find job and verify status
    let job;
    try {
      job = await getJob(jobId);
    } catch (error: any) {
      console.error(`[CATCHMENT-OD] Error fetching job ${jobId}:`, error.message);
      return NextResponse.json(
        { error: 'Internal Server Error', message: 'Failed to fetch job' },
        { status: 500 }
      );
    }

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

    // 4. Check cache (v5 = multi-country GeoJSON, no Nominatim)
    const CATCHMENT_VERSION = 'v5';
    const cacheKey = `catchment-${CATCHMENT_VERSION}-${jobId}`;
    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        console.log(`[CATCHMENT-OD] Serving cached result for job ${jobId}`);
        logger.log(`Serving cached catchment-od for job ${jobId}`);
        return NextResponse.json(cached, {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT' },
        });
      }
    } catch {
      // No cache, continue
    }

    // 5. Run OD analysis
    console.log(`[CATCHMENT-OD] Computing origin analysis for job ${jobId}, dataset: ${datasetName}`);
    logger.log(`Computing catchment-od for job ${jobId}, dataset: ${datasetName}`);

    let res;
    try {
      res = await analyzeOrigins(datasetName, {});
    } catch (error: any) {
      console.error(`[CATCHMENT-OD ERROR] Analysis failed:`, error.message);
      logger.error(`Catchment-OD analysis failed for job ${jobId}`, { error: error.message });

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
          message: 'Failed to compute catchment analysis',
          details: error.message,
        },
        { status: 500 }
      );
    }

    // 6. Build response — backward compatible with cloud-garritz
    const zipcodes = res.origins.map((z) => ({
      zipcode: z.zipcode,
      city: z.city,
      province: z.province,
      region: z.region,
      devices: z.devices,
      percentage: z.percentOfTotal,
      percentOfTotal: z.percentOfTotal,
      source: z.source,
    }));

    const totalMatched = zipcodes.reduce((s, z) => s + z.devices, 0);

    const response = {
      job_id: jobId,
      analyzed_at: res.analyzedAt,
      methodology: {
        approach: 'origin_first_ping',
        description: 'First GPS ping of each device-day, reverse geocoded to postal code.',
      },
      coverage: {
        totalDevicesVisitedPois: res.totalDevicesVisitedPois,
        totalDeviceDays: res.totalDeviceDays,
        devicesMatchedToZipcode: totalMatched,
        coverageRatePercent: res.coverageRatePercent,
        geocodingComplete: res.geocodingComplete,
      },
      summary: {
        total_devices_analyzed: res.totalDevicesVisitedPois,
        devices_matched_to_zipcode: totalMatched,
        total_zipcodes: zipcodes.length,
        top_zipcode: zipcodes[0]?.zipcode ?? null,
        top_city: zipcodes[0]?.city ?? null,
      },
      zipcodes,
    };

    // 7. Cache result
    try {
      await putConfig(cacheKey, response);
      logger.log(`Cached catchment-od for job ${jobId}`);
    } catch (err: any) {
      logger.warn(`Failed to cache catchment-od for job ${jobId}`, { error: err.message });
    }

    return NextResponse.json(response, {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'X-Cache': 'MISS' },
    });

  } catch (error: any) {
    const errorJobId = jobId || 'unknown';
    console.error(`[CATCHMENT-OD ERROR] GET /api/external/jobs/${errorJobId}/catchment:`, error.message);
    logger.error(`GET /api/external/jobs/${errorJobId}/catchment error:`, error);

    return NextResponse.json(
      {
        error: 'Internal Server Error',
        message: error.message || 'An unexpected error occurred',
      },
      { status: 500 }
    );
  }
}
