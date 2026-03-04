import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzeOriginDestination } from '@/lib/dataset-analyzer-od';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * Reverse a poiMapping { verasetId: originalId } → { originalId: verasetId }
 */
function reversePoiMapping(poiMapping: Record<string, string>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(poiMapping).map(([verasetId, originalId]) => [originalId, verasetId])
  );
}

/**
 * GET /api/external/jobs/[datasetName]/od/[poiId]
 * Origin-destination analysis for a single POI.
 *
 * datasetName = jobId, poiId = the original POI ID the client sent when creating the job.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ datasetName: string; poiId: string }> }
): Promise<NextResponse> {
  let jobId: string | undefined;

  try {
    const params = await context.params;
    jobId = params.datasetName;
    const poiId = decodeURIComponent(params.poiId);

    console.log(`[OD-POI] GET /api/external/jobs/${jobId}/od/${poiId}`);

    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
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
          message: `Job status is '${job.status}'. OD analysis is only available for completed jobs.`,
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

    // 4. Resolve POI ID to Veraset ID
    const poiMapping = job.poiMapping || {};
    const reversedMap = reversePoiMapping(poiMapping);
    const verasetPoiId = reversedMap[poiId];

    if (!verasetPoiId) {
      const availableIds = Object.values(poiMapping);
      return NextResponse.json(
        {
          error: 'Invalid POI ID',
          message: `POI '${poiId}' not found in this job.`,
          available_poi_ids: availableIds,
        },
        { status: 400 }
      );
    }

    // 5. Check cache
    const OD_VERSION = 'v2';
    const cacheKey = `od-${OD_VERSION}-${jobId}-poi-${poiId}`;
    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        console.log(`[OD-POI] Serving cached result for job ${jobId}, poi ${poiId}`);
        return NextResponse.json(cached, {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT' },
        });
      }
    } catch {
      // No cache, continue
    }

    // 6. Run OD analysis filtered to this POI
    // Query with BOTH the Veraset ID (geo_radius_X) and the original client ID (poi_123)
    // because Parquet data may contain either format in the poi_ids array.
    const poiIdsToQuery = verasetPoiId !== poiId
      ? [verasetPoiId, poiId]
      : [verasetPoiId];
    console.log(`[OD-POI] Computing for job ${jobId}, dataset: ${datasetName}, poi: ${poiId} (veraset: ${verasetPoiId}), querying: [${poiIdsToQuery.join(', ')}]`);
    logger.log(`Computing OD for job ${jobId}, poi ${poiId}`);

    let result;
    try {
      result = await analyzeOriginDestination(datasetName, { poiIds: poiIdsToQuery });
    } catch (error: any) {
      console.error(`[OD-POI ERROR] Analysis failed:`, error.message);
      logger.error(`OD-POI analysis failed for job ${jobId}, poi ${poiId}`, { error: error.message });

      if (error.message?.includes('Access Denied') ||
          error.message?.includes('AccessDeniedException') ||
          error.message?.includes('not authorized')) {
        return NextResponse.json(
          {
            error: 'Dataset not accessible',
            message: 'The job data may not be synced yet or AWS permissions are insufficient.',
          },
          { status: 409 }
        );
      }

      if (error.message?.includes('AWS credentials not configured')) {
        return NextResponse.json(
          { error: 'Configuration Error', message: 'AWS credentials not configured.' },
          { status: 503 }
        );
      }

      return NextResponse.json(
        { error: 'Internal Server Error', message: 'Failed to compute OD analysis' },
        { status: 500 }
      );
    }

    // 7. Build response
    const poiNames = job.poiNames || {};
    const poiName = poiNames[verasetPoiId] || poiId;

    const response = {
      job_id: jobId,
      poi: { id: poiId, name: poiName },
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

    console.log(`[OD-POI] Analysis completed for poi ${poiId}`, {
      totalDeviceDays: response.summary.total_device_days,
      originZipcodes: response.origins.length,
      destinationZipcodes: response.destinations.length,
    });

    // 8. Cache result
    try {
      await putConfig(cacheKey, response);
      logger.log(`Cached OD for job ${jobId}, poi ${poiId}`);
    } catch (err: any) {
      logger.warn(`Failed to cache OD for job ${jobId}, poi ${poiId}`, { error: err.message });
    }

    return NextResponse.json(response, {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'X-Cache': 'MISS' },
    });

  } catch (error: any) {
    const errorJobId = jobId || 'unknown';
    console.error(`[OD-POI ERROR] GET /api/external/jobs/${errorJobId}/od/:poiId:`, error.message);
    logger.error(`GET /api/external/jobs/${errorJobId}/od/:poiId error:`, error);

    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message || 'An unexpected error occurred' },
      { status: 500 }
    );
  }
}
