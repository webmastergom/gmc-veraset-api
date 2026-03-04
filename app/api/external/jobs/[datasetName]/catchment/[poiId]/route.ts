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
 * Reverse a poiMapping { verasetId: originalId } → { originalId: verasetId }
 */
function reversePoiMapping(poiMapping: Record<string, string>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(poiMapping).map(([verasetId, originalId]) => [originalId, verasetId])
  );
}

/**
 * GET /api/external/jobs/[datasetName]/catchment/[poiId]
 * Catchment analysis for a single POI.
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

    console.log(`[CATCHMENT-POI] GET /api/external/jobs/${jobId}/catchment/${poiId}`);

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
    const CATCHMENT_VERSION = 'v6';
    const cacheKey = `catchment-${CATCHMENT_VERSION}-${jobId}-poi-${poiId}`;
    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        console.log(`[CATCHMENT-POI] Serving cached result for job ${jobId}, poi ${poiId}`);
        return NextResponse.json(cached, {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT' },
        });
      }
    } catch {
      // No cache, continue
    }

    // 6. Run analysis filtered to this POI
    // Query with BOTH the Veraset ID (geo_radius_X) and the original client ID (poi_123)
    // because Parquet data may contain either format in the poi_ids array.
    const poiIdsToQuery = verasetPoiId !== poiId
      ? [verasetPoiId, poiId]
      : [verasetPoiId];
    console.log(`[CATCHMENT-POI] Computing for job ${jobId}, dataset: ${datasetName}, poi: ${poiId} (veraset: ${verasetPoiId}), querying: [${poiIdsToQuery.join(', ')}]`);
    logger.log(`Computing catchment for job ${jobId}, poi ${poiId}`);

    let res;
    try {
      res = await analyzeOrigins(datasetName, { poiIds: poiIdsToQuery });
    } catch (error: any) {
      console.error(`[CATCHMENT-POI ERROR] Analysis failed:`, error.message);
      logger.error(`Catchment-POI analysis failed for job ${jobId}, poi ${poiId}`, { error: error.message });

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
        { error: 'Internal Server Error', message: 'Failed to compute catchment analysis' },
        { status: 500 }
      );
    }

    // 7. Build response
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

    // Find POI name from job metadata
    const poiNames = job.poiNames || {};
    const poiName = poiNames[verasetPoiId] || poiId;

    const response = {
      job_id: jobId,
      poi: { id: poiId, name: poiName },
      analyzed_at: res.analyzedAt,
      methodology: {
        approach: 'origin_first_ping',
        description: 'First GPS ping of each device-day, reverse geocoded to postal code. Filtered to visitors of this specific POI.',
      },
      coverage: {
        totalDevicesVisitedPoi: res.totalDevicesVisitedPois,
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

    // 8. Cache result
    try {
      await putConfig(cacheKey, response);
      logger.log(`Cached catchment for job ${jobId}, poi ${poiId}`);
    } catch (err: any) {
      logger.warn(`Failed to cache catchment for job ${jobId}, poi ${poiId}`, { error: err.message });
    }

    return NextResponse.json(response, {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'X-Cache': 'MISS' },
    });

  } catch (error: any) {
    const errorJobId = jobId || 'unknown';
    console.error(`[CATCHMENT-POI ERROR] GET /api/external/jobs/${errorJobId}/catchment/:poiId:`, error.message);
    logger.error(`GET /api/external/jobs/${errorJobId}/catchment/:poiId error:`, error);

    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message || 'An unexpected error occurred' },
      { status: 500 }
    );
  }
}
