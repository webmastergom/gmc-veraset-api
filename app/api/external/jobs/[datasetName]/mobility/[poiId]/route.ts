import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzeMobility } from '@/lib/dataset-analyzer-mobility';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';
// extractPoiCoords is NOT used here — per-POI endpoints extract only the requested POI's coords

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
 * GET /api/external/jobs/[datasetName]/mobility/[poiId]
 * Mobility analysis (POI categories visited before/after) for a single POI.
 *
 * datasetName = jobId, poiId = the original POI ID the client sent when creating the job.
 *
 * Response:
 * {
 *   job_id: string,
 *   poi: { id: string, name: string },
 *   analyzed_at: string,
 *   before: [{ category, deviceDays, hits }],
 *   after: [{ category, deviceDays, hits }],
 *   categories: [{ category, deviceDays, hits }]   // combined
 * }
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

    console.log(`[MOBILITY-POI] GET /api/external/jobs/${jobId}/mobility/${poiId}`);

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
          message: `Job status is '${job.status}'. Mobility analysis is only available for completed jobs.`,
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

    // 4b. Extract ONLY this POI's coordinates from job metadata for spatial proximity
    // verasetPoiId is like "geo_radius_X" — X is the index in the geo_radius/externalPois array
    const poiIndex = parseInt(verasetPoiId.replace('geo_radius_', ''), 10);
    let poiCoords: { lat: number; lng: number; radiusM: number }[] = [];
    if (!isNaN(poiIndex)) {
      if (job.verasetPayload?.geo_radius?.[poiIndex]) {
        const g = job.verasetPayload.geo_radius[poiIndex];
        poiCoords = [{ lat: g.latitude, lng: g.longitude, radiusM: g.distance_in_meters || 200 }];
      } else if (job.externalPois?.[poiIndex]) {
        const p = job.externalPois[poiIndex];
        poiCoords = [{ lat: p.latitude, lng: p.longitude, radiusM: 200 }];
      }
    }
    const poiIdsToQuery = verasetPoiId !== poiId
      ? [verasetPoiId, poiId]
      : [verasetPoiId];

    // 5. Check cache — bump version to invalidate stale study-level results
    const MOBILITY_VERSION = 'v2';
    const cacheKey = `mobility-${MOBILITY_VERSION}-${jobId}-poi-${poiId}`;
    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        console.log(`[MOBILITY-POI] Serving cached result for job ${jobId}, poi ${poiId}`);
        return NextResponse.json(cached, {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT' },
        });
      }
    } catch {
      // No cache, continue
    }

    // 6. Run mobility analysis
    console.log(`[MOBILITY-POI] Computing for job ${jobId}, dataset: ${datasetName}, poi: ${poiId} (veraset: ${verasetPoiId})`);
    logger.log(`Computing mobility for job ${jobId}, poi ${poiId}`);

    let result;
    try {
      result = await analyzeMobility(datasetName, {
        poiIds: poiIdsToQuery,
        ...(poiCoords.length ? { poiCoords } : {}),
      });
    } catch (error: any) {
      console.error(`[MOBILITY-POI ERROR] Analysis failed:`, error.message);
      logger.error(`Mobility-POI analysis failed for job ${jobId}, poi ${poiId}`, { error: error.message });

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
        { error: 'Internal Server Error', message: 'Failed to compute mobility analysis' },
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
      before: result.before,
      after: result.after,
      categories: result.categories,
    };

    console.log(`[MOBILITY-POI] Analysis completed for poi ${poiId}`, {
      categoriesBefore: response.before.length,
      categoriesAfter: response.after.length,
      totalCategories: response.categories.length,
    });

    // 8. Cache result
    try {
      await putConfig(cacheKey, response);
      logger.log(`Cached mobility for job ${jobId}, poi ${poiId}`);
    } catch (err: any) {
      logger.warn(`Failed to cache mobility for job ${jobId}, poi ${poiId}`, { error: err.message });
    }

    return NextResponse.json(response, {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'X-Cache': 'MISS' },
    });

  } catch (error: any) {
    const errorJobId = jobId || 'unknown';
    console.error(`[MOBILITY-POI ERROR] GET /api/external/jobs/${errorJobId}/mobility/:poiId:`, error.message);
    logger.error(`GET /api/external/jobs/${errorJobId}/mobility/:poiId error:`, error);

    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message || 'An unexpected error occurred' },
      { status: 500 }
    );
  }
}
