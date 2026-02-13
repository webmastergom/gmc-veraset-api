import { NextRequest, NextResponse } from 'next/server';
import { getAllJobs, createJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { BUCKET } from '@/lib/s3-config';
import { externalCreateJobSchema, validateRequestBody } from '@/lib/validation';
import { canCreateJob } from '@/lib/usage';
import { logger } from '@/lib/logger';
import { sanitizeError } from '@/lib/security';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60;

/**
 * GET /api/external/jobs
 * List all available jobs/datasets for external clients
 */
export async function GET(request: NextRequest) {
  try {
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    const allJobs = await getAllJobs();

    const availableJobs = allJobs
      .filter(job => job.status === 'SUCCESS' && job.s3DestPath)
      .map(job => {
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

/**
 * POST /api/external/jobs
 * Create a new mobility job from external API
 */
export async function POST(request: NextRequest) {
  try {
    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // 2. Validate request body
    const validation = await validateRequestBody(request, externalCreateJobSchema);
    if (!validation.success) {
      return NextResponse.json(
        { error: validation.error },
        { status: validation.status }
      );
    }

    const body = validation.data;

    // 3. Check usage limit
    const { allowed, reason, remaining } = await canCreateJob();
    if (!allowed) {
      return NextResponse.json(
        { error: 'Limit reached', message: reason, remaining },
        { status: 429 }
      );
    }

    // 4. Build verasetConfig from POIs
    const geoRadius = body.pois.map((poi, index) => ({
      poi_id: poi.id || `poi_${index}`,
      latitude: poi.latitude,
      longitude: poi.longitude,
      distance_in_meters: body.radius,
    }));

    const verasetBody = {
      date_range: {
        from_date: body.date_range.from,
        to_date: body.date_range.to,
      },
      geo_radius: geoRadius,
      schema_type: body.schema || 'BASIC', // Veraset API expects 'schema_type', not 'schema'
    };

    logger.log(`External Job Creation: ${body.name}`, {
      poiCount: body.pois.length,
      country: body.country,
      dateRange: body.date_range,
      type: body.type,
      schema: body.schema,
      hasWebhook: !!body.webhook_url,
    });

    // 5. Call Veraset movement API directly
    const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';
    const verasetEndpoints: Record<string, string> = {
      'pings': '/v1/movement/job/pings',
      'devices': '/v1/movement/job/devices',
      'aggregate': '/v1/movement/job/aggregate',
      'cohort': '/v1/movement/job/cohort',
      'pings_by_device': '/v1/movement/job/pings_by_device',
    };
    const jobType = body.type || 'pings';
    const verasetEndpoint = verasetEndpoints[jobType] || verasetEndpoints['pings'];
    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    
    if (!verasetApiKey) {
      return NextResponse.json(
        { error: 'VERASET_API_KEY not configured' },
        { status: 500 }
      );
    }

    const verasetResponse = await fetch(`${VERASET_BASE_URL}${verasetEndpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': verasetApiKey,
      },
      body: JSON.stringify(verasetBody),
    });

    const responseText = await verasetResponse.text();
    let verasetData;
    try {
      verasetData = JSON.parse(responseText);
    } catch {
      verasetData = { raw: responseText };
    }

    if (!verasetResponse.ok) {
      logger.error('Veraset API error (external)', { status: verasetResponse.status });
      return NextResponse.json(
        { error: 'Veraset API failed', details: responseText.substring(0, 500) },
        { status: 502 }
      );
    }

    const jobId = verasetData.job_id || verasetData.data?.job_id;
    if (!jobId) {
      return NextResponse.json(
        { error: 'No job_id returned from Veraset' },
        { status: 502 }
      );
    }

    // 6. Build POI mapping and names
    const poiMapping: Record<string, string> = {};
    const poiNames: Record<string, string> = {};
    body.pois.forEach((poi, index) => {
      const verasetId = `geo_radius_${index}`;
      poiMapping[verasetId] = poi.id;
      if (poi.name) {
        poiNames[verasetId] = poi.name;
      }
    });

    // 7. Save job to S3
    const job = await createJob({
      jobId,
      name: body.name,
      type: body.type as 'pings' | 'aggregate' | 'devices' | 'cohort',
      poiCount: body.pois.length,
      dateRange: { from: body.date_range.from, to: body.date_range.to },
      radius: body.radius ?? 10,
      schema: (body.schema ?? 'BASIC') as 'BASIC' | 'FULL' | 'ENHANCED' | 'N/A',
      status: 'QUEUED',
      s3SourcePath: `s3://veraset-prd-platform-us-west-2/output/garritz/${jobId}/`,
      external: true,
      poiMapping,
      poiNames,
      country: body.country,
      webhookUrl: body.webhook_url,
      externalPois: body.pois,
    });

    // 8. Return response
    return NextResponse.json({
      job_id: jobId,
      status: 'QUEUED',
      poi_count: body.pois.length,
      date_range: body.date_range,
      created_at: job.createdAt,
      status_url: `/api/external/jobs/${jobId}/status`,
      webhook_registered: !!body.webhook_url,
    }, { status: 201 });

  } catch (error: any) {
    logger.error('POST /api/external/jobs error:', error);
    const isProduction = process.env.NODE_ENV === 'production';
    return NextResponse.json(
      {
        error: 'Failed to create job',
        details: isProduction ? undefined : sanitizeError(error),
      },
      { status: 500 }
    );
  }
}
