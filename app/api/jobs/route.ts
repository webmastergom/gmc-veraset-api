import { NextRequest, NextResponse } from "next/server";
import { getAllJobs, createJob } from "@/lib/jobs";
import { canCreateJob, incrementUsage } from "@/lib/usage";
import { initialJobsData } from "@/lib/seed-jobs";
import { initConfigIfNeeded } from "@/lib/s3-config";
import { validateRequestBody, createJobSchema } from "@/lib/validation";
import { logger } from "@/lib/logger";
import { sanitizeError } from "@/lib/security";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/jobs
 * Get all jobs
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    console.log('[JOBS GET] Fetching all jobs');
    
    // Initialize with seed data if needed
    const jobsData = await initConfigIfNeeded('jobs', initialJobsData);
    
    // Get all jobs
    const jobs = await getAllJobs();
    
    console.log(`[JOBS GET] Found ${jobs.length} jobs`);
    
    return NextResponse.json(jobs, {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    });
  } catch (error: any) {
    console.error('[JOBS GET ERROR]', error);
    logger.error('GET /api/jobs error:', error);
    
    // Fallback to seed data if S3 fails
    try {
      const jobs = Object.values(initialJobsData);
      console.log('[JOBS GET] Using fallback seed data');
      return NextResponse.json(jobs, {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
        },
      });
    } catch (seedError) {
      const isProduction = process.env.NODE_ENV === 'production';
      return NextResponse.json(
        { 
          error: 'Failed to fetch jobs', 
          details: isProduction ? undefined : sanitizeError(error)
        },
        { status: 500 }
      );
    }
  }
}

/**
 * POST /api/jobs
 * Create a new job
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  let jobName: string | undefined;
  
  try {
    console.log('[JOBS POST] Starting job creation');
    
    // 1. Parse and validate request body
    let body: any;
    try {
      body = await request.json();
    } catch (e: any) {
      console.error('[JOBS POST] Failed to parse request body:', e.message);
      return NextResponse.json(
        { 
          error: 'Invalid JSON in request body',
          details: e.message 
        },
        { status: 400 }
      );
    }

    jobName = body.name;

    console.log('[JOBS POST] Validating request body', {
      name: jobName,
      type: body.type,
      poiCount: body.poiCount,
      hasVerasetConfig: !!body.verasetConfig,
    });

    // Validate request body
    const validation = await validateRequestBody(request, createJobSchema);
    if (!validation.success) {
      console.error('[JOBS POST] Validation failed:', validation.error);
      return NextResponse.json(
        { 
          error: 'Validation failed',
          details: validation.error 
        },
        { status: validation.status || 400 }
      );
    }
    
    const validatedBody = validation.data;

    // 2. Check usage limit
    console.log('[JOBS POST] Checking usage limits');
    const { allowed, reason, remaining } = await canCreateJob();
    if (!allowed) {
      console.warn('[JOBS POST] Usage limit reached:', reason);
      return NextResponse.json(
        { 
          error: 'Limit reached', 
          message: reason, 
          remaining 
        },
        { status: 429 }
      );
    }

    const { name, type, poiCount, poiCollectionId, dateRange, radius, schema, verasetConfig, poiMapping, poiNames } = validatedBody;

    // Log POI count being sent
    const placeKeyCount = Array.isArray(verasetConfig?.place_key) ? verasetConfig.place_key.length : 0;
    const geoRadiusCount = Array.isArray(verasetConfig?.geo_radius) ? verasetConfig.geo_radius.length : 0;
    const poiCountToSend = placeKeyCount + geoRadiusCount || (Array.isArray(validatedBody.pois) ? validatedBody.pois.length : 0);
    
    console.log('[JOBS POST] Job details', {
      name,
      type,
      poiCountToSend,
      placeKeyPois: placeKeyCount,
      geoRadiusPois: geoRadiusCount,
      expectedPoiCount: poiCount || 0,
      dateRange: {
        from: dateRange?.from_date || dateRange?.from,
        to: dateRange?.to_date || dateRange?.to,
      },
      schema: schema || 'BASIC',
    });
    
    logger.log(`Job Creation: ${name}`, {
      poiCount: poiCountToSend,
      placeKeyPois: placeKeyCount,
      geoRadiusPois: geoRadiusCount,
      expectedPoiCount: poiCount || 0,
      dateRange: {
        from: dateRange?.from_date || dateRange?.from,
        to: dateRange?.to_date || dateRange?.to,
      },
      type,
      schema: schema || 'BASIC',
    });
    
    if (poiCountToSend !== poiCount && poiCount) {
      console.warn(`[JOBS POST] POI count mismatch: sending ${poiCountToSend} but expected ${poiCount}`);
      logger.warn(`POI count mismatch: sending ${poiCountToSend} but expected ${poiCount}`);
    }
    
    if (poiCountToSend === 0) {
      console.error('[JOBS POST] No POIs to send!');
      logger.error('No POIs to send! This will cause the job to fail.');
      return NextResponse.json(
        { 
          error: 'No POIs provided', 
          details: 'Please ensure POI collection is valid and contains Point geometries.',
          hint: 'Check that your POI collection has valid Point features with coordinates.'
        },
        { status: 400 }
      );
    }
    
    // 3. Build Veraset payload
    const verasetPayload: Record<string, any> = verasetConfig || {
      type,
      date_range: dateRange,
      pois: validatedBody.pois || [],
      schema: schema || 'BASIC',
    };

    // Extract type to determine endpoint
    const { type: jobType = 'pings', ...verasetBodyObj } = verasetPayload;
    
    // Ensure date_range format is correct (Veraset expects from_date/to_date)
    if (verasetBodyObj.date_range) {
      const dr = verasetBodyObj.date_range;
      verasetBodyObj.date_range = {
        from_date: dr.from_date || dr.from || dr.fromDate,
        to_date: dr.to_date || dr.to || dr.toDate,
      };
      
      // Validate dates
      if (!verasetBodyObj.date_range.from_date || !verasetBodyObj.date_range.to_date) {
        return NextResponse.json(
          { 
            error: 'Invalid date range',
            details: 'Both from_date and to_date are required',
            received: verasetBodyObj.date_range
          },
          { status: 400 }
        );
      }
    }
    
    const verasetBody = JSON.stringify(verasetBodyObj);

    const endpoints: Record<string, string> = {
      'pings': '/v1/movement/job/pings',
      'devices': '/v1/movement/job/devices',
      'aggregate': '/v1/movement/job/aggregate',
    };
    const verasetEndpoint = endpoints[jobType] || endpoints['pings'];

    // 4. Get and validate API key
    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) {
      console.error('[JOBS POST] VERASET_API_KEY not configured', {
        nodeEnv: process.env.NODE_ENV,
        hasEnvVar: !!process.env.VERASET_API_KEY,
        envVarLength: process.env.VERASET_API_KEY?.length || 0,
      });
      logger.error('VERASET_API_KEY not configured', {
        nodeEnv: process.env.NODE_ENV,
        hasEnvVar: !!process.env.VERASET_API_KEY,
        envVarLength: process.env.VERASET_API_KEY?.length || 0,
      });
      return NextResponse.json(
        { 
          error: 'VERASET_API_KEY not configured',
          hint: 'Please verify VERASET_API_KEY is set in Vercel environment variables for Production environment',
        },
        { status: 500 }
      );
    }

    console.log('[JOBS POST] Calling Veraset API', {
      endpoint: verasetEndpoint,
      bodySize: verasetBody.length,
      geoRadiusCount: verasetBodyObj.geo_radius?.length || 0,
      placeKeyCount: verasetBodyObj.place_key?.length || 0,
      apiKeyConfigured: !!verasetApiKey,
      apiKeyLength: verasetApiKey.length,
      apiKeyPrefix: verasetApiKey.substring(0, 10) + '...',
    });

    logger.log(`Veraset API call: ${verasetEndpoint}`, {
      bodySize: verasetBody.length,
      geoRadiusCount: verasetBodyObj.geo_radius?.length || 0,
      placeKeyCount: verasetBodyObj.place_key?.length || 0,
      apiKeyConfigured: !!verasetApiKey,
      apiKeyLength: verasetApiKey.length,
    });

    // 5. Call Veraset API
    const verasetUrl = `https://platform.prd.veraset.tech${verasetEndpoint}`;
    console.log('[JOBS POST] Veraset URL:', verasetUrl);
    
    const verasetResponse = await fetch(verasetUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": verasetApiKey,
      },
      body: verasetBody,
    });
    
    // 6. Parse response
    const responseText = await verasetResponse.text();
    let verasetData: any;
    try {
      verasetData = JSON.parse(responseText);
    } catch (e) {
      console.error('[JOBS POST] Failed to parse Veraset response:', responseText.substring(0, 500));
      verasetData = { raw: responseText };
    }
    
    if (!verasetResponse.ok) {
      console.error('[JOBS POST] Veraset API error', {
        status: verasetResponse.status,
        statusText: verasetResponse.statusText,
        responseText: responseText.substring(0, 1000),
        apiKeyLength: verasetApiKey?.length || 0,
        apiKeyPrefix: verasetApiKey?.substring(0, 10) || 'missing',
      });
      
      logger.error('Veraset API error response', { 
        status: verasetResponse.status,
        statusText: verasetResponse.statusText,
        responseText: responseText.substring(0, 1000),
        apiKeyLength: verasetApiKey?.length || 0,
        apiKeyPrefix: verasetApiKey?.substring(0, 10) || 'missing',
      });
      
      // Parse error response for better error messages
      let errorDetails: any = {};
      try {
        errorDetails = verasetData;
      } catch {
        errorDetails = { raw: responseText };
      }
      
      // Provide helpful error messages based on status code
      if (verasetResponse.status === 401) {
        return NextResponse.json(
          { 
            error: 'Veraset API authentication failed',
            details: errorDetails.error_message || errorDetails.message || 'Invalid API key',
            hint: 'Please verify VERASET_API_KEY is correctly set in Vercel environment variables. The API key should match: 9d9d0fb0-c8e2-43db-bc12-d5b3a5398d20',
            statusCode: 401,
            responsePreview: responseText.substring(0, 200),
          },
          { status: 502 }
        );
      }
      
      const isProduction = process.env.NODE_ENV === 'production';
      return NextResponse.json(
        { 
          error: 'Veraset API failed', 
          details: isProduction 
            ? (errorDetails.error_message || errorDetails.message || 'Unknown error')
            : responseText.substring(0, 500),
          statusCode: verasetResponse.status,
          statusText: verasetResponse.statusText,
        },
        { status: 502 }
      );
    }
    
    console.log('[JOBS POST] Veraset API success', {
      jobId: verasetData.job_id || verasetData.data?.job_id,
      processedPois: verasetData.processed_pois,
    });
    
    logger.log('Veraset API success', {
      jobId: verasetData.job_id || verasetData.data?.job_id,
      processedPois: verasetData.processed_pois,
    });
    
    // Log any warnings or info from Veraset
    if (verasetData.warnings || verasetData.info) {
      console.log('[JOBS POST] Veraset API info/warnings', {
        warnings: verasetData.warnings,
        info: verasetData.info,
      });
      logger.info('Veraset API info/warnings', {
        warnings: verasetData.warnings,
        info: verasetData.info,
      });
    }
    
    // Check if Veraset returned a different POI count than sent
    if (verasetData.processed_pois !== undefined && poiCountToSend > 0) {
      const processedCount = verasetData.processed_pois;
      if (processedCount !== poiCountToSend) {
        console.warn(`[JOBS POST] Veraset processed ${processedCount} POIs out of ${poiCountToSend} sent`);
        logger.warn(`Veraset processed ${processedCount} POIs out of ${poiCountToSend} sent`);
      }
    }
    
    const jobId = verasetData.job_id || verasetData.data?.job_id;

    if (!jobId) {
      console.error('[JOBS POST] No job_id returned from Veraset', verasetData);
      return NextResponse.json(
        { 
          error: 'No job_id returned from Veraset', 
          details: 'Veraset API did not return a job_id in the response',
          data: verasetData 
        },
        { status: 502 }
      );
    }

    console.log('[JOBS POST] Saving job to S3', {
      jobId,
      name,
      type,
      poiCount: poiCount || 0,
    });

    // 7. Save job to S3
    const normalizedDateRange: { from: string; to: string } = {
      from: dateRange?.from || dateRange?.from_date || '',
      to: dateRange?.to || dateRange?.to_date || '',
    };
    
    const job = await createJob({
      jobId,
      name,
      type,
      poiCount: poiCount || 0,
      poiCollectionId,
      dateRange: normalizedDateRange,
      radius: radius || 10,
      schema: (schema === 'ENHANCED' ? 'ENHANCED' : schema === 'FULL' ? 'FULL' : 'BASIC') as 'BASIC' | 'FULL' | 'ENHANCED' | 'N/A',
      status: 'QUEUED',
      s3SourcePath: `s3://veraset-prd-platform-us-west-2/output/garritz/${jobId}/`,
      external: false,
      poiMapping: poiMapping || undefined,
      poiNames: poiNames || undefined,
    });

    console.log('[JOBS POST] Job saved successfully', {
      jobId: job.jobId,
      name: job.name,
    });

    // 8. Increment usage
    if (!job.external) {
      await incrementUsage(jobId);
    }

    // 9. Return success
    console.log('[JOBS POST] Job creation completed successfully', {
      jobId: job.jobId,
      remaining: remaining - 1,
    });

    return NextResponse.json({
      success: true,
      job,
      remaining: remaining - 1,
    }, {
      status: 201,
      headers: {
        'Content-Type': 'application/json',
      },
    });

  } catch (error: any) {
    console.error('[JOBS POST ERROR]', {
      error: error.message,
      stack: error.stack,
      name: error.name,
      jobName,
    });
    
    logger.error('POST /api/jobs error:', error);
    const isProduction = process.env.NODE_ENV === 'production';
    
    return NextResponse.json(
      { 
        error: 'Failed to create job',
        details: isProduction ? undefined : sanitizeError(error),
        jobName,
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
