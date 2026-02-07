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

export async function GET(request: NextRequest) {
  try {
    // Initialize with seed data if needed
    const jobsData = await initConfigIfNeeded('jobs', initialJobsData);
    
    // Get all jobs
    const jobs = await getAllJobs();
    
    return NextResponse.json(jobs);
  } catch (error: any) {
    logger.error('GET /api/jobs error:', error);
    
    // Fallback to seed data if S3 fails
    try {
      const jobs = Object.values(initialJobsData);
      return NextResponse.json(jobs);
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

export async function POST(request: NextRequest) {
  try {
    // 1. Validate request body
    const validation = await validateRequestBody(request, createJobSchema);
    if (!validation.success) {
      return NextResponse.json(
        { error: validation.error },
        { status: validation.status }
      );
    }
    
    const body = validation.data;

    // 2. Check usage limit
    const { allowed, reason, remaining } = await canCreateJob();
    if (!allowed) {
      return NextResponse.json(
        { error: 'Limit reached', message: reason, remaining },
        { status: 429 }
      );
    }

    const { name, type, poiCount, poiCollectionId, dateRange, radius, schema, verasetConfig, poiMapping, poiNames } = body;

    // Log POI count being sent (sanitized in production)
    // Handle hybrid mode: place_key + geo_radius can coexist
    const placeKeyCount = Array.isArray(verasetConfig?.place_key) ? verasetConfig.place_key.length : 0;
    const geoRadiusCount = Array.isArray(verasetConfig?.geo_radius) ? verasetConfig.geo_radius.length : 0;
    const poiCountToSend = placeKeyCount + geoRadiusCount || (Array.isArray(body.pois) ? body.pois.length : 0);
    logger.log(`Job Creation: ${name}`, {
      poiCount: poiCountToSend,
      placeKeyPois: placeKeyCount,
      geoRadiusPois: geoRadiusCount,
      expectedPoiCount: poiCount || 0,
      dateRange: {
        from: dateRange.from_date || dateRange.from,
        to: dateRange.to_date || dateRange.to,
      },
      type,
      schema: schema || 'BASIC',
    });
    
    if (poiCountToSend !== poiCount && poiCount) {
      logger.warn(`POI count mismatch: sending ${poiCountToSend} but expected ${poiCount}`);
    }
    
    if (poiCountToSend === 0) {
      logger.error('No POIs to send! This will cause the job to fail.');
      return NextResponse.json(
        { error: 'No POIs provided. Please ensure POI collection is valid and contains Point geometries.' },
        { status: 400 }
      );
    }
    
    // Build Veraset payload - extract type from config and send rest to API
    const verasetPayload: Record<string, any> = verasetConfig || {
      type,
      date_range: dateRange,
      pois: body.pois || [],
      schema: schema || 'BASIC',
    };

    // Extract type to determine endpoint, send rest as body
    const { type: jobType = 'pings', ...verasetBodyObj } = verasetPayload;
    
    // Ensure date_range format is correct (Veraset expects from_date/to_date)
    if (verasetBodyObj.date_range) {
      const dr = verasetBodyObj.date_range;
      verasetBodyObj.date_range = {
        from_date: dr.from_date || dr.from || dr.fromDate,
        to_date: dr.to_date || dr.to || dr.toDate,
      };
    }
    
    const verasetBody = JSON.stringify(verasetBodyObj);

    const endpoints: Record<string, string> = {
      'pings': '/v1/movement/job/pings',
      'devices': '/v1/movement/job/devices',
      'aggregate': '/v1/movement/job/aggregate',
    };
    const verasetEndpoint = endpoints[jobType] || endpoints['pings'];

    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) {
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

    logger.log(`Veraset API call: ${verasetEndpoint}`, {
      bodySize: verasetBody.length,
      geoRadiusCount: verasetBodyObj.geo_radius?.length || 0,
      placeKeyCount: verasetBodyObj.place_key?.length || 0,
      apiKeyConfigured: !!verasetApiKey,
      apiKeyLength: verasetApiKey.length,
    });

    // Call Veraset API directly (server-side, has access to VERASET_API_KEY env var)
    const verasetResponse = await fetch(
      `https://platform.prd.veraset.tech${verasetEndpoint}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": verasetApiKey,
        },
        body: verasetBody,
      }
    );
    
    // Log response for debugging (sanitized in production)
    const responseText = await verasetResponse.text();
    let verasetData;
    try {
      verasetData = JSON.parse(responseText);
    } catch {
      verasetData = { raw: responseText };
    }
    
    if (!verasetResponse.ok) {
      logger.error('Veraset API error response', { 
        status: verasetResponse.status,
        statusText: verasetResponse.statusText,
        responseText: responseText.substring(0, 1000), // Log full error in production for debugging
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
            hint: 'Please verify VERASET_API_KEY is correctly set in Vercel environment variables',
            statusCode: 401,
          },
          { status: 502 }
        );
      }
      
      const isProduction = process.env.NODE_ENV === 'production';
      return NextResponse.json(
        { 
          error: 'Veraset API failed', 
          details: isProduction ? errorDetails.error_message || errorDetails.message : responseText.substring(0, 500),
          statusCode: verasetResponse.status,
        },
        { status: 502 }
      );
    }
    
    logger.log('Veraset API success', {
      jobId: verasetData.job_id || verasetData.data?.job_id,
      processedPois: verasetData.processed_pois,
    });
    
    // Log any warnings or info from Veraset about POI processing
    if (verasetData.warnings || verasetData.info) {
      logger.info('Veraset API info/warnings', {
        warnings: verasetData.warnings,
        info: verasetData.info,
      });
    }
    
    // Check if Veraset returned a different POI count than sent
    if (verasetData.processed_pois !== undefined && poiCountToSend > 0) {
      const processedCount = verasetData.processed_pois;
      if (processedCount !== poiCountToSend) {
        logger.warn(`Veraset processed ${processedCount} POIs out of ${poiCountToSend} sent`);
      }
    }
    
    const jobId = verasetData.job_id || verasetData.data?.job_id;

    if (!jobId) {
      return NextResponse.json(
        { error: 'No job_id returned from Veraset', data: verasetData },
        { status: 502 }
      );
    }

    // 4. Save job to S3 (jobs created through app are NOT external)
    // Normalize dateRange to always have from and to
    const normalizedDateRange: { from: string; to: string } = {
      from: dateRange.from || dateRange.from_date || '',
      to: dateRange.to || dateRange.to_date || '',
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
      external: false, // Jobs created through app are not external
      poiMapping: poiMapping || undefined, // Save POI ID mapping if provided
      poiNames: poiNames || undefined, // Save POI names for display if provided
    });

    // 5. Increment usage (atomic operation) - only for non-external jobs
    if (!job.external) {
      await incrementUsage(jobId);
    }

    // 6. Return success
    return NextResponse.json({
      success: true,
      job,
      remaining: remaining - 1,
    });

  } catch (error: any) {
    logger.error('POST /api/jobs error:', error);
    const isProduction = process.env.NODE_ENV === 'production';
    return NextResponse.json(
      { 
        error: 'Failed to create job', 
        details: isProduction ? undefined : sanitizeError(error)
      },
      { status: 500 }
    );
  }
}
