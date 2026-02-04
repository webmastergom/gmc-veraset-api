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

    // 3. Call Veraset API
    const apiUrl = process.env.NEXT_PUBLIC_API_URL || "https://gmc-mobility-api.vercel.app";
    
    // Log POI count being sent (sanitized in production)
    // Handle both place_key (Veraset POIs) and geo_radius (GeoJSON POIs)
    const poisToSend = verasetConfig?.place_key || verasetConfig?.geo_radius || body.pois || [];
    const poiCountToSend = Array.isArray(poisToSend) ? poisToSend.length : 0;
    logger.log(`Job Creation: ${name}`, {
      poiCount: poiCountToSend,
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
    
    const verasetResponse = await fetch(
      `${apiUrl}/api/veraset/movement`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(verasetConfig || {
          type,
          date_range: dateRange,
          pois: body.pois || [],
          schema: schema || 'BASIC',
        }),
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
      logger.error('Veraset API error response', { status: verasetResponse.status });
      const isProduction = process.env.NODE_ENV === 'production';
      return NextResponse.json(
        { 
          error: 'Veraset API failed', 
          details: isProduction ? undefined : responseText.substring(0, 500)
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
