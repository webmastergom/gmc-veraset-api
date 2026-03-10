import { NextRequest, NextResponse } from "next/server";
import { getAllJobs, getAllJobsSummary, createJob } from "@/lib/jobs";
import { canCreateJob, incrementUsage } from "@/lib/usage";
import { createJobSchema } from "@/lib/validation";
import { logger } from "@/lib/logger";
import { sanitizeError } from "@/lib/security";
import { z } from "zod";

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60;

/**
 * GET /api/jobs
 * Get all jobs
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  // Always return JSON - wrap everything in try-catch
  try {
    console.log('[JOBS GET] Starting...');
    
    // Try to get jobs using imported function
    let jobs: any[] = [];
    
    try {
      jobs = await getAllJobsSummary();
      console.log(`[JOBS GET] Found ${jobs.length} jobs from getAllJobsSummary`);

      // NOTE: Veraset status checking for non-terminal jobs is handled by
      // GET /api/jobs/[id] when the user views a specific job detail page.
      // Doing it here would add N API calls + N S3 writes to the list
      // endpoint, risking timeouts on Vercel serverless (10s limit).

    } catch (getJobsError: any) {
      console.error('[JOBS GET] Error calling getAllJobs:', getJobsError?.message);
      console.error('[JOBS GET] Error stack:', getJobsError?.stack);
      
      // Fallback to seed data
      try {
        const seedModule = await import("@/lib/seed-jobs");
        const initialJobsData = seedModule.initialJobsData || {};
        const seedJobs = Object.values(initialJobsData);
        jobs = Array.isArray(seedJobs) ? seedJobs : [];
        console.log(`[JOBS GET] Using fallback seed data: ${jobs.length} jobs`);
      } catch (seedError: any) {
        console.error('[JOBS GET] Error accessing seed data:', seedError?.message);
        jobs = [];
      }
    }
    
    // Always return JSON
    return NextResponse.json(jobs || [], {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    });
    
  } catch (error: any) {
    console.error('[JOBS GET] CRITICAL ERROR:', error);
    console.error('[JOBS GET] Error name:', error?.name);
    console.error('[JOBS GET] Error message:', error?.message);
    console.error('[JOBS GET] Error stack:', error?.stack);
    
    // Always return JSON, never HTML
    return NextResponse.json(
      { 
        error: 'Failed to fetch jobs',
        message: error?.message || 'Unknown error',
        jobs: [] // Return empty array so UI doesn't break
      },
      { 
        status: 500,
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );
  }
}

/**
 * POST /api/jobs
 * Create a new job
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  let jobName: string | undefined;
  const t0 = Date.now();

  try {
    console.log('[JOBS POST] Starting job creation');

    // 1. Parse and validate request body
    let body: any;
    try {
      body = await request.json();
    } catch (e: any) {
      return NextResponse.json(
        { error: 'Invalid JSON in request body', details: e.message },
        { status: 400 }
      );
    }

    jobName = body.name;
    console.log(`[JOBS POST] ${jobName} | type=${body.type} pois=${body.poiCount} [${Date.now()-t0}ms]`);

    // Validate request body
    let validatedData: any;
    try {
      validatedData = createJobSchema.parse(body);
    } catch (error: any) {
      const msg = error instanceof z.ZodError
        ? error.errors.map((e: any) => `${e.path.join('.')||'root'}: ${e.message}`).join(', ')
        : 'Invalid request body';
      console.error('[JOBS POST] Validation failed:', msg);
      return NextResponse.json({ error: 'Validation failed', details: msg }, { status: 400 });
    }
    const validation = { success: true as const, data: validatedData };
    
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

    // Capture user input for audit (lightweight — no deep copies)
    const userFromDate = dateRange?.from_date || dateRange?.from;
    const userToDate = dateRange?.to_date || dateRange?.to;
    const userInputAudit = {
      timestamp: new Date().toISOString(),
      userRequest: { name, type, poiCount, poiCollectionId,
        dateRange: { from: userFromDate, to: userToDate },
        radius, schema, verasetConfig, poiMapping, poiNames,
        pois: validatedBody.pois,
      },
    };

    const placeKeyCount = Array.isArray(verasetConfig?.place_key) ? verasetConfig.place_key.length : 0;
    const geoRadiusCount = Array.isArray(verasetConfig?.geo_radius) ? verasetConfig.geo_radius.length : 0;
    const poiCountToSend = placeKeyCount + geoRadiusCount || (Array.isArray(validatedBody.pois) ? validatedBody.pois.length : 0);

    console.log(`[JOBS POST] ${name} | ${poiCountToSend} POIs (geo:${geoRadiusCount} pk:${placeKeyCount}) | ${userFromDate}→${userToDate} [${Date.now()-t0}ms]`);

    if (poiCountToSend === 0) {
      return NextResponse.json(
        { error: 'No POIs provided', details: 'Either geo_radius or place_key must contain POIs.' },
        { status: 400 }
      );
    }
    
    // 3. Build Veraset payload
    const verasetPayload: Record<string, any> = verasetConfig || {
      type,
      date_range: dateRange,
      pois: validatedBody.pois || [],
      schema_type: schema || 'BASIC', // Veraset API expects 'schema_type', not 'schema'
    };

    // Extract type to determine endpoint (type is NOT sent to Veraset, only used for endpoint selection)
    const { type: jobType = 'pings', ...verasetBodyObj } = verasetPayload;
    
    // CRITICAL: Ensure schema_type is set correctly (convert 'schema' to 'schema_type' if present)
    if (verasetBodyObj.schema && !verasetBodyObj.schema_type) {
      verasetBodyObj.schema_type = verasetBodyObj.schema;
      delete verasetBodyObj.schema;
    }
    if (!verasetBodyObj.schema_type) {
      verasetBodyObj.schema_type = schema || 'BASIC';
    }
    
    // Validate POI data — fast spot-check of first + last POI (no per-POI loop)
    if (verasetBodyObj.geo_radius?.length) {
      const first = verasetBodyObj.geo_radius[0];
      const last = verasetBodyObj.geo_radius[verasetBodyObj.geo_radius.length - 1];
      for (const poi of [first, last]) {
        if (!poi.poi_id || typeof poi.latitude !== 'number' || typeof poi.longitude !== 'number') {
          return NextResponse.json(
            { error: 'Invalid POI data', details: `POI ${poi.poi_id || '(missing)'} has missing fields` },
            { status: 400 }
          );
        }
      }
    }
    if (verasetBodyObj.place_key?.length) {
      const first = verasetBodyObj.place_key[0];
      if (!first.poi_id || !first.placekey) {
        return NextResponse.json(
          { error: 'Invalid POI data', details: 'place_key POI missing required fields' },
          { status: 400 }
        );
      }
    }
    if (!verasetBodyObj.geo_radius?.length && !verasetBodyObj.place_key?.length) {
      return NextResponse.json(
        { error: 'No POIs in payload', details: 'Either geo_radius or place_key must contain POIs' },
        { status: 400 }
      );
    }
    
    // Validate and normalize date range
    if (!verasetBodyObj.date_range) {
      return NextResponse.json({ error: 'Missing date range' }, { status: 400 });
    }
    {
      const dr = verasetBodyObj.date_range;
      const fromDate = dr.from_date || dr.from || dr.fromDate;
      const toDate = dr.to_date || dr.to || dr.toDate;
      if (!fromDate || !toDate) {
        return NextResponse.json({ error: 'Invalid date range', details: 'Both from_date and to_date required' }, { status: 400 });
      }
      const dateRe = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateRe.test(fromDate) || !dateRe.test(toDate)) {
        return NextResponse.json({ error: 'Invalid date format', details: 'Use YYYY-MM-DD' }, { status: 400 });
      }
      if (new Date(toDate) < new Date(fromDate)) {
        return NextResponse.json({ error: 'Invalid date range', details: 'to_date must be >= from_date' }, { status: 400 });
      }
      const { calculateDaysInclusive } = await import('@/lib/s3');
      const daysDiff = calculateDaysInclusive(fromDate, toDate);
      if (daysDiff > 31 || daysDiff < 1) {
        return NextResponse.json({ error: 'Date range out of bounds', details: `Must be 1-31 days, got ${daysDiff}` }, { status: 400 });
      }
      verasetBodyObj.date_range = { from_date: fromDate, to_date: toDate };
    }
    
    console.log(`[JOBS POST] Payload validated [${Date.now()-t0}ms]`);
    
    // Determine Veraset endpoint based on job type (needed for audit)
    const endpoints: Record<string, string> = {
      'pings': '/v1/movement/job/pings',
      'devices': '/v1/movement/job/devices',
      'aggregate': '/v1/movement/job/aggregate',
      'cohort': '/v1/movement/job/cohort',
      'pings_by_device': '/v1/movement/job/pings_by_device',
    };
    const verasetEndpoint = endpoints[jobType] || endpoints['pings'];
    
    // Lightweight audit (no deep copy — verasetBodyObj is stable by this point)
    const verasetPayloadAudit = {
      payloadToVeraset: verasetBodyObj,
      endpoint: verasetEndpoint,
    };
    
    // Quick verification — count-based, not per-POI (saves seconds on large batches)
    const verificationIssues: string[] = [];
    const verasetFromDate = verasetBodyObj.date_range?.from_date;
    const verasetToDate = verasetBodyObj.date_range?.to_date;
    
    if (userFromDate !== verasetFromDate || userToDate !== verasetToDate) {
      verificationIssues.push(`Date range mismatch: user=${userFromDate}→${userToDate} payload=${verasetFromDate}→${verasetToDate}`);
    }
    if ((schema || 'BASIC') !== verasetBodyObj.schema_type) {
      verificationIssues.push(`Schema mismatch: user=${schema} payload=${verasetBodyObj.schema_type}`);
    }
    
    // Check unexpected fields in payload
    const allowedFields = ['date_range', 'schema_type', 'geo_radius', 'place_key'];
    const unexpectedFields = Object.keys(verasetBodyObj).filter(f => !allowedFields.includes(f));
    if (unexpectedFields.length > 0) {
      verificationIssues.push(`Unexpected fields: ${unexpectedFields.join(', ')}`);
    }

    if (verificationIssues.length > 0) {
      console.error('[JOBS POST] Verification failed:', verificationIssues);
      return NextResponse.json(
        { error: 'Payload verification failed', issues: verificationIssues },
        { status: 400 }
      );
    }

    const verasetBody = JSON.stringify(verasetBodyObj);
    console.log(`[JOBS POST] Verified OK, payload ${(verasetBody.length/1024).toFixed(0)}KB [${Date.now()-t0}ms]`);

    // 4. API key check
    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) {
      return NextResponse.json({ error: 'VERASET_API_KEY not configured' }, { status: 500 });
    }

    // Build audit trail (stored on the job for later review via /api/jobs/[id]/audit)
    const auditTrail: Record<string, any> = {
      userInput: userInputAudit.userRequest,
      verasetPayload: verasetBodyObj,
      verificationPassed: true,
      verificationIssues: [],
      timestamp: new Date().toISOString(),
    };

    // 5. Call Veraset API with 45s timeout (maxDuration=60s gives headroom)
    const verasetUrl = `https://platform.prd.veraset.tech${verasetEndpoint}`;
    console.log(`[JOBS POST] Calling Veraset ${verasetEndpoint} [${Date.now()-t0}ms]`);

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 45000);
    const verasetResponse = await fetch(verasetUrl, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": verasetApiKey,
      },
      body: verasetBody,
    });
    clearTimeout(timeout);
    
    // 6. Parse response
    console.log(`[JOBS POST] Veraset responded ${verasetResponse.status} [${Date.now()-t0}ms]`);
    const responseText = await verasetResponse.text();
    let verasetData: any;
    try { verasetData = JSON.parse(responseText); } catch { verasetData = { raw: responseText.substring(0, 500) }; }

    // Store response in audit
    auditTrail.verasetResponse = verasetData;

    if (!verasetResponse.ok) {
      console.error(`[JOBS POST] Veraset ${verasetResponse.status}: ${responseText.substring(0, 300)}`);
      return NextResponse.json(
        { error: 'Veraset API failed', statusCode: verasetResponse.status, details: verasetData.error_message || verasetData.message || responseText.substring(0, 300) },
        { status: 502 }
      );
    }

    const jobId = verasetData.job_id || verasetData.data?.job_id;
    const processedPois = verasetData.processed_pois || verasetData.data?.processed_pois;
    
    // Response verification (lightweight)
    if (processedPois !== undefined && processedPois !== poiCountToSend) {
      console.warn(`[JOBS POST] POI mismatch: sent ${poiCountToSend}, processed ${processedPois}`);
      auditTrail.responseVerificationIssues = [`Sent ${poiCountToSend} but processed ${processedPois}`];
    }

    if (!jobId) {
      console.error('[JOBS POST] No job_id in Veraset response');
      return NextResponse.json({ error: 'No job_id returned from Veraset', data: verasetData }, { status: 502 });
    }

    console.log(`[JOBS POST] Veraset job ${jobId} created, saving to S3 [${Date.now()-t0}ms]`);

    // 7. Save job to S3
    const normalizedDateRange = {
      from: dateRange?.from || dateRange?.from_date || '',
      to: dateRange?.to || dateRange?.to_date || '',
    };
    const jobRadius = radius ?? 10;

    const job = await createJob({
      jobId,
      name,
      type,
      poiCount: poiCount || 0,
      poiCollectionId,
      dateRange: normalizedDateRange,
      radius: jobRadius,
      schema: (schema === 'ENHANCED' ? 'ENHANCED' : schema === 'FULL' ? 'FULL' : 'BASIC') as 'BASIC' | 'FULL' | 'ENHANCED' | 'N/A',
      status: 'QUEUED',
      s3SourcePath: `s3://veraset-prd-platform-us-west-2/output/garritz/${jobId}/`,
      external: false,
      poiMapping: poiMapping || undefined,
      poiNames: poiNames || undefined,
      verasetPayload: {
        date_range: verasetBodyObj.date_range,
        schema_type: verasetBodyObj.schema_type || 'BASIC',
        geo_radius: verasetBodyObj.geo_radius,
        place_key: verasetBodyObj.place_key,
      },
      auditTrail: auditTrail as any,
    });

    console.log(`[JOBS POST] Job ${jobId} saved [${Date.now()-t0}ms]`);

    // 8. Increment usage
    await incrementUsage(jobId);

    console.log(`[JOBS POST] DONE ${jobId} in ${Date.now()-t0}ms`);
    return NextResponse.json({ success: true, job, remaining: remaining - 1 }, { status: 201 });

  } catch (error: any) {
    const isAbort = error.name === 'AbortError';
    console.error(`[JOBS POST] ${isAbort ? 'TIMEOUT' : 'ERROR'} after ${Date.now()-t0}ms:`, error.message);
    return NextResponse.json(
      { error: isAbort ? 'Veraset API timeout (45s)' : 'Failed to create job', details: error.message, jobName },
      { status: isAbort ? 504 : 500 }
    );
  }
}
