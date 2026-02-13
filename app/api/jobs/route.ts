import { NextRequest, NextResponse } from "next/server";
import { getAllJobs, createJob } from "@/lib/jobs";
import { canCreateJob, incrementUsage } from "@/lib/usage";
import { createJobSchema } from "@/lib/validation";
import { logger } from "@/lib/logger";
import { sanitizeError } from "@/lib/security";
import { z } from "zod";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

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
      jobs = await getAllJobs();
      console.log(`[JOBS GET] Found ${jobs.length} jobs from getAllJobs`);
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
      dateRange: body.dateRange,
    });

    // Validate request body - validate the already parsed body
    let validation: { success: true; data: any } | { success: false; error: string; status: number };
    try {
      const validatedData = createJobSchema.parse(body);
      validation = { success: true, data: validatedData };
    } catch (error: any) {
      if (error instanceof z.ZodError) {
        const errorDetails = error.errors.map(e => ({
          path: e.path.join('.') || 'root',
          message: e.message,
          code: e.code,
        }));
        const errorMessages = errorDetails.map(e => `${e.path}: ${e.message}`).join(', ');
        console.error('[JOBS POST] Validation errors:', JSON.stringify(errorDetails, null, 2));
        validation = {
          success: false,
          error: `Validation error: ${errorMessages}`,
          status: 400,
        };
      } else {
        validation = {
          success: false,
          error: 'Invalid request body',
          status: 400,
        };
      }
    }
    if (!validation.success) {
      console.error('[JOBS POST] Validation failed:', validation.error);
      console.error('[JOBS POST] Request body keys:', Object.keys(body));
      console.error('[JOBS POST] dateRange:', JSON.stringify(body.dateRange, null, 2));
      console.error('[JOBS POST] verasetConfig:', JSON.stringify(body.verasetConfig, null, 2).substring(0, 500));
      console.error('[JOBS POST] Full body (first 2000 chars):', JSON.stringify(body, null, 2).substring(0, 2000));
      return NextResponse.json(
        { 
          error: 'Validation failed',
          details: validation.error,
          received: {
            name: body.name,
            type: body.type,
            hasDateRange: !!body.dateRange,
            dateRange: body.dateRange,
            hasVerasetConfig: !!body.verasetConfig,
            verasetConfigKeys: body.verasetConfig ? Object.keys(body.verasetConfig) : [],
            verasetConfigGeoRadius: body.verasetConfig?.geo_radius?.length || 0,
            verasetConfigPlaceKey: body.verasetConfig?.place_key?.length || 0,
          }
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

    // AUDIT: Capture exact user input before any transformations
    const userInputAudit = {
      timestamp: new Date().toISOString(),
      userRequest: {
        name,
        type,
        poiCount,
        poiCollectionId,
        dateRange: {
          from: dateRange?.from_date || dateRange?.from,
          to: dateRange?.to_date || dateRange?.to,
          raw: dateRange, // Keep original structure
        },
        radius,
        schema,
        poiMapping,
        poiNames,
        verasetConfig: verasetConfig ? JSON.parse(JSON.stringify(verasetConfig)) : null, // Deep copy
        pois: validatedBody.pois ? JSON.parse(JSON.stringify(validatedBody.pois)) : null,
      },
    };
    
    console.log('[JOBS POST] 📋 USER INPUT AUDIT:', JSON.stringify(userInputAudit, null, 2));

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
    
    // CRITICAL: Validate POI data before sending (after verasetBodyObj is defined)
    if (verasetBodyObj.geo_radius && Array.isArray(verasetBodyObj.geo_radius)) {
      for (let i = 0; i < verasetBodyObj.geo_radius.length; i++) {
        const poi = verasetBodyObj.geo_radius[i];
        if (!poi.poi_id || poi.poi_id.trim() === '') {
          console.error(`[JOBS POST] POI ${i} missing poi_id`, poi);
          return NextResponse.json(
            { 
              error: 'Invalid POI data',
              details: `POI at index ${i} is missing poi_id`,
            },
            { status: 400 }
          );
        }
        if (typeof poi.latitude !== 'number' || isNaN(poi.latitude) || poi.latitude < -90 || poi.latitude > 90) {
          console.error(`[JOBS POST] POI ${i} invalid latitude`, poi);
          return NextResponse.json(
            { 
              error: 'Invalid POI data',
              details: `POI ${poi.poi_id} has invalid latitude: ${poi.latitude}`,
            },
            { status: 400 }
          );
        }
        if (typeof poi.longitude !== 'number' || isNaN(poi.longitude) || poi.longitude < -180 || poi.longitude > 180) {
          console.error(`[JOBS POST] POI ${i} invalid longitude`, poi);
          return NextResponse.json(
            { 
              error: 'Invalid POI data',
              details: `POI ${poi.poi_id} has invalid longitude: ${poi.longitude}`,
            },
            { status: 400 }
          );
        }
        if (!poi.distance_in_meters || poi.distance_in_meters < 1 || poi.distance_in_meters > 1000) {
          console.warn(`[JOBS POST] POI ${i} has unusual distance_in_meters: ${poi.distance_in_meters}`);
        }
      }
    }
    
    if (verasetBodyObj.place_key && Array.isArray(verasetBodyObj.place_key)) {
      for (let i = 0; i < verasetBodyObj.place_key.length; i++) {
        const poi = verasetBodyObj.place_key[i];
        if (!poi.poi_id || poi.poi_id.trim() === '') {
          console.error(`[JOBS POST] Place key POI ${i} missing poi_id`, poi);
          return NextResponse.json(
            { 
              error: 'Invalid POI data',
              details: `Place key POI at index ${i} is missing poi_id`,
            },
            { status: 400 }
          );
        }
        if (!poi.placekey || poi.placekey.trim() === '') {
          console.error(`[JOBS POST] Place key POI ${i} missing placekey`, poi);
          return NextResponse.json(
            { 
              error: 'Invalid POI data',
              details: `Place key POI ${poi.poi_id} is missing placekey`,
            },
            { status: 400 }
          );
        }
      }
    }
    
    // Ensure at least one POI array is present
    if ((!verasetBodyObj.geo_radius || verasetBodyObj.geo_radius.length === 0) &&
        (!verasetBodyObj.place_key || verasetBodyObj.place_key.length === 0)) {
      console.error('[JOBS POST] No POIs in verasetConfig!');
      return NextResponse.json(
        { 
          error: 'No POIs in verasetConfig',
          details: 'Either geo_radius or place_key array must contain at least one POI',
        },
        { status: 400 }
      );
    }
    
    // CRITICAL: Ensure date_range format is correct (Veraset expects from_date/to_date)
    if (verasetBodyObj.date_range) {
      const dr = verasetBodyObj.date_range;
      const fromDate = dr.from_date || dr.from || dr.fromDate;
      const toDate = dr.to_date || dr.to || dr.toDate;
      
      // Validate dates exist
      if (!fromDate || !toDate) {
        console.error('[JOBS POST] Missing date range', { dr });
        return NextResponse.json(
          { 
            error: 'Invalid date range',
            details: 'Both from_date and to_date are required',
            received: dr
          },
          { status: 400 }
        );
      }
      
      // Validate date format (YYYY-MM-DD)
      const dateFormatRegex = /^\d{4}-\d{2}-\d{2}$/;
      if (!dateFormatRegex.test(fromDate) || !dateFormatRegex.test(toDate)) {
        console.error('[JOBS POST] Invalid date format', { fromDate, toDate });
        return NextResponse.json(
          { 
            error: 'Invalid date format',
            details: 'Dates must be in YYYY-MM-DD format',
            received: { from_date: fromDate, to_date: toDate }
          },
          { status: 400 }
        );
      }
      
      // Validate dates are valid calendar dates
      const fromDateObj = new Date(fromDate);
      const toDateObj = new Date(toDate);
      
      if (isNaN(fromDateObj.getTime()) || isNaN(toDateObj.getTime())) {
        console.error('[JOBS POST] Invalid date values', { fromDate, toDate });
        return NextResponse.json(
          { 
            error: 'Invalid date values',
            details: 'Dates must be valid calendar dates',
            received: { from_date: fromDate, to_date: toDate }
          },
          { status: 400 }
        );
      }
      
      // Validate date range (to must be after from)
      if (toDateObj < fromDateObj) {
        console.error('[JOBS POST] Invalid date range order', { fromDate, toDate });
        return NextResponse.json(
          { 
            error: 'Invalid date range',
            details: 'to_date must be after from_date',
            received: { from_date: fromDate, to_date: toDate }
          },
          { status: 400 }
        );
      }
      
      // Calculate days difference (inclusive) - use consistent calculation
      const { calculateDaysInclusive } = await import('@/lib/s3');
      const daysDiff = calculateDaysInclusive(fromDate, toDate);
      
      // Validate date range limits (Veraset typically allows up to 31 days)
      if (daysDiff > 31) {
        console.error('[JOBS POST] Date range too large', { daysDiff, fromDate, toDate });
        return NextResponse.json(
          { 
            error: 'Date range too large',
            details: `Date range cannot exceed 31 days. Requested: ${daysDiff} days`,
            received: { from_date: fromDate, to_date: toDate, days: daysDiff }
          },
          { status: 400 }
        );
      }
      
      if (daysDiff < 1) {
        console.error('[JOBS POST] Date range too small', { daysDiff, fromDate, toDate });
        return NextResponse.json(
          { 
            error: 'Invalid date range',
            details: 'Date range must be at least 1 day',
            received: { from_date: fromDate, to_date: toDate }
          },
          { status: 400 }
        );
      }
      
      // Set normalized date range
      verasetBodyObj.date_range = {
        from_date: fromDate,
        to_date: toDate,
      };
      
      console.log('[JOBS POST] Date range validated', {
        from_date: fromDate,
        to_date: toDate,
        days: daysDiff,
      });
    } else {
      console.error('[JOBS POST] No date_range in verasetConfig!');
      return NextResponse.json(
        { 
          error: 'Missing date range',
          details: 'date_range is required in verasetConfig',
        },
        { status: 400 }
      );
    }
    
    // CRITICAL: Final validation before sending to Veraset
    console.log('[JOBS POST] Final payload validation', {
      type: jobType,
      hasDateRange: !!verasetBodyObj.date_range,
      dateRange: verasetBodyObj.date_range,
      geoRadiusCount: verasetBodyObj.geo_radius?.length || 0,
      placeKeyCount: verasetBodyObj.place_key?.length || 0,
      schema_type: verasetBodyObj.schema_type || 'BASIC',
    });
    
    // Log a sample of the payload (first POI of each type) for verification
    if (verasetBodyObj.geo_radius && verasetBodyObj.geo_radius.length > 0) {
      console.log('[JOBS POST] Sample geo_radius POI:', {
        poi_id: verasetBodyObj.geo_radius[0].poi_id,
        latitude: verasetBodyObj.geo_radius[0].latitude,
        longitude: verasetBodyObj.geo_radius[0].longitude,
        distance_in_meters: verasetBodyObj.geo_radius[0].distance_in_meters,
      });
    }
    if (verasetBodyObj.place_key && verasetBodyObj.place_key.length > 0) {
      console.log('[JOBS POST] Sample place_key POI:', {
        poi_id: verasetBodyObj.place_key[0].poi_id,
        placekey: verasetBodyObj.place_key[0].placekey?.substring(0, 20) + '...',
      });
    }
    
    // Determine Veraset endpoint based on job type (needed for audit)
    const endpoints: Record<string, string> = {
      'pings': '/v1/movement/job/pings',
      'devices': '/v1/movement/job/devices',
      'aggregate': '/v1/movement/job/aggregate',
      'cohort': '/v1/movement/job/cohort',
      'pings_by_device': '/v1/movement/job/pings_by_device',
    };
    const verasetEndpoint = endpoints[jobType] || endpoints['pings'];
    
    // AUDIT: Capture exact payload that will be sent to Veraset (before stringify)
    const verasetPayloadAudit = {
      timestamp: new Date().toISOString(),
      payloadToVeraset: JSON.parse(JSON.stringify(verasetBodyObj)), // Deep copy
      endpoint: verasetEndpoint,
      url: `https://platform.prd.veraset.tech${verasetEndpoint}`,
    };
    
    console.log('[JOBS POST] 📤 VERASET PAYLOAD AUDIT:', JSON.stringify(verasetPayloadAudit, null, 2));
    
    // 🔒 CRITICAL VERIFICATION: Compare user input with Veraset payload
    // This is a LOCK - if verification fails, job creation is BLOCKED
    const verificationIssues: string[] = [];
    
    // Verify date range (CRITICAL - must match exactly)
    const userFromDate = userInputAudit.userRequest.dateRange.from;
    const userToDate = userInputAudit.userRequest.dateRange.to;
    const verasetFromDate = verasetPayloadAudit.payloadToVeraset.date_range?.from_date;
    const verasetToDate = verasetPayloadAudit.payloadToVeraset.date_range?.to_date;
    
    if (!userFromDate || !userToDate) {
      verificationIssues.push(`CRITICAL: User date range is missing: from=${userFromDate}, to=${userToDate}`);
    }
    if (!verasetFromDate || !verasetToDate) {
      verificationIssues.push(`CRITICAL: Veraset payload date range is missing: from_date=${verasetFromDate}, to_date=${verasetToDate}`);
    }
    if (userFromDate !== verasetFromDate) {
      verificationIssues.push(`CRITICAL: Date range FROM mismatch - User requested "${userFromDate}" but payload has "${verasetFromDate}"`);
    }
    if (userToDate !== verasetToDate) {
      verificationIssues.push(`CRITICAL: Date range TO mismatch - User requested "${userToDate}" but payload has "${verasetToDate}"`);
    }
    
    // Verify schema (CRITICAL - must match exactly)
    const userSchema = userInputAudit.userRequest.schema || 'BASIC';
    const verasetSchema = verasetPayloadAudit.payloadToVeraset.schema_type;
    if (userSchema !== verasetSchema) {
      verificationIssues.push(`CRITICAL: Schema mismatch - User requested "${userSchema}" but payload has "${verasetSchema}"`);
    }
    
    // Verify POI counts (CRITICAL - must match exactly)
    const userGeoRadiusCount = userInputAudit.userRequest.verasetConfig?.geo_radius?.length || 0;
    const verasetGeoRadiusCount = verasetPayloadAudit.payloadToVeraset.geo_radius?.length || 0;
    if (userGeoRadiusCount !== verasetGeoRadiusCount) {
      verificationIssues.push(`CRITICAL: Geo radius POI count mismatch - User sent ${userGeoRadiusCount} POIs but payload has ${verasetGeoRadiusCount} POIs`);
    }
    
    const userPlaceKeyCount = userInputAudit.userRequest.verasetConfig?.place_key?.length || 0;
    const verasetPlaceKeyCount = verasetPayloadAudit.payloadToVeraset.place_key?.length || 0;
    if (userPlaceKeyCount !== verasetPlaceKeyCount) {
      verificationIssues.push(`CRITICAL: Place key POI count mismatch - User sent ${userPlaceKeyCount} POIs but payload has ${verasetPlaceKeyCount} POIs`);
    }
    
    // 🔒 CRITICAL: Verify user's radius input matches distance_in_meters in payload
    const userRadius = userInputAudit.userRequest.radius;
    console.log('[JOBS POST] 🔒 RADIUS VERIFICATION:', {
      userRequestedRadius: userRadius,
      geoRadiusPoisCount: verasetPayloadAudit.payloadToVeraset.geo_radius?.length || 0,
    });
    
    if (userRadius !== undefined && userRadius !== null) {
      if (verasetPayloadAudit.payloadToVeraset.geo_radius && verasetPayloadAudit.payloadToVeraset.geo_radius.length > 0) {
        // Check ALL geo_radius POIs have the same distance_in_meters as user's radius
        for (let i = 0; i < verasetPayloadAudit.payloadToVeraset.geo_radius.length; i++) {
          const poiDistance = verasetPayloadAudit.payloadToVeraset.geo_radius[i].distance_in_meters;
          console.log(`[JOBS POST] 🔒 Checking POI[${i}] radius:`, {
            userRequested: userRadius,
            poiDistanceInMeters: poiDistance,
            match: poiDistance === userRadius,
          });
          
          if (poiDistance !== userRadius) {
            verificationIssues.push(`CRITICAL: Radius mismatch - User requested ${userRadius}m but POI[${i}] has distance_in_meters=${poiDistance}m`);
          }
        }
      } else {
        console.warn('[JOBS POST] 🔒 WARNING: User provided radius but no geo_radius POIs in payload');
      }
    } else {
      console.warn('[JOBS POST] 🔒 WARNING: User did not provide radius value');
    }
    
    // Verify POI data integrity (CRITICAL - sample check of all POIs)
    if (verasetPayloadAudit.payloadToVeraset.geo_radius && userInputAudit.userRequest.verasetConfig?.geo_radius) {
      const userPois = userInputAudit.userRequest.verasetConfig.geo_radius;
      const verasetPois = verasetPayloadAudit.payloadToVeraset.geo_radius;
      
      if (userPois.length !== verasetPois.length) {
        verificationIssues.push(`CRITICAL: Geo radius array length mismatch - User: ${userPois.length}, Payload: ${verasetPois.length}`);
      } else {
        // Check ALL POIs, not just a sample
        for (let i = 0; i < userPois.length; i++) {
          const userPoi = userPois[i];
          const verasetPoi = verasetPois[i];
          
          if (userPoi.poi_id !== verasetPoi.poi_id) {
            verificationIssues.push(`CRITICAL: Geo radius POI[${i}] ID mismatch - User="${userPoi.poi_id}" vs Payload="${verasetPoi.poi_id}"`);
          }
          if (Math.abs(userPoi.latitude - verasetPoi.latitude) > 0.0001) {
            verificationIssues.push(`CRITICAL: Geo radius POI[${i}] latitude mismatch - User=${userPoi.latitude} vs Payload=${verasetPoi.latitude}`);
          }
          if (Math.abs(userPoi.longitude - verasetPoi.longitude) > 0.0001) {
            verificationIssues.push(`CRITICAL: Geo radius POI[${i}] longitude mismatch - User=${userPoi.longitude} vs Payload=${verasetPoi.longitude}`);
          }
          if (userPoi.distance_in_meters !== verasetPoi.distance_in_meters) {
            verificationIssues.push(`CRITICAL: Geo radius POI[${i}] distance_in_meters mismatch - User=${userPoi.distance_in_meters} vs Payload=${verasetPoi.distance_in_meters}`);
          }
        }
      }
    }
    
    // Verify place_key POIs (CRITICAL - check all)
    if (verasetPayloadAudit.payloadToVeraset.place_key && userInputAudit.userRequest.verasetConfig?.place_key) {
      const userPois = userInputAudit.userRequest.verasetConfig.place_key;
      const verasetPois = verasetPayloadAudit.payloadToVeraset.place_key;
      
      if (userPois.length !== verasetPois.length) {
        verificationIssues.push(`CRITICAL: Place key array length mismatch - User: ${userPois.length}, Payload: ${verasetPois.length}`);
      } else {
        // Check ALL POIs
        for (let i = 0; i < userPois.length; i++) {
          const userPoi = userPois[i];
          const verasetPoi = verasetPois[i];
          
          if (userPoi.poi_id !== verasetPoi.poi_id) {
            verificationIssues.push(`CRITICAL: Place key POI[${i}] ID mismatch - User="${userPoi.poi_id}" vs Payload="${verasetPoi.poi_id}"`);
          }
          if (userPoi.placekey !== verasetPoi.placekey) {
            verificationIssues.push(`CRITICAL: Place key POI[${i}] placekey mismatch - User="${userPoi.placekey}" vs Payload="${verasetPoi.placekey}"`);
          }
        }
      }
    }
    
    // Verify that we're not sending unexpected fields
    const allowedFields = ['date_range', 'schema_type', 'geo_radius', 'place_key'];
    const payloadFields = Object.keys(verasetPayloadAudit.payloadToVeraset);
    const unexpectedFields = payloadFields.filter(f => !allowedFields.includes(f));
    if (unexpectedFields.length > 0) {
      verificationIssues.push(`CRITICAL: Unexpected fields in payload: ${unexpectedFields.join(', ')}. Only ${allowedFields.join(', ')} are allowed.`);
    }
    
    // Verify that required fields are present
    if (!verasetPayloadAudit.payloadToVeraset.date_range) {
      verificationIssues.push(`CRITICAL: Missing date_range in payload`);
    }
    if (!verasetPayloadAudit.payloadToVeraset.schema_type) {
      verificationIssues.push(`CRITICAL: Missing schema_type in payload`);
    }
    if (!verasetPayloadAudit.payloadToVeraset.geo_radius && !verasetPayloadAudit.payloadToVeraset.place_key) {
      verificationIssues.push(`CRITICAL: Missing POIs - neither geo_radius nor place_key arrays are present`);
    }
    if (verasetPayloadAudit.payloadToVeraset.geo_radius && verasetPayloadAudit.payloadToVeraset.geo_radius.length === 0 &&
        verasetPayloadAudit.payloadToVeraset.place_key && verasetPayloadAudit.payloadToVeraset.place_key.length === 0) {
      verificationIssues.push(`CRITICAL: Empty POI arrays - both geo_radius and place_key are empty`);
    }
    
    // 🔒 CRITICAL LOCK: Fail job creation if payload doesn't match user input
    // This prevents sending incorrect jobs to Veraset (which costs money)
    if (verificationIssues.length > 0) {
      console.error('[JOBS POST] 🔒 LOCK ENGAGED - Job creation BLOCKED due to payload mismatch');
      console.error('[JOBS POST] Verification issues:', verificationIssues);
      console.error('[JOBS POST] User input:', JSON.stringify(userInputAudit.userRequest, null, 2));
      console.error('[JOBS POST] Veraset payload:', JSON.stringify(verasetPayloadAudit.payloadToVeraset, null, 2));
      
      logger.error('🔒 JOB CREATION BLOCKED - Payload verification failed', { 
        issues: verificationIssues,
        userInput: userInputAudit.userRequest,
        verasetPayload: verasetPayloadAudit.payloadToVeraset,
      });
      
      return NextResponse.json(
        {
          error: 'Job creation blocked: Payload verification failed',
          message: 'The system detected that the payload being sent to Veraset does not match your request. Job creation has been blocked to prevent incorrect charges.',
          details: 'This is a safety mechanism to ensure Veraset receives exactly what you requested.',
          issues: verificationIssues,
          userRequested: {
            dateRange: `${userInputAudit.userRequest.dateRange.from} to ${userInputAudit.userRequest.dateRange.to}`,
            schema: userInputAudit.userRequest.schema || 'BASIC',
            geoRadiusPois: userInputAudit.userRequest.verasetConfig?.geo_radius?.length || 0,
            placeKeyPois: userInputAudit.userRequest.verasetConfig?.place_key?.length || 0,
          },
          wouldSendToVeraset: {
            dateRange: `${verasetPayloadAudit.payloadToVeraset.date_range.from_date} to ${verasetPayloadAudit.payloadToVeraset.date_range.to_date}`,
            schema: verasetPayloadAudit.payloadToVeraset.schema_type,
            geoRadiusPois: verasetPayloadAudit.payloadToVeraset.geo_radius?.length || 0,
            placeKeyPois: verasetPayloadAudit.payloadToVeraset.place_key?.length || 0,
          },
          actionableSteps: [
            'Review the discrepancies listed above',
            'Check that your input data is correctly formatted',
            'Contact support if this error persists',
            'Do not retry until the issue is resolved to avoid incorrect charges',
          ],
        },
        { status: 400 } // 400 Bad Request - user input issue, not server error
      );
    }
    
    console.log('[JOBS POST] ✅ VERIFICATION PASSED: User input matches Veraset payload - proceeding with job creation');
    
    const verasetBody = JSON.stringify(verasetBodyObj);
    
    // Log payload size for monitoring
    const payloadSizeKB = (verasetBody.length / 1024).toFixed(2);
    console.log(`[JOBS POST] Payload size: ${payloadSizeKB} KB`);

    // 4. Get and validate API key
    // Note: endpoints and verasetEndpoint are already defined above (line 494-499) for audit purposes
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

    console.log('[JOBS POST] 🔓 LOCK VERIFIED - Preparing Veraset API call', {
      endpoint: verasetEndpoint,
      url: `https://platform.prd.veraset.tech${verasetEndpoint}`,
      bodySize: verasetBody.length,
      bodySizeKB: payloadSizeKB,
      geoRadiusCount: verasetBodyObj.geo_radius?.length || 0,
      placeKeyCount: verasetBodyObj.place_key?.length || 0,
      dateRange: verasetBodyObj.date_range,
      schema_type: verasetBodyObj.schema_type || 'BASIC',
      type: jobType,
      apiKeyConfigured: !!verasetApiKey,
      apiKeyLength: verasetApiKey.length,
      apiKeyPrefix: verasetApiKey.substring(0, 10) + '...',
      verificationStatus: 'PASSED',
    });
    
    // Log full payload for debugging (truncated if too large)
    // CRITICAL: This is the exact payload being sent - use for verification
    if (verasetBody.length < 5000) {
      console.log('[JOBS POST] 📤 EXACT PAYLOAD TO VERASET:', JSON.stringify(verasetBodyObj, null, 2));
    } else {
      console.log('[JOBS POST] 📤 EXACT PAYLOAD TO VERASET (preview):', verasetBody.substring(0, 2000));
      console.log('[JOBS POST] 📤 EXACT PAYLOAD STRUCTURE:', {
        date_range: verasetBodyObj.date_range,
        schema_type: verasetBodyObj.schema_type,
        geo_radius_count: verasetBodyObj.geo_radius?.length || 0,
        place_key_count: verasetBodyObj.place_key?.length || 0,
        geo_radius_sample: verasetBodyObj.geo_radius?.slice(0, 2),
        place_key_sample: verasetBodyObj.place_key?.slice(0, 2),
      });
    }
    
    // Save audit trail for later verification
    const auditTrail: {
      userInput: any;
      verasetPayload: any;
      verificationPassed: boolean;
      verificationIssues: string[];
      timestamp: string;
      verasetResponse?: any;
      responseVerificationPassed?: boolean;
      responseVerificationIssues?: string[];
    } = {
      userInput: userInputAudit.userRequest,
      verasetPayload: verasetPayloadAudit.payloadToVeraset,
      verificationPassed: verificationIssues.length === 0,
      verificationIssues,
      timestamp: new Date().toISOString(),
    };

    logger.log(`Veraset API call: ${verasetEndpoint}`, {
      bodySize: verasetBody.length,
      geoRadiusCount: verasetBodyObj.geo_radius?.length || 0,
      placeKeyCount: verasetBodyObj.place_key?.length || 0,
      apiKeyConfigured: !!verasetApiKey,
      apiKeyLength: verasetApiKey.length,
    });

    // 🔒 FINAL CHECK: Ensure verification passed before calling Veraset (costs money!)
    // This is the absolute last check before the API call - if this fails, we abort
    if (verificationIssues.length > 0) {
      console.error('[JOBS POST] 🔒 FINAL LOCK CHECK FAILED - Aborting Veraset API call');
      console.error('[JOBS POST] Verification issues detected:', verificationIssues);
      logger.error('🔒 FINAL LOCK CHECK FAILED - Job creation blocked', { 
        issues: verificationIssues,
        userInput: userInputAudit.userRequest,
        verasetPayload: verasetPayloadAudit.payloadToVeraset,
      });
      return NextResponse.json(
        {
          error: 'Job creation blocked: Final verification failed',
          message: 'The system detected payload mismatches at the final check. Job creation has been blocked to prevent incorrect charges.',
          details: 'This is a critical safety mechanism - the payload does not match your request.',
          issues: verificationIssues,
          userRequested: {
            dateRange: `${userInputAudit.userRequest.dateRange.from} to ${userInputAudit.userRequest.dateRange.to}`,
            schema: userInputAudit.userRequest.schema || 'BASIC',
            geoRadiusPois: userInputAudit.userRequest.verasetConfig?.geo_radius?.length || 0,
            placeKeyPois: userInputAudit.userRequest.verasetConfig?.place_key?.length || 0,
          },
          wouldSendToVeraset: {
            dateRange: `${verasetPayloadAudit.payloadToVeraset.date_range?.from_date} to ${verasetPayloadAudit.payloadToVeraset.date_range?.to_date}`,
            schema: verasetPayloadAudit.payloadToVeraset.schema_type,
            geoRadiusPois: verasetPayloadAudit.payloadToVeraset.geo_radius?.length || 0,
            placeKeyPois: verasetPayloadAudit.payloadToVeraset.place_key?.length || 0,
          },
        },
        { status: 400 }
      );
    }

    // 5. Call Veraset API (only if verification passed)
    // 🔒 This is the point of no return - once we call Veraset, the job is created and charged
    const verasetUrl = `https://platform.prd.veraset.tech${verasetEndpoint}`;
    console.log('[JOBS POST] 🔓 Calling Veraset API (verification passed, lock released)', {
      url: verasetUrl,
      payloadSize: verasetBody.length,
      dateRange: verasetBodyObj.date_range,
      schema: verasetBodyObj.schema_type,
      poiCount: (verasetBodyObj.geo_radius?.length || 0) + (verasetBodyObj.place_key?.length || 0),
    });
    
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
    
    // AUDIT: Capture Veraset response for verification
    const verasetResponseAudit = {
      timestamp: new Date().toISOString(),
      status: verasetResponse.status,
      statusText: verasetResponse.statusText,
      response: JSON.parse(JSON.stringify(verasetData)), // Deep copy
      rawResponse: responseText.substring(0, 2000), // Keep raw for debugging
    };
    
    console.log('[JOBS POST] 📥 VERASET RESPONSE AUDIT:', JSON.stringify(verasetResponseAudit, null, 2));
    
    // Log full response for debugging
    console.log('[JOBS POST] Veraset API response', {
      status: verasetResponse.status,
      statusText: verasetResponse.statusText,
      hasJobId: !!(verasetData.job_id || verasetData.data?.job_id),
      jobId: verasetData.job_id || verasetData.data?.job_id,
      hasWarnings: !!(verasetData.warnings || verasetData.data?.warnings),
      warnings: verasetData.warnings || verasetData.data?.warnings,
      hasInfo: !!(verasetData.info || verasetData.data?.info),
      info: verasetData.info || verasetData.data?.info,
      processedPois: verasetData.processed_pois || verasetData.data?.processed_pois,
      fullResponse: JSON.stringify(verasetData).substring(0, 1000),
    });
    
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
    
    const jobId = verasetData.job_id || verasetData.data?.job_id;
    const processedPois = verasetData.processed_pois || verasetData.data?.processed_pois;
    const warnings = verasetData.warnings || verasetData.data?.warnings;
    const info = verasetData.info || verasetData.data?.info;
    
    console.log('[JOBS POST] Veraset API success', {
      jobId,
      processedPois,
      warnings: warnings ? JSON.stringify(warnings) : null,
      info: info ? JSON.stringify(info) : null,
    });
    
    logger.log('Veraset API success', {
      jobId,
      processedPois,
      warnings: warnings ? JSON.stringify(warnings) : null,
      info: info ? JSON.stringify(info) : null,
    });
    
    // CRITICAL: Log any warnings or info from Veraset - these might indicate issues
    if (warnings) {
      console.warn('[JOBS POST] ⚠️ Veraset API WARNINGS:', JSON.stringify(warnings, null, 2));
      logger.warn('Veraset API warnings', { warnings });
    }
    
    if (info) {
      console.log('[JOBS POST] ℹ️ Veraset API INFO:', JSON.stringify(info, null, 2));
      logger.info('Veraset API info', { info });
    }
    
    // AUDIT: Verify Veraset response matches what we sent
    const responseVerificationIssues: string[] = [];
    
    // CRITICAL: Check if Veraset returned a different POI count than sent
    if (processedPois !== undefined && poiCountToSend > 0) {
      if (processedPois !== poiCountToSend) {
        const issue = `POI count mismatch: Sent ${poiCountToSend} POIs, but Veraset processed ${processedPois} (missing ${poiCountToSend - processedPois})`;
        console.error(`[JOBS POST] ❌ CRITICAL: ${issue}`);
        responseVerificationIssues.push(issue);
        logger.error(`Veraset processed ${processedPois} POIs out of ${poiCountToSend} sent`, {
          sent: poiCountToSend,
          processed: processedPois,
          missing: poiCountToSend - processedPois,
        });
      } else {
        console.log(`[JOBS POST] ✅ All ${poiCountToSend} POIs were processed by Veraset`);
      }
    }
    
    // Check if Veraset returned any date range modifications
    if (verasetData.date_range || verasetData.data?.date_range) {
      const verasetDateRange = verasetData.date_range || verasetData.data.date_range;
      const requestedRange = verasetBodyObj.date_range;
      if (verasetDateRange.from_date !== requestedRange.from_date || 
          verasetDateRange.to_date !== requestedRange.to_date) {
        const issue = `Date range modified by Veraset: Requested ${requestedRange.from_date} to ${requestedRange.to_date}, but Veraset returned ${verasetDateRange.from_date} to ${verasetDateRange.to_date}`;
        console.warn(`[JOBS POST] ⚠️ ${issue}`);
        responseVerificationIssues.push(issue);
        logger.warn('Veraset modified date range', {
          requested: requestedRange,
          actual: verasetDateRange,
        });
      } else {
        console.log(`[JOBS POST] ✅ Date range confirmed: Veraset accepted ${requestedRange.from_date} to ${requestedRange.to_date}`);
      }
    }
    
    // Update audit trail with response verification
    auditTrail.verasetResponse = verasetResponseAudit.response;
    auditTrail.responseVerificationPassed = responseVerificationIssues.length === 0;
    auditTrail.responseVerificationIssues = responseVerificationIssues;
    
    if (!jobId) {
      console.error('[JOBS POST] ❌ CRITICAL: No job_id returned from Veraset!');
      console.error('[JOBS POST] Full Veraset response:', JSON.stringify(verasetData, null, 2));
      logger.error('No job_id returned from Veraset', { response: verasetData });
      return NextResponse.json(
        { 
          error: 'No job_id returned from Veraset', 
          details: 'Veraset API did not return a job_id in the response. The job may not have been created.',
          data: verasetData,
          hint: 'Check Veraset API response for errors or warnings'
        },
        { status: 502 }
      );
    }
    
    console.log(`[JOBS POST] ✅ Job created successfully: ${jobId}`);

    console.log('[JOBS POST] Saving job to S3', {
      jobId,
      name,
      type,
      poiCount: poiCount || 0,
    });

    // 7. Save job to S3 with exact payload sent to Veraset
    const normalizedDateRange: { from: string; to: string } = {
      from: dateRange?.from || dateRange?.from_date || '',
      to: dateRange?.to || dateRange?.to_date || '',
    };
    
    // Calculate requested days for verification (inclusive count)
    let requestedDays = 0;
    if (verasetBodyObj.date_range?.from_date && verasetBodyObj.date_range?.to_date) {
      try {
        const { calculateDaysInclusive } = await import('@/lib/s3');
        requestedDays = calculateDaysInclusive(
          verasetBodyObj.date_range.from_date,
          verasetBodyObj.date_range.to_date
        );
        console.log('[JOBS POST] Calculated requested days:', {
          from: verasetBodyObj.date_range.from_date,
          to: verasetBodyObj.date_range.to_date,
          days: requestedDays,
        });
      } catch (error: any) {
        console.error('[JOBS POST] Error calculating requested days:', error);
        // Don't fail job creation, but log the error
        requestedDays = 0;
      }
    }
    
    // 🔒 CRITICAL: Ensure radius saved in job matches user input (not modified)
    const jobRadius = radius !== undefined && radius !== null ? radius : 10;
    console.log('[JOBS POST] 🔒 FINAL RADIUS CHECK BEFORE SAVING:', {
      userRequested: userInputAudit.userRequest.radius,
      radiusFromBody: radius,
      jobRadiusToSave: jobRadius,
      match: userInputAudit.userRequest.radius === jobRadius,
    });
    
    if (userInputAudit.userRequest.radius !== undefined && userInputAudit.userRequest.radius !== null) {
      if (jobRadius !== userInputAudit.userRequest.radius) {
        console.error('[JOBS POST] 🔒 CRITICAL: Radius mismatch detected when saving job!', {
          userRequested: userInputAudit.userRequest.radius,
          wouldSave: jobRadius,
          difference: Math.abs(userInputAudit.userRequest.radius - jobRadius),
        });
        throw new Error(`CRITICAL: Radius mismatch - User requested ${userInputAudit.userRequest.radius}m but system would save ${jobRadius}m. Job creation blocked.`);
      }
      console.log('[JOBS POST] ✅ Radius verification passed - saving correct radius:', jobRadius);
    }
    
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
      dateRangeDiscrepancy: {
        requestedDays,
        actualDays: 0, // Will be updated after sync
        missingDays: 0,
      },
      // AUDIT: Store complete audit trail for verification
      auditTrail: auditTrail as any, // Store full audit trail
    });
    
    console.log('[JOBS POST] Job saved with complete audit trail', {
      jobId,
      requestedDays,
      dateRange: verasetBodyObj.date_range,
      verificationPassed: auditTrail.verificationPassed,
      responseVerificationPassed: auditTrail.responseVerificationPassed,
      issues: [...auditTrail.verificationIssues, ...auditTrail.responseVerificationIssues],
    });
    
    // Log audit summary for quick verification
    console.log('[JOBS POST] 📊 AUDIT SUMMARY:', {
      jobId,
      userRequested: {
        dateRange: `${userInputAudit.userRequest.dateRange.from} to ${userInputAudit.userRequest.dateRange.to}`,
        schema: userInputAudit.userRequest.schema,
        poiCount: poiCountToSend,
      },
      sentToVeraset: {
        dateRange: `${verasetPayloadAudit.payloadToVeraset.date_range.from_date} to ${verasetPayloadAudit.payloadToVeraset.date_range.to_date}`,
        schema: verasetPayloadAudit.payloadToVeraset.schema_type,
        geoRadiusCount: verasetPayloadAudit.payloadToVeraset.geo_radius?.length || 0,
        placeKeyCount: verasetPayloadAudit.payloadToVeraset.place_key?.length || 0,
      },
      verasetResponse: {
        processedPois,
        dateRange: verasetData.date_range || verasetData.data?.date_range || 'not returned',
      },
      verification: {
        payloadMatches: auditTrail.verificationPassed,
        responseMatches: auditTrail.responseVerificationPassed,
      },
    });
    
    // CRITICAL: Log complete audit trail for verification (can be retrieved via /api/jobs/[id]/audit)
    logger.log('Job created with complete audit trail', {
      jobId,
      jobName: name,
      auditTrail: {
        verificationPassed: auditTrail.verificationPassed,
        responseVerificationPassed: auditTrail.responseVerificationPassed,
        issues: [...auditTrail.verificationIssues, ...auditTrail.responseVerificationIssues],
        userInput: {
          dateRange: userInputAudit.userRequest.dateRange,
          schema: userInputAudit.userRequest.schema,
          poiCount: poiCountToSend,
        },
        verasetPayload: {
          dateRange: verasetPayloadAudit.payloadToVeraset.date_range,
          schema: verasetPayloadAudit.payloadToVeraset.schema_type,
          poiCounts: {
            geoRadius: verasetPayloadAudit.payloadToVeraset.geo_radius?.length || 0,
            placeKey: verasetPayloadAudit.payloadToVeraset.place_key?.length || 0,
          },
        },
      },
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
