import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/jobs/[id]/audit
 * Get complete audit trail for a job to verify what was sent to Veraset matches user input
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> | { id: string } }
) {
  try {
    const params =
      typeof context.params === 'object' && context.params instanceof Promise
        ? await context.params
        : context.params;
    const jobId = params.id;

    const job = await getJob(jobId);
    if (!job) {
      return NextResponse.json(
        { error: 'Job not found' },
        { status: 404 }
      );
    }

    if (!job.auditTrail) {
      return NextResponse.json(
        {
          jobId,
          jobName: job.name,
          message: 'No audit trail available for this job (created before audit system was implemented)',
          hasAuditTrail: false,
        },
        { status: 200 }
      );
    }

    const audit = job.auditTrail;

    // Perform verification checks
    const verificationResults = {
      payloadVerification: {
        passed: audit.verificationPassed,
        issues: audit.verificationIssues || [],
      },
      responseVerification: {
        passed: audit.responseVerificationPassed ?? true,
        issues: audit.responseVerificationIssues || [],
      },
    };

    // Detailed comparison
    const comparisons = {
      dateRange: {
        userRequested: {
          from: audit.userInput.dateRange.from,
          to: audit.userInput.dateRange.to,
        },
        sentToVeraset: {
          from: audit.verasetPayload.date_range.from_date,
          to: audit.verasetPayload.date_range.to_date,
        },
        matches: 
          audit.userInput.dateRange.from === audit.verasetPayload.date_range.from_date &&
          audit.userInput.dateRange.to === audit.verasetPayload.date_range.to_date,
      },
      schema: {
        userRequested: audit.userInput.schema || 'BASIC',
        sentToVeraset: audit.verasetPayload.schema_type,
        matches: (audit.userInput.schema || 'BASIC') === audit.verasetPayload.schema_type,
      },
      poiCounts: {
        userGeoRadius: audit.userInput.verasetConfig?.geo_radius?.length || 0,
        verasetGeoRadius: audit.verasetPayload.geo_radius?.length || 0,
        userPlaceKey: audit.userInput.verasetConfig?.place_key?.length || 0,
        verasetPlaceKey: audit.verasetPayload.place_key?.length || 0,
        matches: 
          (audit.userInput.verasetConfig?.geo_radius?.length || 0) === (audit.verasetPayload.geo_radius?.length || 0) &&
          (audit.userInput.verasetConfig?.place_key?.length || 0) === (audit.verasetPayload.place_key?.length || 0),
      },
    };

    // Sample POI verification (first 5)
    const poiVerification: Array<{
      index: number;
      type: 'geo_radius' | 'place_key';
      userPoi: any;
      verasetPoi: any;
      matches: boolean;
      differences?: string[];
      poiName?: string; // Human-readable name from job metadata
    }> = [];

    // Get POI names from job metadata
    const poiNames = job.poiNames || {};

    if (audit.userInput.verasetConfig?.geo_radius && audit.verasetPayload.geo_radius) {
      const sampleSize = Math.min(5, audit.userInput.verasetConfig.geo_radius.length);
      for (let i = 0; i < sampleSize; i++) {
        const userPoi = audit.userInput.verasetConfig.geo_radius[i];
        const verasetPoi = audit.verasetPayload.geo_radius[i];
        const differences: string[] = [];
        
        if (userPoi.poi_id !== verasetPoi.poi_id) {
          differences.push(`poi_id: ${userPoi.poi_id} !== ${verasetPoi.poi_id}`);
        }
        if (Math.abs(userPoi.latitude - verasetPoi.latitude) > 0.0001) {
          differences.push(`latitude: ${userPoi.latitude} !== ${verasetPoi.latitude}`);
        }
        if (Math.abs(userPoi.longitude - verasetPoi.longitude) > 0.0001) {
          differences.push(`longitude: ${userPoi.longitude} !== ${verasetPoi.longitude}`);
        }
        if (userPoi.distance_in_meters !== verasetPoi.distance_in_meters) {
          differences.push(`distance_in_meters: ${userPoi.distance_in_meters} !== ${verasetPoi.distance_in_meters}`);
        }
        
        // Get POI name from job metadata
        const poiId = verasetPoi.poi_id || userPoi.poi_id;
        const poiName = poiNames[poiId] || null;
        
        poiVerification.push({
          index: i,
          type: 'geo_radius',
          userPoi,
          verasetPoi,
          matches: differences.length === 0,
          differences: differences.length > 0 ? differences : undefined,
          poiName: poiName || undefined,
        });
      }
    }

    if (audit.userInput.verasetConfig?.place_key && audit.verasetPayload.place_key) {
      const sampleSize = Math.min(5, audit.userInput.verasetConfig.place_key.length);
      for (let i = 0; i < sampleSize; i++) {
        const userPoi = audit.userInput.verasetConfig.place_key[i];
        const verasetPoi = audit.verasetPayload.place_key[i];
        const differences: string[] = [];
        
        if (userPoi.poi_id !== verasetPoi.poi_id) {
          differences.push(`poi_id: ${userPoi.poi_id} !== ${verasetPoi.poi_id}`);
        }
        if (userPoi.placekey !== verasetPoi.placekey) {
          differences.push(`placekey: ${userPoi.placekey} !== ${verasetPoi.placekey}`);
        }
        
        // Get POI name from job metadata
        const poiId = verasetPoi.poi_id || userPoi.poi_id;
        const poiName = poiNames[poiId] || null;
        
        poiVerification.push({
          index: i,
          type: 'place_key',
          userPoi,
          verasetPoi,
          matches: differences.length === 0,
          differences: differences.length > 0 ? differences : undefined,
          poiName: poiName || undefined,
        });
      }
    }

    const overallVerificationPassed = 
      verificationResults.payloadVerification.passed &&
      verificationResults.responseVerification.passed &&
      comparisons.dateRange.matches &&
      comparisons.schema.matches &&
      comparisons.poiCounts.matches &&
      poiVerification.every(p => p.matches);

    return NextResponse.json({
      jobId,
      jobName: job.name,
      timestamp: audit.timestamp,
      hasAuditTrail: true,
      verification: {
        overall: overallVerificationPassed ? 'PASSED' : 'FAILED',
        payloadVerification: verificationResults.payloadVerification,
        responseVerification: verificationResults.responseVerification,
      },
      comparisons,
      poiVerification: {
        samples: poiVerification,
        allSamplesMatch: poiVerification.every(p => p.matches),
      },
      userInput: audit.userInput,
      verasetPayload: audit.verasetPayload,
      verasetResponse: audit.verasetResponse,
      summary: {
        userRequested: {
          dateRange: `${audit.userInput.dateRange.from} to ${audit.userInput.dateRange.to}`,
          schema: audit.userInput.schema || 'BASIC',
          geoRadiusPois: audit.userInput.verasetConfig?.geo_radius?.length || 0,
          placeKeyPois: audit.userInput.verasetConfig?.place_key?.length || 0,
        },
        sentToVeraset: {
          dateRange: `${audit.verasetPayload.date_range.from_date} to ${audit.verasetPayload.date_range.to_date}`,
          schema: audit.verasetPayload.schema_type,
          geoRadiusPois: audit.verasetPayload.geo_radius?.length || 0,
          placeKeyPois: audit.verasetPayload.place_key?.length || 0,
        },
        verasetProcessed: {
          processedPois: audit.verasetResponse?.processed_pois || audit.verasetResponse?.data?.processed_pois || 'unknown',
          dateRange: audit.verasetResponse?.date_range || audit.verasetResponse?.data?.date_range || 'not returned',
        },
      },
    });
  } catch (error: any) {
    console.error('GET /api/jobs/[id]/audit error:', error);
    return NextResponse.json(
      {
        error: 'Failed to retrieve audit trail',
        details: error.message,
      },
      { status: 500 }
    );
  }
}
