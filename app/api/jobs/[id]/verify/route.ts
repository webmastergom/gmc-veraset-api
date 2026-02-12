import { NextRequest, NextResponse } from "next/server";
import { getJob } from "@/lib/jobs";
import { logger } from "@/lib/logger";

/**
 * GET /api/jobs/[id]/verify
 * Verify that a job's actual data matches what was requested
 * This is a post-creation verification endpoint
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> | { id: string } }
) {
  try {
    const params = await Promise.resolve(context.params);
    const jobId = params.id;

    const job = await getJob(jobId);
    if (!job) {
      return NextResponse.json(
        { error: 'Job not found' },
        { status: 404 }
      );
    }

    const issues: string[] = [];
    const warnings: string[] = [];

    // Verify 1: Check if audit trail exists
    if (!job.auditTrail) {
      warnings.push('No audit trail available for this job');
    }

    // Verify 2: Compare requested radius with saved radius
    if (job.auditTrail?.userInput?.radius !== undefined) {
      const requestedRadius = job.auditTrail.userInput.radius;
      const savedRadius = job.radius;
      if (requestedRadius !== savedRadius) {
        issues.push(`CRITICAL: Radius mismatch - Requested ${requestedRadius}m but saved ${savedRadius}m`);
      }
    }

    // Verify 3: Compare requested radius with payload radius
    if (job.auditTrail?.userInput?.radius !== undefined && job.verasetPayload?.geo_radius) {
      const requestedRadius = job.auditTrail.userInput.radius;
      for (const poi of job.verasetPayload.geo_radius) {
        if (poi.distance_in_meters !== requestedRadius) {
          issues.push(`CRITICAL: Payload radius mismatch - Requested ${requestedRadius}m but POI has ${poi.distance_in_meters}m`);
        }
      }
    }

    // Verify 4: Check date range discrepancy
    if (job.dateRangeDiscrepancy) {
      const { requestedDays, actualDays, missingDays } = job.dateRangeDiscrepancy;
      if (missingDays > 0) {
        issues.push(`CRITICAL: Missing ${missingDays} days - Requested ${requestedDays} days but only ${actualDays} days available`);
      }
    }

    // Verify 5: Check if sync completed successfully
    if (!job.syncedAt && job.status === 'SUCCESS') {
      warnings.push('Job marked as SUCCESS but sync timestamp is missing');
    }

    // Verify 6: Compare requested date range with payload date range
    if (job.auditTrail?.userInput?.dateRange && job.verasetPayload?.date_range) {
      const requestedFrom = job.auditTrail.userInput.dateRange.from;
      const requestedTo = job.auditTrail.userInput.dateRange.to;
      const payloadFrom = job.verasetPayload.date_range.from_date;
      const payloadTo = job.verasetPayload.date_range.to_date;

      if (requestedFrom !== payloadFrom) {
        issues.push(`CRITICAL: Date range FROM mismatch - Requested ${requestedFrom} but payload has ${payloadFrom}`);
      }
      if (requestedTo !== payloadTo) {
        issues.push(`CRITICAL: Date range TO mismatch - Requested ${requestedTo} but payload has ${payloadTo}`);
      }
    }

    const verificationStatus = issues.length === 0 ? 'verified' : 'failed';
    const severity = issues.length > 0 ? 'critical' : warnings.length > 0 ? 'warning' : 'ok';

    logger.log(`Job verification: ${jobId}`, {
      status: verificationStatus,
      severity,
      issuesCount: issues.length,
      warningsCount: warnings.length,
    });

    return NextResponse.json({
      jobId,
      status: verificationStatus,
      severity,
      timestamp: new Date().toISOString(),
      verification: {
        issues,
        warnings,
        summary: {
          totalIssues: issues.length,
          totalWarnings: warnings.length,
          isReliable: issues.length === 0,
        },
      },
      job: {
        requestedRadius: job.auditTrail?.userInput?.radius,
        savedRadius: job.radius,
        requestedDateRange: job.auditTrail?.userInput?.dateRange,
        payloadDateRange: job.verasetPayload?.date_range,
        dateRangeDiscrepancy: job.dateRangeDiscrepancy,
      },
    }, {
      status: 200,
    });

  } catch (error: any) {
    logger.error('Job verification error:', error);
    return NextResponse.json(
      {
        error: 'Verification failed',
        message: error.message,
      },
      { status: 500 }
    );
  }
}
