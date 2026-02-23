import { NextRequest, NextResponse } from "next/server";
import { getJob, updateJobStatus } from "@/lib/jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * Force refresh job status from Veraset API
 * GET /api/jobs/[id]/refresh
 *
 * Changed from POST to GET because POST requests systematically return
 * 405 Method Not Allowed on the current Vercel deployment.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const job = await getJob(params.id);

    if (!job) {
      return NextResponse.json(
        { error: 'Job not found' },
        { status: 404 }
      );
    }

    console.log(`[REFRESH] Force refreshing status for job ${params.id} (current: ${job.status})`);

    const apiKey = process.env.VERASET_API_KEY?.trim();

    if (!apiKey) {
      return NextResponse.json({
        success: false,
        error: 'VERASET_API_KEY not configured',
        job: job,
      }, { status: 500 });
    }

    // Use AbortController with 8s timeout to prevent Vercel function timeout (10s limit)
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);

    try {
      const verasetResponse = await fetch(
        `https://platform.prd.veraset.tech/v1/job/${params.id}`,
        {
          cache: 'no-store',
          signal: controller.signal,
          headers: {
            'X-API-Key': apiKey,
            'Content-Type': 'application/json',
          },
        }
      );
      clearTimeout(timeout);

      if (verasetResponse.ok) {
        const verasetData = await verasetResponse.json();
        const newStatus = verasetData.status || verasetData.data?.status;

        console.log(`[REFRESH] Veraset response for ${params.id}:`, {
          currentStatus: job.status,
          newStatus,
        });

        if (newStatus) {
          const updatedJob = await updateJobStatus(
            params.id,
            newStatus as any,
            verasetData.error_message || verasetData.data?.error_message
          );

          return NextResponse.json({
            success: true,
            job: updatedJob,
            statusChanged: newStatus !== job.status,
            oldStatus: job.status,
            newStatus: newStatus,
          });
        } else {
          return NextResponse.json({
            success: false,
            error: 'No status returned from Veraset API',
            job: job,
          }, { status: 502 });
        }
      } else {
        const errorText = await verasetResponse.text();
        console.error(`[REFRESH] Veraset API error for ${params.id}: ${verasetResponse.status}`, errorText);
        return NextResponse.json({
          success: false,
          error: `Veraset API returned ${verasetResponse.status}`,
          details: errorText.substring(0, 500),
          job: job,
        }, { status: 502 });
      }
    } catch (fetchError: any) {
      clearTimeout(timeout);
      const isAbort = fetchError.name === 'AbortError';
      console.error(`[REFRESH] ${isAbort ? 'Timeout' : 'Error'} checking Veraset for ${params.id}:`, fetchError.message);
      return NextResponse.json({
        success: false,
        error: isAbort ? 'Veraset API timed out (8s)' : 'Failed to check Veraset status',
        details: fetchError.message,
        job: job,
      }, { status: 504 });
    }

  } catch (error: any) {
    console.error(`GET /api/jobs/[id]/refresh error:`, error);
    return NextResponse.json(
      { error: 'Failed to refresh job status', details: error.message },
      { status: 500 }
    );
  }
}
