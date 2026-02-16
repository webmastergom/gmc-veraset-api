import { NextRequest, NextResponse } from "next/server";
import { getJob, updateJobStatus } from "@/lib/jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * Force refresh job status from Veraset API
 * POST /api/jobs/[id]/refresh
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> | { id: string } }
) {
  try {
    const params =
      typeof context.params === 'object' && context.params instanceof Promise
        ? await context.params
        : context.params;
    const job = await getJob(params.id);
    
    if (!job) {
      return NextResponse.json(
        { error: 'Job not found' },
        { status: 404 }
      );
    }
    
    console.log(`🔄 Force refreshing status for job ${params.id} (current: ${job.status})`);
    
    // Always check Veraset API directly, regardless of current status
    try {
      const apiKey = process.env.VERASET_API_KEY?.trim();
      
      if (!apiKey) {
        return NextResponse.json({
          success: false,
          error: 'VERASET_API_KEY not configured',
          job: job,
        }, { status: 500 });
      }
      
      const verasetResponse = await fetch(
        `https://platform.prd.veraset.tech/v1/job/${params.id}`,
        {
          cache: 'no-store',
          headers: {
            'X-API-Key': apiKey,
            'Content-Type': 'application/json',
          },
        }
      );
      
      if (verasetResponse.ok) {
        const verasetData = await verasetResponse.json();
        const newStatus = verasetData.status || verasetData.data?.status;
        
        console.log(`📊 Veraset API response:`, {
          currentStatus: job.status,
          newStatus: newStatus,
          fullResponse: verasetData
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
        console.error(`❌ Veraset API error: ${verasetResponse.status}`, errorText);
        return NextResponse.json({
          success: false,
          error: `Veraset API returned ${verasetResponse.status}`,
          details: errorText,
          job: job,
        }, { status: verasetResponse.status });
      }
    } catch (verasetError: any) {
      console.error(`❌ Error checking Veraset status:`, verasetError);
      return NextResponse.json({
        success: false,
        error: 'Failed to check Veraset status',
        details: verasetError.message,
        job: job,
      }, { status: 500 });
    }
    
  } catch (error: any) {
    console.error(`POST /api/jobs/[id]/refresh error:`, error);
    return NextResponse.json(
      { error: 'Failed to refresh job status', details: error.message },
      { status: 500 }
    );
  }
}
