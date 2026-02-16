import { NextRequest, NextResponse } from "next/server";
import { getJob, updateJobStatus } from "@/lib/jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

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
    
    // If job is QUEUED, RUNNING, or SCHEDULED, check Veraset for status update
    if (job.status === 'QUEUED' || job.status === 'RUNNING' || job.status === 'SCHEDULED') {
      try {
        const apiKey = process.env.VERASET_API_KEY?.trim();
        
        if (apiKey) {
          console.log(`🔄 Checking Veraset status for job ${params.id} (current: ${job.status})`);
          const verasetResponse = await fetch(
            `https://platform.prd.veraset.tech/v1/job/${params.id}`,
            {
              cache: 'no-store', // Always fetch fresh status
              headers: {
                'X-API-Key': apiKey,
                'Content-Type': 'application/json',
              },
            }
          );
          
          if (verasetResponse.ok) {
            const verasetData = await verasetResponse.json();
            const newStatus = verasetData.status || verasetData.data?.status;
            
            console.log(`📊 Veraset API response for ${params.id}:`, {
              currentStatus: job.status,
              newStatus: newStatus,
              fullResponse: verasetData
            });
            
            if (newStatus && newStatus !== job.status) {
              console.log(`✅ Status changed: ${job.status} -> ${newStatus}`);
              const updatedJob = await updateJobStatus(
                params.id,
                newStatus as any,
                verasetData.error_message || verasetData.data?.error_message
              );
              return NextResponse.json(updatedJob);
            } else if (newStatus) {
              console.log(`ℹ️ Status unchanged: ${newStatus}`);
            }
          } else {
            const errorText = await verasetResponse.text();
            console.warn(`⚠️ Veraset API returned ${verasetResponse.status} for ${params.id}:`, errorText);
          }
        } else {
          console.warn(`⚠️ VERASET_API_KEY not configured, skipping status check`);
        }
      } catch (verasetError: any) {
        // Continue with stored status if Veraset API fails
        console.warn(`❌ Failed to check Veraset status for ${params.id}:`, verasetError.message || verasetError);
      }
    }
    
    return NextResponse.json(job);
    
  } catch (error: any) {
    console.error(`GET /api/jobs/[id] error:`, error);
    return NextResponse.json(
      { error: 'Failed to fetch job', details: error.message },
      { status: 500 }
    );
  }
}
