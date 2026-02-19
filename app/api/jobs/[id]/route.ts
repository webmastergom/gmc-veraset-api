import { NextRequest, NextResponse } from "next/server";
import { getJob, updateJob, updateJobStatus } from "@/lib/jobs";

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

    // Veraset status check is now opt-in via ?refresh=true query param.
    // This prevents the 1-5s blocking call from slowing down every page load.
    // The client-side JobStatusPolling component handles automatic polling.
    const shouldRefresh = request.nextUrl.searchParams.get('refresh') === 'true';

    if (shouldRefresh && (job.status === 'QUEUED' || job.status === 'RUNNING' || job.status === 'SCHEDULED')) {
      try {
        const apiKey = process.env.VERASET_API_KEY?.trim();

        if (apiKey) {
          console.log(`🔄 Checking Veraset status for job ${params.id} (current: ${job.status})`);
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

            if (newStatus && newStatus !== job.status) {
              console.log(`✅ Status changed: ${job.status} -> ${newStatus}`);
              const updatedJob = await updateJobStatus(
                params.id,
                newStatus as any,
                verasetData.error_message || verasetData.data?.error_message
              );
              return NextResponse.json(updatedJob);
            }
          } else {
            const errorText = await verasetResponse.text();
            console.warn(`⚠️ Veraset API returned ${verasetResponse.status} for ${params.id}:`, errorText);
          }
        }
      } catch (verasetError: any) {
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

export async function PATCH(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const body = await request.json();

    // Only allow safe fields to be updated via PATCH
    const allowedFields: Record<string, boolean> = {
      audienceAgentEnabled: true,
      country: true,
    };

    const updates: Record<string, any> = {};
    for (const [key, value] of Object.entries(body)) {
      if (allowedFields[key]) {
        updates[key] = value;
      }
    }

    if (Object.keys(updates).length === 0) {
      return NextResponse.json(
        { error: 'No valid fields to update' },
        { status: 400 },
      );
    }

    const updated = await updateJob(params.id, updates);
    if (!updated) {
      return NextResponse.json(
        { error: 'Job not found' },
        { status: 404 },
      );
    }

    return NextResponse.json(updated);
  } catch (error: any) {
    console.error('PATCH /api/jobs/[id] error:', error);
    return NextResponse.json(
      { error: 'Failed to update job', details: error.message },
      { status: 500 },
    );
  }
}
