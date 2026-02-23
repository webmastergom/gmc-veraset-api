import { NextRequest, NextResponse } from "next/server";
import { getJob, updateJob } from "@/lib/jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const id = params?.id;
    if (!id) {
      return NextResponse.json({ error: 'Job ID required' }, { status: 400 });
    }

    const job = await getJob(id);

    if (!job) {
      return NextResponse.json(
        { error: 'Job not found' },
        { status: 404 }
      );
    }

    // NOTE: Veraset status checking has been moved to GET /api/jobs/[id]/refresh.
    // Doing it here caused Vercel serverless 500 errors because the Veraset API call
    // has no timeout and can exceed Vercel's 10s function limit, killing the process.
    // The frontend should call /api/jobs/[id]/refresh when status updates are needed.

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
