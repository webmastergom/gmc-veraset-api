import { NextRequest, NextResponse } from 'next/server';
import { getRunStatus, saveRunStatus } from '@/lib/audience-run-status';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/laboratory/audiences/status?datasetId=X&country=Y
 *
 * Returns the current batch run status for a dataset+country.
 * The dashboard polls this every 4 seconds while a run is active.
 */
export async function GET(request: NextRequest): Promise<Response> {
  const datasetId = request.nextUrl.searchParams.get('datasetId');
  const country = request.nextUrl.searchParams.get('country');

  if (!datasetId || !country) {
    return NextResponse.json(
      { error: 'datasetId and country are required' },
      { status: 400 },
    );
  }

  const status = await getRunStatus(datasetId, country);

  if (!status) {
    return NextResponse.json({ active: false });
  }

  // Safety: if the run is "running" but started > 6 min ago, it likely timed out
  // (Vercel maxDuration = 300s). Mark as failed so the client stops polling.
  if (status.status === 'running') {
    const elapsedMs = Date.now() - new Date(status.startedAt).getTime();
    if (elapsedMs > 6 * 60 * 1000) {
      status.status = 'failed';
      status.error = 'Run timed out (exceeded maximum execution time)';
      status.completedAt = new Date().toISOString();
      await saveRunStatus(status);
    }
  }

  return NextResponse.json({
    active: status.status === 'running',
    ...status,
  });
}
