import { NextRequest, NextResponse } from 'next/server';
import { requestCancellation } from '@/lib/audience-run-status';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * POST /api/laboratory/audiences/stop
 * Body: { datasetId, country }
 *
 * Requests cooperative cancellation of the active batch run.
 * The runner checks this flag between audience iterations and stops gracefully.
 */
export async function POST(request: NextRequest): Promise<Response> {
  let body: any;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const { datasetId, country } = body;

  if (!datasetId || !country) {
    return NextResponse.json(
      { error: 'datasetId and country are required' },
      { status: 400 },
    );
  }

  const cancelled = await requestCancellation(datasetId, country);

  if (!cancelled) {
    return NextResponse.json(
      { error: 'No active run to cancel' },
      { status: 404 },
    );
  }

  return NextResponse.json({ success: true, message: 'Cancellation requested' });
}
