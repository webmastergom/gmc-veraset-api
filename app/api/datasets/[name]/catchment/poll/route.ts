import { NextRequest, NextResponse } from 'next/server';
import { catchmentMultiPhase, resetCatchmentState } from '@/lib/catchment-multiphase';
import { isAuthenticated } from '@/lib/auth';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60;

/**
 * POST /api/datasets/[name]/catchment/poll
 *
 * Multi-phase catchment analysis. Call repeatedly (every 2-3s) until status === 'completed'.
 * Each call completes within 60s.
 *
 * Query params:
 *   ?reset=true  — clear previous state and re-run
 *   ?minPings=N  — minimum pings per device-day
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await context.params;
  const datasetName = params.name;

  if (request.nextUrl.searchParams.get('reset') === 'true') {
    await resetCatchmentState(datasetName);
  }

  // Parse optional filters
  const minPingsStr = request.nextUrl.searchParams.get('minPings');
  const minPings = minPingsStr ? parseInt(minPingsStr, 10) : undefined;
  const filters = minPings && minPings > 1 ? { minPings } : undefined;

  try {
    const state = await catchmentMultiPhase(datasetName, filters);
    return NextResponse.json(state);
  } catch (error: any) {
    console.error(`[CATCHMENT-POLL] ${datasetName} error:`, error.message);
    return NextResponse.json(
      {
        status: 'error',
        progress: { step: 'error', percent: 0, message: error.message || 'Catchment analysis failed' },
        error: error.message,
      },
      { status: 500 }
    );
  }
}
