import { NextRequest, NextResponse } from 'next/server';
import { analyzeMultiPhase, resetAnalysisState } from '@/lib/dataset-analysis-multiphase';
import { isAuthenticated } from '@/lib/auth';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/datasets/[name]/analyze/poll
 *
 * Multi-phase analysis. Call repeatedly (every 2-3s) until status === 'completed'.
 * Each call completes within 60s.
 *
 * Query params:
 *   ?reset=true — clear previous state and re-run
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
    await resetAnalysisState(datasetName);
  }

  try {
    const state = await analyzeMultiPhase(datasetName);
    return NextResponse.json(state);
  } catch (error: any) {
    console.error(`[ANALYZE-POLL] ${datasetName} error:`, error.message);
    return NextResponse.json(
      {
        status: 'error',
        progress: { step: 'error', percent: 0, message: error.message || 'Analysis failed' },
        error: error.message,
      },
      { status: 500 }
    );
  }
}
