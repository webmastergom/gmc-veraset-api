import { NextRequest, NextResponse } from 'next/server';
import { analyzeLaboratoryMultiPhase, resetLabState } from '@/lib/laboratory-multiphase';
import { isAuthenticated } from '@/lib/auth';
import type { LabConfig } from '@/lib/laboratory-types';
import { MIN_VISITS_DEFAULT, SPATIAL_JOIN_RADIUS_DEFAULT } from '@/lib/laboratory-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60;

/**
 * POST /api/laboratory/analyze/poll
 *
 * Multi-phase laboratory analysis endpoint. Call repeatedly (every 2.5s)
 * to advance the analysis state machine. Each call completes within ~50s.
 *
 * Body: LabConfig (same as the SSE stream endpoint)
 * Query params:
 *   ?reset=true — clear previous state and start fresh
 */
export async function POST(request: NextRequest) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  let config: LabConfig;
  try {
    config = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  if (!config.datasetId) {
    return NextResponse.json({ error: 'datasetId is required' }, { status: 400 });
  }
  if (!config.recipe?.steps?.length) {
    return NextResponse.json({ error: 'recipe with at least one step is required' }, { status: 400 });
  }

  // Defaults
  config.minVisitsPerZipcode = config.minVisitsPerZipcode || MIN_VISITS_DEFAULT;
  config.spatialJoinRadiusMeters = config.spatialJoinRadiusMeters || SPATIAL_JOIN_RADIUS_DEFAULT;

  // Allow resetting stuck/errored analyses
  if (request.nextUrl.searchParams.get('reset') === 'true') {
    await resetLabState(config.datasetId);
  }

  try {
    const state = await analyzeLaboratoryMultiPhase(config);
    return NextResponse.json(state);
  } catch (error: any) {
    console.error(`POST /api/laboratory/analyze/poll error:`, error);
    return NextResponse.json(
      {
        status: 'error',
        progress: { step: 'error', percent: 0, message: error.message || 'Analysis failed' },
        error: error.message,
      },
      { status: 500 },
    );
  }
}
