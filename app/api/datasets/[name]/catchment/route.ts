import { NextRequest, NextResponse } from 'next/server';
import { analyzeResidentialZipcodes } from '@/lib/dataset-analyzer-residential';
import type { ResidentialFilters } from '@/lib/catchment-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/datasets/[name]/catchment
 * Reporte: catchment por código postal (origen residencial de visitantes).
 * Query params: dateFrom, dateTo, minNightPings, minDistinctNights, poiIds (comma-separated)
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> | { name: string } }
): Promise<NextResponse> {
  try {
    const params = await (typeof context.params === 'object' && context.params instanceof Promise
      ? context.params
      : Promise.resolve(context.params as { name: string }));
    const datasetName = params.name;

    if (!datasetName) {
      return NextResponse.json(
        { error: 'Dataset name is required' },
        { status: 400 }
      );
    }

    const { searchParams } = new URL(request.url);
    const filters: ResidentialFilters = {};
    const dateFrom = searchParams.get('dateFrom');
    const dateTo = searchParams.get('dateTo');
    if (dateFrom) filters.dateFrom = dateFrom;
    if (dateTo) filters.dateTo = dateTo;
    const minNightPings = searchParams.get('minNightPings');
    if (minNightPings) filters.minNightPings = parseInt(minNightPings, 10);
    const minDistinctNights = searchParams.get('minDistinctNights');
    if (minDistinctNights) filters.minDistinctNights = parseInt(minDistinctNights, 10);
    const poiIds = searchParams.get('poiIds');
    if (poiIds) filters.poiIds = poiIds.split(',').map((s) => s.trim()).filter(Boolean);

    const result = await analyzeResidentialZipcodes(datasetName, filters);
    return NextResponse.json(result, {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error: any) {
    console.error('[CATCHMENT] Error:', error?.message);

    if (
      error?.message?.includes('Access denied') ||
      error?.message?.includes('not authorized')
    ) {
      return NextResponse.json(
        { error: 'Athena access denied', details: error.message },
        { status: 403 }
      );
    }
    if (error?.message?.includes('AWS credentials not configured')) {
      return NextResponse.json(
        { error: 'AWS not configured', details: error.message },
        { status: 503 }
      );
    }
    return NextResponse.json(
      { error: 'Catchment failed', details: error?.message },
      { status: 500 }
    );
  }
}
