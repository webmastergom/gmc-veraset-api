import { NextRequest, NextResponse } from 'next/server';
import { analyzeResidentialZipcodes } from '@/lib/dataset-analyzer-residential';

export const dynamic = 'force-dynamic';
export const maxDuration = 300; // 5 minutes for Athena queries + reverse geocoding

/**
 * POST /api/datasets/[name]/analyze/residential
 * Analyze residential zipcodes of visitors in a dataset.
 */
export async function POST(
  request: NextRequest,
  { params }: { params: { name: string } }
) {
  try {
    const datasetName = params.name;

    if (!datasetName) {
      return NextResponse.json(
        { error: 'Dataset name is required' },
        { status: 400 }
      );
    }

    const body = await request.json().catch(() => ({}));
    const filters = body.filters || {};

    console.log(`Starting residential analysis for dataset: ${datasetName}`, filters);

    const result = await analyzeResidentialZipcodes(datasetName, {
      dateFrom: filters.dateFrom,
      dateTo: filters.dateTo,
      poiIds: filters.poiIds,
      minNightPings: filters.minNightPings,
      minDistinctNights: filters.minDistinctNights,
    });

    return NextResponse.json(result);
  } catch (error: any) {
    console.error('Residential analysis error:', error);

    if (error.message?.includes('Access denied') || error.message?.includes('not authorized')) {
      return NextResponse.json(
        {
          error: 'Athena access denied',
          details: error.message,
          hint: 'Ensure your IAM user has Athena and Glue permissions. See ATHENA_SETUP.md.',
        },
        { status: 403 }
      );
    }

    return NextResponse.json(
      {
        error: 'Residential analysis failed',
        details: error.message,
      },
      { status: 500 }
    );
  }
}
