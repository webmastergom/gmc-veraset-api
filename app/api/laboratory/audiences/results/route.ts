import { NextRequest, NextResponse } from 'next/server';
import { loadAudienceResults } from '@/lib/audience-runner';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/laboratory/audiences/results
 *
 * Fetches all latest audience results for a dataset+country.
 * Query params: ?datasetId=X&country=Y
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

  const results = await loadAudienceResults(datasetId, country);

  return NextResponse.json({ results });
}
