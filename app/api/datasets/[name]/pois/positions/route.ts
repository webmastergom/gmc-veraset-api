import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getPOIPositionsForDataset } from '@/lib/poi-storage';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/datasets/[name]/pois/positions
 * Returns POI positions (lat, lng) for the dataset's analysis POIs.
 * Used for map overlay.
 */
export async function GET(
  req: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await context.params;
  const datasetName = params.name;

  try {
    const positions = await getPOIPositionsForDataset(datasetName);
    return NextResponse.json({ positions });
  } catch (error: any) {
    console.error(`[POIS POSITIONS] GET /api/datasets/${datasetName}/pois/positions:`, error);
    return NextResponse.json(
      { error: 'Failed to fetch POI positions', details: error.message },
      { status: 500 }
    );
  }
}
