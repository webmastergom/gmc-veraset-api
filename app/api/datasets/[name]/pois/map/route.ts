import { NextRequest, NextResponse } from 'next/server';
import { mapPoiIds, getPoiNames } from '@/lib/poi-mapping';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/datasets/[name]/pois/map?jobId=[jobId]
 * Map Veraset POI IDs to original GeoJSON IDs for a dataset
 */
export async function GET(
  req: NextRequest,
  { params }: { params: { name: string } }
) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = searchParams.get('jobId');
    
    if (!jobId) {
      return NextResponse.json(
        { error: 'jobId query parameter is required' },
        { status: 400 }
      );
    }

    // Get POIs from the dataset
    const poisRes = await fetch(`${req.nextUrl.origin}/api/datasets/${params.name}/pois`);
    if (!poisRes.ok) {
      return NextResponse.json(
        { error: 'Failed to fetch POIs' },
        { status: poisRes.status }
      );
    }

    const poisData = await poisRes.json();
    const verasetPoiIds = (poisData.pois || []).map((p: any) => p.poiId);

    // Map Veraset IDs to original GeoJSON IDs
    const mapping = await mapPoiIds(jobId, verasetPoiIds);
    
    // Get POI names for display
    const names = await getPoiNames(jobId);

    return NextResponse.json({
      mapping,
      names: names || {},
      total: Object.keys(mapping).length,
    });

  } catch (error: any) {
    console.error(`GET /api/datasets/${params.name}/pois/map error:`, error);
    return NextResponse.json(
      { error: 'Failed to map POI IDs', details: error.message },
      { status: 500 }
    );
  }
}
