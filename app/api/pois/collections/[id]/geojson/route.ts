import { NextRequest, NextResponse } from 'next/server';
import { getPOICollection } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/pois/collections/[id]/geojson
 * Get GeoJSON for a POI collection
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> | { id: string } }
): Promise<NextResponse> {
  let collectionId: string | undefined;
  
  try {
    // Handle params - Next.js 14+ may pass params as Promise
    let params: { id: string };
    if (context.params instanceof Promise) {
      params = await context.params;
    } else {
      params = context.params;
    }
    collectionId = params.id;

    if (!collectionId || typeof collectionId !== 'string') {
      return NextResponse.json(
        { 
          error: 'Collection ID is required',
          received: collectionId 
        },
        { status: 400 }
      );
    }

    console.log(`[POI-GEOJSON] Fetching collection: ${collectionId}`);

    // Get GeoJSON from S3 or local fallback
    const geojson = await getPOICollection(collectionId);

    if (!geojson) {
      console.error(`[POI-GEOJSON] Collection ${collectionId} not found`);
      return NextResponse.json(
        { 
          error: 'POI collection not found',
          id: collectionId 
        },
        { status: 404 }
      );
    }

    console.log(`[POI-GEOJSON] Success: ${collectionId}, ${JSON.stringify(geojson).length} bytes`);

    return NextResponse.json(geojson, {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-store',
      },
    });

  } catch (error: any) {
    const errorCollectionId = collectionId || 'unknown';
    console.error(`[POI-GEOJSON ERROR] GET /api/pois/collections/${errorCollectionId}/geojson:`, {
      error: error.message,
      stack: error.stack,
      name: error.name,
    });

    return NextResponse.json(
      { 
        error: 'Failed to fetch POI collection',
        details: error.message,
        id: errorCollectionId,
      },
      { 
        status: 500,
        headers: {
          'Content-Type': 'application/json',
        }
      }
    );
  }
}
