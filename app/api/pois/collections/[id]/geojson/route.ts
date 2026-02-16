import { NextRequest, NextResponse } from 'next/server';
import { getPOICollection, putPOICollection } from '@/lib/s3-config';
import { getConfig, putConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/pois/collections/[id]/geojson
 * Get GeoJSON for a POI collection
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  let collectionId: string | undefined;

  try {
    const params = await context.params;
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

/**
 * PUT /api/pois/collections/[id]/geojson
 * Update GeoJSON for a POI collection
 */
export async function PUT(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  let collectionId: string | undefined;

  try {
    const params = await context.params;
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

    const body = await request.json();

    // Validate GeoJSON structure
    if (!body.type || body.type !== 'FeatureCollection') {
      return NextResponse.json(
        { 
          error: 'Invalid GeoJSON: must be a FeatureCollection',
        },
        { status: 400 }
      );
    }

    if (!Array.isArray(body.features)) {
      return NextResponse.json(
        { 
          error: 'Invalid GeoJSON: features must be an array',
        },
        { status: 400 }
      );
    }

    // Validate features
    const validFeatures = body.features.filter((f: any) => {
      return (
        f.geometry &&
        f.geometry.type === 'Point' &&
        Array.isArray(f.geometry.coordinates) &&
        f.geometry.coordinates.length >= 2 &&
        typeof f.geometry.coordinates[0] === 'number' &&
        typeof f.geometry.coordinates[1] === 'number' &&
        !isNaN(f.geometry.coordinates[0]) &&
        !isNaN(f.geometry.coordinates[1]) &&
        f.geometry.coordinates[0] >= -180 &&
        f.geometry.coordinates[0] <= 180 &&
        f.geometry.coordinates[1] >= -90 &&
        f.geometry.coordinates[1] <= 90
      );
    });

    const invalidCount = body.features.length - validFeatures.length;
    
    if (invalidCount > 0) {
      console.warn(`[POI-GEOJSON] ${invalidCount} invalid features filtered out`);
    }

    // Update GeoJSON with only valid features
    const updatedGeoJSON = {
      ...body,
      features: validFeatures,
    };

    console.log(`[POI-GEOJSON] Updating collection: ${collectionId}, ${validFeatures.length} valid POIs`);

    // Save to S3
    await putPOICollection(collectionId, updatedGeoJSON);

    // Update collection metadata
    const collections = await getConfig<Record<string, any>>('poi-collections') || {};
    if (collections[collectionId]) {
      collections[collectionId].poiCount = validFeatures.length;
      collections[collectionId].updatedAt = new Date().toISOString();
      await putConfig('poi-collections', collections);
    }

    console.log(`[POI-GEOJSON] Successfully updated collection: ${collectionId}`);

    return NextResponse.json({
      success: true,
      collectionId,
      poiCount: validFeatures.length,
      invalidFeaturesFiltered: invalidCount,
    }, {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    });

  } catch (error: any) {
    const errorCollectionId = collectionId || 'unknown';
    console.error(`[POI-GEOJSON ERROR] PUT /api/pois/collections/${errorCollectionId}/geojson:`, {
      error: error.message,
      stack: error.stack,
      name: error.name,
    });

    return NextResponse.json(
      { 
        error: 'Failed to update POI collection',
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
