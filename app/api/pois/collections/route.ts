import { NextRequest, NextResponse } from "next/server";
import { getConfig, initConfigIfNeeded } from "@/lib/s3-config";
import { initialPOICollectionsData } from "@/lib/seed-jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET(request: NextRequest) {
  try {
    const collectionsData = await initConfigIfNeeded('poi-collections', initialPOICollectionsData);
    
    // Convert object to array and sort by createdAt
    const collections = Object.values(collectionsData).sort((a: any, b: any) => {
      const dateA = new Date(a.createdAt || 0).getTime();
      const dateB = new Date(b.createdAt || 0).getTime();
      return dateB - dateA;
    });

    return NextResponse.json(collections);
  } catch (error: any) {
    console.error("Error fetching POI collections:", error);
    
    // Fallback to seed data
    try {
      const collections = Object.values(initialPOICollectionsData);
      return NextResponse.json(collections);
    } catch (seedError) {
      return NextResponse.json(
        { error: "Failed to fetch POI collections", details: error.message },
        { status: 500 }
      );
    }
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    
    // Generate ID from name if not provided
    const collectionId = body.id || body.name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
    
    const collectionsData = await getConfig<Record<string, any>>("poi-collections") || {};
    
    // If GeoJSON is provided, validate and count valid POIs
    let actualPoiCount = body.poiCount || body.poi_count || 0;
    let totalFeatures = 0;
    let invalidFeatures = 0;
    
    if (body.geojson) {
      const geojson = body.geojson;
      totalFeatures = geojson.features?.length || 0;
      
      // Count valid Point features (same validation as in job creation)
      const validPoints = (geojson.features || []).filter((f: any) => {
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
      }).length;
      
      invalidFeatures = totalFeatures - validPoints;
      actualPoiCount = validPoints; // Use actual count, not provided count
      
      console.log(`📊 Collection ${collectionId}: ${totalFeatures} total features, ${validPoints} valid POIs, ${invalidFeatures} invalid`);
      
      if (invalidFeatures > 0) {
        console.warn(`⚠️ Collection ${collectionId}: ${invalidFeatures} invalid features will be filtered out`);
      }
      
      // Warn if provided count doesn't match actual count
      if (body.poiCount && body.poiCount !== validPoints) {
        console.warn(`⚠️ Collection ${collectionId}: Provided POI count (${body.poiCount}) doesn't match actual valid count (${validPoints})`);
      }
    }
    
    const collection = {
      id: collectionId,
      name: body.name,
      description: body.description || '',
      poiCount: actualPoiCount, // Use actual validated count
      totalFeatures, // Store total features for reference
      invalidFeatures, // Store invalid count for reference
      sources: body.sources || {},
      geojsonPath: `pois/${collectionId}.geojson`,
      createdAt: body.createdAt || new Date().toISOString(),
    };

    collectionsData[collectionId] = collection;
    
    const { putConfig, putPOICollection } = await import("@/lib/s3-config");
    await putConfig("poi-collections", collectionsData);

    // If GeoJSON is provided, save it to S3
    if (body.geojson) {
      await putPOICollection(collectionId, body.geojson);
      console.log(`✅ Saved collection ${collectionId} with ${actualPoiCount} valid POIs to S3`);
    }

    return NextResponse.json(collection);
  } catch (error: any) {
    console.error("Error creating POI collection:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to create POI collection", details: error.message },
      { status: 500 }
    );
  }
}
