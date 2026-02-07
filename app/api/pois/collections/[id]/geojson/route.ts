import { NextRequest, NextResponse } from "next/server"
import { getPOICollection } from "@/lib/s3-config"

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    console.log(`[GeoJSON] Fetching collection: ${params.id}`);
    console.log(`[GeoJSON] AWS configured: ${!!process.env.AWS_ACCESS_KEY_ID}`);
    const geojson = await getPOICollection(params.id)

    if (!geojson) {
      console.error(`[GeoJSON] Collection ${params.id} returned null`);
      return NextResponse.json(
        { error: "POI collection not found", id: params.id },
        { status: 404 }
      )
    }

    // Add diagnostic info about POI count
    const totalFeatures = geojson.features?.length || 0
    const validPointFeatures = (geojson.features || []).filter((f: any) => {
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
      )
    }).length
    
    const invalidFeatures = totalFeatures - validPointFeatures
    
    if (invalidFeatures > 0) {
      console.warn(`⚠️ Collection ${params.id}: ${invalidFeatures} invalid features out of ${totalFeatures} total`)
    }
    
    console.log(`📊 Collection ${params.id}: ${validPointFeatures} valid Point features`)

    return NextResponse.json(geojson)
  } catch (error) {
    console.error("Error fetching POI collection GeoJSON:", error)
    return NextResponse.json(
      { error: "Failed to fetch POI collection" },
      { status: 500 }
    )
  }
}
