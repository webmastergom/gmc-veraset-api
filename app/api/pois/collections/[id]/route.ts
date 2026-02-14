import { NextRequest, NextResponse } from "next/server"
import { getConfig, initConfigIfNeeded } from "@/lib/s3-config"
import { initialPOICollectionsData } from "@/lib/seed-jobs"
import { putConfig, s3Client, BUCKET } from "@/lib/s3-config"
import { DeleteObjectCommand } from "@aws-sdk/client-s3"

export const dynamic = "force-dynamic"
export const revalidate = 0

export async function DELETE(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id: collectionId } = await params

    const rawCollections =
      (await initConfigIfNeeded("poi-collections", initialPOICollectionsData)) ||
      {}
    const collectionsData = rawCollections as Record<string, any>

    const collection = collectionsData[collectionId]
    if (!collection) {
      return NextResponse.json(
        { error: "Collection not found" },
        { status: 404 }
      )
    }

    // Delete GeoJSON file from S3
    const geojsonKey = collection.geojsonPath || `pois/${collectionId}.geojson`
    try {
      await s3Client.send(
        new DeleteObjectCommand({ Bucket: BUCKET, Key: geojsonKey })
      )
      console.log(`🗑️ Deleted S3 object: ${geojsonKey}`)
    } catch (s3Err: any) {
      // Log but don't fail — the config entry is more important to remove
      console.warn(`⚠️ Could not delete S3 object ${geojsonKey}:`, s3Err.message)
    }

    // Remove from config
    delete collectionsData[collectionId]
    await putConfig("poi-collections", collectionsData)

    console.log(`✅ Deleted POI collection: ${collectionId}`)
    return NextResponse.json({ success: true, id: collectionId })
  } catch (error: any) {
    console.error("Error deleting POI collection:", error)
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to delete collection",
        details: error.message,
      },
      { status: 500 }
    )
  }
}

export async function PATCH(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id: collectionId } = await params
    const body = await request.json()

    const rawCollections =
      (await initConfigIfNeeded("poi-collections", initialPOICollectionsData)) ||
      {}
    const collectionsData = rawCollections as Record<string, {
      id: string
      name: string
      description?: string
      poiCount: number
      geojsonPath: string
      createdAt: string
      sources?: Record<string, number>
    }>

    let collection = collectionsData[collectionId]

    // Create minimal entry if collection doesn't exist (e.g. GeoJSON exists but config was never created)
    if (!collection) {
      collection = {
        id: collectionId,
        name: typeof body.name === "string" && body.name.trim()
          ? body.name.trim()
          : collectionId,
        description: typeof body.description === "string" ? body.description.trim() : "",
        poiCount: 0,
        geojsonPath: `pois/${collectionId}.geojson`,
        createdAt: new Date().toISOString(),
      }
      collectionsData[collectionId] = collection
    } else {
      // Update only allowed fields
      if (typeof body.name === "string" && body.name.trim()) {
        collection.name = body.name.trim()
      }
      if (body.description !== undefined) {
        collection.description = String(body.description || "").trim()
      }
    }

    await putConfig("poi-collections", collectionsData)

    return NextResponse.json(collection)
  } catch (error: any) {
    console.error("Error updating POI collection:", error)
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Failed to update collection",
        details: error.message,
      },
      { status: 500 }
    )
  }
}
