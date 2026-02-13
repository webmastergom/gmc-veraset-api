import { NextRequest, NextResponse } from "next/server";
import { PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { s3Client, BUCKET } from "@/lib/s3-config";

export const dynamic = "force-dynamic";

/**
 * POST /api/pois/upload-url
 * Generate a presigned S3 URL for direct GeoJSON upload.
 * This bypasses the Vercel 4.5MB body limit for serverless functions.
 *
 * Body: { collectionId: string }
 * Returns: { uploadUrl: string, key: string }
 */
export async function POST(request: NextRequest) {
  try {
    const { collectionId } = await request.json();

    if (!collectionId || typeof collectionId !== "string") {
      return NextResponse.json(
        { error: "collectionId is required" },
        { status: 400 }
      );
    }

    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      return NextResponse.json(
        { error: "AWS credentials not configured" },
        { status: 500 }
      );
    }

    const key = `pois/${collectionId}.geojson`;

    const command = new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      ContentType: "application/json",
    });

    // URL valid for 15 minutes
    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 900 });

    return NextResponse.json({ uploadUrl, key });
  } catch (error: any) {
    console.error("Error generating upload URL:", error);
    return NextResponse.json(
      { error: error.message || "Failed to generate upload URL" },
      { status: 500 }
    );
  }
}
