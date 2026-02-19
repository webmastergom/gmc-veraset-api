import { NextRequest, NextResponse } from 'next/server';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { s3Client, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/laboratory/audiences/download
 *
 * Returns a presigned URL for downloading audience segment CSV or result JSON.
 * Query params: ?key=audiences/...segment.csv
 */
export async function GET(request: NextRequest): Promise<Response> {
  const key = request.nextUrl.searchParams.get('key');

  if (!key) {
    return NextResponse.json({ error: 'key is required' }, { status: 400 });
  }

  // Security: only allow downloads from the audiences/ prefix
  if (!key.startsWith('audiences/')) {
    return NextResponse.json({ error: 'Invalid key' }, { status: 400 });
  }

  try {
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });

    const downloadUrl = await getSignedUrl(s3Client, command, { expiresIn: 900 });

    return NextResponse.json({ downloadUrl });
  } catch (error: any) {
    console.error('Error generating download URL:', error);
    return NextResponse.json(
      { error: error.message || 'Failed to generate download URL' },
      { status: 500 },
    );
  }
}
