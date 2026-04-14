import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

export const dynamic = 'force-dynamic';

/**
 * GET /api/compare/download?queryId=xxx
 * Redirect to a presigned S3 URL for the Athena overlap CSV.
 * No memory buffering — works for any file size.
 */
export async function GET(request: NextRequest) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const queryId = request.nextUrl.searchParams.get('queryId');
  if (!queryId || !/^[a-f0-9-]+$/.test(queryId)) {
    return NextResponse.json({ error: 'Invalid queryId' }, { status: 400 });
  }

  try {
    const key = `athena-results/${queryId}.csv`;

    // Verify the file exists
    await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));

    // Generate presigned URL (valid for 1 hour)
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
      ResponseContentDisposition: `attachment; filename="compare-overlap-${queryId.slice(0, 8)}.csv"`,
      ResponseContentType: 'text/csv',
    });
    const presignedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });

    return NextResponse.redirect(presignedUrl);
  } catch (error: any) {
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      return NextResponse.json({ error: 'Overlap CSV not found' }, { status: 404 });
    }
    console.error('[COMPARE-DOWNLOAD]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
