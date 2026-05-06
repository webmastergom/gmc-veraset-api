import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

export const dynamic = 'force-dynamic';

/**
 * GET /api/compare/download
 *   ?queryId=xxx       → athena-results/<queryId>.csv (overlap MAIDs / reach MAIDs)
 *   ?key=compare/...   → arbitrary key under known compare/ prefixes
 *                        (currently `compare/catchment-zips/` for reach catchment)
 *
 * Both paths return a 1-hour presigned S3 URL. The `key` form is restricted
 * by an allow-list of safe prefixes so this endpoint can't be turned into a
 * generic S3 reader.
 */
const ALLOWED_KEY_PREFIXES = ['compare/catchment-zips/'];

export async function GET(request: NextRequest) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const queryId = request.nextUrl.searchParams.get('queryId');
  const rawKey = request.nextUrl.searchParams.get('key');

  let key: string;
  let downloadName: string;

  if (rawKey) {
    if (!ALLOWED_KEY_PREFIXES.some((p) => rawKey.startsWith(p))) {
      return NextResponse.json({ error: 'Invalid key — allowed prefixes only' }, { status: 400 });
    }
    if (rawKey.includes('..') || rawKey.includes('//')) {
      return NextResponse.json({ error: 'Invalid key' }, { status: 400 });
    }
    key = rawKey;
    downloadName = rawKey.split('/').pop() || 'compare.csv';
  } else if (queryId) {
    if (!/^[a-f0-9-]+$/.test(queryId)) {
      return NextResponse.json({ error: 'Invalid queryId' }, { status: 400 });
    }
    key = `athena-results/${queryId}.csv`;
    downloadName = `compare-overlap-${queryId.slice(0, 8)}.csv`;
  } else {
    return NextResponse.json({ error: 'queryId or key required' }, { status: 400 });
  }

  try {
    await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
      ResponseContentDisposition: `attachment; filename="${downloadName}"`,
      ResponseContentType: 'text/csv',
    });
    const presignedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    return NextResponse.redirect(presignedUrl);
  } catch (error: any) {
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      return NextResponse.json({ error: 'CSV not found' }, { status: 404 });
    }
    console.error('[COMPARE-DOWNLOAD]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
