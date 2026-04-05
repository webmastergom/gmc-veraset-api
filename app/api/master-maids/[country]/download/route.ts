import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getCountryContributions } from '@/lib/master-maids';
import { GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

/**
 * GET /api/master-maids/[country]/download
 *
 * Download the consolidated Parquet file(s) as a stream.
 * The consolidated data is written by Athena CTAS to master-maids/master_{cc}_{ts}/.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ country: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { country } = await context.params;
  const cc = country.toUpperCase();

  try {
    const entry = await getCountryContributions(cc);
    if (!entry?.lastConsolidatedAt) {
      return NextResponse.json({ error: 'No consolidated data. Run consolidation first.' }, { status: 404 });
    }

    // Find the latest consolidated Parquet files
    const prefix = `master-maids/master_${cc.toLowerCase()}_`;
    const listResp = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
      MaxKeys: 100,
    }));

    const files = (listResp.Contents || [])
      .filter(f => f.Key && f.Size && f.Size > 0)
      .sort((a, b) => (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0));

    if (files.length === 0) {
      return NextResponse.json({ error: 'Consolidated files not found in S3' }, { status: 404 });
    }

    // Stream the first (or largest) Parquet file
    const targetKey = files[0].Key!;
    const obj = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: targetKey,
    }));

    const stream = obj.Body as ReadableStream;
    const fileName = `master-maids-${cc}-${new Date().toISOString().slice(0, 10)}.parquet`;

    return new Response(stream as any, {
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${fileName}"`,
        'Content-Length': String(obj.ContentLength || ''),
      },
    });
  } catch (error: any) {
    console.error(`[MASTER-MAIDS] Download error for ${cc}:`, error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
