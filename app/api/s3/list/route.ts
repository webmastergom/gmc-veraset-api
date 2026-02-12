import { NextResponse } from 'next/server';
import { ListObjectsV2Command } from '@aws-sdk/client-s3';
import { BUCKET, s3Client } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/s3/list
 * Lista directa del bucket S3 de Garritz: prefijos (carpetas) de primer nivel
 * con número de objetos y tamaño total por prefijo.
 */
export async function GET() {
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      return NextResponse.json(
        { error: 'AWS credentials not configured' },
        { status: 503 }
      );
    }

    const listRes = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Delimiter: '/',
      MaxKeys: 1000,
    }));

    const prefixes = (listRes.CommonPrefixes || []).map(p => p.Prefix?.replace(/\/$/, '') || '').filter(Boolean);
    const systemFolders = ['config', 'exports', 'pois', 'athena-results'];

    const items: Array<{
      prefix: string;
      objectCount: number;
      totalBytes: number;
      isSystem: boolean;
    }> = [];

    for (const prefix of prefixes) {
      const isSystem = systemFolders.includes(prefix);
      let objectCount = 0;
      let totalBytes = 0;
      let continuationToken: string | undefined;

      do {
        const res = await s3Client.send(new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: `${prefix}/`,
          MaxKeys: 1000,
          ContinuationToken: continuationToken,
        }));

        const contents = res.Contents || [];
        objectCount += contents.filter(o => o.Key && !o.Key.endsWith('/')).length;
        totalBytes += contents.reduce((sum, o) => sum + (Number(o.Size) || 0), 0);
        continuationToken = res.NextContinuationToken;
      } while (continuationToken);

      items.push({
        prefix,
        objectCount,
        totalBytes,
        isSystem,
      });
    }

    items.sort((a, b) => a.prefix.localeCompare(b.prefix));

    return NextResponse.json({
      bucket: BUCKET,
      listedAt: new Date().toISOString(),
      totalPrefixes: items.length,
      items,
    });
  } catch (error: any) {
    console.error('GET /api/s3/list error:', error);
    return NextResponse.json(
      { error: 'Failed to list S3 bucket', details: error.message },
      { status: 500 }
    );
  }
}
