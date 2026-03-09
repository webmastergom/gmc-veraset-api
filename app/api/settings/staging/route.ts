import { NextRequest, NextResponse } from 'next/server';
import { ListObjectsV2Command } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { isAuthenticated } from '@/lib/auth';

export const dynamic = 'force-dynamic';

const PREFIX = 'staging/';

export async function GET(req: NextRequest) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    const res = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: PREFIX,
      MaxKeys: 500,
    }));

    const objects = (res.Contents || [])
      .filter(o => o.Key && o.Key !== PREFIX) // skip the folder itself
      .map(o => ({
        key: o.Key!,
        name: o.Key!.slice(PREFIX.length),
        size: o.Size || 0,
        lastModified: o.LastModified?.toISOString() || '',
      }));

    // Group by handle: {handle}.csv + {handle}.csv.spec.yml + {handle}._error_.txt etc.
    const handles = new Map<string, {
      handle: string;
      csv: { size: number; lastModified: string } | null;
      spec: boolean;
      error: string | null;
      importing: boolean;
    }>();

    for (const obj of objects) {
      let handle: string | null = null;

      if (obj.name.endsWith('.csv.spec.yml')) {
        handle = obj.name.replace('.csv.spec.yml', '');
      } else if (obj.name.endsWith('.csv')) {
        handle = obj.name.replace('.csv', '');
      } else if (obj.name.endsWith('._error_.txt')) {
        handle = obj.name.replace('._error_.txt', '');
      } else if (obj.name.endsWith('._importing_')) {
        handle = obj.name.replace('._importing_', '');
      }

      if (!handle) continue;

      if (!handles.has(handle)) {
        handles.set(handle, { handle, csv: null, spec: false, error: null, importing: false });
      }

      const entry = handles.get(handle)!;

      if (obj.name.endsWith('.csv') && !obj.name.endsWith('.spec.yml')) {
        entry.csv = { size: obj.size, lastModified: obj.lastModified };
      } else if (obj.name.endsWith('.csv.spec.yml')) {
        entry.spec = true;
      } else if (obj.name.endsWith('._error_.txt')) {
        entry.error = obj.name;
      } else if (obj.name.endsWith('._importing_')) {
        entry.importing = true;
      }
    }

    const listings = Array.from(handles.values()).sort((a, b) => a.handle.localeCompare(b.handle));

    return NextResponse.json({ listings, raw: objects });
  } catch (error: any) {
    console.error('GET /api/settings/staging error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
