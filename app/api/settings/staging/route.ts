import { NextRequest, NextResponse } from 'next/server';
import { ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
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

/**
 * DELETE /api/settings/staging?handle=Spain
 * Deletes all files associated with a staging handle:
 *   {handle}.csv, {handle}.csv.spec.yml, {handle}._error_.txt, {handle}._importing_
 */
export async function DELETE(req: NextRequest) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const handle = req.nextUrl.searchParams.get('handle');
  if (!handle) {
    return NextResponse.json({ error: 'Missing handle parameter' }, { status: 400 });
  }

  try {
    const suffixes = ['.csv', '.csv.spec.yml', '._error_.txt', '._importing_'];
    const deleted: string[] = [];
    const errors: string[] = [];

    for (const suffix of suffixes) {
      const key = `${PREFIX}${handle}${suffix}`;
      try {
        await s3Client.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: key }));
        deleted.push(key);
      } catch (e: any) {
        // Only ignore NoSuchKey — propagate real errors
        if (e.name === 'NoSuchKey' || e.Code === 'NoSuchKey') {
          continue;
        }
        errors.push(`${key}: ${e.name || e.Code || e.message}`);
      }
    }

    if (errors.length > 0) {
      console.error(`[STAGING] Delete errors for "${handle}":`, errors);
      return NextResponse.json({ error: `Delete failed: ${errors.join('; ')}` }, { status: 500 });
    }

    console.log(`[STAGING] Deleted handle "${handle}" (${deleted.length} files)`);
    return NextResponse.json({ success: true, handle, deletedKeys: deleted });
  } catch (error: any) {
    console.error('DELETE /api/settings/staging error:', error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
