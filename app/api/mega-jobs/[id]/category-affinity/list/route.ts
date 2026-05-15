import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';

interface CategoryAffinityListItem {
  slug: string;
  label: string;
  groupKey: string | null;
  categories: string[];
  country: string | null;
  generatedAt: string;
  totalZips: number;
  totalDevicesWithZip: number;
}

/**
 * GET /api/mega-jobs/[id]/category-affinity/list
 *
 * Lists every category-affinity export saved under
 * config/mega-reports/{id}/category-affinity/. Returns lightweight
 * metadata (no byZipCode rows) so the page can render the select-menu
 * options without paying the cost of every report's payload up front.
 *
 * Empty array if nothing has been exported yet.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { id } = await context.params;
  const prefix = `config/mega-reports/${id}/category-affinity/`;
  try {
    const list = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
      MaxKeys: 200,
    }));
    const objects = (list.Contents || []).filter((o) => o.Key?.endsWith('.json'));
    const items: CategoryAffinityListItem[] = [];
    for (const obj of objects) {
      try {
        const got = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: obj.Key! }));
        const text = await got.Body!.transformToString('utf-8');
        const parsed = JSON.parse(text);
        items.push({
          slug: parsed.slug || obj.Key!.split('/').pop()!.replace(/\.json$/, ''),
          label: parsed.label || parsed.slug || 'Untitled',
          groupKey: parsed.groupKey ?? null,
          categories: Array.isArray(parsed.categories) ? parsed.categories : [],
          country: parsed.country ?? null,
          generatedAt: parsed.generatedAt || (obj.LastModified?.toISOString() ?? ''),
          totalZips: typeof parsed.totalZips === 'number' ? parsed.totalZips : 0,
          totalDevicesWithZip: typeof parsed.totalDevicesWithZip === 'number' ? parsed.totalDevicesWithZip : 0,
        });
      } catch {
        // Tolerate one bad file — keep listing the rest.
      }
    }
    items.sort((a, b) => (b.generatedAt || '').localeCompare(a.generatedAt || ''));
    return NextResponse.json({ items });
  } catch (e: any) {
    console.error('[MEGA-CATEGORY-AFFINITY-LIST]', e.message);
    return NextResponse.json({ error: e.message }, { status: 500 });
  }
}
