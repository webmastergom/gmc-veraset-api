import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { buildCategoryAffinityLabel } from '@/lib/affinity-builder';
import { getAllMegaJobs } from '@/lib/mega-jobs';

export const dynamic = 'force-dynamic';

interface LibraryItem {
  /** "dataset:<name>" or "mega:<id>" — combined with slug for download URL */
  sourceType: 'dataset' | 'mega';
  sourceId: string;
  /** Display name of the parent job/megajob (best-effort, falls back to id) */
  sourceLabel: string;
  slug: string;
  label: string;
  groupKey: string | null;
  categories: string[];
  matchMode: 'OR' | 'AND';
  country: string | null;
  generatedAt: string;
  totalZips: number;
  totalDevicesWithZip: number;
  /** Total MAIDs from the originating category-poll, for coverage display.
   *  Missing on reports generated before this field was added. */
  totalMaids?: number;
  /** Download URL ready to drop into an anchor tag */
  downloadUrl: string;
}

/**
 * GET /api/affinity-library
 *
 * Aggregates every category-affinity export the platform has generated:
 *   config/dataset-reports/{name}/category-affinity/*.json
 *   config/mega-reports/{id}/category-affinity/*.json
 *
 * Returns lightweight metadata sorted by generatedAt desc. The full
 * report payload (byZipCode rows) is NOT included — call the dataset
 * or megajob [slug] endpoint for that.
 */
export async function GET(request: NextRequest) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    // Resolve megajob ID → human name once up front so the table can
    // render "Doohyoulike history" instead of a UUID.
    const megaJobs = await getAllMegaJobs();
    const megaNameById = new Map<string, string>();
    for (const mj of megaJobs) {
      if (mj.megaJobId) megaNameById.set(mj.megaJobId, mj.name || mj.megaJobId);
    }

    // Walk both prefixes with paginated ListObjectsV2 — there could be
    // hundreds across many jobs.
    const items: LibraryItem[] = [];

    const walkPrefix = async (
      prefix: string,
      sourceType: 'dataset' | 'mega',
      idFromKey: (key: string) => string | null,
      buildDownloadUrl: (id: string, slug: string) => string,
    ) => {
      let token: string | undefined;
      do {
        const list = await s3Client.send(new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: prefix,
          ContinuationToken: token,
        }));
        const objects = (list.Contents || []).filter((o) => o.Key?.endsWith('.json'));
        for (const obj of objects) {
          if (!obj.Key) continue;
          const id = idFromKey(obj.Key);
          if (!id) continue;
          try {
            const got = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: obj.Key }));
            const text = await got.Body!.transformToString('utf-8');
            const parsed = JSON.parse(text);
            const slug: string = parsed.slug || obj.Key.split('/').pop()!.replace(/\.json$/, '');
            const cats: string[] = Array.isArray(parsed.categories) ? parsed.categories : [];
            const matchMode: 'OR' | 'AND' = parsed.matchMode === 'AND' ? 'AND' : 'OR';
            const sourceLabel = sourceType === 'mega'
              ? (megaNameById.get(id) || id)
              : id;
            items.push({
              sourceType,
              sourceId: id,
              sourceLabel,
              slug,
              label: buildCategoryAffinityLabel(parsed.groupKey ?? null, cats, matchMode),
              groupKey: parsed.groupKey ?? null,
              categories: cats,
              matchMode,
              country: parsed.country ?? null,
              generatedAt: parsed.generatedAt || (obj.LastModified?.toISOString() ?? ''),
              totalZips: typeof parsed.totalZips === 'number' ? parsed.totalZips : 0,
              totalDevicesWithZip: typeof parsed.totalDevicesWithZip === 'number' ? parsed.totalDevicesWithZip : 0,
              totalMaids: typeof parsed.totalMaids === 'number' && parsed.totalMaids > 0 ? parsed.totalMaids : undefined,
              downloadUrl: buildDownloadUrl(id, slug),
            });
          } catch {
            // Tolerate one bad file — keep listing the rest.
          }
        }
        token = list.NextContinuationToken;
      } while (token);
    };

    // Dataset prefix: config/dataset-reports/{name}/category-affinity/{slug}.json
    await walkPrefix(
      'config/dataset-reports/',
      'dataset',
      (key) => {
        const m = key.match(/^config\/dataset-reports\/([^/]+)\/category-affinity\/[^/]+\.json$/);
        return m ? m[1] : null;
      },
      (name, slug) => `/api/datasets/${encodeURIComponent(name)}/category-affinity/${encodeURIComponent(slug)}/download`,
    );

    // Megajob prefix: config/mega-reports/{id}/category-affinity/{slug}.json
    await walkPrefix(
      'config/mega-reports/',
      'mega',
      (key) => {
        const m = key.match(/^config\/mega-reports\/([^/]+)\/category-affinity\/[^/]+\.json$/);
        return m ? m[1] : null;
      },
      (id, slug) => `/api/mega-jobs/${encodeURIComponent(id)}/category-affinity/${encodeURIComponent(slug)}/download`,
    );

    items.sort((a, b) => (b.generatedAt || '').localeCompare(a.generatedAt || ''));
    return NextResponse.json({ items });
  } catch (e: any) {
    console.error('[AFFINITY-LIBRARY]', e.message);
    return NextResponse.json({ error: e.message }, { status: 500 });
  }
}
