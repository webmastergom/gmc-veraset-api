import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

/**
 * GET /api/datasets/[name]/category-affinity/[slug]
 *
 * Returns the full AffinityReport JSON for a specific category-export
 * slug (including byZipCode rows so the page can pipe it straight to
 * the CatchmentMap renderer).
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string; slug: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { name: datasetName, slug } = await context.params;
  if (!/^[a-zA-Z0-9_-]+$/.test(slug)) {
    return NextResponse.json({ error: 'Invalid slug' }, { status: 400 });
  }
  const report = await getConfig<any>(`dataset-reports/${datasetName}/category-affinity/${slug}`);
  if (!report) {
    return NextResponse.json({ error: 'Affinity report not found' }, { status: 404 });
  }
  return NextResponse.json(report);
}
