import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';
import { affinityReportToCsv } from '@/lib/affinity-builder';

export const dynamic = 'force-dynamic';

/**
 * GET /api/mega-jobs/[id]/category-affinity/[slug]/download
 *
 * Stream the canonical 8-column CSV for a previously-generated
 * megajob category-affinity export. Re-serializes from the persisted
 * report JSON so a CSV always exists even if the original
 * athena-results object expired.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string; slug: string }> },
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { id, slug } = await context.params;
  if (!/^[a-zA-Z0-9_-]+$/.test(slug)) {
    return NextResponse.json({ error: 'Invalid slug' }, { status: 400 });
  }
  const report = await getConfig<any>(`mega-reports/${id}/category-affinity/${slug}`);
  if (!report) {
    return NextResponse.json({ error: 'Affinity report not found' }, { status: 404 });
  }
  const csv = affinityReportToCsv({
    analyzedAt: report.generatedAt || new Date().toISOString(),
    subject: `mega-${id}-${slug}`,
    byZipCode: Array.isArray(report.byZipCode) ? report.byZipCode : [],
  });
  const filename = `mega-${id}-${slug}.csv`;
  return new Response(csv, {
    headers: {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="${filename}"`,
      'Cache-Control': 'no-store',
    },
  });
}
