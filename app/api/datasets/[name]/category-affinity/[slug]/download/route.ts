import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';
import { affinityReportToCsv } from '@/lib/affinity-builder';

export const dynamic = 'force-dynamic';

/**
 * GET /api/datasets/[name]/category-affinity/[slug]/download
 *
 * Stream the canonical 8-column CSV for a previously-generated
 * category-affinity export. Re-serializes from the persisted report
 * JSON (rather than reading the cached S3 CSV) so a CSV always exists
 * even if the original athena-results object expired.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string; slug: string }> },
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
  const csv = affinityReportToCsv({
    analyzedAt: report.generatedAt || new Date().toISOString(),
    subject: `dataset-${datasetName}-${slug}`,
    byZipCode: Array.isArray(report.byZipCode) ? report.byZipCode : [],
  });
  const filename = `dataset-${datasetName}-${slug}.csv`;
  return new Response(csv, {
    headers: {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="${filename}"`,
      'Cache-Control': 'no-store',
    },
  });
}
