import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob } from '@/lib/mega-jobs';
import { getConsolidatedReport, type ConsolidatedVisitsReport, type ConsolidatedTemporalTrends } from '@/lib/mega-report-consolidation';

export const dynamic = 'force-dynamic';

/**
 * GET /api/mega-jobs/[id]/reports/download?type=visits|temporal
 * Download consolidated report as CSV.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  try {
    const { id } = await context.params;
    const megaJob = await getMegaJob(id);

    if (!megaJob) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    const reportType = request.nextUrl.searchParams.get('type') || 'visits';

    if (reportType === 'visits') {
      const report = await getConsolidatedReport<ConsolidatedVisitsReport>(id, 'visits');
      if (!report) {
        return NextResponse.json({ error: 'Visits report not found' }, { status: 404 });
      }

      const header = 'poi_id,poi_name,visits,devices';
      const rows = report.visitsByPoi.map((v) =>
        `"${v.poiId}","${(v.poiName || '').replace(/"/g, '""')}",${v.visits},${v.devices}`
      );
      const csv = [header, ...rows].join('\n');

      return new NextResponse(csv, {
        headers: {
          'Content-Type': 'text/csv',
          'Content-Disposition': `attachment; filename="mega-job-${id}-visits.csv"`,
        },
      });
    }

    if (reportType === 'temporal') {
      const report = await getConsolidatedReport<ConsolidatedTemporalTrends>(id, 'temporal');
      if (!report) {
        return NextResponse.json({ error: 'Temporal report not found' }, { status: 404 });
      }

      const header = 'date,pings,devices';
      const rows = report.daily.map((d) => `${d.date},${d.pings},${d.devices}`);
      const csv = [header, ...rows].join('\n');

      return new NextResponse(csv, {
        headers: {
          'Content-Type': 'text/csv',
          'Content-Disposition': `attachment; filename="mega-job-${id}-temporal.csv"`,
        },
      });
    }

    return NextResponse.json({ error: 'Invalid type' }, { status: 400 });
  } catch (error: any) {
    console.error('[MEGA-REPORTS DOWNLOAD]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
