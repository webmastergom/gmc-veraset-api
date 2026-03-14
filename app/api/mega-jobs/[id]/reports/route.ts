import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob } from '@/lib/mega-jobs';
import { getConsolidatedReport } from '@/lib/mega-report-consolidation';

export const dynamic = 'force-dynamic';

/**
 * GET /api/mega-jobs/[id]/reports?type=visits|temporal|catchment
 * Return consolidated report JSON.
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
    const validTypes = ['visits', 'temporal', 'catchment'];

    if (!validTypes.includes(reportType)) {
      return NextResponse.json(
        { error: `Invalid type. Must be one of: ${validTypes.join(', ')}` },
        { status: 400 }
      );
    }

    const report = await getConsolidatedReport(id, reportType);

    if (!report) {
      return NextResponse.json(
        { error: `Report "${reportType}" not found. Run consolidation first.` },
        { status: 404 }
      );
    }

    return NextResponse.json(report);
  } catch (error: any) {
    console.error('[MEGA-REPORTS GET]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
