import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob } from '@/lib/mega-jobs';
import {
  getConsolidatedReport,
  type ConsolidatedVisitsReport,
  type ConsolidatedTemporalTrends,
  type ConsolidatedODReport,
  type ConsolidatedHourlyReport,
  type ConsolidatedCatchmentReport,
  type ConsolidatedMobilityReport,
} from '@/lib/mega-report-consolidation';

export const dynamic = 'force-dynamic';

function csvResponse(csv: string, filename: string): NextResponse {
  return new NextResponse(csv, {
    headers: {
      'Content-Type': 'text/csv',
      'Content-Disposition': `attachment; filename="${filename}"`,
    },
  });
}

function escCsv(s: string): string {
  return `"${(s || '').replace(/"/g, '""')}"`;
}

/**
 * GET /api/mega-jobs/[id]/reports/download?type=visits|temporal|od|hourly|catchment|mobility
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

    // ── Visits ───────────────────────────────────────────────────────
    if (reportType === 'visits') {
      const report = await getConsolidatedReport<ConsolidatedVisitsReport>(id, 'visits');
      if (!report) return NextResponse.json({ error: 'Visits report not found' }, { status: 404 });

      const header = 'poi_id,poi_name,visits,devices';
      const rows = report.visitsByPoi.map((v) =>
        `${escCsv(v.poiId)},${escCsv(v.poiName)},${v.visits},${v.devices}`
      );
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-visits.csv`);
    }

    // ── Temporal ─────────────────────────────────────────────────────
    if (reportType === 'temporal') {
      const report = await getConsolidatedReport<ConsolidatedTemporalTrends>(id, 'temporal');
      if (!report) return NextResponse.json({ error: 'Temporal report not found' }, { status: 404 });

      const header = 'date,pings,devices';
      const rows = report.daily.map((d) => `${d.date},${d.pings},${d.devices}`);
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-temporal.csv`);
    }

    // ── OD (origins + destinations) ──────────────────────────────────
    if (reportType === 'od') {
      const report = await getConsolidatedReport<ConsolidatedODReport>(id, 'od');
      if (!report) return NextResponse.json({ error: 'OD report not found' }, { status: 404 });

      const header = 'type,zip_code,city,country,lat,lng,device_days';
      const originRows = report.origins.map((o) =>
        `origin,${escCsv(o.zipCode)},${escCsv(o.city)},${escCsv(o.country)},${o.lat},${o.lng},${o.deviceDays}`
      );
      const destRows = report.destinations.map((d) =>
        `destination,${escCsv(d.zipCode)},${escCsv(d.city)},${escCsv(d.country)},${d.lat},${d.lng},${d.deviceDays}`
      );
      return csvResponse([header, ...originRows, ...destRows].join('\n'), `mega-job-${id}-od.csv`);
    }

    // ── Hourly ───────────────────────────────────────────────────────
    if (reportType === 'hourly') {
      const report = await getConsolidatedReport<ConsolidatedHourlyReport>(id, 'hourly');
      if (!report) return NextResponse.json({ error: 'Hourly report not found' }, { status: 404 });

      const header = 'hour,pings,devices';
      const rows = report.hourly.map((h) => `${h.hour},${h.pings},${h.devices}`);
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-hourly.csv`);
    }

    // ── Catchment ────────────────────────────────────────────────────
    if (reportType === 'catchment') {
      const report = await getConsolidatedReport<ConsolidatedCatchmentReport>(id, 'catchment');
      if (!report) return NextResponse.json({ error: 'Catchment report not found' }, { status: 404 });

      const header = 'zip_code,city,country,lat,lng,device_days';
      const rows = report.byZipCode.map((z) =>
        `${escCsv(z.zipCode)},${escCsv(z.city)},${escCsv(z.country)},${z.lat},${z.lng},${z.deviceDays}`
      );
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-catchment.csv`);
    }

    // ── Mobility ─────────────────────────────────────────────────────
    if (reportType === 'mobility') {
      const report = await getConsolidatedReport<ConsolidatedMobilityReport>(id, 'mobility');
      if (!report) return NextResponse.json({ error: 'Mobility report not found' }, { status: 404 });

      const header = 'category,device_days,hits';
      const rows = report.categories.map((c) =>
        `${escCsv(c.category)},${c.deviceDays},${c.hits}`
      );
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-mobility.csv`);
    }

    return NextResponse.json({ error: 'Invalid type' }, { status: 400 });
  } catch (error: any) {
    console.error('[MEGA-REPORTS DOWNLOAD]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
