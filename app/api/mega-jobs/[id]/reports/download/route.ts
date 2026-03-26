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
  type ConsolidatedAffinityReport,
} from '@/lib/mega-report-consolidation';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

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
 * GET /api/mega-jobs/[id]/reports/download?type=visits|temporal|od|hourly|catchment|mobility|maids|postcodes|affinity
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

      const header = 'postal_code,city,country,devices,share_percentage';
      const rows = report.byZipCode.map((z) =>
        `${escCsv(z.zipCode)},${escCsv(z.city)},${escCsv(z.country)},${z.deviceDays},${z.sharePercentage ?? 0}`
      );
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-catchment.csv`);
    }

    // ── Mobility ─────────────────────────────────────────────────────
    if (reportType === 'mobility') {
      const report = await getConsolidatedReport<ConsolidatedMobilityReport>(id, 'mobility');
      if (!report) return NextResponse.json({ error: 'Mobility report not found' }, { status: 404 });

      const header = 'timing,category,device_days,hits';
      const rows: string[] = [];
      for (const c of (report.before || [])) {
        rows.push(`before,${escCsv(c.category)},${c.deviceDays},${c.hits}`);
      }
      for (const c of (report.after || [])) {
        rows.push(`after,${escCsv(c.category)},${c.deviceDays},${c.hits}`);
      }
      // Fallback for legacy reports without before/after
      if (rows.length === 0) {
        for (const c of report.categories) {
          rows.push(`combined,${escCsv(c.category)},${c.deviceDays},${c.hits}`);
        }
      }
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-mobility.csv`);
    }

    // ── MAIDs (from Athena output CSV in S3) ────────────────────────
    if (reportType === 'maids') {
      const maidsKey = megaJob.consolidatedReports?.maids;
      if (!maidsKey) return NextResponse.json({ error: 'MAIDs report not found. Re-consolidate to generate.' }, { status: 404 });

      try {
        const s3 = new S3Client({ region: process.env.AWS_REGION || 'us-west-2' });
        const resp = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: maidsKey }));
        const raw = await resp.Body?.transformToString() || '';

        // Athena CSV has header "ad_id" + quoted values. Clean it up to just MAIDs.
        const lines = raw.split('\n').filter((l) => l.trim());
        const header = 'maid';
        const rows = lines.slice(1).map((line) => line.replace(/^"|"$/g, '').trim()).filter(Boolean);
        const csv = [header, ...rows].join('\n');

        return csvResponse(csv, `mega-job-${id}-maids.csv`);
      } catch (err: any) {
        console.error('[MEGA-MAIDS DOWNLOAD]', err.message);
        return NextResponse.json({ error: `Failed to fetch MAIDs: ${err.message}` }, { status: 500 });
      }
    }

    // ── Clean postal codes ───────────────────────────────────────────
    if (reportType === 'postcodes') {
      const report = await getConsolidatedReport<ConsolidatedCatchmentReport>(id, 'catchment');
      if (!report) return NextResponse.json({ error: 'Catchment report not found' }, { status: 404 });

      const header = 'postal_code,device_days';
      const rows = report.byZipCode
        .filter((z) => z.zipCode && z.zipCode !== 'UNKNOWN' && z.zipCode !== 'FOREIGN')
        .map((z) => {
          // Clean postal code: remove any added letters, dashes, or spaces
          const clean = z.zipCode.replace(/[^0-9]/g, '');
          return `${clean},${z.deviceDays}`;
        })
        .filter((r) => r.split(',')[0]); // skip if empty after cleaning
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-postcodes.csv`);
    }

    // ── Affinity Index ──────────────────────────────────────────────
    if (reportType === 'affinity') {
      const report = await getConsolidatedReport<ConsolidatedAffinityReport>(id, 'affinity');
      if (!report) return NextResponse.json({ error: 'Affinity report not found. Re-consolidate to generate.' }, { status: 404 });

      const header = 'postal_code,affinity_index,total_visits,unique_devices,avg_dwell_minutes,avg_frequency';
      const rows = report.byZipCode.map((z) =>
        `${escCsv(z.postalCode)},${z.affinityIndex},${z.totalVisits},${z.uniqueDevices},${z.avgDwell},${z.avgFrequency}`
      );
      return csvResponse([header, ...rows].join('\n'), `mega-job-${id}-affinity.csv`);
    }

    return NextResponse.json({ error: 'Invalid type' }, { status: 400 });
  } catch (error: any) {
    console.error('[MEGA-REPORTS DOWNLOAD]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
