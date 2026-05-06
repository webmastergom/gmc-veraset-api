import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

function REPORT_KEY_FULL(ds: string, type: string, dwellMin: number, dwellMax: number, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false): string {
  let key = `dataset-reports/${ds}/${type}`;
  if (dwellMin > 0 || dwellMax > 0) key += `-dwell-${dwellMin}-${dwellMax}`;
  if (hourFrom > 0 || hourTo < 23) key += `-h${hourFrom}-${hourTo}`;
  if (minVisits > 1) key += `-v${minVisits}`;
  if (gpsOnly) key += `-gps`;
  return key;
}
const REPORT_KEY_LEGACY = (ds: string, type: string) =>
  `dataset-reports/${ds}/${type}`;
const VALID_TYPES = ['od', 'hourly', 'dayhour', 'catchment', 'mobility', 'temporal', 'affinity'];

/**
 * GET /api/datasets/[name]/reports?type=od&dwellMin=5&dwellMax=60&hourFrom=8&hourTo=18
 * Return saved report JSON from S3.
 * - dwellMin/dwellMax: dwell interval in minutes (default: 0/0 = no filter)
 * - hourFrom/hourTo: 0-23 (default: 0/23 = all hours)
 * - Falls back to legacy key (no dwell/hour suffix) for backward compat
 * - Also tries old single-bucket key format for backward compat
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { name: datasetName } = await context.params;
  const reportType = request.nextUrl.searchParams.get('type') || '';
  const dwellMin = parseInt(request.nextUrl.searchParams.get('dwellMin') || '0', 10) || 0;
  const dwellMax = parseInt(request.nextUrl.searchParams.get('dwellMax') || '0', 10) || 0;
  // Legacy: support old "bucket" param as dwellMin fallback
  const legacyBucket = parseInt(request.nextUrl.searchParams.get('bucket') || '0', 10) || 0;
  const effectiveDwellMin = dwellMin || legacyBucket;
  const hourFrom = parseInt(request.nextUrl.searchParams.get('hourFrom') || '0', 10) || 0;
  const hourTo = parseInt(request.nextUrl.searchParams.get('hourTo') || '23', 10);
  const minVisits = parseInt(request.nextUrl.searchParams.get('minVisits') || '1', 10) || 1;
  const gpsOnly = request.nextUrl.searchParams.get('gpsOnly') === 'true';

  if (!VALID_TYPES.includes(reportType)) {
    return NextResponse.json(
      { error: `Invalid type. Must be one of: ${VALID_TYPES.join(', ')}` },
      { status: 400 }
    );
  }

  try {
    // Try full-keyed report first (dwell interval + hour + minVisits + gpsOnly)
    let report = await getConfig<any>(REPORT_KEY_FULL(datasetName, reportType, effectiveDwellMin, dwellMax, hourFrom, hourTo, minVisits, gpsOnly));

    // Fallback: old single-bucket key format (dwell-{N} without max)
    if (!report && effectiveDwellMin > 0 && dwellMax === 0) {
      const oldKey = `dataset-reports/${datasetName}/${reportType}-dwell-${effectiveDwellMin}` +
        ((hourFrom > 0 || hourTo < 23) ? `-h${hourFrom}-${hourTo}` : '');
      report = await getConfig<any>(oldKey);
    }

    // Fallback: legacy key (pre-filter migration) — only when no filters active
    if (!report && effectiveDwellMin === 0 && dwellMax === 0 && hourFrom === 0 && hourTo === 23 && minVisits <= 1) {
      report = await getConfig<any>(REPORT_KEY_LEGACY(datasetName, reportType));
    }

    if (!report) {
      return NextResponse.json(
        { error: `Report "${reportType}" (dwell ${effectiveDwellMin}-${dwellMax || '∞'}, hours ${hourFrom}-${hourTo}${minVisits > 1 ? `, minVisits=${minVisits}` : ''}) not found. Run Analyze first.` },
        { status: 404 }
      );
    }
    return NextResponse.json(report);
  } catch (error: any) {
    console.error('[DS-REPORTS GET]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
