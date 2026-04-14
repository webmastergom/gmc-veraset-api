import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

function REPORT_KEY_FULL(ds: string, type: string, bucket: number, hourFrom = 0, hourTo = 23): string {
  let key = `dataset-reports/${ds}/${type}`;
  if (bucket > 0) key += `-dwell-${bucket}`;
  if (hourFrom > 0 || hourTo < 23) key += `-h${hourFrom}-${hourTo}`;
  return key;
}
const REPORT_KEY_LEGACY = (ds: string, type: string) =>
  `dataset-reports/${ds}/${type}`;
const VALID_TYPES = ['od', 'hourly', 'catchment', 'mobility', 'temporal', 'affinity'];

/**
 * GET /api/datasets/[name]/reports?type=od&bucket=5&hourFrom=8&hourTo=18
 * Return saved report JSON from S3.
 * - bucket param: 0|2|5|10|15|30|60|120|180 (default: 0)
 * - hourFrom/hourTo: 0-23 (default: 0/23 = all hours)
 * - Falls back to legacy key (no dwell/hour suffix) for backward compat
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
  const bucketParam = request.nextUrl.searchParams.get('bucket');
  const bucket = bucketParam != null ? parseInt(bucketParam, 10) : 0;
  const hourFrom = parseInt(request.nextUrl.searchParams.get('hourFrom') || '0', 10) || 0;
  const hourTo = parseInt(request.nextUrl.searchParams.get('hourTo') || '23', 10);

  if (!VALID_TYPES.includes(reportType)) {
    return NextResponse.json(
      { error: `Invalid type. Must be one of: ${VALID_TYPES.join(', ')}` },
      { status: 400 }
    );
  }

  try {
    // Try full-keyed report first (dwell + hour)
    let report = await getConfig<any>(REPORT_KEY_FULL(datasetName, reportType, bucket, hourFrom, hourTo));

    // Fallback: legacy key (pre-filter migration) — only when no filters active
    if (!report && bucket === 0 && hourFrom === 0 && hourTo === 23) {
      report = await getConfig<any>(REPORT_KEY_LEGACY(datasetName, reportType));
    }

    if (!report) {
      return NextResponse.json(
        { error: `Report "${reportType}" (bucket ${bucket}, hours ${hourFrom}-${hourTo}) not found. Run Analyze first.` },
        { status: 404 }
      );
    }
    return NextResponse.json(report);
  } catch (error: any) {
    console.error('[DS-REPORTS GET]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
