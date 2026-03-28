import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

const REPORT_KEY_DWELL = (ds: string, type: string, bucket: number) =>
  `dataset-reports/${ds}/${type}-dwell-${bucket}`;
const REPORT_KEY_LEGACY = (ds: string, type: string) =>
  `dataset-reports/${ds}/${type}`;
const VALID_TYPES = ['od', 'hourly', 'catchment', 'mobility', 'temporal', 'affinity'];

/**
 * GET /api/datasets/[name]/reports?type=od&bucket=5
 * Return saved report JSON from S3.
 * - bucket param: 0|2|5|10|15|30|60|120|180 (default: 0)
 * - Falls back to legacy key (no dwell suffix) for backward compat
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

  if (!VALID_TYPES.includes(reportType)) {
    return NextResponse.json(
      { error: `Invalid type. Must be one of: ${VALID_TYPES.join(', ')}` },
      { status: 400 }
    );
  }

  try {
    // Try dwell-keyed report first
    let report = await getConfig<any>(REPORT_KEY_DWELL(datasetName, reportType, bucket));

    // Fallback: legacy key (pre-dwell-bucket migration)
    if (!report && bucket === 0) {
      report = await getConfig<any>(REPORT_KEY_LEGACY(datasetName, reportType));
    }

    if (!report) {
      return NextResponse.json(
        { error: `Report "${reportType}" (bucket ${bucket}) not found. Run Analyze first.` },
        { status: 404 }
      );
    }
    return NextResponse.json(report);
  } catch (error: any) {
    console.error('[DS-REPORTS GET]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
