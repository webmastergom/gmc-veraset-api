import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

const REPORT_KEY = (ds: string, type: string) => `dataset-reports/${ds}/${type}`;
const VALID_TYPES = ['od', 'hourly', 'catchment', 'mobility', 'temporal'];

/**
 * GET /api/datasets/[name]/reports?type=od|hourly|catchment|mobility
 * Return saved report JSON from S3.
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

  if (!VALID_TYPES.includes(reportType)) {
    return NextResponse.json(
      { error: `Invalid type. Must be one of: ${VALID_TYPES.join(', ')}` },
      { status: 400 }
    );
  }

  try {
    const report = await getConfig<any>(REPORT_KEY(datasetName, reportType));
    if (!report) {
      return NextResponse.json(
        { error: `Report "${reportType}" not found. Generate reports first.` },
        { status: 404 }
      );
    }
    return NextResponse.json(report);
  } catch (error: any) {
    console.error('[DS-REPORTS GET]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
