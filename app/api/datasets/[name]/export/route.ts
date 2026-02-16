import { NextRequest, NextResponse } from 'next/server';
import { exportDevices, ExportFilters } from '@/lib/dataset-exporter';
import { isAuthenticated } from '@/lib/auth';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 180; // 3 minutes — Athena can take 30-90s per query, two queries run sequentially

export async function POST(
  req: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await context.params;
  const datasetName = params.name;

  try {
    const body = await req.json().catch(() => ({})) as { filters?: ExportFilters; format?: string };
    const filters: ExportFilters = body.filters || {};
    const format: 'full' | 'maids' = body.format === 'maids' ? 'maids' : 'full';
    const mergedFilters: ExportFilters = { ...filters, format };

    const result = await exportDevices(datasetName, mergedFilters);
    return NextResponse.json(result);
  } catch (error: any) {
    console.error(`POST /api/datasets/${datasetName}/export error:`, error);
    return NextResponse.json(
      { error: 'Export failed', details: error.message },
      { status: 500 }
    );
  }
}
