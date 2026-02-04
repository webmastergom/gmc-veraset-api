import { NextRequest, NextResponse } from 'next/server';
import { exportDevices, ExportFilters } from '@/lib/dataset-exporter';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60; // Allow up to 60s for Athena queries

export async function POST(
  req: NextRequest,
  { params }: { params: { name: string } }
) {
  try {
    const body = await req.json().catch(() => ({}));
    const filters: ExportFilters = body.filters || {};

    const result = await exportDevices(params.name, filters);

    return NextResponse.json(result);

  } catch (error: any) {
    console.error(`POST /api/datasets/${params.name}/export error:`, error);
    return NextResponse.json(
      { error: 'Export failed', details: error.message },
      { status: 500 }
    );
  }
}
