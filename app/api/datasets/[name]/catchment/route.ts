import { NextRequest, NextResponse } from 'next/server';
import { analyzeOrigins } from '@/lib/dataset-analyzer-od';
import type { ODFilters } from '@/lib/od-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/datasets/[name]/catchment
 * Reporte: origen de visitantes por código postal.
 * Usa primer ping del día por dispositivo, geocodificado a CP.
 * Query params: dateFrom, dateTo, poiIds (comma-separated)
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
): Promise<NextResponse> {
  try {
    const params = await context.params;
    const datasetName = params.name;

    if (!datasetName) {
      return NextResponse.json(
        { error: 'Dataset name is required' },
        { status: 400 }
      );
    }

    const { searchParams } = new URL(request.url);
    const filters: ODFilters = {};
    const dateFrom = searchParams.get('dateFrom');
    const dateTo = searchParams.get('dateTo');
    if (dateFrom) filters.dateFrom = dateFrom;
    if (dateTo) filters.dateTo = dateTo;
    const poiIds = searchParams.get('poiIds');
    if (poiIds) filters.poiIds = poiIds.split(',').map((s) => s.trim()).filter(Boolean);

    const res = await analyzeOrigins(datasetName, filters);

    const zipcodes = res.origins.map((z) => ({
      zipcode: z.zipcode,
      city: z.city,
      province: z.province,
      region: z.region,
      devices: z.devices,
      percentOfTotal: z.percentOfTotal,
      percentOfClassified: z.percentOfTotal,
      percentage: z.percentOfTotal,
      source: z.source,
    }));

    const totalMatched = zipcodes.reduce((s, z) => s + z.devices, 0);

    const result = {
      dataset: res.dataset,
      analyzedAt: res.analyzedAt,
      methodology: {
        approach: 'origin_first_ping',
        description: 'First GPS ping of each device-day, reverse geocoded to postal code.',
      },
      coverage: {
        totalDevicesVisitedPois: res.totalDevicesVisitedPois,
        totalDeviceDays: res.totalDeviceDays,
        devicesMatchedToZipcode: totalMatched,
        geocodingComplete: res.geocodingComplete,
        classificationRatePercent: res.coverageRatePercent,
      },
      summary: {
        totalDevicesInDataset: res.totalDevicesVisitedPois,
        devicesMatchedToZipcode: totalMatched,
        totalZipcodes: zipcodes.length,
        topZipcode: zipcodes[0]?.zipcode ?? null,
        topCity: zipcodes[0]?.city ?? null,
      },
      zipcodes,
    };

    return NextResponse.json(result, {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error: any) {
    console.error('[CATCHMENT-OD] Error:', error?.message);

    if (
      error?.message?.includes('Access denied') ||
      error?.message?.includes('not authorized')
    ) {
      return NextResponse.json(
        { error: 'Athena access denied', details: error.message },
        { status: 403 }
      );
    }
    if (error?.message?.includes('AWS credentials not configured')) {
      return NextResponse.json(
        { error: 'AWS not configured', details: error.message },
        { status: 503 }
      );
    }
    return NextResponse.json(
      { error: 'Catchment failed', details: error?.message },
      { status: 500 }
    );
  }
}
