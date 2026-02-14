import { NextRequest, NextResponse } from 'next/server';
import { analyzeOriginDestination } from '@/lib/dataset-analyzer-od';
import type { ODFilters } from '@/lib/od-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/datasets/[name]/catchment
 * Reporte: origen de visitantes por código postal.
 * Usa metodología OD: primer ping del día = origen, geocodificado a CP.
 * Query params: dateFrom, dateTo, poiIds (comma-separated)
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> | { name: string } }
): Promise<NextResponse> {
  try {
    const params = await (typeof context.params === 'object' && context.params instanceof Promise
      ? context.params
      : Promise.resolve(context.params as { name: string }));
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

    const od = await analyzeOriginDestination(datasetName, filters);

    // Transform OD origins → catchment format expected by frontend
    const zipcodes = od.origins.map((z) => ({
      zipcode: z.zipcode,
      city: z.city,
      province: z.province,
      region: z.region,
      devices: z.devices,
      percentOfTotal: z.percentOfTotal,
      percentOfClassified: z.percentOfTotal, // same in OD context
      percentage: z.percentOfTotal,
      source: z.source,
    }));

    const totalMatched = zipcodes.reduce((s, z) => s + z.devices, 0);

    const result = {
      dataset: od.dataset,
      analyzedAt: od.analyzedAt,
      filters: od.filters,
      methodology: {
        approach: 'origin_destination',
        description: 'First GPS ping of each device-day, reverse geocoded to postal code.',
        accuracyThresholdMeters: od.methodology.accuracyThresholdMeters,
        coordinatePrecision: od.methodology.coordinatePrecision,
      },
      coverage: {
        totalDevicesVisitedPois: od.coverage.totalDevicesVisitedPois,
        totalDeviceDays: od.coverage.totalDeviceDays,
        devicesMatchedToZipcode: totalMatched,
        devicesWithOrigin: od.coverage.devicesWithOrigin,
        originZipcodes: od.coverage.originZipcodes,
        geocodingComplete: od.coverage.geocodingComplete,
        classificationRatePercent: od.coverage.coverageRatePercent,
      },
      summary: {
        totalDevicesInDataset: od.coverage.totalDevicesVisitedPois,
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
