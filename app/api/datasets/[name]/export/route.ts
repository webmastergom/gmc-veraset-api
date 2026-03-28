import { NextRequest, NextResponse } from 'next/server';
import { exportDevices, ExportFilters } from '@/lib/dataset-exporter';
import { isAuthenticated } from '@/lib/auth';
import { runQuery, ensureTableForDataset, getTableName } from '@/lib/athena';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 180;

async function exportMaidsByNse(
  datasetName: string,
  postalCodes: string[],
  minDwell: number,
): Promise<{ downloadUrl: string; totalDevices: number }> {
  await ensureTableForDataset(datasetName);
  const table = getTableName(datasetName);
  const targetCPs = new Set(postalCodes);

  // Query: get all POI-visiting ad_ids with their first-ping-of-day origin
  const dwellHaving = minDwell > 0
    ? `HAVING DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`
    : '';

  const sql = `
    WITH poi_visitors AS (
      SELECT ad_id, date
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      GROUP BY ad_id, date
      ${dwellHaving}
    ),
    origins AS (
      SELECT
        pv.ad_id,
        ROUND(MIN_BY(TRY_CAST(t.latitude AS DOUBLE), t.utc_timestamp), 4) as origin_lat,
        ROUND(MIN_BY(TRY_CAST(t.longitude AS DOUBLE), t.utc_timestamp), 4) as origin_lng
      FROM poi_visitors pv
      INNER JOIN ${table} t ON pv.ad_id = t.ad_id AND pv.date = t.date
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
      GROUP BY pv.ad_id
    )
    SELECT DISTINCT ad_id, origin_lat, origin_lng
    FROM origins
    WHERE origin_lat IS NOT NULL
  `;

  console.log(`[NSE-EXPORT] Querying origins for ${datasetName} (minDwell=${minDwell})`);
  const result = await runQuery(sql);
  console.log(`[NSE-EXPORT] Got ${result.rows.length} devices with origins`);

  // Reverse geocode origins to postal codes
  const points = result.rows.map(r => ({
    lat: parseFloat(r.origin_lat),
    lng: parseFloat(r.origin_lng),
    deviceCount: 1,
  }));

  const geocoded = await batchReverseGeocode(points);

  // Filter by target postal codes
  const matchedAdIds: string[] = [];
  for (let i = 0; i < result.rows.length; i++) {
    const geo = geocoded[i];
    if (geo.type === 'geojson_local') {
      const cp = geo.postcode?.replace(/^[A-Z]{2}[-\s]/, '') || '';
      if (targetCPs.has(cp)) {
        matchedAdIds.push(result.rows[i].ad_id);
      }
    }
  }

  console.log(`[NSE-EXPORT] ${matchedAdIds.length} MAIDs matched postal codes (${postalCodes.length} target CPs)`);

  // Save to S3
  const csvContent = 'ad_id\n' + matchedAdIds.join('\n');
  const fileName = `${datasetName}-maids-nse-${Date.now()}.csv`;
  const key = `exports/${fileName}`;

  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: csvContent,
    ContentType: 'text/csv',
  }));

  return {
    downloadUrl: `/api/datasets/${datasetName}/export/download?file=${encodeURIComponent(fileName)}`,
    totalDevices: matchedAdIds.length,
  };
}

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
    const body = await req.json().catch(() => ({})) as any;

    // Handle MAIDs-by-NSE export
    if (body.format === 'maids-nse') {
      const postalCodes: string[] = body.nsePostalCodes || [];
      const minDwell: number = body.minDwell || 0;
      if (!postalCodes.length) {
        return NextResponse.json({ error: 'nsePostalCodes required' }, { status: 400 });
      }
      const result = await exportMaidsByNse(datasetName, postalCodes, minDwell);
      return NextResponse.json(result);
    }

    // Standard export
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
