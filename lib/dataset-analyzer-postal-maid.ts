/**
 * Postal Code → MAID lookup.
 *
 * Strategy:
 * 1. Get ALL devices in the dataset with their first ping per day (origin)
 * 2. Reverse geocode each origin coordinate to a postal code
 * 3. Filter: keep only devices whose origin postal code is in the requested set
 * 4. Return the list of matching ad_ids with counts
 *
 * This reuses the same methodology as catchment analysis (first-ping-of-day)
 * but inverts the query: instead of "which zipcodes do visitors come from?",
 * it answers "which devices live in these zipcodes?".
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';
import { batchReverseGeocode } from './reverse-geocode';
import type { PostalMaidFilters, PostalMaidDevice, PostalMaidResult } from './postal-maid-types';

export type { PostalMaidFilters, PostalMaidResult } from './postal-maid-types';

const ACCURACY_THRESHOLD_METERS = 500;
const COORDINATE_PRECISION = 4; // ~11m resolution

/** Progress callback for streaming updates */
export type PostalMaidProgressCallback = (progress: {
  step: 'initializing' | 'preparing_table' | 'running_queries' | 'geocoding' | 'matching' | 'completed' | 'error';
  percent: number;
  message: string;
  detail?: string;
}) => void;

/**
 * Find all MAIDs whose residential origin (first ping of day) falls
 * within the requested postal codes.
 */
export async function analyzePostalMaid(
  datasetName: string,
  filters: PostalMaidFilters,
  onProgress?: PostalMaidProgressCallback
): Promise<PostalMaidResult> {
  const tableName = getTableName(datasetName);
  const report = onProgress || (() => {});

  report({ step: 'initializing', percent: 0, message: 'Validating configuration...' });
  console.log(`[POSTAL-MAID] Starting postal→MAID analysis for dataset: ${datasetName}`, {
    postalCodes: filters.postalCodes,
    country: filters.country,
  });

  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials not configured.');
  }

  if (!filters.postalCodes?.length) {
    throw new Error('At least one postal code is required.');
  }

  if (!filters.country) {
    throw new Error('Country code is required for reverse geocoding.');
  }

  // Normalize postal codes (trim, uppercase, strip country prefix like "ES-" or "CO-")
  const requestedPostalCodes = new Set(
    filters.postalCodes.map(pc => {
      let code = pc.trim().toUpperCase();
      // Strip 2-letter country prefix (e.g. "ES-28001" → "28001")
      if (/^[A-Z]{2}-/.test(code)) {
        code = code.slice(3);
      }
      return code;
    })
  );

  // Ensure Athena table exists
  report({ step: 'preparing_table', percent: 5, message: 'Preparing Athena table...', detail: tableName });
  try {
    await createTableForDataset(datasetName);
  } catch (error: any) {
    if (!error.message?.includes('already exists')) {
      if (error.message?.includes('not authorized') || error.message?.includes('Access denied')) {
        throw new Error(`Athena access denied. Original error: ${error.message}`);
      }
      throw error;
    }
  }
  report({ step: 'preparing_table', percent: 10, message: 'Table ready' });

  // Build WHERE conditions
  const dateConditions: string[] = [];
  if (filters.dateFrom) dateConditions.push(`date >= '${filters.dateFrom}'`);
  if (filters.dateTo) dateConditions.push(`date <= '${filters.dateTo}'`);
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  // Query: get first ping per device-day for ALL devices in the dataset
  // We get ad_id-level granularity (not aggregated by coordinate) so we can
  // link each device back to its postal code
  const originsQuery = `
    WITH
    first_pings AS (
      SELECT
        ad_id,
        date,
        MIN_BY(TRY_CAST(latitude AS DOUBLE), utc_timestamp) as origin_lat,
        MIN_BY(TRY_CAST(longitude AS DOUBLE), utc_timestamp) as origin_lng
      FROM ${tableName}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        ${dateWhere}
      GROUP BY ad_id, date
    )
    SELECT
      ad_id,
      ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
      ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
      COUNT(*) as device_days
    FROM first_pings
    WHERE origin_lat IS NOT NULL AND origin_lng IS NOT NULL
    GROUP BY ad_id, ROUND(origin_lat, ${COORDINATE_PRECISION}), ROUND(origin_lng, ${COORDINATE_PRECISION})
    ORDER BY device_days DESC
  `;

  const totalQuery = `
    SELECT COUNT(DISTINCT ad_id) as total_devices
    FROM ${tableName}
    WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
      ${dateWhere}
  `;

  report({ step: 'running_queries', percent: 15, message: 'Running Athena queries...', detail: 'Extracting first ping per device-day' });
  console.log(`[POSTAL-MAID] Executing queries in parallel...`);

  const [originsRes, totalRes] = await Promise.all([
    runQuery(originsQuery),
    runQuery(totalQuery),
  ]);

  const totalDevices = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`[POSTAL-MAID] Total devices: ${totalDevices}, origin rows: ${originsRes.rows.length}`);
  report({ step: 'running_queries', percent: 50, message: 'Queries complete', detail: `${originsRes.rows.length} device-origin rows` });

  if (totalDevices === 0 || originsRes.rows.length === 0) {
    report({ step: 'completed', percent: 100, message: 'No data found' });
    return buildEmptyResult(datasetName, filters, totalDevices);
  }

  // Collect unique coordinates for batch geocoding
  const coordMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
  const deviceOrigins: Array<{ adId: string; coordKey: string; deviceDays: number }> = [];
  let totalDeviceDays = 0;

  for (const row of originsRes.rows) {
    const adId = String(row.ad_id);
    const lat = parseFloat(String(row.origin_lat));
    const lng = parseFloat(String(row.origin_lng));
    const days = parseInt(String(row.device_days)) || 0;

    if (isNaN(lat) || isNaN(lng) || !adId) continue;

    totalDeviceDays += days;
    const coordKey = `${lat.toFixed(COORDINATE_PRECISION)},${lng.toFixed(COORDINATE_PRECISION)}`;

    deviceOrigins.push({ adId, coordKey, deviceDays: days });

    const existing = coordMap.get(coordKey);
    if (existing) {
      existing.deviceCount += days;
    } else {
      coordMap.set(coordKey, { lat, lng, deviceCount: days });
    }
  }

  // Batch reverse geocode all unique coordinates
  report({ step: 'geocoding', percent: 55, message: 'Reverse geocoding origins...', detail: `${coordMap.size} unique coordinates` });
  console.log(`[POSTAL-MAID] Reverse geocoding ${coordMap.size} coordinate clusters...`);

  const coordPoints = Array.from(coordMap.values()).map(p => ({
    lat: p.lat,
    lng: p.lng,
    deviceCount: p.deviceCount,
  }));

  const classified = await batchReverseGeocode(coordPoints);
  report({ step: 'geocoding', percent: 75, message: 'Geocoding complete' });

  // Build coordKey → postalCode map from geocoding results
  const coordToPostal = new Map<string, string>();
  const coordPointsArr = Array.from(coordMap.entries());

  for (let i = 0; i < classified.length; i++) {
    const c = classified[i];
    if (c.type === 'geojson_local') {
      const [coordKey] = coordPointsArr[i];
      coordToPostal.set(coordKey, c.postcode.toUpperCase());
    }
  }

  // Match devices to requested postal codes
  report({ step: 'matching', percent: 80, message: 'Matching devices to postal codes...', detail: `${requestedPostalCodes.size} postal codes` });

  const deviceMap = new Map<string, { deviceDays: number; postalCodes: Set<string> }>();
  const postalBreakdown = new Map<string, { devices: Set<string>; deviceDays: number }>();

  // Initialize breakdown for all requested codes
  for (const pc of requestedPostalCodes) {
    postalBreakdown.set(pc, { devices: new Set(), deviceDays: 0 });
  }

  for (const { adId, coordKey, deviceDays } of deviceOrigins) {
    const postalCode = coordToPostal.get(coordKey);
    if (!postalCode || !requestedPostalCodes.has(postalCode)) continue;

    // Add to device map
    const existing = deviceMap.get(adId);
    if (existing) {
      existing.deviceDays += deviceDays;
      existing.postalCodes.add(postalCode);
    } else {
      deviceMap.set(adId, { deviceDays, postalCodes: new Set([postalCode]) });
    }

    // Add to postal breakdown
    const breakdown = postalBreakdown.get(postalCode)!;
    breakdown.devices.add(adId);
    breakdown.deviceDays += deviceDays;
  }

  // Build result arrays
  const devices: PostalMaidDevice[] = Array.from(deviceMap.entries())
    .map(([adId, data]) => ({
      adId,
      deviceDays: data.deviceDays,
      postalCodes: Array.from(data.postalCodes),
    }))
    .sort((a, b) => b.deviceDays - a.deviceDays);

  const postalCodeBreakdown = Array.from(postalBreakdown.entries())
    .map(([postalCode, data]) => ({
      postalCode,
      devices: data.devices.size,
      deviceDays: data.deviceDays,
    }))
    .sort((a, b) => b.devices - a.devices);

  const matchedDeviceDays = devices.reduce((s, d) => s + d.deviceDays, 0);
  const postalCodesWithDevices = postalCodeBreakdown.filter(p => p.devices > 0).length;
  const topPostal = postalCodeBreakdown[0];

  console.log(`[POSTAL-MAID] Done: ${devices.length} MAIDs matched, ${postalCodesWithDevices}/${requestedPostalCodes.size} postal codes with data`);
  report({ step: 'completed', percent: 100, message: 'Analysis complete', detail: `${devices.length} MAIDs found` });

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology: {
      approach: 'first_ping_per_day_reverse_geocoded',
      description: 'First GPS ping of each device-day reverse geocoded to postal code. Devices whose origin matches the requested postal codes are returned.',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
    },
    coverage: {
      totalDevicesInDataset: totalDevices,
      totalDeviceDays,
      devicesMatchedToPostalCodes: devices.length,
      matchedDeviceDays,
      postalCodesRequested: requestedPostalCodes.size,
      postalCodesWithDevices,
    },
    summary: {
      totalMaids: devices.length,
      topPostalCode: topPostal?.postalCode ?? null,
      topPostalCodeDevices: topPostal?.devices ?? 0,
    },
    devices,
    postalCodeBreakdown,
  };
}

function buildEmptyResult(
  datasetName: string,
  filters: PostalMaidFilters,
  totalDevices: number
): PostalMaidResult {
  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology: {
      approach: 'first_ping_per_day_reverse_geocoded',
      description: 'No data available for the specified filters.',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
    },
    coverage: {
      totalDevicesInDataset: totalDevices,
      totalDeviceDays: 0,
      devicesMatchedToPostalCodes: 0,
      matchedDeviceDays: 0,
      postalCodesRequested: filters.postalCodes.length,
      postalCodesWithDevices: 0,
    },
    summary: {
      totalMaids: 0,
      topPostalCode: null,
      topPostalCodeDevices: 0,
    },
    devices: [],
    postalCodeBreakdown: filters.postalCodes.map(pc => ({
      postalCode: pc.trim().toUpperCase(),
      devices: 0,
      deviceDays: 0,
    })),
  };
}
