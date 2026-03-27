/**
 * Postal Code → MAID lookup.
 *
 * Two-phase approach leveraging the proven catchment (analyzeOrigins) flow:
 *
 * Phase 1: Run catchment analysis (analyzeOrigins) — identical to Dataset Analysis.
 *          This validates the data exists and tells us which postal codes have devices.
 *
 * Phase 2: For postal codes that match, run a second query identical to catchment
 *          but keeping ad_id to extract individual MAIDs.
 *
 * This guarantees consistency with the Dataset Analysis catchment report.
 */

import { analyzeOrigins } from './dataset-analyzer-od';
import type { OriginsResult } from './dataset-analyzer-od';
import { runQuery, getTableName } from './athena';
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
  const report = onProgress || (() => {});

  report({ step: 'initializing', percent: 0, message: 'Validating configuration...' });
  console.log(`[POSTAL-MAID] Starting postal→MAID analysis for dataset: ${datasetName}`, {
    postalCodes: filters.postalCodes,
    country: filters.country,
  });

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
      if (/^[A-Z]{2}-/.test(code)) {
        code = code.slice(3);
      }
      return code;
    })
  );

  // ── Phase 1: Run catchment analysis (same as Dataset Analysis) ──────────
  report({ step: 'initializing', percent: 2, message: 'Running catchment analysis (same as Dataset Analysis)...' });
  console.log(`[POSTAL-MAID] Phase 1: Running analyzeOrigins for dataset: ${datasetName}`);

  const catchment = await analyzeOrigins(datasetName, {
    dateFrom: filters.dateFrom,
    dateTo: filters.dateTo,
  }, (p) => {
    // Scale catchment progress to 0–60%
    report({
      step: p.step as any,
      percent: Math.round(p.percent * 0.6),
      message: p.message,
      detail: p.detail,
    });
  }, { skipCache: true });

  console.log(`[POSTAL-MAID] Catchment done: ${catchment.origins.length} postal codes, ${catchment.totalDevicesVisitedPois} devices`);

  // ── Phase 2: Match postal codes from catchment ──────────────────────────
  report({ step: 'matching', percent: 62, message: 'Matching postal codes from catchment...', detail: `${requestedPostalCodes.size} codes` });

  const matchedOrigins = new Map<string, { devices: number; city: string; province: string }>();
  for (const origin of catchment.origins) {
    // Catchment returns zipcodes like "ES-28001" — strip country prefix
    const rawCode = origin.zipcode.includes('-')
      ? origin.zipcode.split('-').slice(1).join('-').toUpperCase()
      : origin.zipcode.toUpperCase();
    if (requestedPostalCodes.has(rawCode)) {
      matchedOrigins.set(rawCode, {
        devices: origin.devices,
        city: origin.city,
        province: origin.province,
      });
    }
  }

  console.log(`[POSTAL-MAID] Phase 2: ${matchedOrigins.size}/${requestedPostalCodes.size} postal codes found in catchment`);

  if (matchedOrigins.size === 0) {
    report({ step: 'completed', percent: 100, message: 'No matching postal codes found in catchment' });
    return buildEmptyResult(datasetName, filters, catchment.totalDevicesVisitedPois);
  }

  // ── Phase 3: Extract individual MAIDs ───────────────────────────────────
  // Same Athena CTEs as catchment, but final SELECT keeps ad_id
  report({ step: 'running_queries', percent: 65, message: 'Extracting individual MAIDs...', detail: `${matchedOrigins.size} matching postal codes` });

  const tableName = getTableName(datasetName);
  const dateConditions: string[] = [];
  if (filters.dateFrom) dateConditions.push(`date >= '${filters.dateFrom}'`);
  if (filters.dateTo) dateConditions.push(`date <= '${filters.dateTo}'`);
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  const maidsQuery = `
    WITH
    poi_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${dateWhere}
    ),
    valid_pings AS (
      SELECT
        t.ad_id,
        t.date,
        t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${tableName} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        ${dateWhere}
    ),
    first_pings AS (
      SELECT
        ad_id,
        date,
        MIN_BY(lat, utc_timestamp) as origin_lat,
        MIN_BY(lng, utc_timestamp) as origin_lng
      FROM valid_pings
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
    LIMIT 500000
  `;

  console.log(`[POSTAL-MAID] Phase 3: Running MAID extraction query...`);
  const maidsRes = await runQuery(maidsQuery);
  console.log(`[POSTAL-MAID] Got ${maidsRes.rows.length} device-origin rows`);
  report({ step: 'running_queries', percent: 78, message: 'Query complete', detail: `${maidsRes.rows.length} device rows` });

  // ── Phase 4: Geocode and match devices to postal codes ──────────────────
  // Same geocoding as catchment (batchReverseGeocode)
  const coordMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
  const deviceOrigins: Array<{ adId: string; coordKey: string; deviceDays: number }> = [];
  let totalDeviceDays = 0;

  for (const row of maidsRes.rows) {
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

  report({ step: 'geocoding', percent: 80, message: 'Reverse geocoding device origins...', detail: `${coordMap.size} unique coordinates` });
  console.log(`[POSTAL-MAID] Phase 4: Geocoding ${coordMap.size} coordinate clusters...`);

  const coordPoints = Array.from(coordMap.values()).map(p => ({
    lat: p.lat,
    lng: p.lng,
    deviceCount: p.deviceCount,
  }));

  const classified = await batchReverseGeocode(coordPoints, { skipCache: true });
  report({ step: 'geocoding', percent: 90, message: 'Geocoding complete' });

  // Build coordKey → postalCode map (accept any classification with a postcode)
  const coordToPostal = new Map<string, string>();
  const coordEntries = Array.from(coordMap.entries());
  for (let i = 0; i < classified.length; i++) {
    const c = classified[i];
    if (c.type === 'geojson_local' || c.type === 'nominatim_match') {
      const [coordKey] = coordEntries[i];
      coordToPostal.set(coordKey, c.postcode.toUpperCase());
    }
  }

  console.log(`[POSTAL-MAID] Geocoded ${coordToPostal.size}/${coordMap.size} coordinates to postal codes`);

  // Match devices to requested postal codes
  report({ step: 'matching', percent: 92, message: 'Matching devices to postal codes...', detail: `${requestedPostalCodes.size} postal codes` });

  const deviceMap = new Map<string, { deviceDays: number; postalCodes: Set<string> }>();
  const postalBreakdown = new Map<string, { devices: Set<string>; deviceDays: number }>();

  // Initialize breakdown for all requested codes
  for (const pc of requestedPostalCodes) {
    postalBreakdown.set(pc, { devices: new Set(), deviceDays: 0 });
  }

  for (const { adId, coordKey, deviceDays } of deviceOrigins) {
    const postalCode = coordToPostal.get(coordKey);
    if (!postalCode || !requestedPostalCodes.has(postalCode)) continue;

    const existing = deviceMap.get(adId);
    if (existing) {
      existing.deviceDays += deviceDays;
      existing.postalCodes.add(postalCode);
    } else {
      deviceMap.set(adId, { deviceDays, postalCodes: new Set([postalCode]) });
    }

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
      totalDevicesInDataset: catchment.totalDevicesVisitedPois,
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
