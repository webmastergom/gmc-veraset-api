/**
 * Postal Code → MAID lookup (catchment = first GPS ping of each device-day).
 *
 * 1. Ensure Athena table + count distinct POI visitors (coverage).
 * 2. Run one Athena query: first ping per day per device, grouped by device + rounded origin.
 * 3. Reverse-geocode those origins and keep ad_ids whose postcode is in the requested set.
 *
 * Optional: set POSTAL_MAID_SQL_ROW_LIMIT (positive int) to cap rows with
 * ORDER BY device_days DESC LIMIT n — biases toward heavy users; omit for full coverage.
 */

import { ensureTableForDataset, runQuery, getTableName, startQueryAsync, checkQueryStatus, fetchQueryResults } from './athena';
import { batchReverseGeocode, setCountryFilter } from './reverse-geocode';
import type { PostalMaidFilters, PostalMaidDevice, PostalMaidResult } from './postal-maid-types';

export type { PostalMaidFilters, PostalMaidResult } from './postal-maid-types';

const ACCURACY_THRESHOLD_METERS = 500;
const COORDINATE_PRECISION = 4; // ~11m resolution

/** Align user input with GeoJSON (e.g. MX 5-digit CP: "5900" → "05900") */
function normalizePostalForCountry(country: string, raw: string): string {
  const cc = country.toUpperCase();
  let s = raw.trim().toUpperCase().replace(/\s+/g, '');
  if (/^[A-Z]{2}-/.test(s)) s = s.slice(3);
  if (cc === 'MX') {
    const digits = s.replace(/\D/g, '');
    if (digits.length >= 1 && digits.length <= 5) return digits.padStart(5, '0');
  }
  return s;
}

/** Progress callback for streaming updates */
export type PostalMaidProgressCallback = (progress: {
  step:
    | 'initializing'
    | 'preparing_table'
    | 'running_queries'
    | 'geocoding'
    | 'matching'
    | 'aggregating'
    | 'completed'
    | 'error';
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

  try {
    setCountryFilter([filters.country.toUpperCase()]);

  const requestedPostalCodes = new Set(
    filters.postalCodes.map(pc => normalizePostalForCountry(filters.country, pc)),
  );

  const tableName = getTableName(datasetName);
  const dateConditions: string[] = [];
  if (filters.dateFrom) dateConditions.push(`date >= '${filters.dateFrom}'`);
  if (filters.dateTo) dateConditions.push(`date <= '${filters.dateTo}'`);
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  const totalQuery = `
    SELECT COUNT(DISTINCT ad_id) as total_devices
    FROM ${tableName}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      ${dateWhere}
  `;

  const sqlRowCap = parseInt(process.env.POSTAL_MAID_SQL_ROW_LIMIT || '', 10);
  const applyRowCap = Number.isFinite(sqlRowCap) && sqlRowCap > 0;
  if (applyRowCap) {
    console.warn(`[POSTAL-MAID] POSTAL_MAID_SQL_ROW_LIMIT=${sqlRowCap}: results biased toward highest device_days rows`);
  }

  report({ step: 'preparing_table', percent: 5, message: 'Preparing Athena table...', detail: tableName });
  await ensureTableForDataset(datasetName);

  const rowOrderingSql = applyRowCap
    ? `ORDER BY device_days DESC LIMIT ${sqlRowCap}`
    : '';

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
    ${rowOrderingSql}
  `;

  // Launch both queries in parallel, poll with progress updates
  report({
    step: 'running_queries',
    percent: 10,
    message: 'Launching Athena queries...',
    detail: 'Starting total count + origin extraction in parallel',
  });

  const [totalQId, maidsQId] = await Promise.all([
    startQueryAsync(totalQuery),
    startQueryAsync(maidsQuery),
  ]);

  // Poll both queries until done, reporting progress
  let totalDone = false, maidsDone = false;
  let totalRes: { rows: Record<string, string>[] } = { rows: [] };
  let maidsRes: { rows: Record<string, string>[] } = { rows: [] };
  const t0 = Date.now();

  while (!totalDone || !maidsDone) {
    await new Promise(r => setTimeout(r, 3000));
    const elapsed = Math.round((Date.now() - t0) / 1000);

    if (!totalDone) {
      const s = await checkQueryStatus(totalQId);
      if (s.state === 'SUCCEEDED') { totalDone = true; totalRes = await fetchQueryResults(totalQId); }
      else if (s.state === 'FAILED') throw new Error(`Total devices query failed: ${s.error}`);
    }
    if (!maidsDone) {
      const s = await checkQueryStatus(maidsQId);
      if (s.state === 'SUCCEEDED') { maidsDone = true; maidsRes = await fetchQueryResults(maidsQId); }
      else if (s.state === 'FAILED') throw new Error(`Origins query failed: ${s.error}`);
    }

    const done = (totalDone ? 1 : 0) + (maidsDone ? 1 : 0);
    const pct = 10 + Math.round((done / 2) * 50);
    report({
      step: 'running_queries',
      percent: pct,
      message: `Athena queries: ${done}/2 complete · ${elapsed}s`,
      detail: `${totalDone ? '✅' : '⏳'} totalDevices  ${maidsDone ? '✅' : '⏳'} origins`,
    });
  }

  const totalDevicesInDataset = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`[POSTAL-MAID] POI visitors: ${totalDevicesInDataset}`);

  if (totalDevicesInDataset === 0) {
    report({ step: 'completed', percent: 100, message: 'No POI visitors in dataset for these filters' });
    return buildEmptyResult(datasetName, filters, 0);
  }
  console.log(`[POSTAL-MAID] device-origin rows: ${maidsRes.rows.length}`);
  report({
    step: 'running_queries',
    percent: 60,
    message: 'Athena queries complete',
    detail: `${maidsRes.rows.length} device-origin rows`,
  });

  // ── Geocode origins and match to requested postal codes ──────────────────
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

  // Pre-aggregate to 1-decimal precision for geocoding (×10-100x fewer points)
  // This is sufficient for postal code matching (~11km resolution)
  const roundedMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
  for (const p of coordMap.values()) {
    const rKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
    const ex = roundedMap.get(rKey);
    if (ex) ex.deviceCount += p.deviceCount;
    else roundedMap.set(rKey, { lat: Math.round(p.lat * 10) / 10, lng: Math.round(p.lng * 10) / 10, deviceCount: p.deviceCount });
  }

  report({ step: 'geocoding', percent: 80, message: 'Reverse geocoding device origins...', detail: `${roundedMap.size} unique coordinates (from ${coordMap.size} at full precision)` });
  console.log(`[POSTAL-MAID] Geocoding ${roundedMap.size} unique rounded coords (from ${coordMap.size} at 4-decimal precision)`);

  const roundedKeys = Array.from(roundedMap.keys());
  const roundedPoints = Array.from(roundedMap.values()).map(p => ({
    lat: p.lat,
    lng: p.lng,
    deviceCount: p.deviceCount,
  }));

  const classified = await batchReverseGeocode(roundedPoints);
  report({ step: 'geocoding', percent: 90, message: 'Geocoding complete' });

  // Build roundedKey → postalCode lookup
  const roundedToPostal = new Map<string, string>();
  for (let i = 0; i < roundedKeys.length && i < classified.length; i++) {
    const c = classified[i];
    if (c.type === 'geojson_local' || c.type === 'nominatim_match') {
      roundedToPostal.set(roundedKeys[i], normalizePostalForCountry(filters.country, c.postcode));
    }
  }

  // Map 4-decimal coordKeys to postal codes via rounded lookup
  const coordToPostal = new Map<string, string>();
  for (const [coordKey, p] of coordMap.entries()) {
    const rKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
    const postal = roundedToPostal.get(rKey);
    if (postal) coordToPostal.set(coordKey, postal);
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
      totalDevicesInDataset: totalDevicesInDataset,
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
  } finally {
    setCountryFilter(null);
  }
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
      postalCode: normalizePostalForCountry(filters.country, pc),
      devices: 0,
      deviceDays: 0,
    })),
  };
}
