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
import type {
  PostalMaidFilters,
  PostalMaidDevice,
  PostalMaidResult,
  ZipSignature,
  RegionSummary,
  FullSchemaEnrichment,
} from './postal-maid-types';

export type { PostalMaidFilters, PostalMaidResult } from './postal-maid-types';

/**
 * Sniff the dataset for FULL schema (geo_fields populated). Cheap query,
 * scans 1 row at most. Returns true when we should take the fast path.
 */
async function detectFullSchema(tableName: string): Promise<boolean> {
  try {
    const sql = `
      SELECT geo_fields['zipcode'] AS sample_zip
      FROM ${tableName}
      WHERE geo_fields['zipcode'] IS NOT NULL
        AND geo_fields['zipcode'] != ''
      LIMIT 1
    `;
    const res = await runQuery(sql);
    const has = res.rows.length > 0 && !!res.rows[0]?.sample_zip;
    console.log(`[POSTAL-MAID] FULL schema sniff: ${has ? 'YES' : 'NO'}`);
    return has;
  } catch (e: any) {
    console.warn('[POSTAL-MAID] FULL schema sniff failed:', e.message);
    return false;
  }
}

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
 * FULL-schema fast path. Skips Node-side reverse-geocoding entirely:
 * uses geo_fields['zipcode'] from each ping, computes the residential
 * origin via first-ping-per-day (same semantics as BASIC), and returns
 * an enriched result with region/city/quality/persistence/H3 hotspots
 * derived from the same Athena pass.
 */
async function analyzeFullSchemaFast(
  datasetName: string,
  filters: PostalMaidFilters,
  onProgress: PostalMaidProgressCallback,
  totalDevicesInDataset: number,
  tableName: string,
  dateWhere: string,
  requestedPostalCodes: Set<string>,
): Promise<PostalMaidResult> {
  const report = onProgress;
  const requestedList = Array.from(requestedPostalCodes);
  const requestedSql = requestedList.map((c) => `'${c.replace(/'/g, "''")}'`).join(', ');

  // Single Athena query: device-level matched origins.
  // Returns one row per (ad_id, zip) with all the metadata we need to
  // build both the device list AND the per-ZIP signature in Node.
  const devicesQuery = `
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
        TRY(t.geo_fields['zipcode']) as zip,
        TRY(t.geo_fields['region']) as region,
        TRY(t.geo_fields['city']) as city,
        TRY(t.geo_fields['h3_res10']) as h3,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng,
        IF(TRY(t.quality_fields['ping_origin_type']) = 'gps', 1.0, 0.0) as is_gps,
        TRY_CAST(t.quality_fields['ping_circle_score'] AS DOUBLE) as circle_score
      FROM ${tableName} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        AND TRY(t.geo_fields['zipcode']) IS NOT NULL
        AND TRY(t.geo_fields['zipcode']) != ''
        ${dateWhere}
    ),
    first_ping_per_day AS (
      SELECT
        ad_id,
        date,
        MIN_BY(zip, utc_timestamp) as zip,
        MIN_BY(region, utc_timestamp) as region,
        MIN_BY(city, utc_timestamp) as city,
        MIN_BY(h3, utc_timestamp) as h3,
        MIN_BY(lat, utc_timestamp) as lat,
        MIN_BY(lng, utc_timestamp) as lng,
        HOUR(MIN(utc_timestamp)) as origin_hour,
        DAY_OF_WEEK(MIN(utc_timestamp)) as origin_dow,
        MAX(IF(HOUR(utc_timestamp) >= 22 OR HOUR(utc_timestamp) <= 6, 1, 0)) as has_overnight,
        AVG(is_gps) as gps_share,
        AVG(circle_score) as avg_circle
      FROM valid_pings
      GROUP BY ad_id, date
    ),
    matched AS (
      SELECT * FROM first_ping_per_day
      WHERE zip IN (${requestedSql})
    )
    SELECT
      ad_id,
      zip,
      ANY_VALUE(region) as region,
      ANY_VALUE(city) as city,
      COUNT(*) as device_days,
      MAX(has_overnight) as has_overnight,
      AVG(gps_share) as avg_gps,
      AVG(avg_circle) as avg_circle,
      AVG(lat) as avg_lat,
      AVG(lng) as avg_lng,
      COUNT_IF(origin_hour BETWEEN 5 AND 10) as morning_dd,
      COUNT_IF(origin_hour BETWEEN 11 AND 13) as midday_dd,
      COUNT_IF(origin_hour BETWEEN 14 AND 17) as afternoon_dd,
      COUNT_IF(origin_hour BETWEEN 18 AND 21) as evening_dd,
      COUNT_IF(origin_hour >= 22 OR origin_hour <= 4) as night_dd,
      COUNT_IF(origin_dow IN (6, 7)) as weekend_dd
    FROM matched
    GROUP BY ad_id, zip
  `;

  // Parallel: H3 hotspot aggregation (sub-zip ~70m precision).
  const h3Query = `
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
        t.ad_id, t.date, t.utc_timestamp,
        TRY(t.geo_fields['zipcode']) as zip,
        TRY(t.geo_fields['h3_res10']) as h3,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${tableName} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        AND TRY(t.geo_fields['zipcode']) IN (${requestedSql})
        AND TRY(t.geo_fields['h3_res10']) IS NOT NULL
        ${dateWhere}
    ),
    first_ping_per_day AS (
      SELECT
        ad_id, date,
        MIN_BY(zip, utc_timestamp) as zip,
        MIN_BY(h3, utc_timestamp) as h3,
        MIN_BY(lat, utc_timestamp) as lat,
        MIN_BY(lng, utc_timestamp) as lng
      FROM valid_pings
      GROUP BY ad_id, date
    )
    SELECT
      zip, h3,
      COUNT(DISTINCT ad_id) as devices,
      COUNT(*) as pings,
      AVG(lat) as lat,
      AVG(lng) as lng
    FROM first_ping_per_day
    GROUP BY zip, h3
    HAVING COUNT(*) >= 2
  `;

  report({
    step: 'running_queries',
    percent: 15,
    message: '⚡ FULL schema fast-path · launching enriched Athena queries',
    detail: '2 parallel queries: device-level + H3 hotspots',
  });

  const [devicesQId, h3QId] = await Promise.all([
    startQueryAsync(devicesQuery),
    startQueryAsync(h3Query),
  ]);

  // Poll both
  let devicesDone = false, h3Done = false;
  let devicesRes: { rows: Record<string, string>[] } = { rows: [] };
  let h3Res: { rows: Record<string, string>[] } = { rows: [] };
  const t0 = Date.now();
  while (!devicesDone || !h3Done) {
    await new Promise((r) => setTimeout(r, 3000));
    const elapsed = Math.round((Date.now() - t0) / 1000);
    if (!devicesDone) {
      const s = await checkQueryStatus(devicesQId);
      if (s.state === 'SUCCEEDED') { devicesDone = true; devicesRes = await fetchQueryResults(devicesQId); }
      else if (s.state === 'FAILED') throw new Error(`Devices query failed: ${s.error}`);
    }
    if (!h3Done) {
      const s = await checkQueryStatus(h3QId);
      if (s.state === 'SUCCEEDED') { h3Done = true; h3Res = await fetchQueryResults(h3QId); }
      else if (s.state === 'FAILED') {
        // H3 is optional enrichment — don't block on it.
        console.warn(`[POSTAL-MAID-FAST] H3 query failed: ${s.error} (continuing without hotspots)`);
        h3Done = true;
      }
    }
    const done = (devicesDone ? 1 : 0) + (h3Done ? 1 : 0);
    const pct = 15 + Math.round((done / 2) * 60);
    report({
      step: 'running_queries',
      percent: pct,
      message: `⚡ Fast-path · ${done}/2 queries done · ${elapsed}s`,
      detail: `${devicesDone ? '✅' : '⏳'} devices  ${h3Done ? '✅' : '⏳'} H3 hotspots`,
    });
  }

  // ── Aggregate in Node ─────────────────────────────────────────────
  report({ step: 'aggregating', percent: 80, message: '⚡ Building enriched signatures…' });

  // Per-device buckets
  const deviceMap = new Map<string, {
    deviceDays: number;
    postalCodes: Set<string>;
    region: string | null;
    city: string | null;
    hasOvernight: boolean;
    avgGps: number;
    avgCircle: number;
    samples: number;
  }>();

  // Per-zip accumulators
  type ZipAcc = {
    zip: string;
    region: Map<string, number>;
    city: Map<string, number>;
    deviceDays: number;
    deviceSet: Set<string>;
    persistence: { onceOnly: number; casual: number; regular: number; resident: number };
    hourBuckets: { morning: number; midday: number; afternoon: number; evening: number; night: number };
    weekendDD: number;
    overnightDays: number;
    sumGps: number;
    sumCircle: number;
    sumLat: number;
    sumLng: number;
    rows: number;
  };
  const zipMap = new Map<string, ZipAcc>();
  const newZipAcc = (zip: string): ZipAcc => ({
    zip,
    region: new Map(),
    city: new Map(),
    deviceDays: 0,
    deviceSet: new Set(),
    persistence: { onceOnly: 0, casual: 0, regular: 0, resident: 0 },
    hourBuckets: { morning: 0, midday: 0, afternoon: 0, evening: 0, night: 0 },
    weekendDD: 0,
    overnightDays: 0,
    sumGps: 0, sumCircle: 0, sumLat: 0, sumLng: 0, rows: 0,
  });

  let totalDeviceDays = 0;

  for (const row of devicesRes.rows) {
    const adId = String(row.ad_id);
    const zip = String(row.zip);
    if (!adId || !zip) continue;
    const dd = parseInt(String(row.device_days)) || 0;
    const region = (row.region || '').trim() || null;
    const city = (row.city || '').trim() || null;
    const hasOvernight = String(row.has_overnight) === '1' || String(row.has_overnight).toLowerCase() === 'true';
    const avgGps = parseFloat(String(row.avg_gps)) || 0;
    const avgCircle = parseFloat(String(row.avg_circle)) || 0;
    const lat = parseFloat(String(row.avg_lat)) || 0;
    const lng = parseFloat(String(row.avg_lng)) || 0;
    const morningDd = parseInt(String(row.morning_dd)) || 0;
    const middayDd = parseInt(String(row.midday_dd)) || 0;
    const afternoonDd = parseInt(String(row.afternoon_dd)) || 0;
    const eveningDd = parseInt(String(row.evening_dd)) || 0;
    const nightDd = parseInt(String(row.night_dd)) || 0;
    const weekendDd = parseInt(String(row.weekend_dd)) || 0;

    totalDeviceDays += dd;

    // Device-level
    let dev = deviceMap.get(adId);
    if (!dev) {
      dev = {
        deviceDays: 0, postalCodes: new Set(),
        region: null, city: null,
        hasOvernight: false,
        avgGps: 0, avgCircle: 0, samples: 0,
      };
      deviceMap.set(adId, dev);
    }
    dev.deviceDays += dd;
    dev.postalCodes.add(zip);
    if (region && !dev.region) dev.region = region;
    if (city && !dev.city) dev.city = city;
    dev.hasOvernight = dev.hasOvernight || hasOvernight;
    // Weighted average using device_days as weight
    const w = dd;
    dev.avgGps = (dev.avgGps * dev.samples + avgGps * w) / (dev.samples + w || 1);
    dev.avgCircle = (dev.avgCircle * dev.samples + avgCircle * w) / (dev.samples + w || 1);
    dev.samples += w;

    // Zip-level
    let z = zipMap.get(zip);
    if (!z) { z = newZipAcc(zip); zipMap.set(zip, z); }
    if (region) z.region.set(region, (z.region.get(region) || 0) + dd);
    if (city) z.city.set(city, (z.city.get(city) || 0) + dd);
    z.deviceDays += dd;
    z.deviceSet.add(adId);
    z.hourBuckets.morning += morningDd;
    z.hourBuckets.midday += middayDd;
    z.hourBuckets.afternoon += afternoonDd;
    z.hourBuckets.evening += eveningDd;
    z.hourBuckets.night += nightDd;
    z.weekendDD += weekendDd;
    if (hasOvernight) z.overnightDays += dd; // approximation
    z.sumGps += avgGps * dd;
    z.sumCircle += avgCircle * dd;
    z.sumLat += lat * dd;
    z.sumLng += lng * dd;
    z.rows += 1;
    // Persistence buckets (per device-zip pair)
    if (dd === 1) z.persistence.onceOnly++;
    else if (dd <= 7) z.persistence.casual++;
    else if (dd <= 30) z.persistence.regular++;
    else z.persistence.resident++;
  }

  // Top H3 cells per zip
  const h3PerZip = new Map<string, Array<{ h3: string; lat: number; lng: number; devices: number; pings: number }>>();
  for (const row of h3Res.rows) {
    const zip = String(row.zip);
    if (!zip) continue;
    const arr = h3PerZip.get(zip) || [];
    arr.push({
      h3: String(row.h3),
      lat: parseFloat(String(row.lat)) || 0,
      lng: parseFloat(String(row.lng)) || 0,
      devices: parseInt(String(row.devices)) || 0,
      pings: parseInt(String(row.pings)) || 0,
    });
    h3PerZip.set(zip, arr);
  }
  for (const arr of h3PerZip.values()) arr.sort((a, b) => b.pings - a.pings);

  // ── Build PostalMaidDevice list with FULL extras ────────────────
  const qualityTier = (gps: number, circle: number): 'high' | 'medium' | 'low' => {
    if (gps > 0.7 && circle < 1) return 'high';
    if (gps > 0.4) return 'medium';
    return 'low';
  };
  const devices: PostalMaidDevice[] = Array.from(deviceMap.entries())
    .map(([adId, d]) => ({
      adId,
      deviceDays: d.deviceDays,
      postalCodes: Array.from(d.postalCodes),
      region: d.region || undefined,
      city: d.city || undefined,
      qualityTier: qualityTier(d.avgGps, d.avgCircle),
      overnightPresence: d.hasOvernight,
    }))
    .sort((a, b) => b.deviceDays - a.deviceDays);

  // ── Build ZipSignature[] ────────────────────────────────────────
  const PEAK_BUCKETS: Array<keyof ZipAcc['hourBuckets']> = ['morning','midday','afternoon','evening','night'];
  const zipSignatures: ZipSignature[] = [];
  for (const z of zipMap.values()) {
    const totalDD = z.deviceDays || 1;
    // Mode region (highest device-days)
    let topRegion: string | null = null; let topRegionDD = 0;
    for (const [r, c] of z.region.entries()) if (c > topRegionDD) { topRegion = r; topRegionDD = c; }
    // Top 3 cities
    const topCities = Array.from(z.city.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3)
      .map(([city, devices]) => ({ city, devices }));
    // Peak hour
    let peakBucket: typeof PEAK_BUCKETS[number] = 'midday';
    let peakShare = 0;
    for (const b of PEAK_BUCKETS) {
      const share = z.hourBuckets[b] / totalDD;
      if (share > peakShare) { peakShare = share; peakBucket = b; }
    }
    const gpsShare = z.sumGps / totalDD;
    const avgCircle = z.sumCircle / totalDD;
    const tier: ZipSignature['qualityTier'] =
      gpsShare > 0.65 && avgCircle < 1.2 ? 'high' :
      gpsShare > 0.35 ? 'mixed' : 'low';
    zipSignatures.push({
      postalCode: z.zip,
      region: topRegion,
      topCities,
      devices: z.deviceSet.size,
      deviceDays: z.deviceDays,
      hourBuckets: z.hourBuckets,
      peakHourBucket: peakBucket,
      weekendShare: z.weekendDD / totalDD,
      overnightShare: z.overnightDays / totalDD,
      qualityTier: tier,
      gpsShare,
      avgCircleScore: avgCircle,
      persistence: z.persistence,
      centroid: { lat: z.sumLat / totalDD, lng: z.sumLng / totalDD },
      topH3Cells: (h3PerZip.get(z.zip) || []).slice(0, 5),
    });
  }
  zipSignatures.sort((a, b) => b.devices - a.devices);

  // Region summary
  const regionAgg = new Map<string, { devices: Set<string>; zips: Set<string> }>();
  for (const sig of zipSignatures) {
    if (!sig.region) continue;
    let r = regionAgg.get(sig.region);
    if (!r) { r = { devices: new Set(), zips: new Set() }; regionAgg.set(sig.region, r); }
    r.zips.add(sig.postalCode);
  }
  // Need device list per region — re-walk deviceMap
  for (const [adId, d] of deviceMap.entries()) {
    if (d.region) {
      const r = regionAgg.get(d.region);
      if (r) r.devices.add(adId);
    }
  }
  const totalMatched = devices.length || 1;
  const regionSummary: RegionSummary[] = Array.from(regionAgg.entries())
    .map(([region, r]) => ({
      region,
      devices: r.devices.size,
      zips: r.zips.size,
      shareOfTotal: r.devices.size / totalMatched,
    }))
    .sort((a, b) => b.devices - a.devices);

  // Quality histogram
  const qualityHistogram = { high: 0, medium: 0, low: 0 };
  for (const d of devices) {
    if (d.qualityTier) qualityHistogram[d.qualityTier]++;
  }

  // PostalCodeBreakdown (compatible with existing schema)
  const breakdownMap = new Map<string, { devices: Set<string>; deviceDays: number }>();
  for (const pc of requestedPostalCodes) breakdownMap.set(pc, { devices: new Set(), deviceDays: 0 });
  for (const d of devices) {
    for (const pc of d.postalCodes) {
      const b = breakdownMap.get(pc);
      if (b) { b.devices.add(d.adId); b.deviceDays += d.deviceDays; }
    }
  }
  const postalCodeBreakdown = Array.from(breakdownMap.entries())
    .map(([postalCode, data]) => ({ postalCode, devices: data.devices.size, deviceDays: data.deviceDays }))
    .sort((a, b) => b.devices - a.devices);

  const matchedDeviceDays = devices.reduce((s, d) => s + d.deviceDays, 0);
  const postalCodesWithDevices = postalCodeBreakdown.filter((p) => p.devices > 0).length;
  const topPostal = postalCodeBreakdown[0];

  console.log(
    `[POSTAL-MAID-FAST] Done: ${devices.length} MAIDs, ${zipSignatures.length} ZIP signatures, ${regionSummary.length} regions, ${Array.from(h3PerZip.values()).reduce((s,a)=>s+a.length,0)} H3 cells`,
  );
  report({ step: 'completed', percent: 100, message: '⚡ FULL fast-path complete', detail: `${devices.length} MAIDs · ${zipSignatures.length} ZIP signatures` });

  const enrichment: FullSchemaEnrichment = {
    detectedAt: new Date().toISOString(),
    zipSignatures,
    regionSummary,
    qualityHistogram,
  };

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology: {
      approach: 'first_ping_per_day_geo_fields',
      description: 'FULL schema fast path — geo_fields[zipcode] used directly, no Node-side reverse geocoding. Same residential semantics (first ping per day) as BASIC.',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
      fastPath: true,
    },
    coverage: {
      totalDevicesInDataset,
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
    fullSchema: enrichment,
  };
}

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

  // ── Detect FULL schema and branch ──────────────────────────────────
  report({ step: 'preparing_table', percent: 3, message: 'Preparing Athena table...', detail: tableName });
  await ensureTableForDataset(datasetName);

  const isFull = await detectFullSchema(tableName);
  if (isFull) {
    report({
      step: 'preparing_table',
      percent: 8,
      message: '⚡ FULL schema detected — taking fast path',
      detail: 'geo_fields populated · skipping Node reverse geocode',
    });

    // Need totalDevicesInDataset for coverage stats — quick query first.
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
    const totalRes = await runQuery(totalQuery);
    const totalDevicesInDataset = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
    if (totalDevicesInDataset === 0) {
      report({ step: 'completed', percent: 100, message: 'No POI visitors in dataset' });
      return buildEmptyResult(datasetName, filters, 0);
    }

    return await analyzeFullSchemaFast(
      datasetName,
      filters,
      report,
      totalDevicesInDataset,
      tableName,
      dateWhere,
      requestedPostalCodes,
    );
  }

  console.log('[POSTAL-MAID] BASIC schema path (Node reverse-geocode)');
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

  // (Table already ensured before the FULL-schema sniff above.)

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
