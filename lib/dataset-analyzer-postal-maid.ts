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

import { ensureTableForDataset, runQuery, getTableName, startQueryAsync, checkQueryStatus, fetchQueryResults, fetchQueryResultsViaS3, streamQueryResultsViaS3 } from './athena';
import { batchReverseGeocode, setCountryFilter } from './reverse-geocode';
import { localTimestamp, tzForCountry } from './timezones';
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
  /** When set, the source is a megajob: tableExpr is a UNION ALL across
   *  sub-jobs (used in FROM clauses) and maidsTableName is the pre-built
   *  external table holding the megajob's consolidated MAIDs CSV. The
   *  poi_visitors CTE then becomes a simple SELECT from that table —
   *  skipping the per-table CROSS JOIN UNNEST(poi_ids) scan. */
  megajobOpts?: { tableExpr: string; maidsTableName: string },
): Promise<PostalMaidResult> {
  const report = onProgress;
  const requestedList = Array.from(requestedPostalCodes);
  const requestedSql = requestedList.map((c) => `'${c.replace(/'/g, "''")}'`).join(', ');
  // Convert UTC timestamps to local time for hour-bucket / overnight /
  // weekend extraction. MX afternoon visits would otherwise show as
  // UTC night and skew the per-ZIP peak hour.
  const localTz = tzForCountry(filters.country);
  const lts = (col: string) => localTimestamp(col, localTz);
  if (localTz !== 'UTC') {
    console.log(`[POSTAL-MAID-FAST] Country=${filters.country} → tz=${localTz}`);
  }

  // SQL parameterization: when a megajob is the source, we read pings
  // from the UNION-ALL of all its sub-job tables and use the consolidated
  // MAIDs CSV as the POI-visitor list (skipping the heavy poi_ids scan).
  const fromExpr = megajobOpts?.tableExpr ?? tableName;
  const poiVisitorsCte = megajobOpts
    ? `SELECT ad_id FROM ${megajobOpts.maidsTableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''`
    : `SELECT DISTINCT ad_id
       FROM ${tableName}
       CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
       WHERE poi_id IS NOT NULL AND poi_id != ''
         AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
         ${dateWhere}`;

  // Single Athena query: device-level matched origins.
  // Returns one row per (ad_id, zip) with all the metadata we need to
  // build both the device list AND the per-ZIP signature in Node.
  const devicesQuery = `
    WITH
    poi_visitors AS (
      ${poiVisitorsCte}
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
      FROM ${fromExpr} t
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
        HOUR(${lts('MIN(utc_timestamp)')}) as origin_hour,
        DAY_OF_WEEK(${lts('MIN(utc_timestamp)')}) as origin_dow,
        MAX(IF(HOUR(${lts('utc_timestamp')}) >= 22 OR HOUR(${lts('utc_timestamp')}) <= 6, 1, 0)) as has_overnight,
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
      ${poiVisitorsCte}
    ),
    valid_pings AS (
      SELECT
        t.ad_id, t.date, t.utc_timestamp,
        TRY(t.geo_fields['zipcode']) as zip,
        TRY(t.geo_fields['h3_res10']) as h3,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${fromExpr} t
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
      // devicesQuery returns one row per (ad_id, zip) — can be millions.
      // Stream from S3 (single GetObject) instead of paginating the
      // GetQueryResults API (1000 rows/page, multi-minute round-trips).
      if (s.state === 'SUCCEEDED') {
        devicesDone = true;
        report({
          step: 'running_queries',
          percent: 60,
          message: '⚡ Devices query done · streaming result CSV from S3…',
          detail: 'avoiding paginated GetQueryResults API (slow for million-row results)',
        });
        devicesRes = await fetchQueryResultsViaS3(devicesQId);
      }
      else if (s.state === 'FAILED') throw new Error(`Devices query failed: ${s.error}`);
    }
    if (!h3Done) {
      const s = await checkQueryStatus(h3QId);
      // H3 hotspots are aggregated per (zip, h3) — bounded result set,
      // but we use the S3 path anyway for consistency.
      if (s.state === 'SUCCEEDED') { h3Done = true; h3Res = await fetchQueryResultsViaS3(h3QId); }
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
 * Megajob entry: treats N synced sub-jobs as one virtual dataset by
 * UNION-ALL'ing their tables, and re-uses the consolidated MAIDs CSV
 * (config/mega-reports/{id}/maids.csv) as the POI-visitor set. Skips
 * the per-table CROSS JOIN UNNEST(poi_ids) scan — typically ~2× faster
 * than re-running the POI filter from scratch.
 *
 * Currently FULL-schema only (most production datasets are FULL). If
 * the megajob's first sub-job is BASIC, throws — user can fall back to
 * single-dataset ZCS instead.
 */
export async function analyzePostalMaidMegaJob(
  megaJobId: string,
  filters: PostalMaidFilters,
  onProgress?: PostalMaidProgressCallback,
): Promise<PostalMaidResult> {
  const report = onProgress || (() => {});
  const { getMegaJob } = await import('./mega-jobs');
  const { getJob } = await import('./jobs');
  const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
  const { s3Client, BUCKET } = await import('./s3-config');

  report({ step: 'initializing', percent: 0, message: 'Loading megajob metadata...' });
  const megaJob = await getMegaJob(megaJobId);
  if (!megaJob) throw new Error(`Megajob ${megaJobId} not found`);

  // Validate consolidation has run — we re-use the MAIDs CSV produced there.
  const maidsCsvKey = megaJob.consolidatedReports?.maids;
  if (!maidsCsvKey) {
    throw new Error(
      'This megajob has not been consolidated yet. Run consolidation first ' +
      '(it produces the MAIDs CSV that ZCS reuses to skip the heavy POI scan).',
    );
  }

  // Resolve synced sub-jobs → Athena table names.
  if (!megaJob.subJobIds?.length) throw new Error(`Megajob ${megaJobId} has no sub-jobs`);
  const subJobs = (await Promise.all(megaJob.subJobIds.map((j) => getJob(j))))
    .filter((j): j is NonNullable<typeof j> => j !== null && j.status === 'SUCCESS' && !!j.syncedAt);
  if (subJobs.length === 0) {
    throw new Error('Megajob has no synced sub-jobs to query');
  }
  const datasetNames = subJobs
    .map((j) => j.s3DestPath?.replace(/\/$/, '').split('/').pop()!)
    .filter(Boolean);
  // Parallelize per-sub-job ensureTableForDataset — sequential was eating
  // 5-15s on big megajobs (one HEAD + possible CREATE EXTERNAL TABLE per
  // sub-job) before Athena even started. Promise.all collapses that.
  await Promise.all(datasetNames.map((ds) => ensureTableForDataset(ds)));
  const tableNames = datasetNames.map((ds) => getTableName(ds));

  // Detect FULL schema. Prefer the job-level metadata (schema field set
  // when the megajob was created), then fall back to an Athena sniff in
  // case the metadata is missing on legacy jobs. Raw per-sub-job logging
  // so when we hit weird readings we have ground truth.
  const declaredSchemas = subJobs
    .map((j) => (j as any)?.schema || '')
    .filter((s) => typeof s === 'string') as string[];
  for (let i = 0; i < subJobs.length; i++) {
    const sj: any = subJobs[i];
    console.log(
      `[POSTAL-MAID-MEGAJOB] sub-job[${i}] id=${sj.jobId} name="${sj.name}" ` +
      `schema=${JSON.stringify(sj.schema)} status=${sj.status} ` +
      `keys=[${Object.keys(sj).slice(0, 30).join(',')}]`,
    );
  }
  const declaredFull = declaredSchemas.length > 0 && declaredSchemas.every((s) => s === 'FULL' || s === 'ENHANCED');
  const declaredBasic = declaredSchemas.length > 0 && declaredSchemas.every((s) => s === 'BASIC');
  // Megajob-level schema hint — set on the sourceScope when the megajob
  // was created via auto-split. Trust this BEFORE sniffing: the megajob
  // creator declared the schema explicitly. Only the sub-jobs themselves
  // can override (declaredFull / declaredBasic), because they reflect
  // what was ACTUALLY synced.
  const megaJobScopeSchema = megaJob.sourceScope?.schema;
  const megaJobSaysFull = megaJobScopeSchema === 'FULL' || megaJobScopeSchema === 'ENHANCED';
  let isFull = declaredFull || (declaredSchemas.length === 0 && megaJobSaysFull);
  let usedSniff = false;
  if (!isFull && !declaredBasic && !megaJobSaysFull) {
    // No schema hint anywhere — sniff each sub-job's table until one
    // returns a row with a non-null zipcode. As soon as ONE sub-job
    // has geo_fields populated, treat the whole megajob as FULL —
    // the SQL TRY(...) wrappers handle missing fields gracefully.
    usedSniff = true;
    for (const t of tableNames) {
      // eslint-disable-next-line no-await-in-loop
      if (await detectFullSchema(t)) { isFull = true; break; }
    }
  }
  console.log(
    `[POSTAL-MAID-MEGAJOB] schema check: declared=[${declaredSchemas.join(',')}] → ` +
    `${declaredFull ? 'FULL (metadata)' : declaredBasic ? 'BASIC (metadata)' : 'sniff'} ` +
    `→ isFull=${isFull}`,
  );
  // Surface the schema decision so the user can see WHY we're taking
  // a particular path. The fast path (FULL) skips Node geocoding —
  // it filters geo_fields['zipcode'] IN (target_zips) directly in SQL.
  // The BASIC path has to extract lat/lng for every device-day and
  // reverse-geocode in Node, which is much heavier.
  const schemaSource = declaredFull
    ? 'FULL (sub-jobs declared)'
    : declaredBasic
      ? 'BASIC (sub-jobs declared)'
      : megaJobSaysFull && isFull
        ? `FULL (megajob.sourceScope.schema=${megaJobScopeSchema})`
        : isFull
          ? 'FULL (sniffed)'
          : 'BASIC (sniffed/fallback)';
  report({
    step: 'preparing_table',
    percent: 6,
    message: isFull
      ? `🔍 Schema: ${schemaSource} → fast path via geo_fields[zipcode]`
      : `🔍 Schema: ${schemaSource} → BASIC path with Node reverse-geocode (slower)`,
    detail: isFull
      ? 'Direct SQL filter — no Node geocoding needed.'
      : `Sub-job schemas: [${declaredSchemas.join(', ') || '(none declared)'}]. ` +
        'If the data IS FULL, declare schema=FULL on the sub-jobs to skip the slow BASIC path.',
  });
  // BASIC megajobs are now SUPPORTED (lat/lng + Node reverse-geocode path).
  // We dispatch to analyzePostalMaid with megajobOpts populated. Only
  // throw when even sniff returned no geo_fields AND metadata was unknown.
  if (!isFull && usedSniff && !declaredBasic) {
    throw new Error(
      'Megajob schema unknown — neither metadata nor a sample of the ' +
      `tables (${tableNames.length} sub-jobs) found geo_fields[zipcode] ` +
      'or a declared schema. Sub-jobs may be empty or the data may not ' +
      'have synced yet.',
    );
  }

  try {
    setCountryFilter([filters.country.toUpperCase()]);
    const requestedPostalCodes = new Set(
      filters.postalCodes.map((pc) => normalizePostalForCountry(filters.country, pc)),
    );

    // ── Materialize the consolidated MAIDs CSV as an Athena external table ──
    // The CSV is at `athena-results/<queryId>.csv` (single file). Athena's
    // external-table LOCATION wants a directory containing only the file,
    // so we server-side-copy the CSV to a fresh prefix first.
    report({
      step: 'preparing_table',
      percent: 5,
      message: '⚡ Reusing consolidated MAIDs (β fast path)',
      detail: `${subJobs.length} sub-jobs · maids = ${maidsCsvKey}`,
    });
    const ts = Date.now();
    const safeId = megaJobId.replace(/[^a-z0-9_]/gi, '_').slice(0, 32);
    const maidsTableName = `zcs_maids_${safeId}_${ts}`;
    const maidsPrefix = `athena-temp/zcs-maids/${safeId}/${ts}/`;
    const maidsObjectKey = `${maidsPrefix}data.csv`;
    // CopyObject must complete before the DDL (LOCATION points at the new
    // prefix), but the DDL is fast — we can avoid running a separate
    // COUNT(*) by trusting the CSV exists (it was produced by a prior
    // consolidation run, validated then). totalDevicesInDataset is only
    // used for the coverage % stat at the end; we'll compute it inline
    // from the main query's results later if we need it.
    await s3Client.send(
      new CopyObjectCommand({
        Bucket: BUCKET,
        Key: maidsObjectKey,
        CopySource: `${BUCKET}/${maidsCsvKey}`,
      }),
    );
    await runQuery(`
      CREATE EXTERNAL TABLE IF NOT EXISTS ${maidsTableName} (ad_id STRING)
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
      STORED AS TEXTFILE
      LOCATION 's3://${BUCKET}/${maidsPrefix}'
      TBLPROPERTIES ('skip.header.line.count' = '1')
    `);
    console.log(`[POSTAL-MAID-MEGAJOB] Materialized ${maidsTableName} from ${maidsCsvKey}`);

    // ── totalDevicesInDataset — use the MAIDs count directly. No need
    //    for a separate query: it's the total POI visitors of this megajob.
    const totalRes = await runQuery(`SELECT COUNT(*) as n FROM ${maidsTableName}`);
    const totalDevicesInDataset = parseInt(String(totalRes.rows[0]?.n)) || 0;
    if (totalDevicesInDataset === 0) {
      report({ step: 'completed', percent: 100, message: 'Megajob has no MAIDs to analyze' });
      return buildEmptyResult(megaJob.name || megaJobId, filters, 0);
    }

    // ── Build the UNION ALL of sub-job tables. Different column sets per
    //    schema mode: FULL needs geo_fields + quality_fields; BASIC just
    //    needs lat/lng + horizontal_accuracy. (poi_ids is NOT needed in
    //    either case — we use the pre-computed MAIDs CSV instead.)
    const fullColumns = 'ad_id, date, utc_timestamp, geo_fields, quality_fields, latitude, longitude, horizontal_accuracy';
    const basicColumns = 'ad_id, date, utc_timestamp, latitude, longitude, horizontal_accuracy';
    const tableExpr = `(
      ${tableNames
        .map((t) => `SELECT ${isFull ? fullColumns : basicColumns} FROM ${t}`)
        .join('\n      UNION ALL\n      ')}
    )`;

    // CRITICAL: when the user leaves date fields blank, fall back to the
    // megajob's CATALOG range. Without this, Athena scans EVERY partition
    // of every sub-job table (potentially many months of data the megajob
    // doesn't even cover) and the run blows past the Vercel 5-min cap.
    //
    // Three sources, in order of preference:
    //   1. user input (filters.dateFrom / filters.dateTo)
    //   2. megajob.sourceScope.dateRange — set when megajob was created
    //      via auto-split flow. Old / imported megajobs may not have it.
    //   3. derived = min(subJob.dateRange.from), max(subJob.dateRange.to)
    //      across all synced sub-jobs. ALWAYS available for any megajob
    //      whose sub-jobs have synced data — the safety net that ensures
    //      we never run a "no date range, scan everything" query when we
    //      could have computed one for free.
    let catalogFrom = megaJob.sourceScope?.dateRange?.from;
    let catalogTo = megaJob.sourceScope?.dateRange?.to;
    let catalogSource: 'sourceScope' | 'derivedFromSubJobs' | 'none' = catalogFrom || catalogTo
      ? 'sourceScope'
      : 'none';
    if (!catalogFrom || !catalogTo) {
      let minFrom = '';
      let maxTo = '';
      for (const sj of subJobs) {
        const r = (sj as any)?.dateRange as { from?: string; to?: string } | undefined;
        if (r?.from && (!minFrom || r.from < minFrom)) minFrom = r.from;
        if (r?.to && (!maxTo || r.to > maxTo)) maxTo = r.to;
      }
      if (minFrom && !catalogFrom) catalogFrom = minFrom;
      if (maxTo && !catalogTo) catalogTo = maxTo;
      if ((minFrom || maxTo) && catalogSource === 'none') catalogSource = 'derivedFromSubJobs';
      if (catalogSource === 'derivedFromSubJobs') {
        console.log(
          `[POSTAL-MAID-MEGAJOB] sourceScope.dateRange missing — derived from sub-jobs: ${minFrom}..${maxTo}`,
        );
      }
    }
    const effectiveFrom = filters.dateFrom || catalogFrom;
    const effectiveTo = filters.dateTo || catalogTo;
    const dateConditions: string[] = [];
    if (effectiveFrom) dateConditions.push(`date >= '${effectiveFrom}'`);
    if (effectiveTo) dateConditions.push(`date <= '${effectiveTo}'`);
    const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';
    const userSupplied = !!(filters.dateFrom || filters.dateTo);
    if (effectiveFrom || effectiveTo) {
      console.log(
        `[POSTAL-MAID-MEGAJOB] partition prune: date >= ${effectiveFrom || '∞'} AND date <= ${effectiveTo || '∞'} ` +
          `(source=${userSupplied ? 'user' : catalogSource})`,
      );
      let message: string;
      let detail: string;
      if (userSupplied) {
        message = `📅 Athena partitions filtered: ${effectiveFrom || '∞'} → ${effectiveTo || '∞'}`;
        detail = 'User-supplied date range will be applied to all sub-job tables.';
      } else if (catalogSource === 'sourceScope') {
        message = `📅 Server applied catalog date range: ${effectiveFrom} → ${effectiveTo}`;
        detail = 'You left dates blank → using megaJob.sourceScope.dateRange for partition pruning.';
      } else {
        message = `📅 Date range derived from sub-jobs: ${effectiveFrom} → ${effectiveTo}`;
        detail =
          `Megajob has no sourceScope.dateRange — computed min/max across ${subJobs.length} ` +
          `synced sub-job(s). Athena can still prune partitions.`;
      }
      report({ step: 'preparing_table', percent: 7, message, detail });
    } else {
      console.warn(
        `[POSTAL-MAID-MEGAJOB] NO date range available — Athena will scan ALL partitions`,
      );
      report({
        step: 'preparing_table',
        percent: 7,
        message: '⚠️ No date range available — Athena will scan ALL partitions',
        detail:
          'Neither sourceScope.dateRange nor sub-job dateRanges yielded any value. ' +
          'Set DATE FROM/TO manually to enable partition pruning.',
      });
    }
    // Push the effective range into filters so analyzePostalMaid (BASIC
    // megajob fork) and analyzeFullSchemaFast both see the partition prune
    // through any internal recompute of dateWhere.
    const filtersWithDefaults: PostalMaidFilters = {
      ...filters,
      dateFrom: effectiveFrom,
      dateTo: effectiveTo,
    };

    if (isFull) {
      return await analyzeFullSchemaFast(
        megaJob.name || megaJobId,
        filtersWithDefaults,
        report,
        totalDevicesInDataset,
        tableNames[0], // unused when megajobOpts is set
        dateWhere,
        requestedPostalCodes,
        { tableExpr, maidsTableName },
      );
    }

    // BASIC megajob — uses lat/lng + Node-side reverse-geocode. The
    // existing single-dataset BASIC path handles this when given the
    // tableExpr + maidsTableName. We pass datasetName as the megajob
    // name so log lines + result.dataset are human-readable.
    return await analyzePostalMaid(
      megaJob.name || megaJobId,
      filtersWithDefaults,
      report,
      { tableExpr, maidsTableName },
    );
  } finally {
    setCountryFilter(null);
  }
}

/**
 * Find all MAIDs whose residential origin (first ping of day) falls
 * within the requested postal codes.
 */
export async function analyzePostalMaid(
  datasetName: string,
  filters: PostalMaidFilters,
  onProgress?: PostalMaidProgressCallback,
  /** Megajob mode (BASIC schema): tableExpr is a UNION ALL of sub-job
   *  tables, maidsTableName is the consolidated MAIDs CSV materialized
   *  as an Athena external table. Both are mandatory together. */
  megajobOpts?: { tableExpr: string; maidsTableName: string },
): Promise<PostalMaidResult> {
  const report = onProgress || (() => {});

  report({ step: 'initializing', percent: 0, message: 'Validating configuration...' });
  console.log(`[POSTAL-MAID] Starting postal→MAID analysis for dataset: ${datasetName}`, {
    postalCodes: filters.postalCodes,
    country: filters.country,
    mode: megajobOpts ? 'MEGAJOB-BASIC' : 'SINGLE',
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

  // ── Default the date range to the dataset's catalog range ─────────────
  // The UI tells users "leaving dates blank uses the catalog range" — make
  // that promise true so Athena can do partition pruning instead of
  // scanning every date partition. In megajob mode the caller already
  // populated filters.dateFrom/dateTo from the megajob's sourceScope, so
  // this block is a no-op for that path.
  if (!megajobOpts && (!filters.dateFrom || !filters.dateTo)) {
    try {
      const { getAllJobsSummary } = await import('./jobs');
      const jobs = await getAllJobsSummary().catch(() => []);
      const matching = jobs.find((j: any) => {
        const folderId = j.s3DestPath?.replace('s3://', '').replace(/.*\//, '').replace(/\/$/, '') || '';
        return folderId === datasetName;
      });
      const r = (matching as any)?.dateRange;
      if (r?.from && !filters.dateFrom) filters = { ...filters, dateFrom: r.from };
      if (r?.to && !filters.dateTo) filters = { ...filters, dateTo: r.to };
      if (r?.from || r?.to) {
        console.log(
          `[POSTAL-MAID] partition prune from catalog: date >= ${filters.dateFrom || '∞'} AND date <= ${filters.dateTo || '∞'}`,
        );
      }
    } catch (e: any) {
      console.warn('[POSTAL-MAID] catalog range lookup failed:', e?.message || e);
    }
  }

  // In megajob mode, the FROM clause is the UNION-ALL expression and the
  // POI-visitor set comes from the pre-computed MAIDs CSV. Otherwise it's
  // a single dataset table with the standard CROSS JOIN UNNEST scan.
  const tableName = getTableName(datasetName);
  const fromExpr = megajobOpts?.tableExpr ?? tableName;

  // ── Detect FULL schema and branch ──────────────────────────────────
  // (Megajob mode skips both: the parent dispatcher already decided
  // BASIC vs FULL, and the table-ensure has been done sub-job-by-sub-job.)
  if (!megajobOpts) {
    report({ step: 'preparing_table', percent: 3, message: 'Preparing Athena table...', detail: tableName });
    await ensureTableForDataset(datasetName);
  }

  const isFull = megajobOpts ? false : await detectFullSchema(tableName);
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

  console.log(`[POSTAL-MAID] BASIC schema path (Node reverse-geocode) — ${megajobOpts ? 'MEGAJOB mode' : 'single dataset'}`);
  const dateConditions: string[] = [];
  if (filters.dateFrom) dateConditions.push(`date >= '${filters.dateFrom}'`);
  if (filters.dateTo) dateConditions.push(`date <= '${filters.dateTo}'`);
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  // POI-visitor source: pre-built MAIDs CSV in megajob mode (β fast path,
  // skips the per-table CROSS JOIN UNNEST(poi_ids) scan), OR derived from
  // poi_ids in single-dataset mode.
  const poiVisitorsCte = megajobOpts
    ? `SELECT ad_id FROM ${megajobOpts.maidsTableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''`
    : `SELECT DISTINCT ad_id
       FROM ${tableName}
       CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
       WHERE poi_id IS NOT NULL AND poi_id != ''
         AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
         ${dateWhere}`;

  // Total devices in scope: count from MAIDs table directly when megajob,
  // or run the CROSS JOIN UNNEST otherwise.
  const totalQuery = megajobOpts
    ? `SELECT COUNT(DISTINCT ad_id) as total_devices FROM ${megajobOpts.maidsTableName}`
    : `SELECT COUNT(DISTINCT ad_id) as total_devices
       FROM ${tableName}
       CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
       WHERE poi_id IS NOT NULL AND poi_id != ''
         AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
         ${dateWhere}`;

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
      ${poiVisitorsCte}
    ),
    valid_pings AS (
      SELECT
        t.ad_id,
        t.date,
        t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${fromExpr} t
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

  // Poll both queries until done, reporting progress.
  // maidsRes is NOT declared/loaded here — we stream the maidsQuery
  // result directly from S3 in two passes after the polling loop
  // exits, to keep peak memory flat for million-row results.
  let totalDone = false, maidsDone = false;
  let totalRes: { rows: Record<string, string>[] } = { rows: [] };
  const t0 = Date.now();

  while (!totalDone || !maidsDone) {
    await new Promise(r => setTimeout(r, 3000));
    const elapsed = Math.round((Date.now() - t0) / 1000);

    if (!totalDone) {
      const s = await checkQueryStatus(totalQId);
      // totalQuery is a single COUNT(*) — the paginated API is fine.
      if (s.state === 'SUCCEEDED') { totalDone = true; totalRes = await fetchQueryResults(totalQId); }
      else if (s.state === 'FAILED') throw new Error(`Total devices query failed: ${s.error}`);
    }
    if (!maidsDone) {
      const s = await checkQueryStatus(maidsQId);
      // maidsQuery returns one row per (ad_id, origin coord) — millions
      // for big megajobs. We DON'T materialize them in memory here:
      // fetchQueryResultsViaS3 would load the full ~400 MB CSV into a
      // string and a parsed array (~1.5 GB peak), OOM-killing the Vercel
      // function silently (1 GB cap). Instead we just mark the query as
      // done; the post-loop logic streams the CSV twice (once to build
      // coordMap, once to emit matched devices), peak memory ≈ chunk
      // size + small structures.
      if (s.state === 'SUCCEEDED') {
        maidsDone = true;
        report({
          step: 'running_queries',
          percent: 55,
          message: 'Athena origins query done · ready to stream CSV from S3',
          detail: 'two-pass streaming: pass 1 = coords, pass 2 = match devices',
        });
      }
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

  // ── PASS 1: stream the origins CSV from S3 to collect unique coords ────
  //
  // We DO NOT materialize the full row set in memory — for France-scale
  // megajobs this is 5M+ rows × ~80 chars = a 400 MB CSV. Loading that
  // via transformToString() (string + split-array + parsed-rows array)
  // peaks at ~1.5 GB and OOMs the Vercel 1 GB function silently.
  // Instead, streamQueryResultsViaS3 walks the body chunk-by-chunk and
  // fires onRow() per parsed line; we accumulate ONLY the small coord
  // map here (~5K-50K unique coords for any country).
  report({
    step: 'running_queries',
    percent: 60,
    message: 'Streaming Athena origins (pass 1: collecting unique coords)…',
    detail: 'no row materialization — peak memory stays small',
  });
  const coordMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
  let totalDeviceDays = 0;
  let totalRows = 0;
  await streamQueryResultsViaS3(maidsQId, (row) => {
    totalRows++;
    const lat = parseFloat(String(row.origin_lat));
    const lng = parseFloat(String(row.origin_lng));
    const days = parseInt(String(row.device_days)) || 0;
    if (isNaN(lat) || isNaN(lng)) return;
    totalDeviceDays += days;
    const coordKey = `${lat.toFixed(COORDINATE_PRECISION)},${lng.toFixed(COORDINATE_PRECISION)}`;
    const existing = coordMap.get(coordKey);
    if (existing) {
      existing.deviceCount += days;
    } else {
      coordMap.set(coordKey, { lat, lng, deviceCount: days });
    }
  });
  console.log(
    `[POSTAL-MAID] Pass 1 done: ${totalRows} rows streamed, ${coordMap.size} unique coords (4-decimal)`,
  );
  report({
    step: 'running_queries',
    percent: 70,
    message: `Pass 1 complete · ${coordMap.size.toLocaleString()} unique coords`,
    detail: `${totalRows.toLocaleString()} device-day origins · streaming kept memory flat`,
  });

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

  // ── PASS 2: stream the same CSV again, this time emit matched devices ──
  //
  // We re-stream from S3 (same key, cached in S3 so this is fast). For
  // each row we check whether its coord maps to a target postal code;
  // if yes we add it to deviceMap + postalBreakdown directly. Only
  // matched devices stay in memory — for 20 zip codes out of millions
  // this is typically <1 GB of devices retained, often <100 MB.
  report({
    step: 'matching',
    percent: 92,
    message: 'Matching devices to postal codes (pass 2 streaming)…',
    detail: `${requestedPostalCodes.size} postal codes`,
  });

  const deviceMap = new Map<string, { deviceDays: number; postalCodes: Set<string> }>();
  const postalBreakdown = new Map<string, { devices: Set<string>; deviceDays: number }>();
  for (const pc of requestedPostalCodes) {
    postalBreakdown.set(pc, { devices: new Set(), deviceDays: 0 });
  }

  let matchedRowsSeen = 0;
  await streamQueryResultsViaS3(maidsQId, (row) => {
    const adId = String(row.ad_id || '');
    const lat = parseFloat(String(row.origin_lat));
    const lng = parseFloat(String(row.origin_lng));
    const deviceDays = parseInt(String(row.device_days)) || 0;
    if (!adId || isNaN(lat) || isNaN(lng)) return;
    const coordKey = `${lat.toFixed(COORDINATE_PRECISION)},${lng.toFixed(COORDINATE_PRECISION)}`;
    const postalCode = coordToPostal.get(coordKey);
    if (!postalCode || !requestedPostalCodes.has(postalCode)) return;
    matchedRowsSeen++;
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
  });
  console.log(`[POSTAL-MAID] Pass 2 done: ${matchedRowsSeen} matched rows, ${deviceMap.size} unique devices`);

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
