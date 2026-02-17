/**
 * Affinity Index Laboratory — Analysis Engine.
 *
 * Data flow:
 *   1. Load real POIs from pois_gmc parquet (Athena external table)
 *   2. Spatial join: movement pings ↔ real POIs within radius
 *   3. Compute dwell time per visit (consecutive pings near same POI)
 *   4. Apply recipe filters (category × time window × dwell × frequency)
 *   5. Build segment of matching ad_ids
 *   6. Geocode origins → postal codes → compute affinity indices
 */

import { runQuery, ensureTableForDataset, getTableName } from './athena';
import { batchReverseGeocode, aggregateByZipcode } from './reverse-geocode';
import type {
  LabConfig,
  LabAnalysisResult,
  LabProgressCallback,
  AffinityRecord,
  ZipcodeProfile,
  LabStats,
  CategoryStat,
  AffinityHotspot,
  SegmentDevice,
  PoiCategory,
  RecipeStep,
} from './laboratory-types';
import {
  POI_CATEGORIES,
  CATEGORY_LABELS,
  CATEGORY_GROUPS,
  AFFINITY_WEIGHTS,
  CONCENTRATION_CAP,
  FREQUENCY_CAP,
  DWELL_CAP_MINUTES,
  getCategoryGroup,
} from './laboratory-types';

const ACCURACY_THRESHOLD_METERS = 500;
const COORDINATE_PRECISION = 4;
const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

// ── Main entry point ───────────────────────────────────────────────────

export async function analyzeLaboratory(
  config: LabConfig,
  onProgress?: LabProgressCallback
): Promise<LabAnalysisResult> {
  const report = onProgress || (() => {});
  const { datasetId, recipe, country } = config;
  const tableName = getTableName(datasetId);
  const spatialRadius = config.spatialJoinRadiusMeters || 200;
  const minVisits = config.minVisitsPerZipcode || 5;

  // Collect all categories needed across recipe steps
  const allCategories = new Set<PoiCategory>();
  for (const step of recipe.steps) {
    for (const cat of step.categories) allCategories.add(cat);
  }
  const categoryList = Array.from(allCategories);

  report({ step: 'initializing', percent: 0, message: 'Validating configuration...', detail: `Dataset: ${config.datasetName}` });

  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials not configured.');
  }

  // ── 1. Ensure movement data table ────────────────────────────────────
  report({ step: 'initializing', percent: 3, message: 'Preparing movement data table...', detail: tableName });
  try {
    await ensureTableForDataset(datasetId);
  } catch (error: any) {
    if (!error.message?.includes('already exists')) throw error;
  }

  // ── 2. Ensure POI parquet table ──────────────────────────────────────
  report({ step: 'loading_pois', percent: 8, message: 'Preparing POI catalog table...', detail: `Real POIs from pois_gmc` });
  const poiTableName = 'lab_pois_gmc';
  try {
    await runQuery(`
      CREATE EXTERNAL TABLE IF NOT EXISTS ${poiTableName} (
        id STRING,
        name STRING,
        category STRING,
        city STRING,
        postal_code STRING,
        country STRING,
        latitude DOUBLE,
        longitude DOUBLE
      )
      STORED AS PARQUET
      LOCATION 's3://${BUCKET}/pois_gmc/'
    `);
  } catch (error: any) {
    if (!error.message?.includes('already exists')) {
      console.warn(`[LAB] Warning creating POI table:`, error.message);
    }
  }
  report({ step: 'loading_pois', percent: 12, message: 'POI catalog ready' });

  // ── 3. Build date filters ────────────────────────────────────────────
  const dateConditions: string[] = [];
  if (config.dateFrom) dateConditions.push(`date >= '${config.dateFrom}'`);
  if (config.dateTo) dateConditions.push(`date <= '${config.dateTo}'`);
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  // Category filter for POI table
  const catFilter = `AND p.category IN (${categoryList.map(c => `'${c}'`).join(',')})`;
  // Country filter for POI table
  const countryFilter = country ? `AND p.country = '${country.toUpperCase()}'` : '';

  // ── 4. Spatial join + dwell time query ───────────────────────────────
  // Strategy: Use geohash-bucket JOIN instead of CROSS JOIN.
  //   - Truncate lat/lng to a grid (~0.01° ≈ 1.1km cells)
  //   - JOIN pings to POIs by bucket (equi-join on grid cell + neighbors)
  //   - This reduces O(N×M) to O(N×k) where k ≈ POIs per bucket (~1-5)
  //
  // Haversine approximation:
  //   distance_m ≈ 111320 * SQRT(POW(lat1-lat2,2) + POW((lng1-lng2)*COS(RADIANS((lat1+lat2)/2)),2))

  report({ step: 'spatial_join', percent: 15, message: 'Running spatial join...', detail: `Matching pings to real POIs within ${spatialRadius}m (geohash-bucket strategy)` });

  // Grid cell size in degrees. For 200m radius, we need cells ~0.003°.
  // We use 0.01° (~1.1km) so each ping only needs to check its own cell + 8 neighbors.
  // This is generous enough to catch all POIs within any reasonable radius.
  const GRID_STEP = 0.01;

  const spatialQuery = `
    WITH
    -- Step 1: All pings with grid bucket assignment
    pings AS (
      SELECT
        ad_id,
        date,
        utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(latitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(TRY_CAST(longitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lng_bucket
      FROM ${tableName}
      WHERE TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${dateWhere}
    ),
    -- Step 2: Real POIs with grid buckets + neighbor expansion
    -- Each POI maps to its own cell + 8 neighbors (3×3 grid) to catch edge cases
    poi_base AS (
      SELECT id as poi_id, category, latitude as poi_lat, longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID_STEP}) AS BIGINT) as base_lat_bucket,
        CAST(FLOOR(longitude / ${GRID_STEP}) AS BIGINT) as base_lng_bucket
      FROM ${poiTableName} p
      WHERE p.category IS NOT NULL
        ${catFilter}
        ${countryFilter}
    ),
    -- Expand each POI into 9 bucket entries (3×3 neighborhood)
    poi_buckets AS (
      SELECT poi_id, category, poi_lat, poi_lng,
        base_lat_bucket + dlat as lat_bucket,
        base_lng_bucket + dlng as lng_bucket
      FROM poi_base
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
    ),
    -- Step 3: Bucket-based equi-join (replaces expensive CROSS JOIN)
    -- Only pings in the same bucket as a POI are compared
    matched AS (
      SELECT
        k.ad_id,
        k.date,
        k.utc_timestamp,
        k.lat,
        k.lng,
        p.poi_id,
        p.category,
        111320 * SQRT(
          POW(k.lat - p.poi_lat, 2) +
          POW((k.lng - p.poi_lng) * COS(RADIANS((k.lat + p.poi_lat) / 2)), 2)
        ) as distance_m
      FROM pings k
      INNER JOIN poi_buckets p
        ON k.lat_bucket = p.lat_bucket
        AND k.lng_bucket = p.lng_bucket
    ),
    -- Step 4: Keep only closest POI per ping (within radius)
    closest AS (
      SELECT
        ad_id, date, utc_timestamp, lat, lng, poi_id, category, distance_m,
        ROW_NUMBER() OVER (PARTITION BY ad_id, utc_timestamp ORDER BY distance_m) as rn
      FROM matched
      WHERE distance_m <= ${spatialRadius}
    ),
    poi_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng, poi_id, category
      FROM closest WHERE rn = 1
    ),
    -- Step 5: Group into visits (same device × same POI × same day)
    visits AS (
      SELECT
        ad_id,
        date,
        poi_id,
        category,
        MIN(utc_timestamp) as visit_start,
        MAX(utc_timestamp) as visit_end,
        COUNT(*) as ping_count,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_minutes,
        HOUR(MIN(utc_timestamp)) as visit_hour
      FROM poi_pings
      GROUP BY ad_id, date, poi_id, category
    )
    SELECT
      v.ad_id,
      v.date,
      v.poi_id,
      v.category,
      v.dwell_minutes,
      v.visit_hour,
      v.ping_count,
      CAST(NULL AS DOUBLE) as origin_lat,
      CAST(NULL AS DOUBLE) as origin_lng
    FROM visits v
    WHERE v.dwell_minutes >= 0
    ORDER BY v.ad_id, v.date, v.visit_start
  `;

  // Total devices in dataset (lightweight query)
  const totalDevicesQuery = `
    SELECT COUNT(DISTINCT ad_id) as total
    FROM ${tableName}
    WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
    ${dateWhere}
  `;

  console.log(`[LAB] Running spatial join (geohash-bucket strategy)...`);
  console.log(`[LAB] Grid cell size: ${GRID_STEP}° (~${Math.round(GRID_STEP * 111320)}m)`);
  console.log(`[LAB] Categories requested: ${categoryList.join(', ')}`);
  console.log(`[LAB] Country filter: ${country || 'none'}`);
  console.log(`[LAB] Date filters: ${dateWhere || 'none'}`);
  console.log(`[LAB] Spatial radius: ${spatialRadius}m`);
  let spatialRes, totalRes;
  try {
    [spatialRes, totalRes] = await Promise.all([
      runQuery(spatialQuery),
      runQuery(totalDevicesQuery),
    ]);
  } catch (error: any) {
    console.error(`[LAB] Query failed:`, error.message);
    report({ step: 'error', percent: 0, message: error.message });
    throw new Error(`Laboratory query failed: ${error.message}`);
  }

  const totalDevicesInDataset = parseInt(String(totalRes.rows[0]?.total)) || 0;
  console.log(`[LAB] Total devices in dataset: ${totalDevicesInDataset}`);
  console.log(`[LAB] Spatial join returned ${spatialRes.rows.length} visit rows`);
  report({ step: 'spatial_join', percent: 50, message: 'Spatial join complete', detail: `${spatialRes.rows.length} visits found` });

  if (spatialRes.rows.length === 0) {
    console.log(`[LAB] No visits found — returning empty result`);
    report({ step: 'completed', percent: 100, message: 'No visits found matching criteria', detail: `Checked ${categoryList.length} categories in ${country || 'all countries'} with ${spatialRadius}m radius` });
    return buildEmptyResult(config, totalDevicesInDataset);
  }

  // ── 5. Parse visits and apply recipe ─────────────────────────────────
  report({ step: 'computing_dwell', percent: 55, message: 'Applying recipe filters...', detail: `${recipe.steps.length} steps, logic: ${recipe.logic}` });

  interface ParsedVisit {
    adId: string;
    date: string;
    poiId: string;
    category: PoiCategory;
    dwellMinutes: number;
    visitHour: number;
    originLat: number;
    originLng: number;
  }

  // Parse visits (origins are resolved later in a separate query)
  const allVisits: ParsedVisit[] = spatialRes.rows.map(row => ({
    adId: String(row.ad_id),
    date: String(row.date),
    poiId: String(row.poi_id),
    category: String(row.category) as PoiCategory,
    dwellMinutes: parseFloat(String(row.dwell_minutes)) || 0,
    visitHour: parseInt(String(row.visit_hour)) || 0,
    originLat: 0, // resolved later
    originLng: 0, // resolved later
  }));

  // Group visits by device
  const deviceVisits = new Map<string, ParsedVisit[]>();
  for (const v of allVisits) {
    const list = deviceVisits.get(v.adId) || [];
    list.push(v);
    deviceVisits.set(v.adId, list);
  }

  // Apply recipe to each device
  const segmentDevices: SegmentDevice[] = [];
  const matchedAdIds = new Set<string>();

  for (const [adId, visits] of deviceVisits.entries()) {
    const stepMatches = recipe.steps.map(step => matchStep(step, visits));

    let matches: boolean;
    if (recipe.logic === 'AND') {
      matches = stepMatches.every(m => m);
      if (matches && recipe.ordered) {
        matches = checkOrder(recipe.steps, visits);
      }
    } else {
      matches = stepMatches.some(m => m);
    }

    if (matches) {
      const matchedStepCount = stepMatches.filter(m => m).length;
      const cats = new Set<PoiCategory>();
      let totalDwell = 0;
      for (const v of visits) {
        cats.add(v.category);
        totalDwell += v.dwellMinutes;
      }

      segmentDevices.push({
        adId,
        matchedSteps: matchedStepCount,
        totalVisits: visits.length,
        avgDwellMinutes: Math.round((totalDwell / visits.length) * 10) / 10,
        categories: Array.from(cats),
      });

      matchedAdIds.add(adId);
    }
  }

  console.log(`[LAB] Recipe matching: ${segmentDevices.length} devices matched out of ${deviceVisits.size} total`);
  console.log(`[LAB] Recipe: ${recipe.steps.length} steps, logic=${recipe.logic}, ordered=${recipe.ordered}`);
  for (const step of recipe.steps) {
    console.log(`[LAB]   Step ${step.id}: categories=[${step.categories.join(',')}] timeWindow=${step.timeWindow ? `${step.timeWindow.hourFrom}-${step.timeWindow.hourTo}` : 'none'} minDwell=${step.minDwellMinutes ?? 'none'} maxDwell=${step.maxDwellMinutes ?? 'none'} minFreq=${step.minFrequency ?? 1}`);
  }
  report({ step: 'building_segments', percent: 65, message: 'Segment built', detail: `${segmentDevices.length} devices matched out of ${deviceVisits.size}` });

  if (segmentDevices.length === 0) {
    console.log(`[LAB] No devices matched the recipe — returning empty result`);
    report({ step: 'completed', percent: 100, message: 'No devices matched the recipe' });
    return buildEmptyResult(config, totalDevicesInDataset);
  }

  // ── 5b. Fetch origins for matched devices only ──────────────────────
  // This is a SEPARATE query that only scans rows for matched ad_ids,
  // instead of duplicating the full 6B+ row table scan inside the spatial join.
  report({ step: 'geocoding', percent: 66, message: 'Fetching device origins...', detail: `${matchedAdIds.size} devices` });

  // Collect unique (ad_id, date) pairs from matched visits
  const matchedPairs = new Set<string>();
  const matchedVisits: ParsedVisit[] = [];
  for (const v of allVisits) {
    if (matchedAdIds.has(v.adId)) {
      matchedVisits.push(v);
      matchedPairs.add(`${v.adId}|${v.date}`);
    }
  }

  // Query origins: first ping of day for each matched device
  // We pass the ad_ids as a filter to avoid full table scan
  const uniqueAdIds = Array.from(matchedAdIds);
  // Split into batches of 500 to avoid SQL IN clause limits
  const BATCH_SIZE = 500;
  const originMap = new Map<string, { lat: number; lng: number }>();

  for (let batchStart = 0; batchStart < uniqueAdIds.length; batchStart += BATCH_SIZE) {
    const batch = uniqueAdIds.slice(batchStart, batchStart + BATCH_SIZE);
    const adIdFilter = batch.map(id => `'${id.replace(/'/g, "''")}'`).join(',');

    const originQuery = `
      SELECT
        ad_id,
        date,
        ROUND(MIN_BY(TRY_CAST(latitude AS DOUBLE), utc_timestamp), ${COORDINATE_PRECISION}) as origin_lat,
        ROUND(MIN_BY(TRY_CAST(longitude AS DOUBLE), utc_timestamp), ${COORDINATE_PRECISION}) as origin_lng
      FROM ${tableName}
      WHERE ad_id IN (${adIdFilter})
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        ${dateWhere}
      GROUP BY ad_id, date
    `;

    try {
      const originRes = await runQuery(originQuery);
      for (const row of originRes.rows) {
        const key = `${row.ad_id}|${row.date}`;
        const lat = parseFloat(String(row.origin_lat));
        const lng = parseFloat(String(row.origin_lng));
        if (!isNaN(lat) && !isNaN(lng)) {
          originMap.set(key, { lat, lng });
        }
      }
    } catch (err: any) {
      console.warn(`[LAB] Origin batch query failed:`, err.message);
    }

    if (batchStart + BATCH_SIZE < uniqueAdIds.length) {
      report({ step: 'geocoding', percent: 66 + Math.round((batchStart / uniqueAdIds.length) * 4), message: 'Fetching device origins...', detail: `${Math.min(batchStart + BATCH_SIZE, uniqueAdIds.length)}/${uniqueAdIds.length} devices` });
    }
  }

  console.log(`[LAB] Origins resolved: ${originMap.size} device-day pairs`);

  // Assign origins to matched visits
  for (const v of matchedVisits) {
    const origin = originMap.get(`${v.adId}|${v.date}`);
    if (origin) {
      v.originLat = origin.lat;
      v.originLng = origin.lng;
    }
  }

  // Filter out visits without valid origins
  const geoVisits = matchedVisits.filter(v => v.originLat !== 0 || v.originLng !== 0);
  console.log(`[LAB] Visits with valid origins: ${geoVisits.length}/${matchedVisits.length}`);

  // ── 6. Geocode origins → postal codes ────────────────────────────────
  report({ step: 'geocoding', percent: 72, message: 'Geocoding device origins...', detail: 'Resolving to postal codes' });

  // Aggregate unique origin coordinates
  const coordMap = new Map<string, { lat: number; lng: number; devices: number }>();
  for (const v of geoVisits) {
    const key = `${v.originLat},${v.originLng}`;
    const existing = coordMap.get(key);
    if (existing) {
      existing.devices++;
    } else {
      coordMap.set(key, { lat: v.originLat, lng: v.originLng, devices: 1 });
    }
  }

  const geocodePoints = Array.from(coordMap.values()).map(p => ({
    lat: p.lat, lng: p.lng, deviceCount: p.devices,
  }));

  const geocoded = await batchReverseGeocode(geocodePoints);

  const coordToZip = new Map<string, { zipcode: string; city: string; province: string; region: string }>();
  for (let i = 0; i < geocodePoints.length; i++) {
    const result = geocoded[i];
    const point = geocodePoints[i];
    const key = `${point.lat},${point.lng}`;
    if (result.type === 'geojson_local') {
      coordToZip.set(key, {
        zipcode: result.postcode,
        city: result.city,
        province: result.province,
        region: result.region,
      });
    }
  }

  report({ step: 'geocoding', percent: 78, message: 'Geocoding complete', detail: `${coordToZip.size}/${geocodePoints.length} points matched` });

  // ── 7. Compute affinity indices ──────────────────────────────────────
  report({ step: 'computing_affinity', percent: 82, message: 'Computing affinity indices...' });

  // Build: zipcode × category → { visits, devices, totalDwell }
  const zipCatMap = new Map<string, {
    zipcode: string; city: string; province: string; region: string;
    category: PoiCategory;
    visits: number; devices: Set<string>; totalDwell: number;
  }>();

  for (const v of geoVisits) {
    const coordKey = `${v.originLat},${v.originLng}`;
    const geo = coordToZip.get(coordKey);
    if (!geo) continue;

    const key = `${geo.zipcode}|${v.category}`;
    const existing = zipCatMap.get(key);
    if (existing) {
      existing.visits++;
      existing.devices.add(v.adId);
      existing.totalDwell += v.dwellMinutes;
    } else {
      zipCatMap.set(key, {
        ...geo, category: v.category,
        visits: 1, devices: new Set([v.adId]), totalDwell: v.dwellMinutes,
      });
    }
  }

  // Totals for affinity calculation
  const zipTotals = new Map<string, { total: number; devices: Set<string>; city: string; province: string; region: string; totalDwell: number }>();
  const catTotals = new Map<PoiCategory, { total: number; totalDwell: number }>();
  let grandTotal = 0;

  for (const entry of zipCatMap.values()) {
    // Zipcode totals
    const zt = zipTotals.get(entry.zipcode) || { total: 0, devices: new Set<string>(), city: entry.city, province: entry.province, region: entry.region, totalDwell: 0 };
    zt.total += entry.visits;
    for (const d of entry.devices) zt.devices.add(d);
    zt.totalDwell += entry.totalDwell;
    zipTotals.set(entry.zipcode, zt);

    // Category totals
    const ct = catTotals.get(entry.category) || { total: 0, totalDwell: 0 };
    ct.total += entry.visits;
    ct.totalDwell += entry.totalDwell;
    catTotals.set(entry.category, ct);

    grandTotal += entry.visits;
  }

  // National shares
  const nationalShares = new Map<PoiCategory, number>();
  for (const [cat, t] of catTotals.entries()) {
    nationalShares.set(cat, grandTotal > 0 ? t.total / grandTotal : 0);
  }

  // Category median dwell times (for dwell score)
  const catMedianDwell = new Map<PoiCategory, number>();
  for (const [cat, t] of catTotals.entries()) {
    catMedianDwell.set(cat, t.total > 0 ? t.totalDwell / t.total : 0);
  }

  // Build affinity records
  const records: AffinityRecord[] = [];

  for (const entry of zipCatMap.values()) {
    const zt = zipTotals.get(entry.zipcode)!;
    if (zt.total < minVisits) continue;

    const devCount = entry.devices.size;
    const avgDwell = entry.visits > 0 ? entry.totalDwell / entry.visits : 0;
    const freq = devCount > 0 ? entry.visits / devCount : 0;

    const localShare = zt.total > 0 ? entry.visits / zt.total : 0;
    const natShare = nationalShares.get(entry.category) || 0;

    // Concentration score (0-100)
    const rawRatio = natShare > 0 ? localShare / natShare : 0;
    const concentrationScore = Math.round(Math.min(rawRatio / CONCENTRATION_CAP, 1) * 100);

    // Frequency score (0-100)
    const freqNorm = freq > 0 ? Math.log2(freq) / Math.log2(FREQUENCY_CAP) : 0;
    const frequencyScore = Math.round(Math.min(Math.max(freqNorm, 0), 1) * 100);

    // Dwell score (0-100): how does avg dwell compare to category median
    const medianDwell = catMedianDwell.get(entry.category) || 1;
    const dwellRatio = medianDwell > 0 ? avgDwell / medianDwell : 0;
    const dwellScore = Math.round(Math.min(dwellRatio / (DWELL_CAP_MINUTES / medianDwell), 1) * 100);

    const affinityIndex = Math.round(
      AFFINITY_WEIGHTS.concentration * concentrationScore +
      AFFINITY_WEIGHTS.frequency * frequencyScore +
      AFFINITY_WEIGHTS.dwell * dwellScore
    );

    records.push({
      zipcode: entry.zipcode,
      city: entry.city,
      province: entry.province,
      region: entry.region,
      category: entry.category,
      visits: entry.visits,
      uniqueDevices: devCount,
      avgDwellMinutes: Math.round(avgDwell * 10) / 10,
      frequency: Math.round(freq * 100) / 100,
      totalVisitsFromZipcode: zt.total,
      concentrationScore,
      frequencyScore,
      dwellScore,
      affinityIndex,
    });
  }

  records.sort((a, b) => b.affinityIndex - a.affinityIndex);

  // Build zipcode profiles
  const profileMap = new Map<string, ZipcodeProfile>();
  for (const rec of records) {
    let profile = profileMap.get(rec.zipcode);
    if (!profile) {
      const zt = zipTotals.get(rec.zipcode)!;
      profile = {
        zipcode: rec.zipcode,
        city: rec.city,
        province: rec.province,
        region: rec.region,
        totalVisits: zt.total,
        uniqueDevices: zt.devices.size,
        avgDwellMinutes: zt.total > 0 ? Math.round((zt.totalDwell / zt.total) * 10) / 10 : 0,
        affinities: {},
        topCategory: rec.category,
        topAffinity: rec.affinityIndex,
        dominantGroup: getCategoryGroup(rec.category),
      };
      profileMap.set(rec.zipcode, profile);
    }
    profile.affinities[rec.category] = rec.affinityIndex;
    if (rec.affinityIndex > profile.topAffinity) {
      profile.topAffinity = rec.affinityIndex;
      profile.topCategory = rec.category;
    }
  }

  // Recompute dominantGroup
  for (const profile of profileMap.values()) {
    const groupScores = new Map<string, { sum: number; count: number }>();
    for (const [cat, score] of Object.entries(profile.affinities)) {
      const group = getCategoryGroup(cat as PoiCategory);
      const gs = groupScores.get(group) || { sum: 0, count: 0 };
      gs.sum += score as number;
      gs.count += 1;
      groupScores.set(group, gs);
    }
    let bestGroup = profile.dominantGroup;
    let bestAvg = 0;
    for (const [group, gs] of groupScores.entries()) {
      const avg = gs.sum / gs.count;
      if (avg > bestAvg) { bestAvg = avg; bestGroup = group; }
    }
    profile.dominantGroup = bestGroup;
  }

  const profiles = Array.from(profileMap.values()).sort((a, b) => b.topAffinity - a.topAffinity);

  report({ step: 'computing_affinity', percent: 92, message: 'Affinity computation complete', detail: `${profiles.length} postal codes profiled` });

  // ── 8. Build stats ───────────────────────────────────────────────────
  const stats = computeStats(records, profiles, segmentDevices, geoVisits, totalDevicesInDataset);

  // Sort segment by totalVisits desc, limit to top 1000 for response
  segmentDevices.sort((a, b) => b.totalVisits - a.totalVisits);
  const segmentForResponse = segmentDevices.slice(0, 1000);

  console.log(`[LAB] Analysis complete: ${segmentDevices.length} devices, ${profiles.length} postal codes, ${records.length} affinity records`);
  report({ step: 'completed', percent: 100, message: 'Analysis complete', detail: `${segmentDevices.length} devices in segment, ${profiles.length} postal codes` });

  return {
    config,
    analyzedAt: new Date().toISOString(),
    segment: {
      totalDevices: segmentDevices.length,
      devices: segmentForResponse,
    },
    records,
    profiles,
    stats,
  };
}


// ── Recipe matching helpers ────────────────────────────────────────────

function matchStep(step: RecipeStep, visits: { category: PoiCategory; dwellMinutes: number; visitHour: number }[]): boolean {
  const matching = visits.filter(v => {
    // Category match (OR within step)
    if (!step.categories.includes(v.category)) return false;

    // Time window
    if (step.timeWindow) {
      const { hourFrom, hourTo } = step.timeWindow;
      if (hourFrom <= hourTo) {
        if (v.visitHour < hourFrom || v.visitHour >= hourTo) return false;
      } else {
        // Wrapping (e.g. 22-6)
        if (v.visitHour < hourFrom && v.visitHour >= hourTo) return false;
      }
    }

    // Dwell time
    if (step.minDwellMinutes != null && v.dwellMinutes < step.minDwellMinutes) return false;
    if (step.maxDwellMinutes != null && v.dwellMinutes > step.maxDwellMinutes) return false;

    return true;
  });

  // Frequency check
  const minFreq = step.minFrequency || 1;
  return matching.length >= minFreq;
}

function checkOrder(steps: RecipeStep[], visits: { category: PoiCategory; date: string; visitHour: number; dwellMinutes: number }[]): boolean {
  // For ordered AND: the first qualifying visit of step[i] must be on a date
  // <= the first qualifying visit of step[i+1]
  let lastDate = '';
  for (const step of steps) {
    const qualifying = visits
      .filter(v => {
        if (!step.categories.includes(v.category)) return false;
        if (step.timeWindow) {
          const { hourFrom, hourTo } = step.timeWindow;
          if (hourFrom <= hourTo) {
            if (v.visitHour < hourFrom || v.visitHour >= hourTo) return false;
          } else {
            if (v.visitHour < hourFrom && v.visitHour >= hourTo) return false;
          }
        }
        if (step.minDwellMinutes != null && v.dwellMinutes < step.minDwellMinutes) return false;
        return true;
      })
      .sort((a, b) => a.date.localeCompare(b.date));

    if (qualifying.length === 0) return false;
    const firstDate = qualifying[0].date;
    if (lastDate && firstDate < lastDate) return false;
    lastDate = firstDate;
  }
  return true;
}


// ── Stats computation ──────────────────────────────────────────────────

function computeStats(
  records: AffinityRecord[],
  profiles: ZipcodeProfile[],
  segment: SegmentDevice[],
  matchedVisits: { category: PoiCategory; dwellMinutes: number }[],
  totalDevicesInDataset: number
): LabStats {
  const totalPingsAnalyzed = matchedVisits.length;
  const segmentSize = segment.length;
  const totalDwell = matchedVisits.reduce((s, v) => s + v.dwellMinutes, 0);

  const catMap = new Map<PoiCategory, {
    visits: number; devices: Set<string>; totalDwell: number;
    records: AffinityRecord[]; maxAffinity: number; maxRec: AffinityRecord | null;
  }>();

  for (const rec of records) {
    const c = catMap.get(rec.category) || {
      visits: 0, devices: new Set<string>(), totalDwell: 0,
      records: [], maxAffinity: 0, maxRec: null,
    };
    c.visits += rec.visits;
    c.totalDwell += rec.avgDwellMinutes * rec.visits;
    c.records.push(rec);
    if (rec.affinityIndex > c.maxAffinity) { c.maxAffinity = rec.affinityIndex; c.maxRec = rec; }
    catMap.set(rec.category, c);
  }

  const totalVisits = records.reduce((s, r) => s + r.visits, 0);

  const categoryBreakdown: CategoryStat[] = Array.from(catMap.entries()).map(([cat, data]) => ({
    category: cat,
    label: CATEGORY_LABELS[cat],
    group: getCategoryGroup(cat),
    visits: data.visits,
    uniqueDevices: new Set(data.records.flatMap(r => Array(r.uniqueDevices).fill(r.zipcode))).size,
    avgDwellMinutes: data.visits > 0 ? Math.round((data.totalDwell / data.visits) * 10) / 10 : 0,
    percentOfTotal: totalVisits > 0 ? Math.round((data.visits / totalVisits) * 10000) / 100 : 0,
    postalCodesWithVisits: new Set(data.records.map(r => r.zipcode)).size,
    avgAffinity: data.records.length > 0 ? Math.round(data.records.reduce((s, r) => s + r.affinityIndex, 0) / data.records.length) : 0,
    maxAffinity: data.maxAffinity,
    maxAffinityZipcode: data.maxRec?.zipcode || '',
    maxAffinityCity: data.maxRec?.city || '',
  })).sort((a, b) => b.visits - a.visits);

  const topHotspots: AffinityHotspot[] = records
    .filter(r => r.affinityIndex >= 60)
    .slice(0, 25)
    .map(r => ({
      zipcode: r.zipcode,
      city: r.city,
      category: r.category,
      categoryLabel: CATEGORY_LABELS[r.category],
      affinityIndex: r.affinityIndex,
      visits: r.visits,
      uniqueDevices: r.uniqueDevices,
      avgDwellMinutes: r.avgDwellMinutes,
    }));

  const avgAffinity = records.length > 0
    ? Math.round(records.reduce((s, r) => s + r.affinityIndex, 0) / records.length)
    : 0;

  return {
    totalPingsAnalyzed,
    totalDevicesInDataset,
    segmentSize,
    segmentPercent: totalDevicesInDataset > 0 ? Math.round((segmentSize / totalDevicesInDataset) * 10000) / 100 : 0,
    totalPostalCodes: profiles.length,
    categoriesAnalyzed: new Set(records.map(r => r.category)).size,
    avgAffinityIndex: avgAffinity,
    avgDwellMinutes: totalPingsAnalyzed > 0 ? Math.round((totalDwell / totalPingsAnalyzed) * 10) / 10 : 0,
    categoryBreakdown,
    topHotspots,
  };
}


// ── Empty result ───────────────────────────────────────────────────────

function buildEmptyResult(config: LabConfig, totalDevices = 0): LabAnalysisResult {
  return {
    config,
    analyzedAt: new Date().toISOString(),
    segment: { totalDevices: 0, devices: [] },
    records: [],
    profiles: [],
    stats: {
      totalPingsAnalyzed: 0,
      totalDevicesInDataset: totalDevices,
      segmentSize: 0,
      segmentPercent: 0,
      totalPostalCodes: 0,
      categoriesAnalyzed: 0,
      avgAffinityIndex: 0,
      avgDwellMinutes: 0,
      categoryBreakdown: [],
      topHotspots: [],
    },
  };
}
