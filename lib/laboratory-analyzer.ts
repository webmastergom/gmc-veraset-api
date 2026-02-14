/**
 * Affinity Index Laboratory — Analysis Engine.
 *
 * Computes affinity indices (0–100) per postal code × POI category.
 * Uses Athena queries on movement datasets + reverse geocoding to GeoJSON polygons.
 *
 * The heavy lifting is a single Athena query that returns:
 *   origin_lat, origin_lng, poi_category, device_count, visit_count, hour_bucket
 *
 * Then we geocode origins → postal codes, and compute three-signal affinity.
 */

import { runQuery, createTableForDataset, getTableName } from './athena';
import { batchReverseGeocode, aggregateByZipcode } from './reverse-geocode';
import type {
  LabFilters,
  LabAnalysisResult,
  LabProgressCallback,
  AffinityRecord,
  ZipcodeProfile,
  LabStats,
  CategoryStat,
  AffinityHotspot,
  PoiCategory,
  TimeWindow,
} from './laboratory-types';
import {
  POI_CATEGORIES,
  CATEGORY_LABELS,
  CATEGORY_GROUPS,
  LAB_COUNTRIES,
  AFFINITY_WEIGHTS,
  CONCENTRATION_CAP,
  FREQUENCY_CAP,
  MIN_VISITS_DEFAULT,
  getCategoryGroup,
} from './laboratory-types';

const ACCURACY_THRESHOLD_METERS = 500;
const COORDINATE_PRECISION = 4;

// ── Types for intermediate data ────────────────────────────────────────

interface RawOriginRow {
  origin_lat: number;
  origin_lng: number;
  category: string;
  device_count: number;
  visit_count: number;
  hour_bucket: number; // 0-23
}

interface GeocodedVisit {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  category: PoiCategory;
  devices: number;
  visits: number;
  hourBucket: number;
}

// ── Main analysis function ─────────────────────────────────────────────

export async function analyzeLaboratory(
  filters: LabFilters,
  onProgress?: LabProgressCallback
): Promise<LabAnalysisResult> {
  const report = onProgress || (() => {});
  const countryInfo = LAB_COUNTRIES.find(c => c.code === filters.country);
  if (!countryInfo) {
    throw new Error(`Unsupported country: ${filters.country}. Supported: ${LAB_COUNTRIES.map(c => c.code).join(', ')}`);
  }

  const datasetName = countryInfo.datasetName;
  const tableName = getTableName(datasetName);
  const categories = filters.categories.length > 0 ? filters.categories : [...POI_CATEGORIES];
  const minVisits = filters.minVisits ?? MIN_VISITS_DEFAULT;

  report({ step: 'initializing', percent: 0, message: 'Initializing analysis...', detail: `${countryInfo.name} — ${categories.length} categories` });

  // ── 1. Ensure Athena table ───────────────────────────────────────────
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials not configured.');
  }

  report({ step: 'loading_pois', percent: 5, message: 'Preparing Athena table...', detail: tableName });
  try {
    await createTableForDataset(datasetName);
  } catch (error: any) {
    if (!error.message?.includes('already exists')) {
      throw error;
    }
  }
  report({ step: 'loading_pois', percent: 10, message: 'Table ready' });

  // ── 2. Build and run Athena query ────────────────────────────────────
  const dateConditions: string[] = [];
  if (filters.dateFrom) dateConditions.push(`date >= '${filters.dateFrom}'`);
  if (filters.dateTo) dateConditions.push(`date <= '${filters.dateTo}'`);
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  // Build POI category filter
  // We need the POI parquet loaded as an Athena table to join with movement data.
  // But POI categories are in a separate parquet. We'll use a two-step approach:
  // 1. First query: get POI IDs + categories from the POI parquet
  // 2. Second query: join with movement data to get origins per category
  //
  // Actually, the movement data has poi_ids array. We need to match those to
  // the POI parquet which has id + category. Let's create a POI lookup table.

  report({ step: 'loading_pois', percent: 12, message: 'Creating POI lookup table...', detail: `${countryInfo.totalPois.toLocaleString()} POIs` });

  // Create the POI lookup table from the parquet
  const poiTableName = `lab_pois_${countryInfo.code.toLowerCase()}`;
  const poiTableSql = `
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
    LOCATION 's3://${process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2'}/${countryInfo.poiParquetKey.replace(/\/[^/]+$/, '/')}'
  `;

  try {
    await runQuery(poiTableSql);
  } catch (error: any) {
    if (!error.message?.includes('already exists')) {
      console.warn(`[LAB] Warning creating POI table:`, error.message);
    }
  }

  report({ step: 'querying_visits', percent: 15, message: 'Running Athena queries...', detail: 'Joining movement data with POI categories' });

  // Build category filter SQL
  const categoryFilter = categories.length < POI_CATEGORIES.length
    ? `AND p.category IN (${categories.map(c => `'${c}'`).join(',')})`
    : '';

  // Main query: for each device-day with a POI visit, get the first ping (origin),
  // the visited POI's category, and the hour of the visit.
  // This is the KEY query that powers the entire laboratory.
  const labQuery = `
    WITH
    -- Step 1: Explode POI visits and join with POI categories
    poi_visits AS (
      SELECT
        t.ad_id,
        t.date,
        t.utc_timestamp,
        poi_id,
        p.category
      FROM ${tableName} t
      CROSS JOIN UNNEST(t.poi_ids) AS x(poi_id)
      INNER JOIN ${poiTableName} p ON poi_id = p.id
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND p.category IS NOT NULL
        ${dateWhere}
        ${categoryFilter}
    ),
    -- Step 2: Per device-day-category, count visits and get visit hour
    device_day_category AS (
      SELECT
        ad_id,
        date,
        category,
        COUNT(*) as visit_count,
        HOUR(MIN(utc_timestamp)) as first_visit_hour
      FROM poi_visits
      GROUP BY ad_id, date, category
    ),
    -- Step 3: Get distinct devices that visited POIs
    poi_device_ids AS (
      SELECT DISTINCT ad_id FROM device_day_category
    ),
    -- Step 4: For each device-day, get origin (first ping of the day)
    origins AS (
      SELECT
        t.ad_id,
        t.date,
        MIN_BY(TRY_CAST(t.latitude AS DOUBLE), t.utc_timestamp) as origin_lat,
        MIN_BY(TRY_CAST(t.longitude AS DOUBLE), t.utc_timestamp) as origin_lng
      FROM ${tableName} t
      INNER JOIN poi_device_ids v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS}
        ${dateWhere}
      GROUP BY t.ad_id, t.date
    ),
    -- Step 5: Join origins with category visits
    origin_category AS (
      SELECT
        o.ad_id,
        ROUND(o.origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
        ROUND(o.origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
        d.category,
        d.visit_count,
        d.first_visit_hour
      FROM origins o
      INNER JOIN device_day_category d ON o.ad_id = d.ad_id AND o.date = d.date
      WHERE o.origin_lat IS NOT NULL AND o.origin_lng IS NOT NULL
    )
    -- Step 6: Aggregate by origin location × category × hour
    SELECT
      origin_lat,
      origin_lng,
      category,
      COUNT(DISTINCT ad_id) as device_count,
      SUM(visit_count) as visit_count,
      first_visit_hour as hour_bucket
    FROM origin_category
    GROUP BY origin_lat, origin_lng, category, first_visit_hour
    ORDER BY device_count DESC
    LIMIT 500000
  `;

  // Total devices query (for coverage)
  const totalQuery = `
    SELECT
      COUNT(DISTINCT ad_id) as total_devices,
      COUNT(*) as total_records
    FROM ${tableName}
    CROSS JOIN UNNEST(poi_ids) AS x(poi_id)
    INNER JOIN ${poiTableName} p ON poi_id = p.id
    WHERE poi_id IS NOT NULL AND poi_id != ''
      AND p.category IS NOT NULL
      ${dateWhere}
      ${categoryFilter}
  `;

  console.log(`[LAB] Executing laboratory queries for ${countryInfo.name}...`);
  let labRes, totalRes;
  try {
    [labRes, totalRes] = await Promise.all([
      runQuery(labQuery),
      runQuery(totalQuery),
    ]);
  } catch (error: any) {
    // If the POI table failed because location points to directory but parquet
    // is a single file, try with direct file path
    if (error.message?.includes('not found') || error.message?.includes('does not exist')) {
      console.warn(`[LAB] Query failed, trying to recreate POI table with direct file path...`);
      const directPoiSql = `
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
        LOCATION 's3://${process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2'}/pois_gmc/'
      `;
      try {
        await runQuery(`DROP TABLE IF EXISTS ${poiTableName}`);
        await runQuery(directPoiSql);
        [labRes, totalRes] = await Promise.all([
          runQuery(labQuery),
          runQuery(totalQuery),
        ]);
      } catch (retryError: any) {
        throw new Error(`Laboratory query failed: ${retryError.message}`);
      }
    } else {
      throw new Error(`Laboratory query failed: ${error.message}`);
    }
  }

  report({ step: 'querying_visits', percent: 55, message: 'Queries complete', detail: `${labRes.rows.length} origin-category clusters` });

  const totalDevices = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`[LAB] Total devices: ${totalDevices}, clusters: ${labRes.rows.length}`);

  if (totalDevices === 0 || labRes.rows.length === 0) {
    report({ step: 'completed', percent: 100, message: 'No data found' });
    return buildEmptyResult(countryInfo, filters);
  }

  // ── 3. Parse query results ───────────────────────────────────────────
  const rawRows: RawOriginRow[] = labRes.rows.map(row => ({
    origin_lat: parseFloat(String(row.origin_lat)),
    origin_lng: parseFloat(String(row.origin_lng)),
    category: String(row.category),
    device_count: parseInt(String(row.device_count)) || 0,
    visit_count: parseInt(String(row.visit_count)) || 0,
    hour_bucket: parseInt(String(row.hour_bucket)) || 0,
  })).filter(r => !isNaN(r.origin_lat) && !isNaN(r.origin_lng) && r.device_count > 0);

  // ── 4. Reverse geocode unique origin points ──────────────────────────
  report({ step: 'geocoding', percent: 60, message: 'Geocoding origin coordinates...', detail: `Resolving to postal codes` });

  // Aggregate by unique coordinate (across all categories) for geocoding
  const coordMap = new Map<string, { lat: number; lng: number; totalDevices: number }>();
  for (const row of rawRows) {
    const key = `${row.origin_lat},${row.origin_lng}`;
    const existing = coordMap.get(key);
    if (existing) {
      existing.totalDevices += row.device_count;
    } else {
      coordMap.set(key, { lat: row.origin_lat, lng: row.origin_lng, totalDevices: row.device_count });
    }
  }

  const geocodePoints = Array.from(coordMap.values()).map(p => ({
    lat: p.lat,
    lng: p.lng,
    deviceCount: p.totalDevices,
  }));

  console.log(`[LAB] Geocoding ${geocodePoints.length} unique coordinate clusters...`);
  const geocoded = await batchReverseGeocode(geocodePoints);

  // Build coordinate → zipcode lookup
  const coordToZip = new Map<string, { zipcode: string; city: string; province: string; region: string }>();

  // Results are returned in the same order as input points
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

  report({ step: 'geocoding', percent: 75, message: 'Geocoding complete', detail: `${coordToZip.size}/${geocodePoints.length} points matched` });

  // ── 5. Build geocoded visit records ──────────────────────────────────
  report({ step: 'computing_affinity', percent: 78, message: 'Computing affinity indices...', detail: 'Building postal code profiles' });

  const geocodedVisits: GeocodedVisit[] = [];
  for (const row of rawRows) {
    const key = `${row.origin_lat},${row.origin_lng}`;
    const geo = coordToZip.get(key);
    if (!geo) continue;
    if (!POI_CATEGORIES.includes(row.category as PoiCategory)) continue;

    geocodedVisits.push({
      zipcode: geo.zipcode,
      city: geo.city,
      province: geo.province,
      region: geo.region,
      category: row.category as PoiCategory,
      devices: row.device_count,
      visits: row.visit_count,
      hourBucket: row.hour_bucket,
    });
  }

  // ── 6. Compute affinity indices ──────────────────────────────────────
  const result = computeAffinityIndices(geocodedVisits, categories, minVisits, filters.timeWindows);

  report({ step: 'aggregating', percent: 90, message: 'Building result profiles...', detail: `${result.profiles.length} postal codes profiled` });

  // ── 7. Build final result ────────────────────────────────────────────
  const stats = computeStats(result.records, result.profiles, totalDevices);

  report({ step: 'completed', percent: 100, message: 'Analysis complete', detail: `${result.profiles.length} postal codes × ${categories.length} categories` });

  return {
    country: countryInfo.code,
    countryName: countryInfo.name,
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    records: result.records,
    profiles: result.profiles,
    stats,
  };
}


// ── Affinity computation ───────────────────────────────────────────────

function computeAffinityIndices(
  visits: GeocodedVisit[],
  categories: PoiCategory[],
  minVisits: number,
  timeWindows?: TimeWindow[]
): { records: AffinityRecord[]; profiles: ZipcodeProfile[] } {

  // Aggregate: zipcode × category → { devices, visits, hourBuckets }
  const zipCatMap = new Map<string, {
    zipcode: string; city: string; province: string; region: string;
    category: PoiCategory;
    devices: number; visits: number; hourCounts: Map<number, number>;
  }>();

  for (const v of visits) {
    const key = `${v.zipcode}|${v.category}`;
    const existing = zipCatMap.get(key);
    if (existing) {
      existing.devices += v.devices;
      existing.visits += v.visits;
      existing.hourCounts.set(v.hourBucket, (existing.hourCounts.get(v.hourBucket) || 0) + v.visits);
    } else {
      const hourCounts = new Map<number, number>();
      hourCounts.set(v.hourBucket, v.visits);
      zipCatMap.set(key, {
        zipcode: v.zipcode, city: v.city, province: v.province, region: v.region,
        category: v.category,
        devices: v.devices, visits: v.visits, hourCounts,
      });
    }
  }

  // Compute totals per zipcode and per category (nationally)
  const zipTotals = new Map<string, { totalVisits: number; totalDevices: number; city: string; province: string; region: string }>();
  const catTotals = new Map<PoiCategory, { totalVisits: number; totalDevices: number }>();
  let grandTotalVisits = 0;

  for (const entry of zipCatMap.values()) {
    // Zipcode totals
    const zt = zipTotals.get(entry.zipcode) || { totalVisits: 0, totalDevices: 0, city: entry.city, province: entry.province, region: entry.region };
    zt.totalVisits += entry.visits;
    zt.totalDevices += entry.devices;
    zipTotals.set(entry.zipcode, zt);

    // Category totals (national)
    const ct = catTotals.get(entry.category) || { totalVisits: 0, totalDevices: 0 };
    ct.totalVisits += entry.visits;
    ct.totalDevices += entry.devices;
    catTotals.set(entry.category, ct);

    grandTotalVisits += entry.visits;
  }

  // National share per category
  const nationalShares = new Map<PoiCategory, number>();
  for (const [cat, totals] of catTotals.entries()) {
    nationalShares.set(cat, grandTotalVisits > 0 ? totals.totalVisits / grandTotalVisits : 0);
  }

  // Compute affinity records
  const records: AffinityRecord[] = [];

  for (const entry of zipCatMap.values()) {
    const zt = zipTotals.get(entry.zipcode)!;
    if (zt.totalVisits < minVisits) continue;

    const localShare = zt.totalVisits > 0 ? entry.visits / zt.totalVisits : 0;
    const natShare = nationalShares.get(entry.category) || 0;

    // Signal 1: Concentration (0–100)
    const rawRatio = natShare > 0 ? localShare / natShare : 0;
    const cappedRatio = Math.min(rawRatio, CONCENTRATION_CAP);
    const concentrationScore = Math.round((cappedRatio / CONCENTRATION_CAP) * 100);

    // Signal 2: Frequency (0–100)
    const avgFreq = entry.devices > 0 ? entry.visits / entry.devices : 0;
    const freqNorm = avgFreq > 0 ? Math.log2(avgFreq) / Math.log2(FREQUENCY_CAP) : 0;
    const frequencyScore = Math.round(Math.min(Math.max(freqNorm, 0), 1) * 100);

    // Signal 3: Temporal relevance (0–100)
    const temporalScore = computeTemporalScore(entry.hourCounts, entry.visits, timeWindows);

    // Composite affinity index (0–100)
    const affinityIndex = Math.round(
      AFFINITY_WEIGHTS.concentration * concentrationScore +
      AFFINITY_WEIGHTS.frequency * frequencyScore +
      AFFINITY_WEIGHTS.temporal * temporalScore
    );

    records.push({
      zipcode: entry.zipcode,
      city: entry.city,
      province: entry.province,
      region: entry.region,
      category: entry.category,
      visits: entry.visits,
      uniqueDevices: entry.devices,
      frequency: Math.round(avgFreq * 100) / 100,
      totalVisits: zt.totalVisits,
      concentrationScore,
      frequencyScore,
      temporalScore,
      affinityIndex,
    });
  }

  // Sort by affinity index descending
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
        totalVisits: zt.totalVisits,
        uniqueDevices: zt.totalDevices,
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
      profile.dominantGroup = getCategoryGroup(rec.category);
    }
  }

  // Recompute dominantGroup as group with highest AVERAGE affinity
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
      if (avg > bestAvg) {
        bestAvg = avg;
        bestGroup = group;
      }
    }
    profile.dominantGroup = bestGroup;
  }

  const profiles = Array.from(profileMap.values())
    .sort((a, b) => b.topAffinity - a.topAffinity);

  return { records, profiles };
}


// ── Temporal score ─────────────────────────────────────────────────────

function computeTemporalScore(
  hourCounts: Map<number, number>,
  totalVisits: number,
  timeWindows?: TimeWindow[]
): number {
  if (!timeWindows || timeWindows.length === 0) {
    // No time windows configured → base score of 50 (neutral)
    return 50;
  }

  // For each time window, compute what % of visits fall within it
  let weightedScore = 0;
  let totalWeight = 0;

  for (const tw of timeWindows) {
    let windowVisits = 0;
    for (const [hour, count] of hourCounts.entries()) {
      if (tw.hourFrom <= tw.hourTo) {
        // Normal range (e.g., 6–18)
        if (hour >= tw.hourFrom && hour < tw.hourTo) windowVisits += count;
      } else {
        // Wrapping range (e.g., 22–6)
        if (hour >= tw.hourFrom || hour < tw.hourTo) windowVisits += count;
      }
    }
    const windowShare = totalVisits > 0 ? windowVisits / totalVisits : 0;
    weightedScore += windowShare * tw.weight * 100;
    totalWeight += tw.weight;
  }

  if (totalWeight === 0) return 50;
  return Math.round(Math.min(weightedScore / totalWeight, 100));
}


// ── Stats computation ──────────────────────────────────────────────────

function computeStats(
  records: AffinityRecord[],
  profiles: ZipcodeProfile[],
  totalDevices: number
): LabStats {
  const totalDeviceDays = records.reduce((sum, r) => sum + r.visits, 0);
  const totalUniqueDevices = totalDevices;
  const totalPostalCodes = profiles.length;

  // Category breakdown
  const catMap = new Map<PoiCategory, {
    visits: number; records: AffinityRecord[]; maxAffinity: number; maxRec: AffinityRecord | null;
  }>();

  for (const rec of records) {
    const cat = catMap.get(rec.category) || { visits: 0, records: [], maxAffinity: 0, maxRec: null };
    cat.visits += rec.visits;
    cat.records.push(rec);
    if (rec.affinityIndex > cat.maxAffinity) {
      cat.maxAffinity = rec.affinityIndex;
      cat.maxRec = rec;
    }
    catMap.set(rec.category, cat);
  }

  const categoryBreakdown: CategoryStat[] = Array.from(catMap.entries()).map(([cat, data]) => ({
    category: cat,
    label: CATEGORY_LABELS[cat],
    group: getCategoryGroup(cat),
    visits: data.visits,
    percentOfTotal: totalDeviceDays > 0 ? Math.round((data.visits / totalDeviceDays) * 10000) / 100 : 0,
    postalCodesWithVisits: new Set(data.records.map(r => r.zipcode)).size,
    avgAffinity: data.records.length > 0
      ? Math.round(data.records.reduce((s, r) => s + r.affinityIndex, 0) / data.records.length)
      : 0,
    maxAffinity: data.maxAffinity,
    maxAffinityZipcode: data.maxRec?.zipcode || '',
    maxAffinityCity: data.maxRec?.city || '',
  })).sort((a, b) => b.visits - a.visits);

  // Unique categories in records
  const uniqueCategories = new Set(records.map(r => r.category));

  // Top hotspots (highest affinity indices)
  const topHotspots: AffinityHotspot[] = records
    .filter(r => r.affinityIndex >= 70)
    .slice(0, 25)
    .map(r => ({
      zipcode: r.zipcode,
      city: r.city,
      category: r.category,
      categoryLabel: CATEGORY_LABELS[r.category],
      affinityIndex: r.affinityIndex,
      visits: r.visits,
      uniqueDevices: r.uniqueDevices,
    }));

  const avgAffinityIndex = records.length > 0
    ? Math.round(records.reduce((s, r) => s + r.affinityIndex, 0) / records.length)
    : 0;

  return {
    totalDeviceDays,
    totalUniqueDevices,
    totalPostalCodes,
    categoriesAnalyzed: uniqueCategories.size,
    avgAffinityIndex,
    categoryBreakdown,
    topHotspots,
  };
}


// ── Empty result builder ───────────────────────────────────────────────

function buildEmptyResult(
  countryInfo: { code: string; name: string; datasetName: string },
  filters: LabFilters
): LabAnalysisResult {
  return {
    country: countryInfo.code,
    countryName: countryInfo.name,
    dataset: countryInfo.datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    records: [],
    profiles: [],
    stats: {
      totalDeviceDays: 0,
      totalUniqueDevices: 0,
      totalPostalCodes: 0,
      categoriesAnalyzed: 0,
      avgAffinityIndex: 0,
      categoryBreakdown: [],
      topHotspots: [],
    },
  };
}
