/**
 * Origin-Destination analysis using AWS Athena + reverse geocoding.
 *
 * Strategy:
 * 1. For each device that visited a POI on a given day, get ALL pings ordered by timestamp
 * 2. First ping of the day = Origin (where the device came from)
 * 3. Last ping of the day = Destination (where it went after)
 * 4. Reverse geocode origin/destination coordinates to zipcodes
 * 5. Aggregate by zipcode with device counts and temporal patterns
 *
 * This replaces the nighttime-based residential catchment with observed movement data.
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';
import { batchReverseGeocode, aggregateByZipcode } from './reverse-geocode';
import type {
  ODFilters,
  ODZipcode,
  ODTemporalPattern,
  ODAnalysisResult,
  ODMethodology,
  ODCoverage,
} from './od-types';

export type { ODFilters, ODAnalysisResult } from './od-types';

const ACCURACY_THRESHOLD_METERS = 500; // More relaxed than residential (500 vs 200)
const COORDINATE_PRECISION = 4; // 4 decimal places ≈ 11m resolution

/**
 * Analyze origin-destination patterns of POI visitors.
 *
 * For each device-day that includes a POI visit:
 * - Origin = first GPS ping of the day
 * - Destination = last GPS ping of the day
 * Then reverse geocode and aggregate by zipcode.
 */
export async function analyzeOriginDestination(
  datasetName: string,
  filters: ODFilters = {}
): Promise<ODAnalysisResult> {
  const tableName = getTableName(datasetName);

  console.log(`[OD] Starting origin-destination analysis for dataset: ${datasetName}`, {
    filters,
    timestamp: new Date().toISOString(),
  });

  // Check AWS credentials
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error(
      'AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.'
    );
  }

  // Ensure table exists
  let tableExistsResult = false;
  try {
    tableExistsResult = await tableExists(datasetName);
  } catch (error: any) {
    if (error.message?.includes('not authorized') ||
        error.message?.includes('Access denied') ||
        error.name === 'AccessDeniedException') {
      throw new Error(
        `Athena access denied. Please ensure your AWS IAM user has Athena and Glue permissions.\n\n` +
        `Original error: ${error.message}`
      );
    }
    throw error;
  }

  if (!tableExistsResult) {
    console.log(`[OD] Creating table for dataset: ${datasetName}`);
    try {
      await createTableForDataset(datasetName);
    } catch (error: any) {
      if (error.message?.includes('not authorized') ||
          error.message?.includes('Access denied')) {
        throw new Error(
          `Cannot create Athena table: Access denied.\n\nOriginal error: ${error.message}`
        );
      }
      throw error;
    }
  } else {
    // Try to create anyway (ensures schema is up to date)
    try {
      await createTableForDataset(datasetName);
    } catch (error: any) {
      if (!error.message?.includes('already exists')) {
        console.warn(`[OD] Warning checking table schema:`, error.message);
      }
    }
  }

  // Build WHERE conditions
  const dateConditions: string[] = [];
  if (filters.dateFrom) {
    dateConditions.push(`date >= '${filters.dateFrom}'`);
  }
  if (filters.dateTo) {
    dateConditions.push(`date <= '${filters.dateTo}'`);
  }
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  // POI filter for inclusion
  let poiFilter = '';
  if (filters.poiIds?.length) {
    const poiList = filters.poiIds.map(p => {
      const escaped = p.replace(/'/g, "''");
      return `'${escaped}'`;
    }).join(',');
    poiFilter = `AND poi_id IN (${poiList})`;
  }

  console.log(`[OD] Running Athena OD query for ${datasetName}...`);

  // Main OD query: one CTE that extracts origin + destination per device-day
  const odQuery = `
    WITH
    poi_visits AS (
      SELECT
        ad_id,
        date,
        MIN(utc_timestamp) as first_poi_visit,
        MAX(utc_timestamp) as last_poi_visit
      FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${dateWhere} ${poiFilter}
      GROUP BY ad_id, date
    ),
    all_pings AS (
      SELECT
        t.ad_id,
        t.date,
        t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${tableName} t
      INNER JOIN (SELECT DISTINCT ad_id FROM poi_visits) v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS}
        ${dateWhere}
    ),
    device_day_trips AS (
      SELECT
        p.ad_id,
        p.date,
        FIRST_VALUE(p.lat) OVER (
          PARTITION BY p.ad_id, p.date ORDER BY p.utc_timestamp ASC
        ) as origin_lat,
        FIRST_VALUE(p.lng) OVER (
          PARTITION BY p.ad_id, p.date ORDER BY p.utc_timestamp ASC
        ) as origin_lng,
        FIRST_VALUE(p.utc_timestamp) OVER (
          PARTITION BY p.ad_id, p.date ORDER BY p.utc_timestamp ASC
        ) as origin_time,
        FIRST_VALUE(p.lat) OVER (
          PARTITION BY p.ad_id, p.date ORDER BY p.utc_timestamp DESC
        ) as dest_lat,
        FIRST_VALUE(p.lng) OVER (
          PARTITION BY p.ad_id, p.date ORDER BY p.utc_timestamp DESC
        ) as dest_lng,
        ROW_NUMBER() OVER (PARTITION BY p.ad_id, p.date ORDER BY p.utc_timestamp) as rn
      FROM all_pings p
      INNER JOIN poi_visits v ON p.ad_id = v.ad_id AND p.date = v.date
    )
    SELECT
      ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
      ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
      ROUND(dest_lat, ${COORDINATE_PRECISION}) as dest_lat,
      ROUND(dest_lng, ${COORDINATE_PRECISION}) as dest_lng,
      COUNT(*) as device_days,
      HOUR(origin_time) as arrival_hour
    FROM device_day_trips
    WHERE rn = 1
    GROUP BY
      ROUND(origin_lat, ${COORDINATE_PRECISION}),
      ROUND(origin_lng, ${COORDINATE_PRECISION}),
      ROUND(dest_lat, ${COORDINATE_PRECISION}),
      ROUND(dest_lng, ${COORDINATE_PRECISION}),
      HOUR(origin_time)
    ORDER BY device_days DESC
    LIMIT 100000
  `;

  // Also get total unique devices for coverage calculation
  const totalQuery = `
    SELECT COUNT(DISTINCT ad_id) as total_devices
    FROM ${tableName}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      ${dateWhere} ${poiFilter}
  `;

  let odRes, totalRes;
  try {
    console.log(`[OD] Executing OD + total queries in parallel...`);
    [odRes, totalRes] = await Promise.all([
      runQuery(odQuery),
      runQuery(totalQuery),
    ]);
  } catch (error: any) {
    const errorMsg = error.message || String(error);
    console.error(`[OD ERROR] Athena query failed:`, errorMsg);

    if (errorMsg.includes('Access denied') || errorMsg.includes('not authorized')) {
      throw new Error(`Athena access denied. Original error: ${errorMsg}`);
    } else if (errorMsg.includes('does not exist') || errorMsg.includes('Table')) {
      throw new Error(`Table not found for dataset ${datasetName}. Original error: ${errorMsg}`);
    } else if (errorMsg.includes('timeout') || errorMsg.includes('Timeout')) {
      throw new Error(`Query timed out. Try reducing the date range. Original error: ${errorMsg}`);
    }

    throw new Error(`Athena query failed: ${errorMsg}`);
  }

  const totalDevices = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`[OD] Total devices: ${totalDevices}, OD rows: ${odRes.rows.length}`);

  if (totalDevices === 0 || odRes.rows.length === 0) {
    console.warn(`[OD] No data found for dataset ${datasetName}`);
    return buildEmptyResult(datasetName, filters);
  }

  // Parse OD results — aggregate origins and destinations separately
  const originMap = new Map<string, { lat: number; lng: number; deviceDays: number }>();
  const destMap = new Map<string, { lat: number; lng: number; deviceDays: number }>();
  const hourMap = new Map<number, number>();
  let totalDeviceDays = 0;

  for (const row of odRes.rows) {
    const oLat = parseFloat(String(row.origin_lat));
    const oLng = parseFloat(String(row.origin_lng));
    const dLat = parseFloat(String(row.dest_lat));
    const dLng = parseFloat(String(row.dest_lng));
    const deviceDays = parseInt(String(row.device_days)) || 0;
    const hour = parseInt(String(row.arrival_hour)) || 0;

    if (isNaN(oLat) || isNaN(oLng) || isNaN(dLat) || isNaN(dLng)) continue;

    totalDeviceDays += deviceDays;

    // Aggregate origin points
    const oKey = `${oLat.toFixed(COORDINATE_PRECISION)},${oLng.toFixed(COORDINATE_PRECISION)}`;
    const existingO = originMap.get(oKey);
    if (existingO) {
      existingO.deviceDays += deviceDays;
    } else {
      originMap.set(oKey, { lat: oLat, lng: oLng, deviceDays });
    }

    // Aggregate destination points
    const dKey = `${dLat.toFixed(COORDINATE_PRECISION)},${dLng.toFixed(COORDINATE_PRECISION)}`;
    const existingD = destMap.get(dKey);
    if (existingD) {
      existingD.deviceDays += deviceDays;
    } else {
      destMap.set(dKey, { lat: dLat, lng: dLng, deviceDays });
    }

    // Temporal patterns
    hourMap.set(hour, (hourMap.get(hour) || 0) + deviceDays);
  }

  console.log(`[OD] Total device-days: ${totalDeviceDays}`);
  console.log(`[OD] Unique origin locations: ${originMap.size}`);
  console.log(`[OD] Unique destination locations: ${destMap.size}`);

  // Convert maps to arrays for reverse geocoding
  const originPoints = Array.from(originMap.values()).map(p => ({
    lat: p.lat,
    lng: p.lng,
    deviceCount: p.deviceDays,
  }));

  const destPoints = Array.from(destMap.values()).map(p => ({
    lat: p.lat,
    lng: p.lng,
    deviceCount: p.deviceDays,
  }));

  // Reverse geocode origins and destinations
  // Note: batchReverseGeocode has a 200-call Nominatim limit per invocation,
  // but local GeoJSON matching is unlimited and fast
  console.log(`[OD] Reverse geocoding ${originPoints.length} origin clusters...`);
  const originClassified = await batchReverseGeocode(originPoints);
  const originAgg = aggregateByZipcode(originClassified, totalDeviceDays);

  console.log(`[OD] Reverse geocoding ${destPoints.length} destination clusters...`);
  const destClassified = await batchReverseGeocode(destPoints);
  const destAgg = aggregateByZipcode(destClassified, totalDeviceDays);

  // Convert ResidentialZipcode[] to ODZipcode[] (drop the percentOfClassified field)
  const origins: ODZipcode[] = originAgg.zipcodes.map(z => ({
    zipcode: z.zipcode,
    city: z.city,
    province: z.province,
    region: z.region,
    devices: z.devices,
    percentOfTotal: z.percentOfTotal,
    source: z.source,
  }));

  const destinations: ODZipcode[] = destAgg.zipcodes.map(z => ({
    zipcode: z.zipcode,
    city: z.city,
    province: z.province,
    region: z.region,
    devices: z.devices,
    percentOfTotal: z.percentOfTotal,
    source: z.source,
  }));

  // Build temporal patterns (24 hours)
  const temporalPatterns: ODTemporalPattern[] = [];
  for (let h = 0; h < 24; h++) {
    const dd = hourMap.get(h) || 0;
    temporalPatterns.push({
      hour: h,
      deviceDays: dd,
      percentOfTotal: totalDeviceDays > 0 ? Math.round((dd / totalDeviceDays) * 10000) / 100 : 0,
    });
  }

  const devicesWithOrigin = originAgg.zipcodes.reduce((s, z) => s + z.devices, 0);
  const devicesWithDest = destAgg.zipcodes.reduce((s, z) => s + z.devices, 0);
  const geocodingComplete = originAgg.nominatimTruncated === 0 && destAgg.nominatimTruncated === 0;

  const coverage: ODCoverage = {
    totalDevicesVisitedPois: totalDevices,
    totalDeviceDays,
    devicesWithOrigin: devicesWithOrigin,
    devicesWithDestination: devicesWithDest,
    originZipcodes: origins.length,
    destinationZipcodes: destinations.length,
    coverageRatePercent: totalDeviceDays > 0
      ? Math.round((devicesWithOrigin / totalDeviceDays) * 10000) / 100
      : 0,
    geocodingComplete,
  };

  const methodology: ODMethodology = {
    approach: 'first_last_ping_per_day',
    description: 'For each device-day with a POI visit, origin = first GPS ping of the day, destination = last GPS ping of the day. Coordinates reverse geocoded to postal codes.',
    accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
    coordinatePrecision: COORDINATE_PRECISION,
  };

  console.log(`[OD] Analysis complete:`, {
    totalDeviceDays,
    originZipcodes: origins.length,
    destinationZipcodes: destinations.length,
    topOrigin: origins[0]?.zipcode || 'none',
    topDestination: destinations[0]?.zipcode || 'none',
    coverageRate: coverage.coverageRatePercent,
  });

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology,
    coverage,
    summary: {
      totalDeviceDays,
      topOriginZipcode: origins[0]?.zipcode ?? null,
      topOriginCity: origins[0]?.city ?? null,
      topDestinationZipcode: destinations[0]?.zipcode ?? null,
      topDestinationCity: destinations[0]?.city ?? null,
    },
    origins,
    destinations,
    temporalPatterns,
  };
}

/**
 * Lightweight origin-only analysis for catchment.
 * Uses MIN_BY instead of window functions — much faster for large datasets.
 * Only computes origins (first ping of each device-day), not destinations.
 */
export interface OriginsResult {
  dataset: string;
  analyzedAt: string;
  totalDevicesVisitedPois: number;
  totalDeviceDays: number;
  origins: ODZipcode[];
  geocodingComplete: boolean;
  coverageRatePercent: number;
}

export async function analyzeOrigins(
  datasetName: string,
  filters: ODFilters = {}
): Promise<OriginsResult> {
  const tableName = getTableName(datasetName);

  console.log(`[ORIGINS] Starting origin analysis for dataset: ${datasetName}`);

  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials not configured.');
  }

  // Ensure table exists
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

  // Build WHERE conditions
  const dateConditions: string[] = [];
  if (filters.dateFrom) dateConditions.push(`date >= '${filters.dateFrom}'`);
  if (filters.dateTo) dateConditions.push(`date <= '${filters.dateTo}'`);
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  let poiFilter = '';
  if (filters.poiIds?.length) {
    const poiList = filters.poiIds.map(p => `'${p.replace(/'/g, "''")}'`).join(',');
    poiFilter = `AND poi_id IN (${poiList})`;
  }

  // Optimized query: MIN_BY to get first ping coordinates per device-day
  // No window functions, no self-join on all_pings — just one pass
  const originsQuery = `
    WITH
    poi_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${dateWhere} ${poiFilter}
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
        AND TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS}
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
      ROUND(origin_lat, ${COORDINATE_PRECISION}) as origin_lat,
      ROUND(origin_lng, ${COORDINATE_PRECISION}) as origin_lng,
      COUNT(*) as device_days
    FROM first_pings
    GROUP BY
      ROUND(origin_lat, ${COORDINATE_PRECISION}),
      ROUND(origin_lng, ${COORDINATE_PRECISION})
    ORDER BY device_days DESC
    LIMIT 100000
  `;

  const totalQuery = `
    SELECT COUNT(DISTINCT ad_id) as total_devices
    FROM ${tableName}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      ${dateWhere} ${poiFilter}
  `;

  console.log(`[ORIGINS] Executing queries in parallel...`);
  const [originsRes, totalRes] = await Promise.all([
    runQuery(originsQuery),
    runQuery(totalQuery),
  ]);

  const totalDevices = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`[ORIGINS] Total devices: ${totalDevices}, origin clusters: ${originsRes.rows.length}`);

  if (totalDevices === 0 || originsRes.rows.length === 0) {
    return {
      dataset: datasetName,
      analyzedAt: new Date().toISOString(),
      totalDevicesVisitedPois: totalDevices,
      totalDeviceDays: 0,
      origins: [],
      geocodingComplete: true,
      coverageRatePercent: 0,
    };
  }

  let totalDeviceDays = 0;
  const originPoints = originsRes.rows.map(row => {
    const deviceDays = parseInt(String(row.device_days)) || 0;
    totalDeviceDays += deviceDays;
    return {
      lat: parseFloat(String(row.origin_lat)),
      lng: parseFloat(String(row.origin_lng)),
      deviceCount: deviceDays,
    };
  }).filter(p => !isNaN(p.lat) && !isNaN(p.lng));

  console.log(`[ORIGINS] Reverse geocoding ${originPoints.length} origin clusters...`);
  const classified = await batchReverseGeocode(originPoints);
  const agg = aggregateByZipcode(classified, totalDeviceDays);

  const origins: ODZipcode[] = agg.zipcodes.map(z => ({
    zipcode: z.zipcode,
    city: z.city,
    province: z.province,
    region: z.region,
    devices: z.devices,
    percentOfTotal: z.percentOfTotal,
    source: z.source,
  }));

  const devicesWithOrigin = origins.reduce((s, z) => s + z.devices, 0);

  console.log(`[ORIGINS] Done: ${origins.length} zipcodes, ${devicesWithOrigin} device-days matched`);

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    totalDevicesVisitedPois: totalDevices,
    totalDeviceDays,
    origins,
    geocodingComplete: agg.nominatimTruncated === 0,
    coverageRatePercent: totalDeviceDays > 0
      ? Math.round((devicesWithOrigin / totalDeviceDays) * 10000) / 100
      : 0,
  };
}

function buildEmptyResult(datasetName: string, filters: ODFilters): ODAnalysisResult {
  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology: {
      approach: 'first_last_ping_per_day',
      description: 'No data available for the specified filters.',
      accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
      coordinatePrecision: COORDINATE_PRECISION,
    },
    coverage: {
      totalDevicesVisitedPois: 0,
      totalDeviceDays: 0,
      devicesWithOrigin: 0,
      devicesWithDestination: 0,
      originZipcodes: 0,
      destinationZipcodes: 0,
      coverageRatePercent: 0,
      geocodingComplete: true,
    },
    summary: {
      totalDeviceDays: 0,
      topOriginZipcode: null,
      topOriginCity: null,
      topDestinationZipcode: null,
      topDestinationCity: null,
    },
    origins: [],
    destinations: [],
    temporalPatterns: Array.from({ length: 24 }, (_, h) => ({
      hour: h,
      deviceDays: 0,
      percentOfTotal: 0,
    })),
  };
}
