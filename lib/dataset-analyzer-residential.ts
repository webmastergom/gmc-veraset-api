/**
 * Residential zipcode analysis using AWS Athena + local reverse geocoding.
 *
 * Strategy:
 * 1. Query Athena for nighttime pings (20:00-04:00 UTC → 22:00-06:00 local CET/CEST)
 * 2. Median lat/lng per device to estimate "home" (robust vs outliers)
 * 3. Require pings in ≥2 distinct nights (filters tourists in hotels)
 * 4. Reverse geocode home locations — classify by postal code (any country), foreign, unmatched
 * 5. Aggregate by zipcode with device counts and percentOfTotal/percentOfClassified
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';
import { batchReverseGeocode, aggregateByZipcode } from './reverse-geocode';
import type {
  ResidentialFilters,
  ResidentialZipcode,
  ResidentialAnalysisResult,
  CatchmentMethodology,
  CatchmentCoverage,
} from './catchment-types';

export type { ResidentialFilters, ResidentialZipcode, ResidentialAnalysisResult } from './catchment-types';

const ACCURACY_THRESHOLD_METERS = 200;

/**
 * Analyze residential zipcodes of visitors in a dataset.
 *
 * This function:
 * 1. Finds devices that visited POIs
 * 2. Identifies their nighttime locations (when not at POIs) as potential home locations
 * 3. Reverse geocodes these locations — postal code (any country), foreign, or unmatched
 * 4. Aggregates results by zipcode with coverage and methodology metadata
 */
export async function analyzeResidentialZipcodes(
  datasetName: string,
  filters: ResidentialFilters = {}
): Promise<ResidentialAnalysisResult> {
  const tableName = getTableName(datasetName);
  const minNightPings = filters.minNightPings ?? 3;
  const minDistinctNights = filters.minDistinctNights ?? 2;

  console.log(`[CATCHMENT] Starting residential analysis for dataset: ${datasetName}`, {
    filters,
    minNightPings,
    minDistinctNights,
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
    console.log(`[CATCHMENT] Creating table for dataset: ${datasetName}`);
    try {
      await createTableForDataset(datasetName);
    } catch (error: any) {
      if (error.message?.includes('not authorized') ||
          error.message?.includes('Access denied')) {
        throw new Error(
          `Cannot create Athena table: Access denied. Please ensure your IAM user has Glue CreateTable permissions.\n\n` +
          `Original error: ${error.message}`
        );
      }
      throw error;
    }
  } else {
    console.log(`[CATCHMENT] Table exists, verifying schema compatibility...`);
    try {
      await createTableForDataset(datasetName);
    } catch (error: any) {
      if (!error.message?.includes('already exists')) {
        console.warn(`[CATCHMENT] Warning checking table schema:`, error.message);
      }
    }
  }

  // Build WHERE conditions for date filters
  const dateConditions: string[] = [];
  if (filters.dateFrom) {
    dateConditions.push(`DATE(utc_timestamp) >= DATE '${filters.dateFrom}'`);
  }
  if (filters.dateTo) {
    dateConditions.push(`DATE(utc_timestamp) <= DATE '${filters.dateTo}'`);
  }
  const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

  // Build POI filter for inclusion (only count devices that visited these POIs)
  let poiFilter = '';
  if (filters.poiIds?.length) {
    const poiList = filters.poiIds.map(p => {
      const escaped = p.replace(/'/g, "''");
      return `'${escaped}'`;
    }).join(',');
    poiFilter = `AND poi_id IN (${poiList})`;
  }

  console.log(`[CATCHMENT] Running Athena queries for ${datasetName}...`);

  // Step 1: Get total unique devices (visitors to POIs) — UNNEST for complete coverage
  const totalQuery = `
    SELECT COUNT(DISTINCT ad_id) as total_devices
    FROM ${tableName}
    CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
    WHERE poi_id IS NOT NULL AND poi_id != ''
      ${dateWhere} ${poiFilter}
  `;

  // Step 2: Get home location estimates from nighttime pings
  // Night window: 20:00-04:00 UTC covers 22:00-06:00 local in both CET (UTC+1) and CEST (UTC+2)
  // — Winter: 21:00-05:00 local; Summer: 22:00-06:00 local → avoids capturing morning commuters at 7am
  // Only consider pings NOT at POIs (likely at home)
  // Use median (APPROX_PERCENTILE 0.5) instead of AVG — robust to GPS outliers
  // Require minDistinctNights — filters tourists in hotels (single-night stays)
  const homeQuery = `
    WITH
    poi_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        ${dateWhere}
        ${poiFilter}
    ),
    night_pings AS (
      SELECT
        t.ad_id,
        t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as latitude,
        TRY_CAST(t.longitude AS DOUBLE) as longitude
      FROM ${tableName} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE (HOUR(t.utc_timestamp) >= 20 OR HOUR(t.utc_timestamp) < 4)
        AND CARDINALITY(t.poi_ids) = 0
        AND TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
        ${dateWhere}
    ),
    device_home AS (
      SELECT
        ad_id,
        ROUND(APPROX_PERCENTILE(latitude, 0.5), 4) as home_lat,
        ROUND(APPROX_PERCENTILE(longitude, 0.5), 4) as home_lng,
        COUNT(*) as night_ping_count,
        COUNT(DISTINCT DATE(utc_timestamp)) as distinct_nights
      FROM night_pings
      WHERE latitude IS NOT NULL AND longitude IS NOT NULL
      GROUP BY ad_id
      HAVING COUNT(*) >= ${minNightPings}
        AND COUNT(DISTINCT DATE(utc_timestamp)) >= ${minDistinctNights}
    )
    SELECT
      home_lat,
      home_lng,
      COUNT(*) as device_count,
      SUM(night_ping_count) as total_night_pings
    FROM device_home
    GROUP BY home_lat, home_lng
    ORDER BY device_count DESC
    LIMIT 100000
  `;

  // Run both queries in parallel
  let totalRes, homeRes;
  try {
    console.log(`[CATCHMENT] Executing total devices query...`);
    [totalRes, homeRes] = await Promise.all([
      runQuery(totalQuery),
      runQuery(homeQuery),
    ]);
  } catch (error: any) {
    const errorMsg = error.message || String(error);
    console.error(`[CATCHMENT ERROR] Athena query failed:`, errorMsg);
    console.error(`[CATCHMENT ERROR] Error details:`, {
      dataset: datasetName,
      filters,
      errorName: error.name,
      errorStack: error.stack,
    });

    if (errorMsg.includes('Access denied') || errorMsg.includes('not authorized')) {
      throw new Error(`Athena access denied. Please ensure your AWS IAM user has Athena QueryExecution permissions. Original error: ${errorMsg}`);
    } else if (errorMsg.includes('does not exist') || errorMsg.includes('Table')) {
      throw new Error(`Table not found for dataset ${datasetName}. Ensure the dataset has been synced and partitions are registered. Original error: ${errorMsg}`);
    } else if (errorMsg.includes('timeout') || errorMsg.includes('Timeout')) {
      throw new Error(`Query timed out. Large datasets may take longer. Try reducing the date range or contact support. Original error: ${errorMsg}`);
    }

    throw new Error(`Athena query failed: ${errorMsg}`);
  }

  const totalDevices = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`[CATCHMENT] Total devices in dataset: ${totalDevices}`);
  console.log(`[CATCHMENT] Home location clusters found: ${homeRes.rows.length}`);

  if (totalDevices === 0) {
    console.warn(`[CATCHMENT] No devices found in dataset ${datasetName}`);
    return buildEmptyResult(datasetName, filters);
  }

  // Step 3: Reverse geocode home locations
  const homePoints = homeRes.rows.map(r => ({
    lat: parseFloat(String(r.home_lat)),
    lng: parseFloat(String(r.home_lng)),
    deviceCount: parseInt(String(r.device_count)) || 0,
  }));

  const devicesWithHome = homePoints.reduce((sum, p) => sum + p.deviceCount, 0);
  const noHomeLocation = totalDevices - devicesWithHome;
  console.log(`[CATCHMENT] Devices with estimated home location: ${devicesWithHome}`);
  console.log(`[CATCHMENT] Devices without sufficient night data: ${noHomeLocation}`);

  if (devicesWithHome === 0) {
    console.warn(`[CATCHMENT] No devices with home location found`);
    return buildResultWithNoHome(
      datasetName,
      filters,
      totalDevices,
      noHomeLocation,
    );
  }

  console.log(`[CATCHMENT] Reverse geocoding ${homePoints.length} location clusters...`);
  let classified;
  try {
    classified = await batchReverseGeocode(homePoints);
  } catch (error: any) {
    const errorMsg = error.message || String(error);
    console.error(`[CATCHMENT ERROR] Reverse geocoding failed:`, errorMsg);
    console.error(`[CATCHMENT ERROR] Error details:`, {
      dataset: datasetName,
      homePointsCount: homePoints.length,
      errorName: error.name,
    });

    throw new Error(`Reverse geocoding failed: ${errorMsg}. This may indicate an issue with the geocoding service or invalid coordinates.`);
  }

  // Step 4: Aggregate by zipcode
  const { zipcodes, foreignDevices, unmatchedDomestic, nominatimTruncated } = aggregateByZipcode(
    classified,
    totalDevices,
  );

  const devicesMatchedToZipcode = zipcodes.reduce((sum, z) => sum + z.devices, 0);
  const classifiedTotal = devicesMatchedToZipcode + foreignDevices;
  const classificationRatePercent = totalDevices > 0 ? Math.round((classifiedTotal / totalDevices) * 10000) / 100 : 0;

  console.log(`[CATCHMENT] Devices matched to postal code: ${devicesMatchedToZipcode}`);
  console.log(`[CATCHMENT] Foreign: ${foreignDevices}, unmatched domestic: ${unmatchedDomestic}, nominatim truncated: ${nominatimTruncated}`);
  console.log(`[CATCHMENT] Unique zipcodes found: ${zipcodes.length}`);
  console.log(`[CATCHMENT] Classification rate: ${classificationRatePercent}%`);

  if (zipcodes.length > 0) {
    console.log(`[CATCHMENT] Top zipcode: ${zipcodes[0].zipcode} (${zipcodes[0].city}) - ${zipcodes[0].devices} devices`);
  }

  const methodology: CatchmentMethodology = {
    nightWindowUtc: '20:00-04:00 UTC',
    nightWindowLocal: '22:00-06:00 CET / 22:00-06:00 CEST',
    minNightPings,
    minDistinctNights,
    accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
    homeEstimationMethod: 'median',
    privacyMinDevices: 0,
  };

  const coverage: CatchmentCoverage = {
    totalDevicesVisitedPois: totalDevices,
    devicesWithHomeEstimate: devicesWithHome,
    devicesMatchedToZipcode: devicesMatchedToZipcode,
    devicesForeignOrigin: foreignDevices,
    devicesUnmatched: unmatchedDomestic,
    devicesNominatimTruncated: nominatimTruncated,
    devicesInsufficientNightData: noHomeLocation,
    classificationRatePercent,
    geocodingComplete: nominatimTruncated === 0,
  };

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology,
    coverage,
    summary: {
      totalDevicesInDataset: totalDevices,
      devicesWithHomeLocation: devicesWithHome,
      devicesMatchedToZipcode,
      totalZipcodes: zipcodes.length,
      topZipcode: zipcodes[0]?.zipcode ?? null,
      topCity: zipcodes[0]?.city ?? null,
    },
    zipcodes,
  };
}

function buildEmptyResult(datasetName: string, filters: ResidentialFilters): ResidentialAnalysisResult {
  const methodology: CatchmentMethodology = {
    nightWindowUtc: '20:00-04:00 UTC',
    nightWindowLocal: '22:00-06:00 CET / 22:00-06:00 CEST',
    minNightPings: filters.minNightPings ?? 3,
    minDistinctNights: filters.minDistinctNights ?? 2,
    accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
    homeEstimationMethod: 'median',
    privacyMinDevices: 0,
  };

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology,
    coverage: {
      totalDevicesVisitedPois: 0,
      devicesWithHomeEstimate: 0,
      devicesMatchedToZipcode: 0,
      devicesForeignOrigin: 0,
      devicesUnmatched: 0,
      devicesNominatimTruncated: 0,
      devicesInsufficientNightData: 0,
      classificationRatePercent: 0,
      geocodingComplete: true,
    },
    summary: {
      totalDevicesInDataset: 0,
      devicesWithHomeLocation: 0,
      devicesMatchedToZipcode: 0,
      totalZipcodes: 0,
      topZipcode: null,
      topCity: null,
    },
    zipcodes: [],
  };
}

function buildResultWithNoHome(
  datasetName: string,
  filters: ResidentialFilters,
  totalDevices: number,
  noHomeLocation: number,
): ResidentialAnalysisResult {
  const methodology: CatchmentMethodology = {
    nightWindowUtc: '20:00-04:00 UTC',
    nightWindowLocal: '22:00-06:00 CET / 22:00-06:00 CEST',
    minNightPings: filters.minNightPings ?? 3,
    minDistinctNights: filters.minDistinctNights ?? 2,
    accuracyThresholdMeters: ACCURACY_THRESHOLD_METERS,
    homeEstimationMethod: 'median',
    privacyMinDevices: 0,
  };

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    methodology,
    coverage: {
      totalDevicesVisitedPois: totalDevices,
      devicesWithHomeEstimate: 0,
      devicesMatchedToZipcode: 0,
      devicesForeignOrigin: 0,
      devicesUnmatched: 0,
      devicesNominatimTruncated: 0,
      devicesInsufficientNightData: noHomeLocation,
      classificationRatePercent: 0,
      geocodingComplete: true,
    },
    summary: {
      totalDevicesInDataset: totalDevices,
      devicesWithHomeLocation: 0,
      devicesMatchedToZipcode: 0,
      totalZipcodes: 0,
      topZipcode: null,
      topCity: null,
    },
    zipcodes: [],
  };
}
