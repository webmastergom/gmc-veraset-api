/**
 * Residential zipcode analysis using AWS Athena + local reverse geocoding.
 *
 * Strategy:
 * 1. Query Athena for nighttime pings (22:00-06:00) NOT at POIs
 * 2. Average lat/lng per device to estimate "home" location
 * 3. Reverse geocode home locations to Spanish postal codes
 * 4. Aggregate by zipcode with device counts
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';
import { batchReverseGeocode, aggregateByZipcode } from './reverse-geocode';

export interface ResidentialFilters {
  dateFrom?: string;
  dateTo?: string;
  poiIds?: string[];
  minNightPings?: number; // minimum night pings per device (default 3)
}

export interface ResidentialZipcode {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  devices: number;
  percentage: number;
}

export interface ResidentialAnalysisResult {
  dataset: string;
  analyzedAt: string;
  filters: ResidentialFilters;
  summary: {
    totalDevicesInDataset: number;
    devicesWithHomeLocation: number;
    devicesMatchedToZipcode: number;
    totalZipcodes: number;
    topZipcode: string | null;
    topCity: string | null;
  };
  zipcodes: ResidentialZipcode[];
}

/**
 * Analyze residential zipcodes of visitors in a dataset.
 * 
 * This function:
 * 1. Finds devices that visited POIs
 * 2. Identifies their nighttime locations (when not at POIs) as potential home locations
 * 3. Reverse geocodes these locations to Spanish postal codes
 * 4. Aggregates results by zipcode
 */
export async function analyzeResidentialZipcodes(
  datasetName: string,
  filters: ResidentialFilters = {}
): Promise<ResidentialAnalysisResult> {
  const tableName = getTableName(datasetName);
  const minNightPings = filters.minNightPings || 3;

  console.log(`[CATCHMENT] Starting residential analysis for dataset: ${datasetName}`, {
    filters,
    minNightPings,
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
    poiFilter = `AND poi_ids[1] IN (${poiList})`;
  }

  console.log(`[CATCHMENT] Running Athena queries for ${datasetName}...`);

  // Step 1: Get total unique devices (for context)
  const totalQuery = `
    SELECT COUNT(DISTINCT ad_id) as total_devices
    FROM ${tableName}
    WHERE 1=1 ${dateWhere} ${poiFilter ? `AND poi_ids[1] IS NOT NULL ${poiFilter}` : ''}
  `;

  // Step 2: Get home location estimates from nighttime pings
  // Night = 22:00-06:00 local time (Spain is UTC+1/+2)
  // We use UTC hours 21-05 to approximate Spanish nighttime
  // Only consider pings NOT at POIs (likely at home)
  const homeQuery = `
    WITH
    -- First, get devices that visited our POIs
    poi_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${tableName}
      WHERE poi_ids[1] IS NOT NULL
        ${dateWhere}
        ${poiFilter}
    ),
    -- Then find their nighttime pings (NOT at POIs = likely at home)
    night_pings AS (
      SELECT t.ad_id, t.latitude, t.longitude
      FROM ${tableName} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE (HOUR(t.utc_timestamp) >= 21 OR HOUR(t.utc_timestamp) < 5)
        AND (t.poi_ids[1] IS NULL OR CARDINALITY(t.poi_ids) = 0)
        AND t.latitude IS NOT NULL
        AND t.longitude IS NOT NULL
        AND t.horizontal_accuracy < 200
        ${dateWhere}
    ),
    -- Average position per device = estimated home
    device_home AS (
      SELECT
        ad_id,
        ROUND(AVG(latitude), 4) as home_lat,
        ROUND(AVG(longitude), 4) as home_lng,
        COUNT(*) as night_ping_count
      FROM night_pings
      GROUP BY ad_id
      HAVING COUNT(*) >= ${minNightPings}
    )
    -- Group nearby home locations to reduce reverse geocoding calls
    SELECT
      home_lat,
      home_lng,
      COUNT(*) as device_count,
      SUM(night_ping_count) as total_night_pings
    FROM device_home
    GROUP BY home_lat, home_lng
    ORDER BY device_count DESC
    LIMIT 10000
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
    console.error(`[CATCHMENT ERROR] Athena query failed:`, error.message);
    throw new Error(`Athena query failed: ${error.message}`);
  }

  const totalDevices = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`[CATCHMENT] Total devices in dataset: ${totalDevices}`);
  console.log(`[CATCHMENT] Home location clusters found: ${homeRes.rows.length}`);

  if (totalDevices === 0) {
    console.warn(`[CATCHMENT] No devices found in dataset ${datasetName}`);
    return {
      dataset: datasetName,
      analyzedAt: new Date().toISOString(),
      filters,
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

  // Step 3: Reverse geocode home locations
  const homePoints = homeRes.rows.map(r => ({
    lat: parseFloat(String(r.home_lat)),
    lng: parseFloat(String(r.home_lng)),
    deviceCount: parseInt(String(r.device_count)) || 0,
  }));

  const devicesWithHome = homePoints.reduce((sum, p) => sum + p.deviceCount, 0);
  console.log(`[CATCHMENT] Devices with estimated home location: ${devicesWithHome}`);

  if (devicesWithHome === 0) {
    console.warn(`[CATCHMENT] No devices with home location found`);
    return {
      dataset: datasetName,
      analyzedAt: new Date().toISOString(),
      filters,
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

  console.log(`[CATCHMENT] Reverse geocoding ${homePoints.length} location clusters...`);
  let geocoded;
  try {
    geocoded = batchReverseGeocode(homePoints);
  } catch (error: any) {
    console.error(`[CATCHMENT ERROR] Reverse geocoding failed:`, error.message);
    throw new Error(`Reverse geocoding failed: ${error.message}`);
  }

  // Step 4: Aggregate by zipcode
  const zipcodes = aggregateByZipcode(geocoded, devicesWithHome);

  const devicesMatchedToZipcode = zipcodes.reduce((sum, z) => sum + z.devices, 0);
  console.log(`[CATCHMENT] Devices matched to Spanish zipcodes: ${devicesMatchedToZipcode}`);
  console.log(`[CATCHMENT] Unique zipcodes found: ${zipcodes.length}`);

  if (zipcodes.length > 0) {
    console.log(`[CATCHMENT] Top zipcode: ${zipcodes[0].zipcode} (${zipcodes[0].city}) - ${zipcodes[0].devices} devices`);
  }

  return {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    filters,
    summary: {
      totalDevicesInDataset: totalDevices,
      devicesWithHomeLocation: devicesWithHome,
      devicesMatchedToZipcode,
      totalZipcodes: zipcodes.length,
      topZipcode: zipcodes[0]?.zipcode || null,
      topCity: zipcodes[0]?.city || null,
    },
    zipcodes,
  };
}
