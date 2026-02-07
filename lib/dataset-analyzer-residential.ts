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
 */
export async function analyzeResidentialZipcodes(
  datasetName: string,
  filters: ResidentialFilters = {}
): Promise<ResidentialAnalysisResult> {
  const tableName = getTableName(datasetName);
  const minNightPings = filters.minNightPings || 3;

  // Ensure table exists
  if (!(await tableExists(datasetName))) {
    console.log(`Creating table for dataset: ${datasetName}`);
    await createTableForDataset(datasetName);
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
    const poiList = filters.poiIds.map(p => `'${p.replace(/'/g, "''")}'`).join(',');
    poiFilter = `AND poi_ids[1] IN (${poiList})`;
  }

  console.log(`Starting residential analysis for ${datasetName}...`);

  // Step 1: Get total unique devices (for context)
  const totalQuery = `
    SELECT COUNT(DISTINCT ad_id) as total_devices
    FROM ${tableName}
    WHERE 1=1 ${dateWhere} ${poiFilter ? `AND poi_ids[1] IS NOT NULL ${poiFilter}` : ''}
  `;

  // Step 2: Get home location estimates from nighttime pings
  // Night = 22:00-06:00 local time (Spain is UTC+1/+2)
  // We use UTC hours 21-05 to approximate Spanish nighttime
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

  console.log('Running residential Athena queries...');

  // Run both queries in parallel
  const [totalRes, homeRes] = await Promise.all([
    runQuery(totalQuery),
    runQuery(homeQuery),
  ]);

  const totalDevices = parseInt(String(totalRes.rows[0]?.total_devices)) || 0;
  console.log(`Total devices in dataset: ${totalDevices}`);
  console.log(`Home location clusters found: ${homeRes.rows.length}`);

  // Step 3: Reverse geocode home locations
  const homePoints = homeRes.rows.map(r => ({
    lat: parseFloat(String(r.home_lat)),
    lng: parseFloat(String(r.home_lng)),
    deviceCount: parseInt(String(r.device_count)) || 0,
  }));

  const devicesWithHome = homePoints.reduce((sum, p) => sum + p.deviceCount, 0);
  console.log(`Devices with estimated home location: ${devicesWithHome}`);

  console.log(`Reverse geocoding ${homePoints.length} location clusters...`);
  const geocoded = batchReverseGeocode(homePoints);

  // Step 4: Aggregate by zipcode
  const zipcodes = aggregateByZipcode(geocoded, devicesWithHome);

  const devicesMatchedToZipcode = zipcodes.reduce((sum, z) => sum + z.devices, 0);
  console.log(`Devices matched to Spanish zipcodes: ${devicesMatchedToZipcode}`);
  console.log(`Unique zipcodes found: ${zipcodes.length}`);

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
