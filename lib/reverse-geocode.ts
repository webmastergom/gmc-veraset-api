/**
 * Reverse geocoding service for Spain postal codes.
 * Uses a local GeoJSON file with postal code polygons and point-in-polygon matching.
 */

import * as fs from 'fs';
import * as path from 'path';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point as turfPoint } from '@turf/helpers';

export interface ZipcodeResult {
  zipcode: string;
  city: string;
  province: string;
  region: string;
}

interface ZipcodeFeature {
  type: 'Feature';
  properties: {
    postal_code: string;
    description: string;
  };
  geometry: {
    type: 'Polygon' | 'MultiPolygon';
    coordinates: number[][][] | number[][][][];
  };
}

// Module-level cache for the loaded GeoJSON data
let cachedFeatures: ZipcodeFeature[] | null = null;

// LRU cache for geocoding results (key: "lat,lng" rounded to 4 decimals)
const geocodeCache = new Map<string, ZipcodeResult | null>();
const MAX_CACHE_SIZE = 50000;

/**
 * Parse the description field from the GeoJSON.
 * Format: "01001, Vitoria, Álava, País Vasco, ESP"
 */
function parseDescription(description: string): { city: string; province: string; region: string } {
  const parts = description.split(',').map(s => s.trim());
  // parts[0] = postal code, parts[1] = city, parts[2] = province, parts[3] = region, parts[4] = country
  return {
    city: parts[1] || '',
    province: parts[2] || '',
    region: parts[3] || '',
  };
}

/**
 * Load Spain zipcode boundary data from the local GeoJSON file.
 * Cached in memory after first load.
 */
function loadZipcodeData(): ZipcodeFeature[] {
  if (cachedFeatures) return cachedFeatures;

  const filePath = path.join(process.cwd(), 'data', 'ES_zipcodes.geojson');

  if (!fs.existsSync(filePath)) {
    throw new Error(
      `Spain zipcode data not found at ${filePath}. ` +
      `Please place ES_zipcodes.geojson in the data/ directory.`
    );
  }

  console.log('Loading Spain zipcode boundaries...');
  const raw = fs.readFileSync(filePath, 'utf-8');
  const geojson = JSON.parse(raw);
  cachedFeatures = geojson.features as ZipcodeFeature[];
  console.log(`Loaded ${cachedFeatures.length} postal code polygons`);

  return cachedFeatures;
}

/**
 * Get the zipcode for a given lat/lng coordinate.
 * Returns null if the point is not within any Spanish postal code polygon.
 */
export function getZipcode(lat: number, lng: number): ZipcodeResult | null {
  // Round to 4 decimals (~11m precision) for cache key
  const cacheKey = `${lat.toFixed(4)},${lng.toFixed(4)}`;

  if (geocodeCache.has(cacheKey)) {
    return geocodeCache.get(cacheKey) || null;
  }

  const features = loadZipcodeData();
  const pt = turfPoint([lng, lat]); // turf uses [lng, lat]

  let result: ZipcodeResult | null = null;

  for (const feature of features) {
    try {
      if (booleanPointInPolygon(pt, feature as any)) {
        const { city, province, region } = parseDescription(feature.properties.description);
        result = {
          zipcode: feature.properties.postal_code,
          city,
          province,
          region,
        };
        break;
      }
    } catch {
      // Skip invalid geometries
      continue;
    }
  }

  // Manage cache size
  if (geocodeCache.size >= MAX_CACHE_SIZE) {
    // Delete oldest 20% of entries
    const keysToDelete = Array.from(geocodeCache.keys()).slice(0, MAX_CACHE_SIZE * 0.2);
    keysToDelete.forEach(k => geocodeCache.delete(k));
  }

  geocodeCache.set(cacheKey, result);
  return result;
}

/**
 * Batch reverse geocode multiple points.
 * Returns an array of results in the same order as input.
 */
export function batchReverseGeocode(
  points: Array<{ lat: number; lng: number; deviceCount: number }>
): Array<{ zipcode: string; city: string; province: string; region: string; devices: number } | null> {
  // Ensure data is loaded once before iteration
  loadZipcodeData();

  return points.map(p => {
    const result = getZipcode(p.lat, p.lng);
    if (!result) return null;
    return {
      ...result,
      devices: p.deviceCount,
    };
  });
}

/**
 * Aggregate batch results by zipcode.
 * Combines device counts for the same zipcode and calculates percentages.
 */
export function aggregateByZipcode(
  results: Array<{ zipcode: string; city: string; province: string; region: string; devices: number } | null>,
  totalDevices: number
): Array<{ zipcode: string; city: string; province: string; region: string; devices: number; percentage: number }> {
  const zipcodeMap = new Map<string, { city: string; province: string; region: string; devices: number }>();

  for (const result of results) {
    if (!result) continue;

    const existing = zipcodeMap.get(result.zipcode);
    if (existing) {
      existing.devices += result.devices;
    } else {
      zipcodeMap.set(result.zipcode, {
        city: result.city,
        province: result.province,
        region: result.region,
        devices: result.devices,
      });
    }
  }

  // Convert to array, calculate percentages, filter small counts for privacy
  const MIN_DEVICES = 5;

  return Array.from(zipcodeMap.entries())
    .filter(([, data]) => data.devices >= MIN_DEVICES)
    .map(([zipcode, data]) => ({
      zipcode,
      city: data.city,
      province: data.province,
      region: data.region,
      devices: data.devices,
      percentage: totalDevices > 0 ? Math.round((data.devices / totalDevices) * 10000) / 100 : 0,
    }))
    .sort((a, b) => b.devices - a.devices);
}
