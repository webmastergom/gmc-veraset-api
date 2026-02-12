/**
 * Reverse geocoding for catchment — postal codes from any country.
 * Spain: local GeoJSON (fast). Other countries: Nominatim API.
 * Distinguishes foreign vs unmatched_domestic via Spain bounding box.
 */

import * as fs from 'fs';
import * as path from 'path';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point as turfPoint } from '@turf/helpers';
import type { ResidentialZipcode } from './catchment-types';
import { MAX_NOMINATIM_CALLS } from './catchment-types';

// N3+N4: Spain bounding boxes — distinguish foreign vs unmatched_domestic
const SPAIN_BBOX = {
  peninsula: { minLat: 35.8, maxLat: 43.8, minLng: -9.4, maxLng: 4.4 },
  baleares: { minLat: 38.6, maxLat: 40.1, minLng: 1.1, maxLng: 4.4 },
  canarias: { minLat: 27.5, maxLat: 29.5, minLng: -18.2, maxLng: -13.3 },
  ceuta: { minLat: 35.85, maxLat: 35.92, minLng: -5.38, maxLng: -5.26 },
  melilla: { minLat: 35.26, maxLat: 35.32, minLng: -2.97, maxLng: -2.92 },
};

function isPointInSpainBbox(lat: number, lng: number): boolean {
  return Object.values(SPAIN_BBOX).some(
    (box) => lat >= box.minLat && lat <= box.maxLat && lng >= box.minLng && lng <= box.maxLng
  );
}

export { MAX_NOMINATIM_CALLS } from './catchment-types';

export interface ZipcodeResult {
  country: string;
  postcode: string;
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

export type GeocodeClassification =
  | { type: 'spanish_local'; country: string; postcode: string; city: string; province: string; region: string; devices: number }
  | { type: 'nominatim_match'; country: string; postcode: string; city: string; province: string; region: string; devices: number }
  | { type: 'foreign'; country: string; devices: number }
  | { type: 'unmatched_domestic'; devices: number }
  | { type: 'nominatim_truncated'; devices: number; lat: number; lng: number };

// Module-level cache for the loaded GeoJSON data
let cachedFeatures: ZipcodeFeature[] | null = null;

// LRU cache for geocoding results (key: "lat,lng" rounded to 4 decimals)
const geocodeCache = new Map<string, ZipcodeResult | null>();
const MAX_CACHE_SIZE = 50000;

// Module-level Nominatim cache (read-only between invocations — shared for reuse)
const nominatimCache = new Map<string, { country: string; postcode: string; city: string } | null>();
const NOMINATIM_DELAY_MS = 1100; // 1.1s between requests (usage policy)

/**
 * Parse the description field from the GeoJSON.
 * Format: "01001, Vitoria, Álava, País Vasco, ESP"
 */
function parseDescription(description: string): { postcode: string; city: string; province: string; region: string; country: string } {
  const parts = description.split(',').map(s => s.trim());
  const countryCode = (parts[4] || 'ES').toUpperCase();
  return {
    postcode: parts[0] || '',
    city: parts[1] || '',
    province: parts[2] || '',
    region: parts[3] || '',
    country: countryCode === 'ESP' ? 'ES' : countryCode.slice(0, 2),
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

  console.log('[CATCHMENT] Loading Spain zipcode boundaries...');
  const raw = fs.readFileSync(filePath, 'utf-8');
  const geojson = JSON.parse(raw);
  cachedFeatures = geojson.features as ZipcodeFeature[];
  console.log(`[CATCHMENT] Loaded ${cachedFeatures.length} postal code polygons`);

  return cachedFeatures;
}

/**
 * Get the zipcode for a given lat/lng coordinate.
 * Returns null if the point is not within any Spanish postal code polygon.
 */
export function getZipcode(lat: number, lng: number): ZipcodeResult | null {
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
        const parsed = parseDescription(feature.properties.description);
        result = {
          country: parsed.country,
          postcode: feature.properties.postal_code || parsed.postcode,
          city: parsed.city,
          province: parsed.province,
          region: parsed.region,
        };
        break;
      }
    } catch {
      continue;
    }
  }

  if (geocodeCache.size >= MAX_CACHE_SIZE) {
    const keysToDelete = Array.from(geocodeCache.keys()).slice(0, MAX_CACHE_SIZE * 0.2);
    keysToDelete.forEach(k => geocodeCache.delete(k));
  }

  geocodeCache.set(cacheKey, result);
  return result;
}

/**
 * Batch reverse geocode. N1: State encapsulated per invocation — no race conditions.
 * N3+N4: Distinguishes foreign vs unmatched_domestic via Spain bbox.
 * N2: nominatim_truncated when limit reached.
 */
export async function batchReverseGeocode(
  points: Array<{ lat: number; lng: number; deviceCount: number }>
): Promise<GeocodeClassification[]> {
  loadZipcodeData();

  // N1: Local state — no shared mutable counters
  let callCount = 0;
  let lastCall = 0;

  type NominatimOutcome =
    | { result: { country: string; postcode: string; city: string }; truncated: false }
    | { result: null; truncated: true }
    | { result: null; truncated: false };

  async function localNominatimReverse(lat: number, lng: number): Promise<NominatimOutcome> {
    const key = `${lat.toFixed(4)},${lng.toFixed(4)}`;
    if (nominatimCache.has(key)) {
      const cached = nominatimCache.get(key) ?? null;
      return cached ? { result: cached, truncated: false } : { result: null, truncated: false };
    }
    if (callCount >= MAX_NOMINATIM_CALLS) return { result: null, truncated: true };
    callCount++;
    const now = Date.now();
    const wait = lastCall + NOMINATIM_DELAY_MS - now;
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastCall = Date.now();
    try {
      const url = `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&format=json&addressdetails=1`;
      const res = await fetch(url, {
        headers: { 'User-Agent': 'VerasetCatchment/1.0' },
      });
      if (!res.ok) return { result: null, truncated: false };
      const data = await res.json();
      const addr = data?.address;
      if (!addr) return { result: null, truncated: false };
      const country = addr.country_code?.toUpperCase() || addr.country || '';
      const postcode = addr.postcode || '';
      const city = addr.city || addr.town || addr.village || addr.municipality || addr.county || '';
      const result = country ? { country, postcode, city } : null;
      nominatimCache.set(key, result);
      if (nominatimCache.size > 50000) {
        const keys = Array.from(nominatimCache.keys()).slice(0, 10000);
        keys.forEach((k) => nominatimCache.delete(k));
      }
      return result ? { result, truncated: false } : { result: null, truncated: false };
    } catch {
      nominatimCache.set(key, null);
      return { result: null, truncated: false };
    }
  }

  const results: GeocodeClassification[] = [];

  for (const p of points) {
    const localResult = getZipcode(p.lat, p.lng);
    if (localResult) {
      results.push({
        type: 'spanish_local',
        country: localResult.country,
        postcode: localResult.postcode,
        city: localResult.city,
        province: localResult.province,
        region: localResult.region,
        devices: p.deviceCount,
      });
      continue;
    }

    const nom = await localNominatimReverse(p.lat, p.lng);

    if (nom.truncated) {
      results.push({ type: 'nominatim_truncated', devices: p.deviceCount, lat: p.lat, lng: p.lng });
      continue;
    }

    if (nom.result) {
      if (nom.result.country === 'ES') {
        results.push({
          type: 'nominatim_match',
          country: nom.result.country,
          postcode: nom.result.postcode,
          city: nom.result.city,
          province: '',
          region: nom.result.country,
          devices: p.deviceCount,
        });
      } else {
        results.push({ type: 'foreign', country: nom.result.country, devices: p.deviceCount });
      }
      continue;
    }

    if (isPointInSpainBbox(p.lat, p.lng)) {
      results.push({ type: 'unmatched_domestic', devices: p.deviceCount });
    } else {
      results.push({ type: 'foreign', country: 'UNKNOWN', devices: p.deviceCount });
    }
  }

  return results;
}

/**
 * Aggregate classified results by zipcode.
 * N5: Removed _noHomeLocationCount. N6: source field. N8: correct percentOfClassified denominator.
 */
export function aggregateByZipcode(
  classified: GeocodeClassification[],
  totalDevicesVisitedPois: number
): {
  zipcodes: ResidentialZipcode[];
  foreignDevices: number;
  unmatchedDomestic: number;
  nominatimTruncated: number;
} {
  const zipcodeMap = new Map<string, { city: string; province: string; region: string; devices: number; sources: Set<'geojson' | 'nominatim'> }>();
  let foreignDevices = 0;
  let unmatchedDomestic = 0;
  let nominatimTruncated = 0;

  for (const item of classified) {
    if (item.type === 'spanish_local') {
      const key = `${item.country}-${item.postcode}`;
      const existing = zipcodeMap.get(key);
      const src: 'geojson' | 'nominatim' = 'geojson';
      if (existing) {
        existing.devices += item.devices;
        existing.sources.add(src);
      } else {
        zipcodeMap.set(key, {
          city: item.city || item.country,
          province: item.province,
          region: item.region || item.country,
          devices: item.devices,
          sources: new Set([src]),
        });
      }
    } else if (item.type === 'nominatim_match') {
      const key = `${item.country}-${item.postcode || item.city || 'unknown'}`;
      const existing = zipcodeMap.get(key);
      const src: 'geojson' | 'nominatim' = 'nominatim';
      if (existing) {
        existing.devices += item.devices;
        existing.sources.add(src);
      } else {
        zipcodeMap.set(key, {
          city: item.city || item.country,
          province: item.province,
          region: item.region || item.country,
          devices: item.devices,
          sources: new Set([src]),
        });
      }
    } else if (item.type === 'foreign') {
      foreignDevices += item.devices;
    } else if (item.type === 'unmatched_domestic') {
      unmatchedDomestic += item.devices;
    } else if (item.type === 'nominatim_truncated') {
      nominatimTruncated += item.devices;
    }
  }

  const matchedTotal = Array.from(zipcodeMap.values()).reduce((s, v) => s + v.devices, 0);
  const effectivelyClassified = matchedTotal + foreignDevices;

  const zipcodes: ResidentialZipcode[] = Array.from(zipcodeMap.entries())
    .map(([zipcode, data]) => {
      const percentOfTotal = totalDevicesVisitedPois > 0 ? Math.round((data.devices / totalDevicesVisitedPois) * 10000) / 100 : 0;
      const percentOfClassified = effectivelyClassified > 0 ? Math.round((data.devices / effectivelyClassified) * 10000) / 100 : 0;
      const sources = data.sources;
      const source: 'geojson' | 'nominatim' | 'mixed' = sources.size === 2 ? 'mixed' : sources.has('geojson') ? 'geojson' : 'nominatim';
      return {
        zipcode,
        city: data.city,
        province: data.province,
        region: data.region,
        devices: data.devices,
        percentOfClassified,
        percentOfTotal,
        percentage: percentOfTotal,
        source,
      };
    })
    .sort((a, b) => b.devices - a.devices);

  return { zipcodes, foreignDevices, unmatchedDomestic, nominatimTruncated };
}
