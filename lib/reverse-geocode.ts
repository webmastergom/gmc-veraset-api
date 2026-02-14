/**
 * Reverse geocoding for catchment — postal codes from 24 countries.
 * All local GeoJSON — no external API calls (Nominatim removed).
 *
 * Strategy:
 * 1. Determine candidate countries via bounding-box check on (lat, lng)
 * 2. Lazy-load the GeoJSON for each candidate country (cached in memory)
 * 3. Point-in-polygon test to find the matching postal code
 *
 * Supports: AR, BE, CL, CO, CR, DE, DO, EC, ES, FR, GT, HN, IE, IT,
 *           MX, NI, NL, PA, PE, PT, SE, SV, UK, US
 */

import * as fs from 'fs';
import * as path from 'path';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import booleanPointInPolygon from '@turf/boolean-point-in-polygon';
import { point as turfPoint } from '@turf/helpers';
import type { ResidentialZipcode } from './catchment-types';

// Re-export for backward compatibility (no longer used internally)
export { MAX_NOMINATIM_CALLS } from './catchment-types';

// ── Country bounding boxes (generated from GeoJSON files) ──────────────
// Used for fast pre-filtering: only load+search countries whose bbox contains the point.

interface BBox {
  minLat: number; maxLat: number; minLng: number; maxLng: number;
}

const COUNTRY_BBOXES: Record<string, BBox> = {
  AR: { minLat: -56, maxLat: -21, minLng: -74, maxLng: -25 },
  BE: { minLat: 49, maxLat: 52, minLng: 2, maxLng: 7 },
  CL: { minLat: -56, maxLat: -17, minLng: -110, maxLng: -66 },
  CO: { minLat: -5, maxLat: 14, minLng: -82, maxLng: -66 },
  CR: { minLat: 5, maxLat: 12, minLng: -88, maxLng: -82 },
  DE: { minLat: 47, maxLat: 56, minLng: 5, maxLng: 16 },
  DO: { minLat: 17, maxLat: 20, minLng: -73, maxLng: -68 },
  EC: { minLat: -6, maxLat: 2, minLng: -92, maxLng: -75 },
  ES: { minLat: 27, maxLat: 44, minLng: -19, maxLng: 5 },
  FR: { minLat: -22, maxLat: 52, minLng: -62, maxLng: 56 },
  GT: { minLat: 13, maxLat: 18, minLng: -93, maxLng: -88 },
  HN: { minLat: 12, maxLat: 17, minLng: -90, maxLng: -83 },
  IE: { minLat: 51, maxLat: 56, minLng: -11, maxLng: -5 },
  IT: { minLat: 35, maxLat: 48, minLng: 6, maxLng: 19 },
  MX: { minLat: 14, maxLat: 33, minLng: -118, maxLng: -86 },
  NI: { minLat: 10, maxLat: 16, minLng: -88, maxLng: -82 },
  NL: { minLat: 50, maxLat: 54, minLng: 3, maxLng: 8 },
  PA: { minLat: 7, maxLat: 10, minLng: -84, maxLng: -77 },
  PE: { minLat: -19, maxLat: 1, minLng: -82, maxLng: -68 },
  PT: { minLat: 30, maxLat: 43, minLng: -32, maxLng: -6 },
  SE: { minLat: 55, maxLat: 70, minLng: 10, maxLng: 25 },
  SV: { minLat: 13, maxLat: 15, minLng: -91, maxLng: -87 },
  UK: { minLat: 49, maxLat: 61, minLng: -8, maxLng: 2 },
  US: { minLat: 18, maxLat: 72, minLng: -177, maxLng: -66 },
};

// ── Per-country property parsers ───────────────────────────────────────
// Each country has a different GeoJSON properties schema.
// This normalizes them to { postcode, city, province, region }.

interface NormalizedProps {
  postcode: string;
  city: string;
  province: string;
  region: string;
}

function parseProperties(country: string, props: Record<string, any>): NormalizedProps {
  const pc = String(props.postal_code || '');

  switch (country) {
    case 'ES': {
      // description: "01001, Vitoria, Álava, País Vasco, ESP"
      const parts = (props.description || '').split(',').map((s: string) => s.trim());
      return { postcode: pc, city: parts[1] || '', province: parts[2] || '', region: parts[3] || '' };
    }
    case 'FR':
      return { postcode: pc, city: props.libelle || '', province: props.departement || '', region: '' };
    case 'DE':
      return { postcode: pc, city: (props.note || '').replace(/^\d+\s*/, ''), province: '', region: '' };
    case 'IT':
      return { postcode: pc, city: props.name || '', province: props.provincia || '', region: props.regione || '' };
    case 'UK':
      return { postcode: pc, city: (props.name || '').replace(/ postcode district$/, ''), province: '', region: '' };
    case 'US':
      return { postcode: pc, city: '', province: '', region: '' };
    case 'MX':
      return { postcode: pc, city: '', province: props.estado || '', region: '' };
    case 'CO':
      return { postcode: pc, city: '', province: props.departamento || '', region: '' };
    case 'BE':
    case 'NL':
    case 'IE':
    case 'SE':
      return { postcode: pc, city: props.name || '', province: '', region: '' };
    case 'AR':
    case 'CL':
    case 'PE':
      return { postcode: pc, city: props.nam || props.gna || '', province: props.fna || '', region: '' };
    default:
      return { postcode: pc, city: props.name || props.libelle || '', province: '', region: '' };
  }
}

// ── Types ──────────────────────────────────────────────────────────────

export interface ZipcodeResult {
  country: string;
  postcode: string;
  city: string;
  province: string;
  region: string;
}

interface ZipcodeFeature {
  type: 'Feature';
  properties: Record<string, any>;
  geometry: {
    type: 'Polygon' | 'MultiPolygon';
    coordinates: number[][][] | number[][][][];
  };
}

export type GeocodeClassification =
  | { type: 'geojson_local'; country: string; postcode: string; city: string; province: string; region: string; devices: number }
  | { type: 'nominatim_match'; country: string; postcode: string; city: string; province: string; region: string; devices: number }
  | { type: 'foreign'; country: string; devices: number }
  | { type: 'unmatched'; devices: number }
  | { type: 'nominatim_truncated'; devices: number; lat: number; lng: number };

// ── In-memory caches ───────────────────────────────────────────────────

// Country → loaded features (lazy-loaded per country)
const countryFeaturesCache = new Map<string, ZipcodeFeature[]>();

// Coordinate cache: "lat,lng" → result
const geocodeCache = new Map<string, { country: string; result: NormalizedProps } | null>();
const MAX_CACHE_SIZE = 50000;

// ── GeoJSON loading ────────────────────────────────────────────────────

const GEOJSON_DIR = path.join(process.cwd(), 'data', 'geojson');
const S3_BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
const S3_GEOJSON_PREFIX = 'geojson';

// Pending S3 downloads (avoid duplicate fetches)
const s3DownloadPromises = new Map<string, Promise<ZipcodeFeature[]>>();

function loadCountryFeaturesLocal(country: string): ZipcodeFeature[] | null {
  const filePath = path.join(GEOJSON_DIR, `${country}.geojson`);
  if (!fs.existsSync(filePath)) return null;

  console.log(`[GEOCODE] Loading ${country}.geojson from disk...`);
  const raw = fs.readFileSync(filePath, 'utf-8');
  const geojson = JSON.parse(raw);
  const features = (geojson.features || []) as ZipcodeFeature[];
  console.log(`[GEOCODE] Loaded ${features.length} polygons for ${country}`);
  return features;
}

async function loadCountryFeaturesFromS3(country: string): Promise<ZipcodeFeature[]> {
  const s3 = new S3Client({
    region: process.env.AWS_REGION || 'us-west-2',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
  });

  const key = `${S3_GEOJSON_PREFIX}/${country}.geojson`;
  console.log(`[GEOCODE] Downloading ${country}.geojson from S3 (${S3_BUCKET}/${key})...`);

  try {
    const res = await s3.send(new GetObjectCommand({ Bucket: S3_BUCKET, Key: key }));
    const raw = await res.Body?.transformToString('utf-8');
    if (!raw) {
      console.warn(`[GEOCODE] Empty response from S3 for ${country}`);
      return [];
    }
    const geojson = JSON.parse(raw);
    const features = (geojson.features || []) as ZipcodeFeature[];

    // Cache to disk for next time (best-effort, ignore write errors in serverless)
    try {
      fs.mkdirSync(GEOJSON_DIR, { recursive: true });
      fs.writeFileSync(path.join(GEOJSON_DIR, `${country}.geojson`), raw);
      console.log(`[GEOCODE] Cached ${country}.geojson to disk (${features.length} polygons)`);
    } catch {
      // Disk write may fail in read-only serverless environments — that's OK
    }

    console.log(`[GEOCODE] Downloaded ${features.length} polygons for ${country} from S3`);
    return features;
  } catch (err: any) {
    console.warn(`[GEOCODE] Failed to download ${country}.geojson from S3:`, err.message);
    return [];
  }
}

/**
 * Load country features: local disk first, S3 fallback.
 * Synchronous for cache hits, async for S3 downloads.
 * Returns cached features or [] if loading.
 */
function loadCountryFeatures(country: string): ZipcodeFeature[] {
  if (countryFeaturesCache.has(country)) return countryFeaturesCache.get(country)!;

  // Try local disk
  const local = loadCountryFeaturesLocal(country);
  if (local) {
    countryFeaturesCache.set(country, local);
    return local;
  }

  // Start S3 download in background (returns [] for this call)
  if (!s3DownloadPromises.has(country)) {
    const promise = loadCountryFeaturesFromS3(country).then((features) => {
      countryFeaturesCache.set(country, features);
      s3DownloadPromises.delete(country);
      return features;
    });
    s3DownloadPromises.set(country, promise);
  }

  // For now return empty — next call will have the cached data
  return [];
}

/**
 * Ensure all candidate countries for a set of points are loaded (including from S3).
 * Call this before batchReverseGeocode for S3-fallback environments.
 */
async function ensureCountriesLoaded(points: Array<{ lat: number; lng: number }>): Promise<void> {
  const needed = new Set<string>();
  for (const p of points) {
    for (const c of getCandidateCountries(p.lat, p.lng)) {
      if (!countryFeaturesCache.has(c)) needed.add(c);
    }
  }

  if (needed.size === 0) return;

  // Trigger loads and wait
  const promises: Promise<ZipcodeFeature[]>[] = [];
  for (const country of needed) {
    const local = loadCountryFeaturesLocal(country);
    if (local) {
      countryFeaturesCache.set(country, local);
    } else {
      if (s3DownloadPromises.has(country)) {
        promises.push(s3DownloadPromises.get(country)!);
      } else {
        const p = loadCountryFeaturesFromS3(country).then((features) => {
          countryFeaturesCache.set(country, features);
          s3DownloadPromises.delete(country);
          return features;
        });
        s3DownloadPromises.set(country, p);
        promises.push(p);
      }
    }
  }

  if (promises.length > 0) {
    console.log(`[GEOCODE] Downloading ${promises.length} country GeoJSON files from S3...`);
    await Promise.all(promises);
  }
}

// ── Core geocoding ─────────────────────────────────────────────────────

/**
 * Get candidate countries for a coordinate based on bounding-box overlap.
 * Returns countries sorted by bbox area (smallest first = most specific).
 */
function getCandidateCountries(lat: number, lng: number): string[] {
  const candidates: { country: string; area: number }[] = [];

  for (const [country, bbox] of Object.entries(COUNTRY_BBOXES)) {
    if (lat >= bbox.minLat && lat <= bbox.maxLat && lng >= bbox.minLng && lng <= bbox.maxLng) {
      const area = (bbox.maxLat - bbox.minLat) * (bbox.maxLng - bbox.minLng);
      candidates.push({ country, area });
    }
  }

  // Smallest bbox first → more specific country checked first
  candidates.sort((a, b) => a.area - b.area);
  return candidates.map((c) => c.country);
}

/**
 * Reverse geocode a single coordinate to a postal code.
 * Checks all candidate countries (by bounding box) until a match is found.
 * Returns null if no match found in any of the 24 countries.
 */
export function getZipcode(lat: number, lng: number): ZipcodeResult | null {
  const cacheKey = `${lat.toFixed(4)},${lng.toFixed(4)}`;

  if (geocodeCache.has(cacheKey)) {
    const cached = geocodeCache.get(cacheKey);
    if (!cached) return null;
    return {
      country: cached.country,
      postcode: cached.result.postcode,
      city: cached.result.city,
      province: cached.result.province,
      region: cached.result.region,
    };
  }

  const candidates = getCandidateCountries(lat, lng);
  const pt = turfPoint([lng, lat]);

  let result: { country: string; result: NormalizedProps } | null = null;

  for (const country of candidates) {
    const features = loadCountryFeatures(country);
    for (const feature of features) {
      try {
        if (booleanPointInPolygon(pt, feature as any)) {
          const parsed = parseProperties(country, feature.properties);
          result = { country, result: parsed };
          break;
        }
      } catch {
        continue;
      }
    }
    if (result) break;
  }

  // Cache management
  if (geocodeCache.size >= MAX_CACHE_SIZE) {
    const keysToDelete = Array.from(geocodeCache.keys()).slice(0, Math.floor(MAX_CACHE_SIZE * 0.2));
    keysToDelete.forEach((k) => geocodeCache.delete(k));
  }

  geocodeCache.set(cacheKey, result);

  if (!result) return null;
  return {
    country: result.country,
    postcode: result.result.postcode,
    city: result.result.city,
    province: result.result.province,
    region: result.result.region,
  };
}

// ── Batch geocoding ────────────────────────────────────────────────────

/**
 * Batch reverse geocode. 100% local GeoJSON, with S3 fallback for serverless environments.
 * No external geocoding API calls.
 */
export async function batchReverseGeocode(
  points: Array<{ lat: number; lng: number; deviceCount: number }>
): Promise<GeocodeClassification[]> {
  // Pre-load all needed country GeoJSON files (from disk or S3)
  await ensureCountriesLoaded(points);

  const results: GeocodeClassification[] = [];

  for (const p of points) {
    const localResult = getZipcode(p.lat, p.lng);

    if (localResult && localResult.postcode) {
      results.push({
        type: 'geojson_local',
        country: localResult.country,
        postcode: localResult.postcode,
        city: localResult.city,
        province: localResult.province,
        region: localResult.region,
        devices: p.deviceCount,
      });
    } else {
      // No match in any of the 24 countries
      results.push({ type: 'unmatched', devices: p.deviceCount });
    }
  }

  return results;
}

// ── Aggregation ────────────────────────────────────────────────────────

/**
 * Aggregate classified results by zipcode.
 * nominatimTruncated always 0 now (no external API).
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

  for (const item of classified) {
    if (item.type === 'geojson_local') {
      const key = `${item.country}-${item.postcode}`;
      const existing = zipcodeMap.get(key);
      if (existing) {
        existing.devices += item.devices;
      } else {
        zipcodeMap.set(key, {
          city: item.city || item.country,
          province: item.province,
          region: item.region || item.country,
          devices: item.devices,
          sources: new Set<'geojson' | 'nominatim'>(['geojson']),
        });
      }
    } else if (item.type === 'nominatim_match') {
      // Kept for backward compatibility (won't be produced anymore)
      const key = `${item.country}-${item.postcode || item.city || 'unknown'}`;
      const existing = zipcodeMap.get(key);
      if (existing) {
        existing.devices += item.devices;
        existing.sources.add('nominatim');
      } else {
        zipcodeMap.set(key, {
          city: item.city || item.country,
          province: item.province,
          region: item.region || item.country,
          devices: item.devices,
          sources: new Set<'geojson' | 'nominatim'>(['nominatim']),
        });
      }
    } else if (item.type === 'foreign') {
      foreignDevices += item.devices;
    } else if (item.type === 'unmatched') {
      unmatchedDomestic += item.devices;
    }
    // nominatim_truncated: no longer produced
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

  return { zipcodes, foreignDevices, unmatchedDomestic, nominatimTruncated: 0 };
}
