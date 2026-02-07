/**
 * POI Enrichment service.
 * Matches existing GeoJSON POIs against Veraset's POI database
 * to obtain placekeys for hybrid movement job requests.
 */

const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';
const SEARCH_RADIUS_METERS = 25;
const REQUEST_DELAY_MS = 150;

export interface EnrichmentMatch {
  originalId: string;
  originalName: string;
  originalLat: number;
  originalLng: number;
  verasetName: string | null;
  verasetPlacekey: string | null;
  verasetCategory: string | null;
  verasetNaics: string | null;
  distanceMeters: number | null;
  status: 'matched' | 'no_match' | 'error';
}

export interface EnrichmentResult {
  collectionId: string;
  totalPois: number;
  matched: number;
  unmatched: number;
  errors: number;
  matchRate: number;
  matches: EnrichmentMatch[];
  processedAt: string;
}

/**
 * Calculate distance between two lat/lng points in meters (Haversine).
 */
function haversineDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371000; // Earth radius in meters
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/**
 * Search Veraset POI database for a single point.
 */
async function searchVerasetPOIs(
  apiKey: string,
  lat: number,
  lng: number,
  poiId: string,
  radiusMeters: number = SEARCH_RADIUS_METERS
): Promise<any[]> {
  const body = {
    geo_radius: [{
      poi_id: poiId,
      latitude: lat,
      longitude: lng,
      distance_in_meters: radiusMeters,
      distance_in_miles: 0,
    }],
  };

  const response = await fetch(`${VERASET_BASE_URL}/v1/poi/pois`, {
    method: 'POST',
    headers: {
      'X-API-Key': apiKey,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    throw new Error(`Veraset API error: ${response.status}`);
  }

  const data = await response.json();
  return data.data?.pois || [];
}

/**
 * Find the best matching Veraset POI for a given original POI.
 * Prioritizes: closest distance + matching category.
 */
function findBestMatch(
  originalLat: number,
  originalLng: number,
  originalCategory: string | undefined,
  verasetPois: any[]
): { poi: any; distance: number } | null {
  if (!verasetPois.length) return null;

  // Target NAICS codes for tobacco/vape
  const tobaccoNaics = ['453991', '4539'];
  const isTargetCategory = originalCategory &&
    ['tobacco', 'tobacco_shop', 'e_cigarette', 'vape'].includes(originalCategory.toLowerCase());

  let bestMatch: { poi: any; distance: number } | null = null;
  let bestScore = -1;

  for (const vPoi of verasetPois) {
    // Parse point coordinates
    let vLat = vPoi.latitude;
    let vLng = vPoi.longitude;
    if (vPoi.point && typeof vPoi.point === 'string') {
      const [lat, lng] = vPoi.point.split(',').map(Number);
      if (!isNaN(lat) && !isNaN(lng)) {
        vLat = lat;
        vLng = lng;
      }
    }

    if (!vLat || !vLng) continue;

    const distance = haversineDistance(originalLat, originalLng, vLat, vLng);
    const naics = vPoi.naics_code || '';
    const isTobaccoMatch = tobaccoNaics.some(n => naics.startsWith(n));

    // Score: distance (closer is better) + category match bonus
    let score = Math.max(0, 100 - distance); // 0-100 based on distance
    if (isTargetCategory && isTobaccoMatch) score += 50; // Big bonus for category match
    if (isTobaccoMatch) score += 20; // Some bonus even without category info

    if (score > bestScore) {
      bestScore = score;
      bestMatch = { poi: vPoi, distance };
    }
  }

  return bestMatch;
}

/**
 * Enrich a POI collection by matching against Veraset's POI database.
 *
 * @param features - GeoJSON features to enrich
 * @param apiKey - Veraset API key
 * @param onProgress - Progress callback (current, total)
 */
export async function enrichPOICollection(
  features: any[],
  apiKey: string,
  onProgress?: (current: number, total: number, match: EnrichmentMatch) => void
): Promise<EnrichmentResult> {
  const matches: EnrichmentMatch[] = [];
  let matched = 0;
  let unmatched = 0;
  let errors = 0;

  for (let i = 0; i < features.length; i++) {
    const feature = features[i];
    const coords = feature.geometry?.coordinates;
    if (!coords || coords.length < 2) {
      errors++;
      continue;
    }

    const lng = coords[0];
    const lat = coords[1];
    const props = feature.properties || {};
    const poiId = props.id || feature.id || `poi_${i}`;
    const poiName = props.name || poiId;
    const category = props.category;

    let match: EnrichmentMatch;

    try {
      const verasetPois = await searchVerasetPOIs(apiKey, lat, lng, poiId);
      const bestMatch = findBestMatch(lat, lng, category, verasetPois);

      if (bestMatch && bestMatch.distance <= SEARCH_RADIUS_METERS) {
        match = {
          originalId: poiId,
          originalName: poiName,
          originalLat: lat,
          originalLng: lng,
          verasetName: bestMatch.poi.location_name || null,
          verasetPlacekey: bestMatch.poi.placekey || null,
          verasetCategory: bestMatch.poi.sub_category || null,
          verasetNaics: bestMatch.poi.naics_code || null,
          distanceMeters: Math.round(bestMatch.distance * 10) / 10,
          status: 'matched',
        };
        matched++;
      } else {
        match = {
          originalId: poiId,
          originalName: poiName,
          originalLat: lat,
          originalLng: lng,
          verasetName: null,
          verasetPlacekey: null,
          verasetCategory: null,
          verasetNaics: null,
          distanceMeters: null,
          status: 'no_match',
        };
        unmatched++;
      }
    } catch (err: any) {
      match = {
        originalId: poiId,
        originalName: poiName,
        originalLat: lat,
        originalLng: lng,
        verasetName: null,
        verasetPlacekey: null,
        verasetCategory: null,
        verasetNaics: null,
        distanceMeters: null,
        status: 'error',
      };
      errors++;
    }

    matches.push(match);
    onProgress?.(i + 1, features.length, match);

    // Rate limit
    if (i < features.length - 1) {
      await new Promise(resolve => setTimeout(resolve, REQUEST_DELAY_MS));
    }
  }

  const totalPois = features.length;
  return {
    collectionId: '',
    totalPois,
    matched,
    unmatched,
    errors,
    matchRate: totalPois > 0 ? Math.round((matched / totalPois) * 10000) / 100 : 0,
    matches,
    processedAt: new Date().toISOString(),
  };
}

/**
 * Apply enrichment results to a GeoJSON FeatureCollection.
 * Adds place_key to properties of matched features.
 */
export function applyEnrichmentToGeoJSON(
  geojson: any,
  matches: EnrichmentMatch[]
): any {
  // Build lookup by original ID
  const matchMap = new Map<string, EnrichmentMatch>();
  for (const m of matches) {
    if (m.status === 'matched' && m.verasetPlacekey) {
      matchMap.set(m.originalId, m);
    }
  }

  const enrichedFeatures = geojson.features.map((feature: any) => {
    const featureId = feature.properties?.id || feature.id;
    const match = matchMap.get(featureId);

    if (match) {
      return {
        ...feature,
        properties: {
          ...feature.properties,
          place_key: match.verasetPlacekey,
          veraset_name: match.verasetName,
          veraset_category: match.verasetCategory,
          veraset_naics: match.verasetNaics,
          veraset_distance_m: match.distanceMeters,
        },
      };
    }

    return feature;
  });

  return {
    ...geojson,
    features: enrichedFeatures,
  };
}
