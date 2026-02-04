/**
 * Calculate distance between two points using Haversine formula
 * @returns distance in meters
 */
export function haversineDistance(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const R = 6371000; // Earth radius in meters
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) *
      Math.cos(toRad(lat2)) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function toRad(degrees: number): number {
  return degrees * (Math.PI / 180);
}

export interface POI {
  id?: string;
  latitude: number;
  longitude: number;
  name?: string;
  [key: string]: any;
}

/**
 * Deduplicate POIs within a given radius (default 50m)
 */
export function deduplicatePOIs(pois: POI[], radiusMeters: number = 50): POI[] {
  const result: POI[] = [];
  const used = new Set<number>();
  
  for (let i = 0; i < pois.length; i++) {
    if (used.has(i)) continue;
    
    const poi = pois[i];
    const group = [poi];
    used.add(i);
    
    // Find all POIs within radius
    for (let j = i + 1; j < pois.length; j++) {
      if (used.has(j)) continue;
      
      const distance = haversineDistance(
        poi.latitude,
        poi.longitude,
        pois[j].latitude,
        pois[j].longitude
      );
      
      if (distance <= radiusMeters) {
        group.push(pois[j]);
        used.add(j);
      }
    }
    
    // Use the first POI as representative (or merge properties)
    result.push(group[0]);
  }
  
  return result;
}

/**
 * Convert GeoJSON FeatureCollection to POI array
 */
export function geojsonToPOIs(geojson: any): POI[] {
  if (!geojson.features) {
    return [];
  }
  
  return geojson.features.map((feature: any, index: number) => {
    const [longitude, latitude] = feature.geometry.coordinates;
    return {
      id: feature.id || `poi-${index}`,
      latitude,
      longitude,
      name: feature.properties?.name,
      ...feature.properties,
    };
  });
}

/**
 * Convert POI array to GeoJSON FeatureCollection
 */
export function poisToGeoJSON(pois: POI[]): any {
  return {
    type: 'FeatureCollection',
    features: pois.map((poi) => ({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [poi.longitude, poi.latitude],
      },
      properties: {
        ...poi,
        latitude: undefined,
        longitude: undefined,
      },
    })),
  };
}
