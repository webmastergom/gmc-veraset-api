import { NextRequest, NextResponse } from 'next/server';

const NOMINATIM_BASE_URL = 'https://nominatim.openstreetmap.org';
const OVERPASS_BASE_URL = 'https://overpass-api.de/api/interpreter';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * POST /api/pois/import/osm
 * Search for POIs in OpenStreetMap using Nominatim and Overpass APIs
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const {
      query, // Location name/address search
      category, // OSM category/tag (e.g., "amenity=restaurant", "shop=tobacco")
      bbox, // Bounding box: [minLon, minLat, maxLon, maxLat]
      lat,
      lon,
      radius, // Radius in meters
      limit = 1000,
    } = body;

    let pois: any[] = [];

    // User agent required by Nominatim usage policy
    const headers = {
      'User-Agent': 'Veraset-POI-Importer/1.0',
      'Accept': 'application/json',
    };

    // Strategy 1: Search by location name/address using Nominatim
    if (query) {
      try {
        const nominatimParams = new URLSearchParams({
          q: query,
          format: 'json',
          limit: String(Math.min(limit, 50)), // Nominatim has lower limits
          addressdetails: '1',
        });

        const nominatimResponse = await fetch(
          `${NOMINATIM_BASE_URL}/search?${nominatimParams}`,
          { headers }
        );

        if (nominatimResponse.ok) {
          const nominatimData = await nominatimResponse.json();
          
          // Convert Nominatim results to POI format
          const nominatimPois = nominatimData.map((item: any) => ({
            id: `osm_${item.place_id}`,
            name: item.display_name.split(',')[0] || item.name || 'Unnamed POI',
            latitude: parseFloat(item.lat),
            longitude: parseFloat(item.lon),
            address: item.address?.road || item.address?.house_number || '',
            city: item.address?.city || item.address?.town || item.address?.village || '',
            state: item.address?.state || '',
            zipcode: item.address?.postcode || '',
            country: item.address?.country_code?.toUpperCase() || '',
            category: item.type || item.class || '',
            raw: item,
          }));

          pois = [...pois, ...nominatimPois];
        }
      } catch (error) {
        console.error('Nominatim search error:', error);
      }
    }

    // Strategy 2: Query by category/tags using Overpass API
    if (category || (lat && lon && radius)) {
      try {
        let overpassQuery = '';

        if (category) {
          // Parse category (e.g., "amenity=restaurant" or just "restaurant")
          const [key, value] = category.includes('=') 
            ? category.split('=').map((s: string) => s.trim())
            : ['amenity', category];

          if (bbox) {
            // Query by bounding box
            const [minLon, minLat, maxLon, maxLat] = bbox;
            overpassQuery = `
              [out:json][timeout:25];
              (
                node["${key}"="${value}"](${minLat},${minLon},${maxLat},${maxLon});
                way["${key}"="${value}"](${minLat},${minLon},${maxLat},${maxLon});
                relation["${key}"="${value}"](${minLat},${minLon},${maxLat},${maxLon});
              );
              out center ${Math.min(limit, 10000)} meta;
            `;
          } else if (lat && lon && radius) {
            // Query by radius (convert meters to approximate degrees)
            const radiusDegrees = radius / 111000; // Rough conversion
            const minLat = lat - radiusDegrees;
            const maxLat = lat + radiusDegrees;
            const minLon = lon - radiusDegrees / Math.cos(lat * Math.PI / 180);
            const maxLon = lon + radiusDegrees / Math.cos(lat * Math.PI / 180);

            overpassQuery = `
              [out:json][timeout:25];
              (
                node["${key}"="${value}"](around:${radius},${lat},${lon});
                way["${key}"="${value}"](around:${radius},${lat},${lon});
                relation["${key}"="${value}"](around:${radius},${lat},${lon});
              );
              out center ${Math.min(limit, 10000)} meta;
            `;
          } else {
            // Global query (limited)
            overpassQuery = `
              [out:json][timeout:25];
              (
                node["${key}"="${value}"];
                way["${key}"="${value}"];
                relation["${key}"="${value}"];
              );
              out center ${Math.min(limit, 1000)} meta;
            `;
          }
        } else if (bbox) {
          // Query all POIs in bounding box
          const [minLon, minLat, maxLon, maxLat] = bbox;
          overpassQuery = `
            [out:json][timeout:25];
            (
              node["amenity"](${minLat},${minLon},${maxLat},${maxLon});
              node["shop"](${minLat},${minLon},${maxLat},${maxLon});
              way["amenity"](${minLat},${minLon},${maxLat},${maxLon});
              way["shop"](${minLat},${minLon},${maxLat},${maxLon});
            );
            out center ${Math.min(limit, 10000)} meta;
          `;
        } else if (lat && lon && radius) {
          // Query all POIs in radius
          overpassQuery = `
            [out:json][timeout:25];
            (
              node["amenity"](around:${radius},${lat},${lon});
              node["shop"](around:${radius},${lat},${lon});
              way["amenity"](around:${radius},${lat},${lon});
              way["shop"](around:${radius},${lat},${lon});
            );
            out center ${Math.min(limit, 10000)} meta;
          `;
        }

        if (overpassQuery) {
          const overpassResponse = await fetch(OVERPASS_BASE_URL, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: `data=${encodeURIComponent(overpassQuery)}`,
          });

          if (overpassResponse.ok) {
            const overpassData = await overpassResponse.json();
            
            // Convert Overpass results to POI format
            const overpassPois = (overpassData.elements || []).map((element: any) => {
              const tags = element.tags || {};
              const centerLat = element.lat || element.center?.lat;
              const centerLon = element.lon || element.center?.lon;

              if (!centerLat || !centerLon) return null;

              return {
                id: `osm_${element.type}_${element.id}`,
                name: tags.name || tags['name:en'] || tags.brand || 'Unnamed POI',
                latitude: centerLat,
                longitude: centerLon,
                address: tags['addr:street'] || '',
                city: tags['addr:city'] || tags['addr:town'] || tags['addr:village'] || '',
                state: tags['addr:state'] || tags['addr:province'] || '',
                zipcode: tags['addr:postcode'] || '',
                country: tags['addr:country']?.toUpperCase() || '',
                category: tags.amenity || tags.shop || tags.leisure || tags.tourism || '',
                subcategory: Object.entries(tags)
                  .filter(([k]) => k.startsWith('amenity') || k.startsWith('shop'))
                  .map(([, v]) => v)
                  .join(', '),
                raw: element,
              };
            }).filter(Boolean);

            pois = [...pois, ...overpassPois];
          }
        }
      } catch (error) {
        console.error('Overpass query error:', error);
      }
    }

    // Deduplicate by coordinates (within ~10 meters)
    const uniquePois = [];
    const seen = new Set<string>();

    for (const poi of pois) {
      const key = `${Math.round(poi.latitude * 1000)},${Math.round(poi.longitude * 1000)}`;
      if (!seen.has(key)) {
        seen.add(key);
        uniquePois.push(poi);
      }
    }

    // Limit results
    const limitedPois = uniquePois.slice(0, limit);

    return NextResponse.json({
      pois: limitedPois,
      total: limitedPois.length,
      limit,
      hasMore: uniquePois.length > limit,
    });

  } catch (error: any) {
    console.error('POST /api/pois/import/osm error:', error);
    return NextResponse.json(
      { error: 'Failed to search OSM POIs', details: error.message },
      { status: 500 }
    );
  }
}
