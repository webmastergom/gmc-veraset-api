import { NextRequest, NextResponse } from 'next/server';

// Overture Places API endpoints
// Note: Overture data is typically accessed via S3 or public datasets
// This implementation uses a simplified approach - may need adjustment based on actual Overture API
const OVERTURE_PLACES_API = 'https://places-api.overturemaps.org/v1/places';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * POST /api/pois/import/overture
 * Search for POIs in Overture Places dataset
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const {
      category, // Overture category filter
      bbox, // Bounding box: [minLon, minLat, maxLon, maxLat]
      limit = 1000,
      offset = 0,
    } = body;

    // Overture Places API typically requires:
    // - Bounding box or geometry
    // - Category filters
    // - Pagination

    let pois: any[] = [];

    try {
      // Build query parameters
      const params = new URLSearchParams();
      
      if (bbox && Array.isArray(bbox) && bbox.length === 4) {
        const [minLon, minLat, maxLon, maxLat] = bbox;
        params.append('bbox', `${minLon},${minLat},${maxLon},${maxLat}`);
      }

      if (category) {
        params.append('categories', category);
      }

      params.append('limit', String(Math.min(limit, 1000)));
      params.append('offset', String(offset));

      // Note: Overture API may require authentication or have different endpoints
      // This is a placeholder implementation - adjust based on actual Overture API docs
      const response = await fetch(`${OVERTURE_PLACES_API}?${params}`, {
        headers: {
          'Accept': 'application/json',
        },
      });

      if (response.ok) {
        const data = await response.json();
        
        // Transform Overture Places format to our POI format
        // Overture format may vary - adjust based on actual API response
        pois = (data.features || data.places || []).map((feature: any) => {
          const geometry = feature.geometry || feature.location;
          const properties = feature.properties || feature;

          // Extract coordinates
          let latitude: number;
          let longitude: number;

          if (geometry?.type === 'Point' && geometry.coordinates) {
            [longitude, latitude] = geometry.coordinates;
          } else if (geometry?.lat && geometry?.lon) {
            latitude = geometry.lat;
            longitude = geometry.lon;
          } else if (properties.lat && properties.lon) {
            latitude = properties.lat;
            longitude = properties.lon;
          } else {
            return null;
          }

          // Extract categories
          const categories = properties.categories || properties.category || [];
          const mainCategory = Array.isArray(categories) ? categories[0] : categories;

          return {
            id: `overture_${feature.id || properties.id || Math.random().toString(36).substr(2, 9)}`,
            name: properties.name || properties.names?.en || properties.names?.default || 'Unnamed POI',
            latitude,
            longitude,
            address: properties.address?.street || properties.street_address || '',
            city: properties.address?.city || properties.city || '',
            state: properties.address?.state || properties.state || '',
            zipcode: properties.address?.postcode || properties.postal_code || '',
            country: properties.address?.country_code?.toUpperCase() || properties.country_code?.toUpperCase() || '',
            category: mainCategory || '',
            subcategory: Array.isArray(categories) ? categories.slice(1).join(', ') : '',
            raw: feature,
          };
        }).filter(Boolean);
      } else {
        // If Overture API is not available, return empty results with a note
        console.warn('Overture API not available or returned error:', response.status);
        return NextResponse.json({
          pois: [],
          total: 0,
          limit,
          offset,
          hasMore: false,
          note: 'Overture API endpoint may need configuration. Please check Overture Places API documentation.',
        });
      }
    } catch (error: any) {
      // Overture API may not be publicly accessible
      // Return empty results with helpful message
      console.warn('Overture API error:', error.message);
      return NextResponse.json({
        pois: [],
        total: 0,
        limit,
        offset,
        hasMore: false,
        note: 'Overture Places API may require authentication or different endpoint configuration. Please refer to Overture documentation.',
      });
    }

    return NextResponse.json({
      pois,
      total: pois.length,
      limit,
      offset,
      hasMore: pois.length === limit,
    });

  } catch (error: any) {
    console.error('POST /api/pois/import/overture error:', error);
    return NextResponse.json(
      { error: 'Failed to search Overture POIs', details: error.message },
      { status: 500 }
    );
  }
}
