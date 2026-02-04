import { NextRequest, NextResponse } from 'next/server';

const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * POST /api/pois/import/veraset
 * Search for POIs in Veraset API and return results
 */
export async function POST(request: NextRequest) {
  try {
    const apiKey = process.env.VERASET_API_KEY;
    
    if (!apiKey) {
      return NextResponse.json(
        { error: 'VERASET_API_KEY not configured' },
        { status: 500 }
      );
    }

    const body = await request.json();
    const { 
      brand, 
      category, 
      subcategory, 
      country,
      state, 
      city, 
      zipcode,
      limit = 1000,
      offset = 0
    } = body;

    // Build query parameters
    const params = new URLSearchParams();
    if (brand) params.append('brand', brand);
    if (category) params.append('category', category);
    if (subcategory) params.append('subcategory', subcategory);
    if (state) params.append('state', state);
    if (city) params.append('city', city);
    if (zipcode) params.append('zipcode', zipcode);
    if (limit) params.append('limit', String(limit));
    if (offset) params.append('offset', String(offset));

    // Use GET endpoint to get POIs (as per Veraset API documentation)
    // Build query parameters for GET request
    const queryParams = new URLSearchParams();
    if (brand) queryParams.append('brand', brand);
    if (category) queryParams.append('category', category);
    if (subcategory) queryParams.append('subcategory', subcategory);
    if (country) queryParams.append('country', country);
    if (state) queryParams.append('state', state);
    if (city) queryParams.append('city', city);
    if (zipcode) queryParams.append('zipcode', zipcode);
    if (limit) queryParams.append('limit', String(limit));
    if (offset) queryParams.append('offset', String(offset));
    
    const queryString = queryParams.toString();
    const url = queryString 
      ? `${VERASET_BASE_URL}/v1/poi/pois?${queryString}`
      : `${VERASET_BASE_URL}/v1/poi/pois`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'X-API-Key': apiKey,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      let errorData: any;
      try {
        errorData = await response.json();
      } catch {
        errorData = { error_message: await response.text() };
      }
      
      console.error('Veraset API error:', errorData);
      
      // Provide more helpful error messages
      if (response.status === 401) {
        return NextResponse.json(
          { 
            error: 'Veraset API authentication failed', 
            details: errorData.error_message || 'Invalid API key. Please check your VERASET_API_KEY environment variable.',
            hint: 'Make sure VERASET_API_KEY is set in your .env file'
          },
          { status: 401 }
        );
      }
      
      return NextResponse.json(
        { error: 'Veraset API failed', details: errorData.error_message || JSON.stringify(errorData) },
        { status: response.status }
      );
    }

    const data = await response.json();
    
    // Transform Veraset POI format to our format
    const pois = (data.pois || data.data || []).map((poi: any) => ({
      id: poi.poi_id || poi.id,
      place_key: poi.place_key || poi.placeKey, // Critical: place_key is used for movement API
      name: poi.name || poi.poi_name || poi.brand || 'Unnamed POI',
      latitude: poi.latitude || poi.lat,
      longitude: poi.longitude || poi.lng || poi.lon,
      category: poi.category,
      subcategory: poi.subcategory,
      brand: poi.brand,
      address: poi.address,
      city: poi.city,
      state: poi.state,
      zipcode: poi.zipcode || poi.zip,
      country: poi.country,
      // Include all original data for reference
      raw: poi,
    }));

    return NextResponse.json({
      pois,
      total: data.total || pois.length,
      limit,
      offset,
      hasMore: pois.length === limit,
    });

  } catch (error: any) {
    console.error('POST /api/pois/import/veraset error:', error);
    return NextResponse.json(
      { error: 'Failed to search POIs', details: error.message },
      { status: 500 }
    );
  }
}

/**
 * GET /api/pois/import/veraset?type=[type]
 * Get available POIs, categories, subcategories, brands, etc. from Veraset
 * 
 * Types:
 * - 'pois': Get all available POIs (direct access to /v1/poi/pois)
 * - 'top': Get top categories
 * - 'sub': Get subcategories
 * - 'brands': Get brands
 * - 'countries': Get countries
 * - 'states': Get states
 * - 'cities': Get cities
 * - 'zipcodes': Get zipcodes
 */
export async function GET(request: NextRequest) {
  try {
    const apiKey = process.env.VERASET_API_KEY;
    
    if (!apiKey) {
      return NextResponse.json(
        { error: 'VERASET_API_KEY not configured' },
        { status: 500 }
      );
    }

    const { searchParams } = new URL(request.url);
    const type = searchParams.get('type') || 'top';
    const category = searchParams.get('category');
    const subcategory = searchParams.get('subcategory');
    const brand = searchParams.get('brand');
    const country = searchParams.get('country');
    const state = searchParams.get('state');
    const city = searchParams.get('city');
    const zipcode = searchParams.get('zipcode');

    let endpoint: string;
    const params = new URLSearchParams();

    switch (type) {
      case 'pois':
        // Direct access to POIs endpoint - GET all available POIs
        endpoint = '/v1/poi/pois';
        // Allow optional query parameters for filtering
        if (category) params.append('category', category);
        if (subcategory) params.append('subcategory', subcategory);
        if (brand) params.append('brand', brand);
        if (country) params.append('country', country);
        if (state) params.append('state', state);
        if (city) params.append('city', city);
        if (zipcode) params.append('zipcode', zipcode);
        break;
      
      case 'top':
        endpoint = '/v1/poi/top-categories';
        break;
      
      case 'sub':
        endpoint = '/v1/poi/sub-categories';
        if (category) params.append('category', category);
        break;
      
      case 'brands':
        endpoint = '/v1/poi/brands';
        if (subcategory) params.append('subcategory', subcategory);
        if (country) params.append('country', country);
        if (state) params.append('state', state);
        if (city) params.append('city', city);
        break;
      
      case 'countries':
        endpoint = '/v1/poi/countries';
        break;
      
      case 'states':
        endpoint = '/v1/poi/states';
        if (country) params.append('country', country);
        break;
      
      case 'cities':
        endpoint = '/v1/poi/cities';
        if (country) params.append('country', country);
        if (state) params.append('state', state);
        break;
      
      case 'zipcodes':
        endpoint = '/v1/poi/zipcodes';
        if (country) params.append('country', country);
        if (state) params.append('state', state);
        if (city) params.append('city', city);
        break;

      default:
        return NextResponse.json(
          { error: 'Invalid type. Use: pois, top, sub, brands, countries, states, cities, zipcodes' },
          { status: 400 }
        );
    }

    const url = params.toString() 
      ? `${VERASET_BASE_URL}${endpoint}?${params}`
      : `${VERASET_BASE_URL}${endpoint}`;

    const response = await fetch(url, {
      headers: {
        'X-API-Key': apiKey,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      let errorData: any;
      try {
        errorData = await response.json();
      } catch {
        errorData = { error_message: await response.text() };
      }
      
      console.error('Veraset API error:', errorData);
      
      // Provide more helpful error messages
      if (response.status === 401) {
        return NextResponse.json(
          { 
            error: 'Veraset API authentication failed', 
            details: errorData.error_message || 'Invalid API key. Please check your VERASET_API_KEY environment variable.',
            hint: 'Make sure VERASET_API_KEY is set in your .env file'
          },
          { status: 401 }
        );
      }
      
      return NextResponse.json(
        { error: 'Veraset API failed', details: errorData.error_message || JSON.stringify(errorData) },
        { status: response.status }
      );
    }

    const data = await response.json();
    
    // If type is 'pois', transform the response to match our POI format
    if (type === 'pois') {
      const pois = (data.pois || data.data || []).map((poi: any) => ({
        id: poi.poi_id || poi.id,
        place_key: poi.place_key || poi.placeKey, // Critical: place_key is used for movement API
        name: poi.name || poi.poi_name || poi.brand || 'Unnamed POI',
        latitude: poi.latitude || poi.lat,
        longitude: poi.longitude || poi.lng || poi.lon,
        category: poi.category,
        subcategory: poi.subcategory,
        brand: poi.brand,
        address: poi.address,
        city: poi.city,
        state: poi.state,
        zipcode: poi.zipcode || poi.zip,
        country: poi.country,
        raw: poi,
      }));
      
      return NextResponse.json({
        pois,
        total: data.total || pois.length,
        hasMore: data.hasMore || false,
      });
    }
    
    return NextResponse.json(data);

  } catch (error: any) {
    console.error('GET /api/pois/import/veraset error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch categories', details: error.message },
      { status: 500 }
    );
  }
}
