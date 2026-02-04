// POI Search Endpoint
const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';

export default async function handler(req, res) {
  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    return res.status(200).end();
  }

  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  const apiKey = process.env.VERASET_API_KEY;
  
  if (!apiKey) {
    return res.status(500).json({ 
      error: true, 
      message: 'VERASET_API_KEY not configured' 
    });
  }

  try {
    if (req.method === 'GET') {
      // GET /api/veraset/pois?brand=Starbucks&state=CA
      const { brand, category, subcategory, state, city, zipcode } = req.query;
      
      // Build query params
      const params = new URLSearchParams();
      if (brand) params.append('brand', brand);
      if (category) params.append('category', category);
      if (subcategory) params.append('subcategory', subcategory);
      if (state) params.append('state', state);
      if (city) params.append('city', city);
      if (zipcode) params.append('zipcode', zipcode);

      const endpoint = brand 
        ? `/v1/poi/brands?${params}` 
        : `/v1/poi/top-categories`;

      const response = await fetch(`${VERASET_BASE_URL}${endpoint}`, {
        headers: {
          'X-API-Key': apiKey,
          'Content-Type': 'application/json',
        },
      });

      const data = await response.json();
      return res.status(response.status).json(data);
    }

    if (req.method === 'POST') {
      // POST /api/veraset/pois - Get detailed POI info
      const body = req.body;

      const response = await fetch(`${VERASET_BASE_URL}/v1/poi/pois`, {
        method: 'POST',
        headers: {
          'X-API-Key': apiKey,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });

      const data = await response.json();
      return res.status(response.status).json(data);
    }

    return res.status(405).json({ error: true, message: 'Method not allowed' });

  } catch (error) {
    console.error('Veraset POI error:', error);
    return res.status(500).json({ 
      error: true, 
      message: error.message 
    });
  }
}
