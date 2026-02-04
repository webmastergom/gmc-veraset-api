// Categories & Brands Endpoint
const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';

export default async function handler(req, res) {
  // Handle CORS
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    return res.status(200).end();
  }

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  if (req.method !== 'GET') {
    return res.status(405).json({ error: true, message: 'Method not allowed' });
  }

  const apiKey = process.env.VERASET_API_KEY;
  
  if (!apiKey) {
    return res.status(500).json({ 
      error: true, 
      message: 'VERASET_API_KEY not configured' 
    });
  }

  try {
    // /api/veraset/categories?type=top|sub|brands
    const { type = 'top', category, subcategory, brand, state, city } = req.query;

    let endpoint;
    const params = new URLSearchParams();

    switch (type) {
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
        if (state) params.append('state', state);
        if (city) params.append('city', city);
        break;
      
      case 'states':
        endpoint = '/v1/poi/states';
        break;
      
      case 'cities':
        endpoint = '/v1/poi/cities';
        if (state) params.append('state', state);
        break;
      
      case 'zipcodes':
        endpoint = '/v1/poi/zipcodes';
        if (state) params.append('state', state);
        if (city) params.append('city', city);
        break;

      default:
        return res.status(400).json({
          error: true,
          message: 'Invalid type. Use: top, sub, brands, states, cities, zipcodes'
        });
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

    const data = await response.json();
    return res.status(response.status).json(data);

  } catch (error) {
    console.error('Veraset Categories error:', error);
    return res.status(500).json({ 
      error: true, 
      message: error.message 
    });
  }
}
