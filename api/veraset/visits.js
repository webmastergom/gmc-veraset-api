// Visits Endpoint - Foot Traffic Data
const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';

export default async function handler(req, res) {
  // Handle CORS
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    return res.status(200).end();
  }

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  if (req.method !== 'POST') {
    return res.status(405).json({ error: true, message: 'Method not allowed' });
  }

  const apiKey = process.env.VERASET_API_KEY?.trim();
  
  if (!apiKey) {
    return res.status(500).json({ 
      error: true, 
      message: 'VERASET_API_KEY not configured' 
    });
  }

  try {
    const { type = 'aggregate', ...body } = req.body;

    // Validate required fields
    if (!body.date_range || !body.date_range.from_date || !body.date_range.to_date) {
      return res.status(400).json({
        error: true,
        message: 'date_range with from_date and to_date is required'
      });
    }

    // Must have geo_radius, geometry, or place_key
    if (!body.geo_radius && !body.geometry && !body.place_key) {
      return res.status(400).json({
        error: true,
        message: 'geo_radius, geometry, or place_key is required'
      });
    }

    const endpoint = type === 'aggregate' 
      ? '/v1/visits/job/aggregate'
      : '/v1/visits/job/visits';

    const response = await fetch(`${VERASET_BASE_URL}${endpoint}`, {
      method: 'POST',
      headers: {
        'X-API-Key': apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    const data = await response.json();
    return res.status(response.status).json(data);

  } catch (error) {
    console.error('Veraset Visits error:', error);
    return res.status(500).json({ 
      error: true, 
      message: error.message 
    });
  }
}
