// Movement Endpoint - Pings, Devices, Aggregate
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

  const apiKey = process.env.VERASET_API_KEY;
  
  if (!apiKey) {
    return res.status(500).json({ 
      error: true, 
      message: 'VERASET_API_KEY not configured' 
    });
  }

  try {
    const { type = 'pings', ...body } = req.body;

    // Validate required fields
    if (!body.date_range) {
      return res.status(400).json({
        error: true,
        message: 'date_range is required'
      });
    }

    // Endpoints from Veraset docs
    const endpoints = {
      'pings': '/v1/movement/job/pings',
      'devices': '/v1/movement/job/devices',
      'aggregate': '/v1/movement/job/aggregate',
      'pings_by_device': '/v1/movement/job/pings_by_device',
      'pings_by_ip': '/v1/movement/job/pings_by_ip',
      'cohort': '/v1/movement/job/cohort',
    };

    const endpoint = endpoints[type];
    if (!endpoint) {
      return res.status(400).json({
        error: true,
        message: `Invalid type. Use: ${Object.keys(endpoints).join(', ')}`
      });
    }

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
    console.error('Veraset Movement error:', error);
    return res.status(500).json({ 
      error: true, 
      message: error.message 
    });
  }
}
