// Job Status Endpoint - Check async job status
const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';

export default async function handler(req, res) {
  // Handle CORS
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    return res.status(200).end();
  }

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');

  const apiKey = process.env.VERASET_API_KEY;
  
  if (!apiKey) {
    return res.status(500).json({ 
      error: true, 
      message: 'VERASET_API_KEY not configured' 
    });
  }

  // Get job_id from URL: /api/veraset/job/[id]
  const { id: jobId } = req.query;

  if (!jobId) {
    return res.status(400).json({ 
      error: true, 
      message: 'Job ID is required' 
    });
  }

  try {
    if (req.method === 'GET') {
      // Check if preview requested
      const { preview } = req.query;
      const endpoint = preview === 'true'
        ? `/v1/job/${jobId}/preview`
        : `/v1/job/${jobId}`;

      const response = await fetch(`${VERASET_BASE_URL}${endpoint}`, {
        headers: {
          'X-API-Key': apiKey,
          'Content-Type': 'application/json',
        },
      });

      const data = await response.json();
      return res.status(response.status).json(data);
    }

    if (req.method === 'DELETE') {
      // Cancel job
      const response = await fetch(`${VERASET_BASE_URL}/v1/job/${jobId}`, {
        method: 'DELETE',
        headers: {
          'X-API-Key': apiKey,
          'Content-Type': 'application/json',
        },
      });

      const data = await response.json();
      return res.status(response.status).json(data);
    }

    return res.status(405).json({ error: true, message: 'Method not allowed' });

  } catch (error) {
    console.error('Veraset Job error:', error);
    return res.status(500).json({ 
      error: true, 
      message: error.message 
    });
  }
}
