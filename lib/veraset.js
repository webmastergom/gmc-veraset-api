// Veraset API Helper
const VERASET_BASE_URL = 'https://api.veraset.com';

export async function verasetFetch(endpoint, options = {}) {
  const apiKey = process.env.VERASET_API_KEY;
  
  if (!apiKey) {
    throw new Error('VERASET_API_KEY not configured');
  }

  const url = `${VERASET_BASE_URL}${endpoint}`;
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': apiKey,
      ...options.headers,
    },
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.error_message || `Veraset API error: ${response.status}`);
  }

  return data;
}

export function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  };
}

export function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...corsHeaders(),
    },
  });
}

export function errorResponse(message, status = 500) {
  return jsonResponse({ error: true, message }, status);
}
