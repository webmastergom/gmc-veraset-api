// Veraset API Helper
const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';

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

// Note: For better security, use getCorsHeaders from lib/security.ts instead
// This function is kept for backward compatibility but should be migrated
export function corsHeaders(origin = null) {
  // In production, only allow specific origins
  if (process.env.NODE_ENV === 'production') {
    const allowedOrigins = process.env.ALLOWED_ORIGINS?.split(',').map(o => o.trim()) || [];
    const allowOrigin = origin && allowedOrigins.includes(origin) ? origin : null;
    
    return {
      'Access-Control-Allow-Origin': allowOrigin || (process.env.NODE_ENV !== 'production' ? '*' : ''),
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Allow-Credentials': 'true',
    };
  }
  
  return {
    'Access-Control-Allow-Origin': origin || '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Credentials': 'true',
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
