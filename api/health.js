// Health Check Endpoint
export const config = {
  runtime: 'edge',
};

export default function handler(request) {
  return new Response(
    JSON.stringify({
      status: 'ok',
      service: 'GMC Veraset API',
      timestamp: new Date().toISOString(),
      veraset_configured: !!process.env.VERASET_API_KEY,
    }),
    {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    }
  );
}
