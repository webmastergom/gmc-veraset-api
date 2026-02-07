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
      aws_configured: !!process.env.AWS_ACCESS_KEY_ID && !!process.env.AWS_SECRET_ACCESS_KEY,
      s3_bucket: process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2 (default)',
      aws_region: process.env.AWS_REGION || 'us-west-2 (default)',
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
