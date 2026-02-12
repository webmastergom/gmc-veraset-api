import { NextResponse } from "next/server";

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

/**
 * GET /api/health
 * Health check endpoint for monitoring system reliability
 */
export async function GET() {
  const checks: Record<string, { status: 'ok' | 'error'; message?: string; details?: any }> = {};

  // Check 1: Environment variables
  try {
    const requiredEnvVars = ['VERASET_API_KEY'];
    const missing = requiredEnvVars.filter(v => !process.env[v]);
    checks.environment = {
      status: missing.length === 0 ? 'ok' : 'error',
      message: missing.length === 0 ? 'All required environment variables are set' : `Missing: ${missing.join(', ')}`,
      details: { missing },
    };
  } catch (error: any) {
    checks.environment = {
      status: 'error',
      message: `Error checking environment: ${error.message}`,
    };
  }

  // Check 2: S3 Configuration
  try {
    const { getS3Config } = await import('@/lib/s3-config');
    const config = await getS3Config();
    checks.s3Config = {
      status: config ? 'ok' : 'error',
      message: config ? 'S3 configuration is available' : 'S3 configuration is missing',
      details: config ? { bucket: config.bucket, region: config.region } : undefined,
    };
  } catch (error: any) {
    checks.s3Config = {
      status: 'error',
      message: `Error checking S3 config: ${error.message}`,
    };
  }

  // Check 3: Database/Storage connectivity
  try {
    const { getAllJobs } = await import('@/lib/jobs');
    const jobs = await getAllJobs();
    checks.storage = {
      status: 'ok',
      message: 'Storage is accessible',
      details: { jobCount: jobs.length },
    };
  } catch (error: any) {
    checks.storage = {
      status: 'error',
      message: `Error accessing storage: ${error.message}`,
    };
  }

  // Check 4: Veraset API connectivity (light check)
  try {
    const apiKey = process.env.VERASET_API_KEY;
    checks.verasetApi = {
      status: apiKey ? 'ok' : 'error',
      message: apiKey ? 'Veraset API key is configured' : 'Veraset API key is missing',
      details: apiKey ? { keyLength: apiKey.length, keyPrefix: apiKey.substring(0, 10) + '...' } : undefined,
    };
  } catch (error: any) {
    checks.verasetApi = {
      status: 'error',
      message: `Error checking Veraset API: ${error.message}`,
    };
  }

  // Overall status
  const allChecksOk = Object.values(checks).every(c => c.status === 'ok');
  const overallStatus = allChecksOk ? 'healthy' : 'degraded';

  return NextResponse.json({
    status: overallStatus,
    timestamp: new Date().toISOString(),
    checks,
    reliability: {
      message: 'System reliability checks completed',
      criticalChecks: Object.keys(checks).length,
      passedChecks: Object.values(checks).filter(c => c.status === 'ok').length,
      failedChecks: Object.values(checks).filter(c => c.status === 'error').length,
    },
  }, {
    status: allChecksOk ? 200 : 503,
  });
}
