import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzePostalMaid } from '@/lib/dataset-analyzer-postal-maid';
import { getConfig, putConfig, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/external/jobs/[datasetName]/postal-maid
 *
 * Postal Code → MAID lookup.
 * Given an array of postal codes + country, returns all MAIDs whose
 * residential origin (first ping of day) falls within those postal codes.
 *
 * Request body:
 * {
 *   "postal_codes": ["28001", "28002", "28003"],
 *   "country": "ES",
 *   "date_from": "2024-01-01",    // optional
 *   "date_to": "2024-03-31"       // optional
 * }
 *
 * The datasetName param is actually a jobId.
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ datasetName: string }> }
): Promise<NextResponse> {
  let jobId: string | undefined;

  try {
    const params = await context.params;
    jobId = params.datasetName;

    console.log(`[POSTAL-MAID] POST /api/external/jobs/${jobId}/postal-maid`);

    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      console.error(`[POSTAL-MAID] Unauthorized: ${auth.error}`);
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // 2. Parse request body
    let body: any;
    try {
      body = await request.json();
    } catch {
      return NextResponse.json(
        { error: 'Invalid JSON body' },
        { status: 400 }
      );
    }

    const { postal_codes, country, date_from, date_to } = body;

    if (!postal_codes || !Array.isArray(postal_codes) || postal_codes.length === 0) {
      return NextResponse.json(
        { error: 'postal_codes is required and must be a non-empty array of strings.' },
        { status: 400 }
      );
    }

    if (!country || typeof country !== 'string' || country.length !== 2) {
      return NextResponse.json(
        { error: 'country is required and must be a 2-letter ISO country code (e.g. "ES", "FR", "MX").' },
        { status: 400 }
      );
    }

    // Validate all postal codes are strings
    const invalidCodes = postal_codes.filter((pc: any) => typeof pc !== 'string' || pc.trim() === '');
    if (invalidCodes.length > 0) {
      return NextResponse.json(
        { error: 'All postal_codes must be non-empty strings.' },
        { status: 400 }
      );
    }

    // 3. Find job and verify status
    let job;
    try {
      job = await getJob(jobId);
    } catch (error: any) {
      console.error(`[POSTAL-MAID] Error fetching job ${jobId}:`, error.message);
      return NextResponse.json(
        { error: 'Internal Server Error', message: 'Failed to fetch job' },
        { status: 500 }
      );
    }

    if (!job) {
      return NextResponse.json(
        { error: 'Not Found', message: `Job '${jobId}' not found.` },
        { status: 404 }
      );
    }

    if (job.status !== 'SUCCESS') {
      return NextResponse.json(
        {
          error: 'Job Not Ready',
          message: `Job status is '${job.status}'. Analysis is only available for completed jobs.`,
          status: job.status,
        },
        { status: 409 }
      );
    }

    // 4. Get dataset name from s3DestPath
    if (!job.s3DestPath) {
      return NextResponse.json(
        { error: 'Job data not synced yet. Please try again later.' },
        { status: 409 }
      );
    }

    const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
    const datasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');

    // 5. Check cache
    const POSTAL_MAID_VERSION = 'v1';
    const normalizedCodes = postal_codes.map((pc: string) => pc.trim().toUpperCase()).sort();
    const codesHash = normalizedCodes.join(',');
    const datesSuffix = date_from || date_to ? `-${date_from || ''}_${date_to || ''}` : '';
    const cacheKey = `postal-maid-${POSTAL_MAID_VERSION}-${jobId}-${country.toUpperCase()}-${codesHash}${datesSuffix}`;

    try {
      const cached = await getConfig<any>(cacheKey);
      if (cached) {
        console.log(`[POSTAL-MAID] Serving cached result for job ${jobId}`);
        logger.log(`Serving cached postal-maid for job ${jobId}`);
        return NextResponse.json(cached, {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'X-Cache': 'HIT' },
        });
      }
    } catch {
      // No cache, continue
    }

    // 6. Run analysis
    console.log(`[POSTAL-MAID] Computing postal→MAID for job ${jobId}, dataset: ${datasetName}, postal_codes: [${normalizedCodes.join(', ')}], country: ${country}`);
    logger.log(`Computing postal-maid for job ${jobId}, ${normalizedCodes.length} postal codes in ${country}`);

    let res;
    try {
      res = await analyzePostalMaid(datasetName, {
        postalCodes: normalizedCodes,
        country: country.toUpperCase(),
        dateFrom: date_from,
        dateTo: date_to,
      });
    } catch (error: any) {
      console.error(`[POSTAL-MAID ERROR] Analysis failed:`, error.message);
      logger.error(`Postal-MAID analysis failed for job ${jobId}`, { error: error.message });

      if (error.message?.includes('Access Denied') ||
          error.message?.includes('AccessDeniedException') ||
          error.message?.includes('not authorized')) {
        return NextResponse.json(
          {
            error: 'Dataset not accessible',
            message: 'The job data may not be synced yet or AWS permissions are insufficient.',
            details: error.message,
          },
          { status: 409 }
        );
      }

      if (error.message?.includes('AWS credentials not configured')) {
        return NextResponse.json(
          { error: 'Configuration Error', message: 'AWS credentials not configured.' },
          { status: 503 }
        );
      }

      return NextResponse.json(
        {
          error: 'Internal Server Error',
          message: 'Failed to compute postal→MAID analysis',
          details: error.message,
        },
        { status: 500 }
      );
    }

    // 7. Build response
    const response = {
      job_id: jobId,
      analyzed_at: res.analyzedAt,
      filters: {
        postal_codes: normalizedCodes,
        country: country.toUpperCase(),
        date_from: date_from || null,
        date_to: date_to || null,
      },
      methodology: res.methodology,
      coverage: res.coverage,
      summary: res.summary,
      postal_code_breakdown: res.postalCodeBreakdown,
      devices: res.devices.map(d => ({
        ad_id: d.adId,
        device_days: d.deviceDays,
        postal_codes: d.postalCodes,
      })),
    };

    // 8. Cache result
    try {
      await putConfig(cacheKey, response);
      logger.log(`Cached postal-maid for job ${jobId}`);
    } catch (err: any) {
      logger.warn(`Failed to cache postal-maid for job ${jobId}`, { error: err.message });
    }

    return NextResponse.json(response, {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'X-Cache': 'MISS' },
    });

  } catch (error: any) {
    const errorJobId = jobId || 'unknown';
    console.error(`[POSTAL-MAID ERROR] POST /api/external/jobs/${errorJobId}/postal-maid:`, error.message);
    logger.error(`POST /api/external/jobs/${errorJobId}/postal-maid error:`, error);

    return NextResponse.json(
      {
        error: 'Internal Server Error',
        message: error.message || 'An unexpected error occurred',
      },
      { status: 500 }
    );
  }
}
