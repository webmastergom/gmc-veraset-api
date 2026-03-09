import { NextRequest, NextResponse } from 'next/server';
import { gzipSync } from 'node:zlib';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { logger } from '@/lib/logger';
import { analyzePostalMaid } from '@/lib/dataset-analyzer-postal-maid';
import { getConfig, putConfig } from '@/lib/s3-config';
import { getDatasetForCountry, getConfiguredCountries } from '@/lib/country-dataset-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/external/postal-maid
 *
 * Country-based postal code → MAID lookup.
 * Resolves the dataset automatically from the country code.
 * Returns a gzip-compressed text file with one MAID per line.
 *
 * Request body:
 * {
 *   "postal_codes": ["28001", "28002", "28003"],
 *   "country": "ES",
 *   "date_from": "2024-01-01",    // optional
 *   "date_to": "2024-03-31"       // optional
 * }
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    console.log(`[POSTAL-MAID-EXT] POST /api/external/postal-maid`);

    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      console.error(`[POSTAL-MAID-EXT] Unauthorized: ${auth.error}`);
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

    const invalidCodes = postal_codes.filter((pc: any) => typeof pc !== 'string' || pc.trim() === '');
    if (invalidCodes.length > 0) {
      return NextResponse.json(
        { error: 'All postal_codes must be non-empty strings.' },
        { status: 400 }
      );
    }

    // 3. Resolve dataset from country config
    const countryUpper = country.toUpperCase();
    const entry = getDatasetForCountry(countryUpper);
    if (!entry) {
      const available = getConfiguredCountries();
      return NextResponse.json(
        {
          error: 'Country not configured',
          message: `Country '${countryUpper}' is not yet configured. ${available.length > 0 ? `Available countries: ${available.join(', ')}` : 'No countries configured yet.'}`,
          available_countries: available,
        },
        { status: 404 }
      );
    }

    const datasetName = entry.dataset;
    const normalizedCodes = postal_codes.map((pc: string) => pc.trim().toUpperCase()).sort();

    console.log(`[POSTAL-MAID-EXT] country=${countryUpper}, dataset=${datasetName}, codes=[${normalizedCodes.join(', ')}]`);
    logger.log(`Postal-maid-ext: ${countryUpper} (${entry.label}), ${normalizedCodes.length} codes`);

    // 4. Check S3 cache
    const CACHE_VERSION = 'v1';
    const codesHash = normalizedCodes.join(',');
    const datesSuffix = date_from || date_to ? `-${date_from || ''}_${date_to || ''}` : '';
    const cacheKey = `postal-maid-ext-${CACHE_VERSION}-${countryUpper}-${codesHash}${datesSuffix}`;

    try {
      const cached = await getConfig<{ maids: string[]; analyzedAt: string }>(cacheKey);
      if (cached) {
        console.log(`[POSTAL-MAID-EXT] Cache HIT: ${cached.maids.length} MAIDs`);
        logger.log(`Postal-maid-ext cache HIT for ${countryUpper}: ${cached.maids.length} MAIDs`);
        const text = cached.maids.join('\n') + (cached.maids.length > 0 ? '\n' : '');
        const gzipped = gzipSync(Buffer.from(text, 'utf-8'));
        return buildGzipResponse(gzipped, countryUpper, normalizedCodes, cached.maids.length, true);
      }
    } catch {
      // No cache, continue
    }

    // 5. Run analysis
    console.log(`[POSTAL-MAID-EXT] Cache MISS — running analyzePostalMaid...`);

    let res;
    try {
      res = await analyzePostalMaid(datasetName, {
        postalCodes: normalizedCodes,
        country: countryUpper,
        dateFrom: date_from,
        dateTo: date_to,
      });
    } catch (error: any) {
      console.error(`[POSTAL-MAID-EXT] Analysis failed:`, error.message);
      logger.error(`Postal-maid-ext analysis failed for ${countryUpper}`, { error: error.message });

      if (error.message?.includes('Access Denied') ||
          error.message?.includes('AccessDeniedException') ||
          error.message?.includes('not authorized')) {
        return NextResponse.json(
          {
            error: 'Dataset not accessible',
            message: 'The dataset for this country may not be set up correctly or AWS permissions are insufficient.',
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
          message: 'Failed to compute postal-maid analysis',
          details: error.message,
        },
        { status: 500 }
      );
    }

    // 6. Extract MAIDs
    const maids = res.devices.map(d => d.adId);
    console.log(`[POSTAL-MAID-EXT] Analysis complete: ${maids.length} MAIDs, ${res.coverage.postalCodesWithDevices}/${res.coverage.postalCodesRequested} postal codes matched`);

    // 7. Cache result
    try {
      await putConfig(cacheKey, {
        maids,
        analyzedAt: res.analyzedAt,
        country: countryUpper,
        postalCodes: normalizedCodes,
        deviceCount: maids.length,
      });
      logger.log(`Cached postal-maid-ext for ${countryUpper}: ${maids.length} MAIDs`);
    } catch (err: any) {
      logger.warn(`Failed to cache postal-maid-ext for ${countryUpper}`, { error: err.message });
    }

    // 8. Gzip and return
    const text = maids.join('\n') + (maids.length > 0 ? '\n' : '');
    const gzipped = gzipSync(Buffer.from(text, 'utf-8'));
    return buildGzipResponse(gzipped, countryUpper, normalizedCodes, maids.length, false);

  } catch (error: any) {
    console.error(`[POSTAL-MAID-EXT] Unhandled error:`, error.message);
    logger.error(`POST /api/external/postal-maid error:`, error);
    return NextResponse.json(
      {
        error: 'Internal Server Error',
        message: error.message || 'An unexpected error occurred',
      },
      { status: 500 }
    );
  }
}

function buildGzipResponse(
  gzipped: Buffer,
  country: string,
  codes: string[],
  maidCount: number,
  cacheHit: boolean
): NextResponse {
  const codesLabel = codes.length <= 3
    ? codes.join('-')
    : `${codes.length}codes`;
  const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const filename = `postal-maid-${country}-${codesLabel}-${dateStr}.txt.gz`;

  return new NextResponse(new Uint8Array(gzipped), {
    status: 200,
    headers: {
      'Content-Type': 'application/gzip',
      'Content-Disposition': `attachment; filename="${filename}"`,
      'X-Cache': cacheHit ? 'HIT' : 'MISS',
      'X-Total-Maids': String(maidCount),
    },
  });
}
