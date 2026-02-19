import { NextRequest, NextResponse } from 'next/server';
import { AUDIENCE_CATALOG, AUDIENCE_GROUP_LABELS } from '@/lib/audience-catalog';
import { loadAudienceResults } from '@/lib/audience-runner';
import { getAudienceEnabledJobs } from '@/lib/jobs';
import { inferCountryFromName } from '@/lib/country-inference';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/laboratory/audiences/catalog
 *
 * Returns the audience catalog with optional latest-run status for a dataset+country.
 * Also returns enabled datasets if no datasetId is provided.
 *
 * Query params:
 *   ?datasetId=X&country=Y  — include run status per audience
 */
export async function GET(request: NextRequest): Promise<Response> {
  const datasetId = request.nextUrl.searchParams.get('datasetId');
  const country = request.nextUrl.searchParams.get('country');

  // Load enabled datasets
  const enabledJobs = await getAudienceEnabledJobs();

  // Load run results if dataset+country specified
  let results: Record<string, any> = {};
  if (datasetId && country) {
    const runResults = await loadAudienceResults(datasetId, country);
    for (const r of runResults) {
      results[r.audienceId] = r;
    }
  }

  return NextResponse.json({
    catalog: AUDIENCE_CATALOG,
    groupLabels: AUDIENCE_GROUP_LABELS,
    results,
    enabledDatasets: enabledJobs.map(j => ({
      jobId: j.jobId,
      name: j.name,
      datasetId: j.s3DestPath?.replace(/^s3:\/\/[^/]+\//, '').replace(/\/$/, '') || j.jobId,
      country: j.country || inferCountryFromName(j.name),
      dateRange: j.dateRange,
      actualDateRange: j.actualDateRange,
      poiCount: j.poiCount,
    })),
  });
}
