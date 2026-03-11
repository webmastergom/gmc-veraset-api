import { NextRequest, NextResponse } from 'next/server';
import { activateDevicesMultiPhase, resetActivationState } from '@/lib/dataset-exporter';
import { getAllJobs } from '@/lib/jobs';
import { isAuthenticated } from '@/lib/auth';
import { getCountryForDataset } from '@/lib/country-dataset-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60;

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/**
 * POST /api/datasets/[name]/activate/poll
 *
 * Multi-phase activation endpoint. Call repeatedly (every 2s) to advance
 * the activation state machine. Each call completes within ~50s.
 *
 * Query params:
 *   ?reset=true  — clear previous state and start fresh
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await context.params;
  const datasetName = params.name;

  // Allow resetting stuck/errored activations
  if (request.nextUrl.searchParams.get('reset') === 'true') {
    await resetActivationState(datasetName);
  }

  try {
    // Resolve job name and country code
    const jobs = await getAllJobs();
    const job = jobs.find((j) => {
      if (!j.s3DestPath) return false;
      const path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
      const folder = path.split('/').filter(Boolean)[0] || path.replace(/\/$/, '');
      return folder === datasetName;
    });

    const jobName = job?.name || datasetName;
    const countryCode = await getCountryForDataset(datasetName);

    const state = await activateDevicesMultiPhase(datasetName, jobName, countryCode);
    return NextResponse.json(state);
  } catch (error: any) {
    console.error(`POST /api/datasets/${datasetName}/activate/poll error:`, error);
    return NextResponse.json(
      {
        status: 'error',
        progress: { step: 'error', percent: 0, message: error.message || 'Activation failed' },
        error: error.message,
      },
      { status: 500 }
    );
  }
}
