import { NextRequest, NextResponse } from 'next/server';
import { activateDevices } from '@/lib/dataset-exporter';
import { getAllJobs } from '@/lib/jobs';
import { isAuthenticated } from '@/lib/auth';
import { getCountryForDataset } from '@/lib/country-dataset-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

export async function POST(
  req: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await context.params;
  const datasetName = params.name;

  try {
    // Find the job associated with this dataset to get the human-readable name
    const jobs = await getAllJobs();
    const job = jobs.find((j) => {
      if (!j.s3DestPath) return false;
      const path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
      const folder = path.split('/').filter(Boolean)[0] || path.replace(/\/$/, '');
      return folder === datasetName;
    });

    const jobName = job?.name || datasetName;

    // Resolve country code from country→dataset config (reverse lookup)
    const countryCode = await getCountryForDataset(datasetName);

    const result = await activateDevices(datasetName, jobName, countryCode);
    return NextResponse.json(result);
  } catch (error: any) {
    console.error(`POST /api/datasets/${datasetName}/activate error:`, error);
    return NextResponse.json(
      { error: 'Activation failed', details: error.message, stack: error.stack?.split('\n').slice(0, 5) },
      { status: 500 }
    );
  }
}
