import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import {
  createMegaDatasetView,
  megaJobNameToDatasetId,
  ensureTableForDataset,
  dropMegaDatasetView,
} from '@/lib/athena';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * POST /api/mega-jobs/[id]/view
 * Create or refresh the integrated dataset VIEW for a mega-job.
 * This makes all synced sub-job data queryable as a single dataset.
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { id } = await context.params;
  const megaJob = await getMegaJob(id);

  if (!megaJob) {
    return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
  }

  if (!megaJob.subJobIds?.length) {
    return NextResponse.json({ error: 'No sub-jobs in this mega-job' }, { status: 400 });
  }

  try {
    // Load sub-jobs and filter to synced ones
    const subJobs = (
      await Promise.all(megaJob.subJobIds.map(jid => getJob(jid).catch(() => null)))
    ).filter((j): j is NonNullable<typeof j> => j !== null);

    const syncedJobs = subJobs.filter(j => j.status === 'SUCCESS' && j.syncedAt);
    if (syncedJobs.length === 0) {
      return NextResponse.json(
        { error: 'No synced sub-jobs to create integrated dataset' },
        { status: 400 }
      );
    }

    const subDatasetNames = syncedJobs
      .map(j => j.s3DestPath?.replace(/\/$/, '').split('/').pop())
      .filter((n): n is string => !!n);

    const megaDatasetId = megaJobNameToDatasetId(megaJob.name || id);
    if (!megaDatasetId) {
      return NextResponse.json({ error: 'Cannot derive dataset name from mega-job' }, { status: 400 });
    }

    // Create the VIEW
    const viewName = await createMegaDatasetView(megaDatasetId, subDatasetNames);

    return NextResponse.json({
      success: true,
      datasetId: megaDatasetId,
      viewName,
      subDatasets: subDatasetNames,
      syncedSubJobs: syncedJobs.length,
      totalSubJobs: megaJob.subJobIds.length,
    });
  } catch (error: any) {
    console.error('[MEGA-VIEW] Error creating view:', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

/**
 * DELETE /api/mega-jobs/[id]/view
 * Drop the integrated dataset VIEW for a mega-job.
 */
export async function DELETE(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { id } = await context.params;
  const megaJob = await getMegaJob(id);

  if (!megaJob) {
    return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
  }

  try {
    const megaDatasetId = megaJobNameToDatasetId(megaJob.name || id);
    await dropMegaDatasetView(megaDatasetId);
    return NextResponse.json({ success: true, dropped: megaDatasetId });
  } catch (error: any) {
    console.error('[MEGA-VIEW] Error dropping view:', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
