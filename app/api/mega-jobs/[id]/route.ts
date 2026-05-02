import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob, updateMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';

export const dynamic = 'force-dynamic';

/**
 * GET /api/mega-jobs/[id]
 * Full mega-job detail including sub-job statuses.
 */
export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  try {
    const { id } = await context.params;
    const megaJob = await getMegaJob(id);

    if (!megaJob) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    // Enrich with sub-job statuses
    const subJobs = await Promise.all(
      megaJob.subJobIds.map(async (jobId) => {
        const job = await getJob(jobId);
        if (!job) return { jobId, status: 'UNKNOWN' as const, name: jobId };
        return {
          jobId: job.jobId,
          name: job.name,
          status: job.status,
          dateRange: job.dateRange,
          poiCount: job.poiCount,
          syncedAt: job.syncedAt,
          s3DestPath: job.s3DestPath,
          megaJobIndex: job.megaJobIndex,
        };
      })
    );

    // Update mega-job progress based on current sub-job statuses
    const synced = subJobs.filter((j) => j.status === 'SUCCESS' && 'syncedAt' in j && j.syncedAt).length;
    const failed = subJobs.filter((j) => j.status === 'FAILED').length;

    if (synced !== megaJob.progress.synced || failed !== megaJob.progress.failed) {
      megaJob.progress.synced = synced;
      megaJob.progress.failed = failed;

      // Auto-transition status
      if (synced + failed === megaJob.progress.total) {
        if (failed === 0) {
          megaJob.status = megaJob.status === 'consolidating' || megaJob.status === 'completed'
            ? megaJob.status
            : 'running'; // Ready for consolidation
        } else if (synced > 0) {
          megaJob.status = 'partial';
        } else {
          megaJob.status = 'error';
          megaJob.error = 'All sub-jobs failed';
        }
      }

      await updateMegaJob(id, {
        progress: megaJob.progress,
        status: megaJob.status,
        error: megaJob.error,
      });
    }

    return NextResponse.json({ ...megaJob, subJobs });
  } catch (error: any) {
    console.error('[MEGA-JOBS GET /id]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

/**
 * PATCH /api/mega-jobs/[id]
 * Partial update of mega-job fields. Currently used to set `country`
 * (so MAIDs by Category / NSE modals can resolve country without requiring
 * an extra job-level lookup).
 */
const ALLOWED_PATCH_FIELDS = ['country', 'name', 'description'] as const;

export async function PATCH(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  try {
    const { id } = await context.params;
    const body = await request.json().catch(() => ({}));

    // Whitelist updatable fields — no status/queries/progress mass-edits via this endpoint
    const updates: Record<string, unknown> = {};
    for (const k of ALLOWED_PATCH_FIELDS) {
      if (k in body) updates[k] = body[k];
    }
    if (Object.keys(updates).length === 0) {
      return NextResponse.json({ error: 'No allowed fields to update' }, { status: 400 });
    }

    const updated = await updateMegaJob(id, updates as any);
    if (!updated) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    return NextResponse.json({ success: true, megaJob: updated });
  } catch (error: any) {
    console.error('[MEGA-JOBS PATCH /id]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
