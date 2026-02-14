import { NextRequest, NextResponse } from 'next/server';
import { waitUntil } from '@vercel/functions';
import { getJob, updateJob, tryAcquireSyncLock, releaseSyncLock } from '@/lib/jobs';
import { parseS3Path } from '@/lib/s3';
import { runSync } from '@/lib/sync/sync-orchestrator';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 600; // 10 min for large syncs

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> | { id: string } }
) {
  try {
    const params =
      typeof context.params === 'object' && context.params instanceof Promise
        ? await context.params
        : context.params;
    const jobId = params.id;

    const body = await request.json().catch(() => ({}));
    const destPath = body.destPath as string | undefined;
    const force = body.force === true;

    if (!destPath) {
      return NextResponse.json(
        { error: 'destPath is required' },
        { status: 400 }
      );
    }

    const job = await getJob(jobId);
    if (!job) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }
    if (job.status !== 'SUCCESS') {
      return NextResponse.json(
        { error: 'Job must be SUCCESS before syncing' },
        { status: 400 }
      );
    }
    if (!job.s3SourcePath) {
      return NextResponse.json(
        { error: 'Job has no source path' },
        { status: 400 }
      );
    }

    // Idempotency: already completed with same destination (skip if force resync)
    if (!force && job.syncedAt && job.s3DestPath === destPath && (job.objectCount ?? 0) >= (job.expectedObjectCount ?? 0) && (job.expectedObjectCount ?? 0) > 0) {
      return NextResponse.json({
        success: true,
        message: 'Sync already completed. No re-run.',
        jobId,
        alreadyCompleted: true,
      });
    }

    let acquired = await tryAcquireSyncLock(jobId);
    if (!acquired && force) {
      console.log(`[SYNC] Force resync: releasing lock for job ${jobId}`);
      await releaseSyncLock(jobId);
      acquired = await tryAcquireSyncLock(jobId);
    }
    if (!acquired) {
      return NextResponse.json(
        {
          error: 'Another sync is already in progress for this job',
          code: 'SYNC_IN_PROGRESS',
        },
        { status: 409 }
      );
    }

    const sourcePath = parseS3Path(job.s3SourcePath);
    const destPathParsed = parseS3Path(destPath);

    await updateJob(jobId, {
      objectCount: 0,
      totalBytes: 0,
      syncedAt: null,
      syncCancelledAt: null,
      errorMessage: '',
    });

    // Use waitUntil to keep the serverless function alive after sending the response.
    // Without this, Vercel kills the process as soon as the response is sent,
    // and the sync never completes.
    const syncPromise = runSync({
      jobId,
      sourcePath,
      destPathParsed,
      destPath,
    }).catch((err) => {
      console.error(`[SYNC] Async sync failed for job ${jobId}:`, err);
    });

    waitUntil(syncPromise);

    return NextResponse.json({
      success: true,
      message: 'Sync started. Check status endpoint or use SSE stream for progress.',
      jobId,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to start sync';
    console.error('POST /api/jobs/[id]/sync error:', error);
    return NextResponse.json(
      { error: 'Failed to start sync', details: message },
      { status: 500 }
    );
  }
}
