import { NextRequest, NextResponse } from 'next/server';
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

    // Use a streaming response to keep the Vercel function alive.
    // waitUntil() is unreliable for long-running tasks — Vercel can freeze/evict
    // the function after the response is sent. By streaming, we keep the
    // connection open and Vercel keeps the function running until maxDuration.
    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      async start(controller) {
        // Send initial message
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify({ status: 'started', jobId })}\n\n`)
        );

        // Keepalive: send a heartbeat every 10s so the connection stays open
        const heartbeat = setInterval(() => {
          try {
            controller.enqueue(encoder.encode(`: heartbeat\n\n`));
          } catch {
            // Stream closed by client — ignore
            clearInterval(heartbeat);
          }
        }, 10000);

        try {
          await runSync({
            jobId,
            sourcePath,
            destPathParsed,
            destPath,
          });

          controller.enqueue(
            encoder.encode(`data: ${JSON.stringify({ status: 'completed', jobId })}\n\n`)
          );
        } catch (err) {
          const msg = err instanceof Error ? err.message : 'Sync failed';
          console.error(`[SYNC] Sync failed for job ${jobId}:`, msg);
          controller.enqueue(
            encoder.encode(`data: ${JSON.stringify({ status: 'error', jobId, error: msg })}\n\n`)
          );
        } finally {
          clearInterval(heartbeat);
          controller.close();
        }
      },
    });

    return new Response(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      },
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
