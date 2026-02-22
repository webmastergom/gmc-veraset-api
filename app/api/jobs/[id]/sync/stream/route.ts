import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { determineSyncStatus } from '@/lib/sync/determine-sync-status';
import { getSyncState } from '@/lib/sync/sync-state';
import type { SyncStatusResponse } from '@/lib/sync-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

function errorStatus(message: string): SyncStatusResponse {
  return {
    status: 'error',
    message,
    progress: 0,
    total: 0,
    totalBytes: 0,
    copied: 0,
    copiedBytes: 0,
  };
}

/**
 * GET /api/jobs/[id]/sync/stream
 * Server-Sent Events stream for sync progress. Client uses EventSource.
 * Sends "progress" events with same JSON shape as GET /sync/status.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const jobId = params?.id;
    if (!jobId) {
      return NextResponse.json({ error: 'Job ID required' }, { status: 400 });
    }

    const job = await getJob(jobId);
    if (!job) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      async start(controller) {
        const send = (event: string, data: SyncStatusResponse) => {
          try {
            controller.enqueue(
              encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
            );
          } catch {
            // Stream closed
          }
        };

        const interval = setInterval(async () => {
          try {
            const jobSnapshot = await getJob(jobId);
            if (!jobSnapshot) {
              clearInterval(interval);
              send('error', errorStatus('Job not found'));
              controller.close();
              return;
            }
            const syncState = await getSyncState(jobId);
            const status = determineSyncStatus(jobSnapshot, syncState);
            send('progress', status);
            if (
              status.status === 'completed' ||
              status.status === 'cancelled' ||
              status.status === 'error'
            ) {
              clearInterval(interval);
              controller.close();
            }
          } catch (err) {
            console.error('[SYNC STREAM] Poll error:', err);
            clearInterval(interval);
            send('error', errorStatus(err instanceof Error ? err.message : 'Failed to get status'));
            controller.close();
          }
        }, 2000);

        request.signal.addEventListener('abort', () => {
          clearInterval(interval);
          controller.close();
        });
      },
    });

    return new Response(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      },
    });
  } catch (error) {
    console.error('GET /api/jobs/[id]/sync/stream error:', error);
    return NextResponse.json(
      {
        error: 'Failed to start sync stream',
        details: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}
