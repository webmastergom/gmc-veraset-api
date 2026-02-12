import { NextRequest } from 'next/server';
import { getJob } from '@/lib/jobs';
import { determineSyncStatus } from '@/lib/sync/determine-sync-status';
import type { SyncStatusResponse } from '@/lib/sync-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/jobs/[id]/sync/stream
 * Server-Sent Events stream for sync progress. Client uses EventSource.
 * Sends "progress" events with same JSON shape as GET /sync/status.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> | { id: string } }
) {
  const params =
    typeof context.params === 'object' && context.params instanceof Promise
      ? await context.params
      : context.params;

  const job = await getJob(params.id);
  if (!job) {
    return new Response('Job not found', { status: 404 });
  }

  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      const send = (event: string, data: SyncStatusResponse) => {
        controller.enqueue(
          encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
        );
      };

      const interval = setInterval(async () => {
        const jobSnapshot = await getJob(params.id);
        if (!jobSnapshot) {
          clearInterval(interval);
          send('error', {
            status: 'error',
            message: 'Job not found',
            progress: 0,
            total: 0,
            totalBytes: 0,
            copied: 0,
            copiedBytes: 0,
          });
          controller.close();
          return;
        }
        const status = determineSyncStatus(jobSnapshot);
        send('progress', status);
        if (
          status.status === 'completed' ||
          status.status === 'cancelled' ||
          status.status === 'error'
        ) {
          clearInterval(interval);
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
}
