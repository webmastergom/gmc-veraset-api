import { NextRequest } from 'next/server';
import { runBatchAudienceAnalysis } from '@/lib/audience-runner';
import { AUDIENCE_CATALOG } from '@/lib/audience-catalog';
import {
  saveRunStatus,
  getRunStatus,
  isCancellationRequested,
  type AudienceRunStatus,
} from '@/lib/audience-run-status';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/laboratory/audiences/run-batch
 *
 * Run multiple audiences in a single batch (optimized: shared spatial join).
 * Body: { audienceIds: string[], datasetId, datasetName, jobId, country, dateFrom?, dateTo? }
 *
 * Key design: the stream stays open regardless of client disconnect.
 * The client can navigate away and come back — polling /status will show progress.
 * Results are saved to S3 as each audience completes.
 *
 * Pattern follows app/api/jobs/[id]/sync/route.ts: heartbeat keeps Vercel alive.
 */
export async function POST(request: NextRequest): Promise<Response> {
  let body: any;
  try {
    body = await request.json();
  } catch {
    return new Response('Invalid JSON body', { status: 400 });
  }

  const { audienceIds, datasetId, datasetName, jobId, country, dateFrom, dateTo } = body;

  if (!audienceIds?.length || !datasetId || !country) {
    return new Response('audienceIds[], datasetId, and country are required', { status: 400 });
  }

  // Validate all audience IDs
  for (const id of audienceIds) {
    if (!AUDIENCE_CATALOG.find(a => a.id === id)) {
      return new Response(`Unknown audience: ${id}`, { status: 400 });
    }
  }

  // Check if a run is already active for this dataset+country
  const existing = await getRunStatus(datasetId, country);
  if (existing && existing.status === 'running') {
    // Check if stale (> 6 min = timed out)
    const elapsed = Date.now() - new Date(existing.startedAt).getTime();
    if (elapsed < 6 * 60 * 1000) {
      return Response.json(
        { error: 'A run is already in progress', runId: existing.runId },
        { status: 409 },
      );
    }
    // Stale — allow overwrite
  }

  // Create initial run status in S3
  const runId = crypto.randomUUID();
  const currentStatus: AudienceRunStatus = {
    runId,
    datasetId,
    country,
    status: 'running',
    phase: 'spatial_join',
    audienceIds,
    current: 0,
    total: audienceIds.length,
    currentAudienceName: '',
    percent: 0,
    message: 'Starting batch analysis...',
    startedAt: new Date().toISOString(),
    completedAt: null,
    error: null,
    completedAudiences: [],
    cancelRequested: false,
  };
  await saveRunStatus(currentStatus);

  const encoder = new TextEncoder();

  // Use streaming response to keep Vercel function alive.
  // The stream stays open but the client can disconnect without
  // killing server-side processing. (Pattern: sync/route.ts)
  const stream = new ReadableStream({
    async start(controller) {
      const send = (event: string, data: Record<string, unknown>) => {
        try {
          controller.enqueue(
            encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
          );
        } catch { /* stream closed — client disconnected, processing continues */ }
      };

      // Heartbeat: keep connection alive (Vercel / load balancers)
      const heartbeat = setInterval(() => {
        try {
          controller.enqueue(encoder.encode(`: heartbeat\n\n`));
        } catch {
          // Stream closed by client — stop heartbeat but DON'T stop processing
          clearInterval(heartbeat);
        }
      }, 15_000);

      // Send runId so client can identify this run
      send('started', { runId });

      try {
        const results = await runBatchAudienceAnalysis(
          audienceIds,
          { id: datasetId, name: datasetName || datasetId, jobId: jobId || '' },
          country,
          dateFrom,
          dateTo,
          // SSE progress callback (works if client is still connected)
          (progress) => {
            send('progress', progress as unknown as Record<string, unknown>);
          },
          // Background mode hooks: S3 status + cancellation
          {
            checkCancelled: () => isCancellationRequested(datasetId, country),
            saveStatus: async (update) => {
              // Merge update into current status
              Object.assign(currentStatus, update);
              try {
                await saveRunStatus(currentStatus);
              } catch (err) {
                console.warn('[AUDIENCE-BATCH] Failed to save status to S3:', err);
              }
            },
          },
        );

        // Check if cancelled
        const wasCancelled = await isCancellationRequested(datasetId, country);
        currentStatus.status = wasCancelled ? 'cancelled' : 'completed';
        currentStatus.completedAt = new Date().toISOString();
        currentStatus.percent = 100;
        currentStatus.completedAudiences = Object.keys(results).filter(
          id => results[id].status === 'completed'
        );
        currentStatus.message = wasCancelled
          ? `Cancelled after ${currentStatus.completedAudiences.length} audiences`
          : `Batch complete: ${currentStatus.completedAudiences.length} audiences processed`;
        await saveRunStatus(currentStatus);

        // Send final result over SSE (if client still connected)
        send('result', { results } as unknown as Record<string, unknown>);
      } catch (error: any) {
        console.error('[AUDIENCE-BATCH] Fatal error:', error.message);
        currentStatus.status = 'failed';
        currentStatus.completedAt = new Date().toISOString();
        currentStatus.error = error.message || 'Batch analysis failed';
        try { await saveRunStatus(currentStatus); } catch {}

        send('error', { message: error.message || 'Batch analysis failed' });
      } finally {
        clearInterval(heartbeat);
        await new Promise(resolve => setTimeout(resolve, 100));
        try { controller.close(); } catch {}
      }
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Run-Id': runId,
    },
  });
}
