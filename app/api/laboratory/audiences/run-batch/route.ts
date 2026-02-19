import { NextRequest } from 'next/server';
import { runBatchAudienceAnalysis } from '@/lib/audience-runner';
import { AUDIENCE_CATALOG } from '@/lib/audience-catalog';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/laboratory/audiences/run-batch
 *
 * Run multiple audiences in a single batch (optimized: shared spatial join).
 * Body: { audienceIds: string[], datasetId, datasetName, jobId, country, dateFrom?, dateTo? }
 *
 * Returns SSE stream with batch progress + final results.
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

  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      const send = (event: string, data: Record<string, unknown>) => {
        try {
          controller.enqueue(
            encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
          );
        } catch { /* stream closed */ }
      };

      let aborted = false;
      request.signal.addEventListener('abort', () => {
        aborted = true;
        try { controller.close(); } catch { /* already closed */ }
      });

      try {
        const results = await runBatchAudienceAnalysis(
          audienceIds,
          { id: datasetId, name: datasetName || datasetId, jobId: jobId || '' },
          country,
          dateFrom,
          dateTo,
          (progress) => {
            if (aborted) return;
            send('progress', progress as unknown as Record<string, unknown>);

            // Also send individual audience result as it completes
            // (the runner will report each audience in the processing phase)
          },
        );

        if (aborted) return;
        send('result', { results } as unknown as Record<string, unknown>);
      } catch (error: any) {
        if (!aborted) {
          send('error', { message: error.message || 'Batch analysis failed' });
        }
      } finally {
        await new Promise(resolve => setTimeout(resolve, 100));
        try { controller.close(); } catch { /* already closed */ }
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
}
