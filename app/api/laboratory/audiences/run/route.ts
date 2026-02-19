import { NextRequest } from 'next/server';
import { runAudienceAnalysis } from '@/lib/audience-runner';
import { AUDIENCE_CATALOG } from '@/lib/audience-catalog';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/laboratory/audiences/run
 *
 * Run a single audience analysis with SSE streaming.
 * Body: { audienceId, datasetId, datasetName, jobId, country, dateFrom?, dateTo? }
 */
export async function POST(request: NextRequest): Promise<Response> {
  let body: any;
  try {
    body = await request.json();
  } catch {
    return new Response('Invalid JSON body', { status: 400 });
  }

  const { audienceId, datasetId, datasetName, jobId, country, dateFrom, dateTo } = body;

  if (!audienceId || !datasetId || !country) {
    return new Response('audienceId, datasetId, and country are required', { status: 400 });
  }

  if (!AUDIENCE_CATALOG.find(a => a.id === audienceId)) {
    return new Response(`Unknown audience: ${audienceId}`, { status: 400 });
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
        const result = await runAudienceAnalysis(
          audienceId,
          { id: datasetId, name: datasetName || datasetId, jobId: jobId || '' },
          country,
          dateFrom,
          dateTo,
          (progress) => {
            if (aborted) return;
            send('progress', {
              step: progress.step,
              percent: progress.percent,
              message: progress.message,
              detail: progress.detail,
            });
          },
        );

        if (aborted) return;
        send('result', result as unknown as Record<string, unknown>);
      } catch (error: any) {
        if (!aborted) {
          send('error', { message: error.message || 'Analysis failed' });
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
