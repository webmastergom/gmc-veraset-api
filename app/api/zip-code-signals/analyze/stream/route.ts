import { NextRequest } from 'next/server';
import { analyzePostalMaid } from '@/lib/dataset-analyzer-postal-maid';
import type { PostalMaidFilters } from '@/lib/postal-maid-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/zip-code-signals/analyze/stream
 * SSE endpoint for Postal Code -> MAID analysis.
 * Streams progress + final result.
 *
 * Body:
 * {
 *   datasetName: string,     // S3 folder name
 *   postalCodes: string[],
 *   country: string,         // ISO 2-letter
 *   dateFrom?: string,
 *   dateTo?: string,
 * }
 */
export async function POST(request: NextRequest): Promise<Response> {
  let body: {
    datasetName: string;
    postalCodes: string[];
    country: string;
    dateFrom?: string;
    dateTo?: string;
  };

  try {
    body = await request.json();
  } catch {
    return new Response('Invalid JSON body', { status: 400 });
  }

  if (!body.datasetName) {
    return new Response('datasetName is required', { status: 400 });
  }
  if (!body.postalCodes?.length) {
    return new Response('At least one postal code is required', { status: 400 });
  }
  if (!body.country || body.country.length !== 2) {
    return new Response('country must be a 2-letter ISO code', { status: 400 });
  }

  const filters: PostalMaidFilters = {
    postalCodes: body.postalCodes.map(pc => pc.trim().toUpperCase()),
    country: body.country.toUpperCase(),
    dateFrom: body.dateFrom,
    dateTo: body.dateTo,
  };

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

      // Keepalive: send SSE comment every 15s to prevent connection idle timeout
      const keepalive = setInterval(() => {
        if (aborted) return;
        try { controller.enqueue(encoder.encode(': keepalive\n\n')); } catch { /* closed */ }
      }, 15_000);

      try {
        const result = await analyzePostalMaid(body.datasetName, filters, (progress) => {
          if (aborted) return;
          send('progress', {
            step: progress.step,
            percent: progress.percent,
            message: progress.message,
            detail: progress.detail,
          });
        });

        if (aborted) return;
        send('result', result as unknown as Record<string, unknown>);
      } catch (error: any) {
        if (!aborted) {
          send('progress', {
            step: 'error',
            percent: 0,
            message: error.message || 'Analysis failed',
          });
        }
      } finally {
        clearInterval(keepalive);
        await new Promise(resolve => setTimeout(resolve, 200));
        try { controller.close(); } catch { /* already closed */ }
      }
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
