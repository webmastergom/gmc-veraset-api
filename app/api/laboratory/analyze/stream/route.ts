import { NextRequest } from 'next/server';
import { analyzeLaboratory } from '@/lib/laboratory-analyzer';
import type { LabConfig, PoiCategory, RecipeStep, Recipe } from '@/lib/laboratory-types';
import { POI_CATEGORIES, SPATIAL_JOIN_RADIUS_DEFAULT, MIN_VISITS_DEFAULT } from '@/lib/laboratory-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/laboratory/analyze/stream
 * SSE endpoint for laboratory affinity analysis.
 * Accepts LabConfig as JSON body, streams progress + final result.
 */
export async function POST(request: NextRequest): Promise<Response> {
  let config: LabConfig;
  try {
    config = await request.json();
  } catch {
    return new Response('Invalid JSON body', { status: 400 });
  }

  if (!config.datasetId) {
    return new Response('datasetId is required', { status: 400 });
  }
  if (!config.recipe?.steps?.length) {
    return new Response('recipe with at least one step is required', { status: 400 });
  }

  // Defaults
  config.minVisitsPerZipcode = config.minVisitsPerZipcode || MIN_VISITS_DEFAULT;
  config.spatialJoinRadiusMeters = config.spatialJoinRadiusMeters || SPATIAL_JOIN_RADIUS_DEFAULT;

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
        const result = await analyzeLaboratory(config, (progress) => {
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
