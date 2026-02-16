import { NextRequest } from 'next/server';
import { analyzeOrigins } from '@/lib/dataset-analyzer-od';
import type { ODFilters } from '@/lib/od-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/datasets/[name]/catchment/stream
 * Server-Sent Events endpoint for catchment analysis with progress updates.
 * Streams progress events while running, then sends a final "result" event.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
): Promise<Response> {
  const params = await context.params;
  const datasetName = params.name;

  if (!datasetName) {
    return new Response('Dataset name is required', { status: 400 });
  }

  const { searchParams } = new URL(request.url);
  const filters: ODFilters = {};
  const dateFrom = searchParams.get('dateFrom');
  const dateTo = searchParams.get('dateTo');
  if (dateFrom) filters.dateFrom = dateFrom;
  if (dateTo) filters.dateTo = dateTo;
  const poiIds = searchParams.get('poiIds');
  if (poiIds) filters.poiIds = poiIds.split(',').map((s) => s.trim()).filter(Boolean);

  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      const send = (event: string, data: Record<string, unknown>) => {
        try {
          controller.enqueue(
            encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
          );
        } catch {
          // Stream may already be closed
        }
      };

      // Listen for client disconnect
      let aborted = false;
      request.signal.addEventListener('abort', () => {
        aborted = true;
        try { controller.close(); } catch { /* already closed */ }
      });

      try {
        const result = await analyzeOrigins(datasetName, filters, (progress) => {
          if (aborted) return;
          send('progress', {
            step: progress.step,
            percent: progress.percent,
            message: progress.message,
            detail: progress.detail,
          });
        });

        if (aborted) return;

        // Build the same response shape as the non-streaming endpoint
        const zipcodes = result.origins.map((z) => ({
          zipcode: z.zipcode,
          city: z.city,
          province: z.province,
          region: z.region,
          devices: z.devices,
          percentOfTotal: z.percentOfTotal,
          percentOfClassified: z.percentOfTotal,
          percentage: z.percentOfTotal,
          source: z.source,
        }));

        const totalMatched = zipcodes.reduce((s, z) => s + z.devices, 0);

        const responseData = {
          dataset: result.dataset,
          analyzedAt: result.analyzedAt,
          coverage: {
            totalDevicesVisitedPois: result.totalDevicesVisitedPois,
            totalDeviceDays: result.totalDeviceDays,
            devicesMatchedToZipcode: totalMatched,
            geocodingComplete: result.geocodingComplete,
            classificationRatePercent: result.coverageRatePercent,
          },
          summary: {
            totalDevicesInDataset: result.totalDevicesVisitedPois,
            devicesMatchedToZipcode: totalMatched,
            totalZipcodes: zipcodes.length,
            topZipcode: zipcodes[0]?.zipcode ?? null,
            topCity: zipcodes[0]?.city ?? null,
          },
          zipcodes,
        };

        send('result', responseData);
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
