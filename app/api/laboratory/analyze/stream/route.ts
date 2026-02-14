import { NextRequest } from 'next/server';
import { analyzeLaboratory } from '@/lib/laboratory-analyzer';
import type { LabFilters, PoiCategory } from '@/lib/laboratory-types';
import { POI_CATEGORIES } from '@/lib/laboratory-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/laboratory/analyze/stream
 * SSE endpoint for laboratory affinity analysis with progress updates.
 * Streams progress events while running, then sends a final "result" event.
 *
 * Query params:
 *   country  - ISO 2-letter code (FR, DE, ES) [required]
 *   categories - comma-separated category list (empty = all)
 *   dateFrom - start date (YYYY-MM-DD)
 *   dateTo   - end date (YYYY-MM-DD)
 *   cities   - comma-separated city names
 *   minVisits - minimum visits per postal code (default 5)
 *   timeWindows - JSON array of TimeWindow objects
 */
export async function GET(request: NextRequest): Promise<Response> {
  const { searchParams } = new URL(request.url);

  const country = searchParams.get('country');
  if (!country) {
    return new Response('Country is required', { status: 400 });
  }

  // Parse filters
  const filters: LabFilters = {
    country: country.toUpperCase(),
    categories: [],
  };

  const categoriesParam = searchParams.get('categories');
  if (categoriesParam) {
    filters.categories = categoriesParam.split(',')
      .map(s => s.trim())
      .filter(s => POI_CATEGORIES.includes(s as PoiCategory)) as PoiCategory[];
  }

  const dateFrom = searchParams.get('dateFrom');
  const dateTo = searchParams.get('dateTo');
  if (dateFrom) filters.dateFrom = dateFrom;
  if (dateTo) filters.dateTo = dateTo;

  const cities = searchParams.get('cities');
  if (cities) filters.cities = cities.split(',').map(s => s.trim()).filter(Boolean);

  const minVisits = searchParams.get('minVisits');
  if (minVisits) filters.minVisits = parseInt(minVisits) || 5;

  const timeWindowsParam = searchParams.get('timeWindows');
  if (timeWindowsParam) {
    try {
      filters.timeWindows = JSON.parse(timeWindowsParam);
    } catch { /* ignore invalid JSON */ }
  }

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
        const result = await analyzeLaboratory(filters, (progress) => {
          if (aborted) return;
          send('progress', {
            step: progress.step,
            percent: progress.percent,
            message: progress.message,
            detail: progress.detail,
          });
        });

        if (aborted) return;

        // Send the full result
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
