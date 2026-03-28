import { randomBytes } from 'crypto';
import { NextRequest } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { analyzePostalMaid } from '@/lib/dataset-analyzer-postal-maid';
import type { PostalMaidFilters, PostalMaidResult } from '@/lib/postal-maid-types';
import { putConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
/**
 * Vercel Pro: default hard cap is often 300s unless you raise it in
 * Project → Settings → Functions (max 800 on some accounts).
 * Heavy Athena + geocode regularly exceeds a single invocation — see UI note + long-term async job.
 */
export const maxDuration = 300;

const MAX_SSE_PAYLOAD_CHARS = 2_000_000;
const MAX_DEVICES_INLINE = 60_000;
const SPILL_PREVIEW_DEVICES = 5_000;

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
  if (!isAuthenticated(request)) {
    return new Response('Unauthorized cookie auth required', { status: 401 });
  }

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

        let outbound: PostalMaidResult = result;
        const needsSpill =
          result.devices.length > MAX_DEVICES_INLINE ||
          // avoid double stringify on huge arrays: rough lower bound (~35 chars / device)
          result.devices.length * 40 > MAX_SSE_PAYLOAD_CHARS;

        if (needsSpill) {
          const safeDs = body.datasetName.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 72);
          const spillKey = `postal-maid-spill/${safeDs}/${Date.now()}-${randomBytes(6).toString('hex')}`;
          await putConfig(spillKey, result, { compact: true });
          outbound = {
            ...result,
            devices: result.devices.slice(0, SPILL_PREVIEW_DEVICES),
            devicesSpillKey: spillKey,
            devicesSpillTotal: result.devices.length,
          };
          console.log(
            `[ZIP-SIGNALS-STREAM] Spilled ${result.devices.length} devices to ${spillKey}; SSE preview ${outbound.devices.length}`,
          );
        }

        const payload = JSON.stringify(outbound);
        if (!needsSpill && payload.length > MAX_SSE_PAYLOAD_CHARS) {
          const safeDs = body.datasetName.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 72);
          const spillKey = `postal-maid-spill/${safeDs}/${Date.now()}-${randomBytes(6).toString('hex')}`;
          await putConfig(spillKey, result, { compact: true });
          outbound = {
            ...result,
            devices: result.devices.slice(0, SPILL_PREVIEW_DEVICES),
            devicesSpillKey: spillKey,
            devicesSpillTotal: result.devices.length,
          };
          console.log(`[ZIP-SIGNALS-STREAM] Spilled after size check (${payload.length} chars) → ${spillKey}`);
        }
        const payloadFinal = JSON.stringify(outbound);
        console.log(
          `[ZIP-SIGNALS-STREAM] SSE payload: ${payloadFinal.length} chars, devices in packet: ${outbound.devices?.length ?? 0}, matched CPs: ${result.coverage?.postalCodesWithDevices ?? 0}/${result.coverage?.postalCodesRequested ?? 0}`,
        );
        controller.enqueue(encoder.encode(`event: result\ndata: ${payloadFinal}\n\n`));
      } catch (error: any) {
        console.error(`[ZIP-SIGNALS-STREAM] Error:`, error.message);
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
