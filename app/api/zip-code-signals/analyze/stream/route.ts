import { randomBytes } from 'crypto';
import { NextRequest } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { analyzePostalMaid, analyzePostalMaidMegaJob } from '@/lib/dataset-analyzer-postal-maid';
import type { PostalMaidFilters, PostalMaidResult } from '@/lib/postal-maid-types';
import { putConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
/**
 * Vercel timeout policy:
 *   - Hobby:      300s hard cap (ignores higher values).
 *   - Pro:        up to 800s when set explicitly.
 *   - Enterprise: up to 900s.
 *
 * We declare 800 so Pro accounts get the full Pro budget; Hobby clamps
 * silently. France Grid 50k April 2026 needs ~250-350s end-to-end
 * (49s preamble + 100-200s Athena origins + 30s pass-1 stream + 30s
 * geocode + 30s pass-2 stream). The diagnostic timeout below fires at
 * declared_max - 25s, so on Pro it gives a useful error at 775s rather
 * than the silent kill at the hard cap.
 *
 * Long-term: see /api/zip-code-signals/poll (multi-phase) — each phase
 * fits in <60s so the whole pipeline is plan-agnostic.
 */
export const maxDuration = 800;
const DECLARED_MAX_S = 800;

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
    /** Single-dataset mode: S3 folder name. */
    datasetName?: string;
    /** Megajob mode: union of all synced sub-jobs, reuses consolidated MAIDs CSV. */
    megaJobId?: string;
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

  if (!body.datasetName && !body.megaJobId) {
    return new Response('Either datasetName or megaJobId is required', { status: 400 });
  }
  if (body.datasetName && body.megaJobId) {
    return new Response('Provide either datasetName or megaJobId, not both', { status: 400 });
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
    megaJobId: body.megaJobId,
  };

  const sourceLabel = body.megaJobId ? `megajob:${body.megaJobId}` : (body.datasetName || '');

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

      // Track the LAST progress event so a graceful timeout can tell the user
      // exactly where they were when Vercel was about to kill the function.
      // Without this the user sees the generic "stream closed without result"
      // — which is what we're trying to replace with a precise diagnostic.
      let lastProgress: { step?: string; percent?: number; message?: string; detail?: string } | null = null;

      // Hard server-side timeout that fires ~25s BEFORE Vercel's hard cap.
      // On Pro this is DECLARED_MAX_S=800, on Hobby it's silently clamped to
      // 300 — the soft timeout uses the declared value so Pro accounts get
      // the full budget. Worst case on Hobby: Vercel kills at 300 before
      // this timer fires, and the user sees the generic "stream closed"
      // message (same as before this fix).
      const SOFT_TIMEOUT_MS = (DECLARED_MAX_S - 25) * 1000;
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => {
          const last = lastProgress;
          const lastWhere = last
            ? `last step: "${last.step}" @ ${last.percent}% — ${last.message ?? ''}${last.detail ? ` (${last.detail})` : ''}`
            : 'no progress events received';
          reject(
            new Error(
              `Server-side timeout (${SOFT_TIMEOUT_MS / 1000}s) — Vercel was about to hard-stop. ${lastWhere}. ` +
                `Try a narrower date range, fewer postal codes, or split the megajob.`,
            ),
          );
        }, SOFT_TIMEOUT_MS);
      });

      const reportWrap = (progress: any) => {
        lastProgress = {
          step: progress?.step,
          percent: progress?.percent,
          message: progress?.message,
          detail: progress?.detail,
        };
        if (aborted) return;
        send('progress', {
          step: progress.step,
          percent: progress.percent,
          message: progress.message,
          detail: progress.detail,
        });
      };

      try {
        const work = body.megaJobId
          ? analyzePostalMaidMegaJob(body.megaJobId, filters, reportWrap)
          : analyzePostalMaid(body.datasetName!, filters, reportWrap);
        const result = await Promise.race([work, timeoutPromise]);

        if (aborted) return;

        let outbound: PostalMaidResult = result;
        const needsSpill =
          result.devices.length > MAX_DEVICES_INLINE ||
          // avoid double stringify on huge arrays: rough lower bound (~35 chars / device)
          result.devices.length * 40 > MAX_SSE_PAYLOAD_CHARS;

        if (needsSpill) {
          const safeDs = sourceLabel.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 72);
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
          const safeDs = sourceLabel.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 72);
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
