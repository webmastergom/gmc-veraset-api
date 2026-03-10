import { NextRequest } from 'next/server';
import { activateDevices } from '@/lib/dataset-exporter';
import { getAllJobs } from '@/lib/jobs';
import { isAuthenticated } from '@/lib/auth';
import { getCountryForDataset } from '@/lib/country-dataset-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/**
 * GET /api/datasets/[name]/activate/stream
 * SSE endpoint — streams progress while activating devices for Karlsgate.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
): Promise<Response> {
  if (!isAuthenticated(request)) {
    return new Response('Unauthorized', { status: 401 });
  }

  const params = await context.params;
  const datasetName = params.name;

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

      const keepalive = setInterval(() => {
        if (aborted) return;
        try { controller.enqueue(encoder.encode(': keepalive\n\n')); } catch { /* closed */ }
      }, 15_000);

      try {
        // Resolve job name and country code
        const jobs = await getAllJobs();
        const job = jobs.find((j) => {
          if (!j.s3DestPath) return false;
          const path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
          const folder = path.split('/').filter(Boolean)[0] || path.replace(/\/$/, '');
          return folder === datasetName;
        });

        const jobName = job?.name || datasetName;
        const countryCode = await getCountryForDataset(datasetName);

        const result = await activateDevices(datasetName, jobName, countryCode, (step, percent, message) => {
          if (aborted) return;
          send('progress', { step, percent, message });
        });

        if (!aborted) {
          send('result', result as unknown as Record<string, unknown>);
        }
      } catch (error: any) {
        if (!aborted) {
          send('error', { error: error.message || 'Activation failed' });
        }
      } finally {
        clearInterval(keepalive);
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
