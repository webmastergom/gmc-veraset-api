import { NextRequest } from 'next/server';
import { continueBatchProcessing } from '@/lib/audience-runner';
import {
  getRunStatus,
  saveRunStatus,
  isCancellationRequested,
} from '@/lib/audience-run-status';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300; // 5 minutes — reads from small temp tables + geocoding + processing

/**
 * POST /api/laboratory/audiences/continue
 *
 * Phase 3 of the async CTAS pipeline. Called automatically by the /status endpoint
 * when ALL Athena queries (Q1 spatial CTAS, Q2 total devices, Q3 origins CTAS)
 * are SUCCEEDED.
 *
 * Reads from materialized temp tables (tiny ~200MB Parquet, not the 54GB+ dataset),
 * then runs geocoding + per-audience processing + S3 persistence.
 *
 * Body: { datasetId, country, runId }
 */
export async function POST(request: NextRequest): Promise<Response> {
  let body: any;
  try {
    body = await request.json();
  } catch {
    return new Response('Invalid JSON body', { status: 400 });
  }

  const { datasetId, country, runId } = body;
  if (!datasetId || !country || !runId) {
    return Response.json({ error: 'datasetId, country, and runId are required' }, { status: 400 });
  }

  // Load status from S3
  const status = await getRunStatus(datasetId, country);
  if (!status || status.runId !== runId) {
    return Response.json({ error: 'Run not found or runId mismatch' }, { status: 404 });
  }

  // Prevent double-trigger
  if (status.continueTriggered && status.pipelinePhase === 'processing') {
    // Allow re-entry if this is the first call (continueTriggered set by /status)
    // but block if already actively processing
    const elapsedSincePhaseChange = Date.now() - new Date(status.startedAt).getTime();
    if (elapsedSincePhaseChange > 10000) {
      // More than 10s since start — check if already returning results
    }
  }

  // Validate temp table names exist in status
  if (!status.visitsTableName || !status.originsTableName) {
    return Response.json({ error: 'Temp table names not found in status — CTAS pipeline incomplete' }, { status: 400 });
  }

  // Validate total devices query ID exists
  if (!status.athenaQueryIds?.totalDevices) {
    return Response.json({ error: 'No total devices query ID found in status' }, { status: 400 });
  }

  // Mark as processing
  status.pipelinePhase = 'processing';
  status.phase = 'processing';
  status.percent = 68;
  status.message = 'Reading from materialized temp tables...';
  await saveRunStatus(status);

  try {
    const results = await continueBatchProcessing(
      status.audienceIds,
      {
        id: datasetId,
        name: status.datasetName || datasetId,
        jobId: status.jobId || '',
      },
      country,
      status.athenaQueryIds.totalDevices,
      status.visitsTableName,
      status.originsTableName,
      status.dateFrom,
      status.dateTo,
      {
        checkCancelled: () => isCancellationRequested(datasetId, country),
        saveStatus: async (update) => {
          Object.assign(status, update);
          try {
            await saveRunStatus(status);
          } catch (err) {
            console.warn('[CONTINUE] Failed to save status to S3:', err);
          }
        },
      },
    );

    // Update final status
    const wasCancelled = await isCancellationRequested(datasetId, country);
    status.status = wasCancelled ? 'cancelled' : 'completed';
    status.completedAt = new Date().toISOString();
    status.percent = 100;
    status.pipelinePhase = 'done';
    status.completedAudiences = Object.keys(results).filter(
      id => results[id].status === 'completed',
    );
    status.message = wasCancelled
      ? `Cancelled after ${status.completedAudiences.length} audiences`
      : `Batch complete: ${status.completedAudiences.length} audiences processed`;
    await saveRunStatus(status);

    console.log(`[CONTINUE] Done: ${status.completedAudiences.length} audiences completed`);
    return Response.json({ success: true, completedAudiences: status.completedAudiences });
  } catch (error: any) {
    console.error('[CONTINUE] Fatal error:', error.message);
    status.status = 'failed';
    status.completedAt = new Date().toISOString();
    status.error = error.message || 'Processing failed';
    status.pipelinePhase = 'done';
    try { await saveRunStatus(status); } catch {}
    return Response.json({ error: error.message }, { status: 500 });
  }
}
