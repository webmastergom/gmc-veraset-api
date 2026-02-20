import { NextRequest } from 'next/server';
import { startBatchAsync } from '@/lib/audience-runner';
import { AUDIENCE_CATALOG } from '@/lib/audience-catalog';
import {
  saveRunStatus,
  getRunStatus,
  type AudienceRunStatus,
} from '@/lib/audience-run-status';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60; // Only needs to fire Athena queries — 60s is plenty

/**
 * POST /api/laboratory/audiences/run-batch
 *
 * Phase 1 of the async pipeline:
 *   1. Collect all categories across requested audiences
 *   2. Ensure Athena tables exist
 *   3. Fire spatial join + total devices queries (async — don't wait)
 *   4. Save QueryExecutionIds to S3 status
 *   5. Return immediately with { runId, spatialQueryId, totalDevicesQueryId }
 *
 * The /status endpoint (polled every 4s) detects when Athena finishes
 * and triggers /continue to process results.
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

  // Check if a run is already active for this dataset+country
  const existing = await getRunStatus(datasetId, country);
  if (existing && existing.status === 'running') {
    // Check if stale (> 30 min = timed out)
    const elapsed = Date.now() - new Date(existing.startedAt).getTime();
    if (elapsed < 30 * 60 * 1000) {
      return Response.json(
        { error: 'A run is already in progress', runId: existing.runId },
        { status: 409 },
      );
    }
    // Stale — allow overwrite
  }

  const runId = crypto.randomUUID();

  try {
    // Fire Athena CTAS queries (non-blocking — just starts them)
    const { spatialQueryId, totalDevicesQueryId, visitsTableName } = await startBatchAsync(
      audienceIds,
      { id: datasetId, name: datasetName || datasetId, jobId: jobId || '' },
      country,
      runId,
      dateFrom,
      dateTo,
    );

    // Save initial status with query IDs and temp table name
    const status: AudienceRunStatus = {
      runId,
      datasetId,
      country,
      status: 'running',
      phase: 'spatial_join',
      audienceIds,
      current: 0,
      total: audienceIds.length,
      currentAudienceName: '',
      percent: 5,
      message: 'Spatial join CTAS query submitted to Athena...',
      startedAt: new Date().toISOString(),
      completedAt: null,
      error: null,
      completedAudiences: [],
      cancelRequested: false,
      // Async CTAS pipeline fields
      athenaQueryIds: { spatialJoin: spatialQueryId, totalDevices: totalDevicesQueryId },
      pipelinePhase: 'athena_spatial',
      continueTriggered: false,
      visitsTableName,
      // Run parameters (needed by /continue)
      datasetName: datasetName || datasetId,
      jobId: jobId || '',
      dateFrom: dateFrom || undefined,
      dateTo: dateTo || undefined,
    };
    await saveRunStatus(status);

    console.log(`[RUN-BATCH] Started CTAS run ${runId}: spatial=${spatialQueryId} (→ ${visitsTableName}), totalDevices=${totalDevicesQueryId}`);

    return Response.json({ runId, spatialQueryId, totalDevicesQueryId, visitsTableName });
  } catch (error: any) {
    console.error('[RUN-BATCH] Failed to start async batch:', error.message);

    // Save failed status
    const failedStatus: AudienceRunStatus = {
      runId,
      datasetId,
      country,
      status: 'failed',
      phase: 'spatial_join',
      audienceIds,
      current: 0,
      total: audienceIds.length,
      currentAudienceName: '',
      percent: 0,
      message: error.message,
      startedAt: new Date().toISOString(),
      completedAt: new Date().toISOString(),
      error: error.message,
      completedAudiences: [],
      cancelRequested: false,
      pipelinePhase: 'athena_spatial',
    };
    try { await saveRunStatus(failedStatus); } catch {}

    return Response.json({ error: error.message }, { status: 500 });
  }
}
