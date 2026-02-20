import { NextRequest, NextResponse } from 'next/server';
import { getRunStatus, saveRunStatus } from '@/lib/audience-run-status';
import { checkQueryStatus } from '@/lib/athena';
import { startOriginsAsync } from '@/lib/audience-runner';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

// Maximum time before we consider a run as stale/timed out.
// With async Athena CTAS, the full pipeline can take 30-40 min for large datasets.
const MAX_RUN_DURATION_MS = 45 * 60 * 1000;

/**
 * GET /api/laboratory/audiences/status?datasetId=X&country=Y
 *
 * Returns the current batch run status for a dataset+country.
 * The dashboard polls this every 4 seconds while a run is active.
 *
 * 3-phase state machine for the CTAS pipeline:
 *
 *   athena_spatial  →  Check Q1 (CTAS spatial) + Q2 (total devices)
 *                      When Q1 SUCCEEDED: fire Q3 (CTAS origins) → transition to athena_origins
 *                      When Q1 or Q2 FAILED: mark run as failed
 *
 *   athena_origins  →  Check Q2 + Q3
 *                      When both SUCCEEDED: fire /continue → transition to processing
 *                      When Q3 FAILED: mark run as failed
 *
 *   processing      →  Just report status, /continue is running
 *
 *   done            →  Terminal state (completed, failed, or cancelled)
 */
export async function GET(request: NextRequest): Promise<Response> {
  const datasetId = request.nextUrl.searchParams.get('datasetId');
  const country = request.nextUrl.searchParams.get('country');

  if (!datasetId || !country) {
    return NextResponse.json(
      { error: 'datasetId and country are required' },
      { status: 400 },
    );
  }

  const status = await getRunStatus(datasetId, country);

  if (!status) {
    return NextResponse.json({ active: false });
  }

  // Safety: if the run is "running" but exceeded max duration, mark as failed
  if (status.status === 'running') {
    const elapsedMs = Date.now() - new Date(status.startedAt).getTime();
    if (elapsedMs > MAX_RUN_DURATION_MS) {
      status.status = 'failed';
      status.error = 'Run timed out (exceeded maximum execution time)';
      status.completedAt = new Date().toISOString();
      status.pipelinePhase = 'done';
      await saveRunStatus(status);
      return NextResponse.json({ active: false, ...status });
    }
  }

  // ── 3-phase CTAS state machine ──────────────────────────────────────────
  if (status.status === 'running' && status.athenaQueryIds?.spatialJoin) {
    try {
      // ── Phase 1: athena_spatial — waiting for Q1 (CTAS spatial) + Q2 (total devices)
      if (status.pipelinePhase === 'athena_spatial') {
        const [spatialStatus, totalStatus] = await Promise.all([
          checkQueryStatus(status.athenaQueryIds.spatialJoin),
          checkQueryStatus(status.athenaQueryIds.totalDevices!),
        ]);

        // Handle failures
        if (spatialStatus.state === 'FAILED') {
          status.status = 'failed';
          status.error = `Spatial join CTAS failed: ${spatialStatus.error}`;
          status.completedAt = new Date().toISOString();
          status.pipelinePhase = 'done';
          await saveRunStatus(status);
          return NextResponse.json({ active: false, ...status });
        }
        if (totalStatus.state === 'FAILED') {
          status.status = 'failed';
          status.error = `Total devices query failed: ${totalStatus.error}`;
          status.completedAt = new Date().toISOString();
          status.pipelinePhase = 'done';
          await saveRunStatus(status);
          return NextResponse.json({ active: false, ...status });
        }

        // Handle cancellation
        if (spatialStatus.state === 'CANCELLED' || totalStatus.state === 'CANCELLED') {
          status.status = 'cancelled';
          status.completedAt = new Date().toISOString();
          status.pipelinePhase = 'done';
          await saveRunStatus(status);
          return NextResponse.json({ active: false, ...status });
        }

        // Update progress while queries are running
        if (spatialStatus.state === 'RUNNING' || spatialStatus.state === 'QUEUED') {
          const scannedGB = spatialStatus.statistics?.dataScannedBytes
            ? (spatialStatus.statistics.dataScannedBytes / (1024 * 1024 * 1024)).toFixed(1)
            : null;
          const elapsedSec = spatialStatus.statistics?.engineExecutionTimeMs
            ? Math.round(spatialStatus.statistics.engineExecutionTimeMs / 1000)
            : null;

          status.percent = 10;
          status.message = scannedGB
            ? `Athena CTAS spatial join... ${scannedGB} GB scanned${elapsedSec ? `, ${elapsedSec}s elapsed` : ''}`
            : 'Athena CTAS spatial join running...';
          await saveRunStatus(status);
        }

        // Q1 SUCCEEDED → fire Q3 (CTAS origins)
        if (spatialStatus.state === 'SUCCEEDED' && status.visitsTableName) {
          console.log(`[STATUS] Q1 CTAS spatial SUCCEEDED → firing Q3 CTAS origins`);

          const { originsQueryId, originsTableName } = await startOriginsAsync(
            status.datasetId,
            status.runId,
            status.visitsTableName,
            status.dateFrom,
            status.dateTo,
          );

          // Transition to athena_origins phase
          status.athenaQueryIds!.origins = originsQueryId;
          status.originsTableName = originsTableName;
          status.pipelinePhase = 'athena_origins';
          status.phase = 'origins';
          status.percent = 30;
          status.message = `Spatial join done → resolving device origins (CTAS)...`;
          await saveRunStatus(status);

          console.log(`[STATUS] Transitioned to athena_origins: origins=${originsQueryId} (→ ${originsTableName})`);
        }
      }

      // ── Phase 2: athena_origins — waiting for Q2 (total devices) + Q3 (CTAS origins)
      else if (status.pipelinePhase === 'athena_origins') {
        const checkPromises: Promise<any>[] = [];

        // Q2 might already be done, but check anyway
        if (status.athenaQueryIds.totalDevices) {
          checkPromises.push(checkQueryStatus(status.athenaQueryIds.totalDevices));
        } else {
          checkPromises.push(Promise.resolve({ state: 'SUCCEEDED' }));
        }

        // Q3 must be running
        if (status.athenaQueryIds.origins) {
          checkPromises.push(checkQueryStatus(status.athenaQueryIds.origins));
        } else {
          // Should not happen, but handle gracefully
          checkPromises.push(Promise.resolve({ state: 'FAILED', error: 'No origins query ID' }));
        }

        const [totalStatus, originsStatus] = await Promise.all(checkPromises);

        // Handle failures
        if (totalStatus.state === 'FAILED') {
          status.status = 'failed';
          status.error = `Total devices query failed: ${totalStatus.error}`;
          status.completedAt = new Date().toISOString();
          status.pipelinePhase = 'done';
          await saveRunStatus(status);
          return NextResponse.json({ active: false, ...status });
        }
        if (originsStatus.state === 'FAILED') {
          status.status = 'failed';
          status.error = `Origins CTAS failed: ${originsStatus.error}`;
          status.completedAt = new Date().toISOString();
          status.pipelinePhase = 'done';
          await saveRunStatus(status);
          return NextResponse.json({ active: false, ...status });
        }

        // Handle cancellation
        if (originsStatus.state === 'CANCELLED') {
          status.status = 'cancelled';
          status.completedAt = new Date().toISOString();
          status.pipelinePhase = 'done';
          await saveRunStatus(status);
          return NextResponse.json({ active: false, ...status });
        }

        // Update progress while origins is running
        if (originsStatus.state === 'RUNNING' || originsStatus.state === 'QUEUED') {
          const scannedGB = originsStatus.statistics?.dataScannedBytes
            ? (originsStatus.statistics.dataScannedBytes / (1024 * 1024 * 1024)).toFixed(1)
            : null;
          const elapsedSec = originsStatus.statistics?.engineExecutionTimeMs
            ? Math.round(originsStatus.statistics.engineExecutionTimeMs / 1000)
            : null;

          status.percent = 45;
          status.message = scannedGB
            ? `Athena CTAS origins... ${scannedGB} GB scanned${elapsedSec ? `, ${elapsedSec}s elapsed` : ''}`
            : 'Athena CTAS origins running...';
          await saveRunStatus(status);
        }

        // Both Q2 + Q3 SUCCEEDED → fire /continue
        if (totalStatus.state === 'SUCCEEDED' && originsStatus.state === 'SUCCEEDED' && !status.continueTriggered) {
          console.log(`[STATUS] Q2 + Q3 SUCCEEDED → firing /continue`);

          status.continueTriggered = true;
          status.pipelinePhase = 'processing';
          status.phase = 'processing';
          status.percent = 65;
          status.message = 'All Athena queries complete → processing results...';
          await saveRunStatus(status);

          // Fire-and-forget: trigger the /continue endpoint
          const baseUrl = process.env.VERCEL_URL
            ? `https://${process.env.VERCEL_URL}`
            : request.nextUrl.origin;

          // Forward the auth cookie so /continue passes middleware
          const cookieHeader = request.headers.get('cookie') || '';

          fetch(`${baseUrl}/api/laboratory/audiences/continue`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Cookie': cookieHeader,
            },
            body: JSON.stringify({
              datasetId,
              country,
              runId: status.runId,
            }),
          }).catch(err => {
            console.error('[STATUS] Failed to trigger /continue:', err);
          });

          console.log(`[STATUS] Triggered /continue for run ${status.runId}`);
        }
      }

      // ── Phase 3: processing — /continue is running, just report status
      // (status updates come from continueBatchProcessing via saveStatus callback)

    } catch (err: any) {
      // Athena status check failed — don't crash the status endpoint
      console.error('[STATUS] Error in state machine:', err.message);
    }
  }

  return NextResponse.json({
    active: status.status === 'running',
    ...status,
  });
}
