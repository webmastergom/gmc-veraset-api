import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob, updateMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import {
  startConsolidatedVisitsQuery,
  parseConsolidatedVisits,
  buildTemporalTrends,
  saveConsolidatedReport,
  getConsolidatedReport,
} from '@/lib/mega-report-consolidation';
import { checkQueryStatus, fetchQueryResults } from '@/lib/athena';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * Consolidation state, stored alongside the mega-job.
 * Tracks multi-phase progress (start query → poll → parse → temporal).
 */
interface ConsolidationState {
  phase: 'starting' | 'polling_visits' | 'parsing_visits' | 'temporal' | 'done';
  visitsQueryId?: string;
  error?: string;
}

const CONSOLIDATION_KEY = (id: string) => `mega-consolidation-state/${id}`;

/**
 * POST /api/mega-jobs/[id]/consolidate
 * Multi-phase consolidation. Frontend polls until done.
 *
 * Phase 1 (starting): Start UNION ALL query for visits.
 * Phase 2 (polling_visits): Poll until visits query completes.
 * Phase 3 (parsing_visits): Parse CSV, build visits report, start temporal.
 * Phase 4 (done): All reports saved.
 */
export async function POST(
  _request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  try {
    const { id } = await context.params;
    const megaJob = await getMegaJob(id);

    if (!megaJob) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    // Load sub-jobs
    const subJobs = (
      await Promise.all(megaJob.subJobIds.map((jid) => getJob(jid)))
    ).filter((j): j is NonNullable<typeof j> => j !== null);

    const syncedJobs = subJobs.filter((j) => j.status === 'SUCCESS' && j.syncedAt);
    if (syncedJobs.length === 0) {
      return NextResponse.json({ error: 'No synced sub-jobs to consolidate' }, { status: 400 });
    }

    // Load or init consolidation state
    const { getConfig: getConf, putConfig: putConf } = await import('@/lib/s3-config');
    let state = await getConf<ConsolidationState>(CONSOLIDATION_KEY(id));

    // Reset if requested
    const url = new URL(_request.url);
    if (url.searchParams.get('reset') === 'true') {
      state = null;
    }

    if (!state || state.phase === 'done') {
      state = { phase: 'starting' };
    }

    // Update mega-job status
    if (megaJob.status !== 'consolidating' && megaJob.status !== 'completed') {
      await updateMegaJob(id, { status: 'consolidating' });
    }

    // ── Phase 1: Start visits query ─────────────────────────────────
    if (state.phase === 'starting') {
      try {
        const queryId = await startConsolidatedVisitsQuery(syncedJobs);
        state = { phase: 'polling_visits', visitsQueryId: queryId };
        await putConf(CONSOLIDATION_KEY(id), state);

        return NextResponse.json({
          phase: state.phase,
          progress: { step: 'visits_query', percent: 10, message: 'Visits query started...' },
        });
      } catch (err: any) {
        state = { phase: 'starting', error: err.message };
        await putConf(CONSOLIDATION_KEY(id), state);
        return NextResponse.json({ error: err.message }, { status: 500 });
      }
    }

    // ── Phase 2: Poll visits query ──────────────────────────────────
    if (state.phase === 'polling_visits' && state.visitsQueryId) {
      const queryStatus = await checkQueryStatus(state.visitsQueryId);

      if (queryStatus.state === 'RUNNING' || queryStatus.state === 'QUEUED') {
        return NextResponse.json({
          phase: state.phase,
          progress: { step: 'visits_query', percent: 30, message: `Visits query: ${queryStatus.state}` },
        });
      }

      if (queryStatus.state === 'FAILED' || queryStatus.state === 'CANCELLED') {
        state = { phase: 'starting', error: `Visits query ${queryStatus.state}: ${queryStatus.error || ''}` };
        await putConf(CONSOLIDATION_KEY(id), state);
        return NextResponse.json({ error: `Query ${queryStatus.state}` }, { status: 500 });
      }

      // SUCCEEDED → advance to parsing
      state = { ...state, phase: 'parsing_visits' };
      await putConf(CONSOLIDATION_KEY(id), state);
      // Fall through to parsing
    }

    // ── Phase 3: Parse visits + build temporal ──────────────────────
    if (state.phase === 'parsing_visits' && state.visitsQueryId) {
      // Fetch and parse visits results
      const queryResult = await fetchQueryResults(state.visitsQueryId);
      const visitsByPoi = parseConsolidatedVisits(queryResult.rows, syncedJobs);

      const visitsReport = {
        megaJobId: id,
        analyzedAt: new Date().toISOString(),
        totalPois: visitsByPoi.length,
        visitsByPoi,
      };

      const visitsKey = await saveConsolidatedReport(id, 'visits', visitsReport);

      // Build temporal trends from sub-job daily data
      // Load each sub-job's analysis result (if cached)
      const dailyDataByJob: Array<{ date: string; pings: number; devices: number }[]> = [];

      for (const job of syncedJobs) {
        const datasetName = job.s3DestPath?.replace(/\/$/, '').split('/').pop();
        if (!datasetName) continue;

        const analysis = await getConf<any>(`dataset-analysis/${datasetName}`);
        if (analysis?.dailyData) {
          dailyDataByJob.push(analysis.dailyData);
        }
      }

      let temporalKey: string | undefined;
      if (dailyDataByJob.length > 0) {
        const temporal = buildTemporalTrends(id, dailyDataByJob);
        temporalKey = await saveConsolidatedReport(id, 'temporal', temporal);
      }

      // Save report keys to mega-job
      await updateMegaJob(id, {
        status: 'completed',
        consolidatedReports: {
          visitsByPoi: visitsKey,
          temporalTrends: temporalKey,
        },
      });

      state = { phase: 'done' };
      await putConf(CONSOLIDATION_KEY(id), state);

      return NextResponse.json({
        phase: 'done',
        progress: { step: 'complete', percent: 100, message: 'Consolidation complete' },
        reports: {
          visitsByPoi: visitsReport.totalPois,
          hasTemporalTrends: !!temporalKey,
        },
      });
    }

    // Already done
    return NextResponse.json({
      phase: 'done',
      progress: { step: 'complete', percent: 100, message: 'Already consolidated' },
    });
  } catch (error: any) {
    console.error('[MEGA-CONSOLIDATE]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
