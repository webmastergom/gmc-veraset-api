import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig, putConfig } from '@/lib/s3-config';
import {
  getCountryContributions,
  buildConsolidationSQL,
  buildStatsQuery,
  buildTotalQuery,
  saveConsolidationStats,
  type ConsolidationState,
  type AttributeStat,
} from '@/lib/master-maids';
import {
  startQueryAsync,
  checkQueryStatus,
  dropTempTable,
  runQuery,
} from '@/lib/athena';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const STATE_KEY = (cc: string) => `master-maids-consolidation/${cc}`;

/**
 * POST /api/master-maids/[country]/consolidate
 *
 * Multi-phase polling endpoint for master MAID consolidation.
 *
 * Phases:
 * 1. creating_tables: Create exports external table + start CTAS
 * 2. running_ctas: Poll CTAS Athena query
 * 3. running_stats: Start + poll stats query on consolidated table
 * 4. done: Parse stats, save to index, return results
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ country: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { country } = await context.params;
  const cc = country.toUpperCase();

  try {
    // Check for reset request
    let body: any;
    try { body = await request.json(); } catch { body = {}; }
    const isReset = body.reset === true;

    let state = await getConfig<ConsolidationState>(STATE_KEY(cc));

    // Reset on error or explicit reset
    if (state?.phase === 'error' || isReset) state = null;

    // Return cached done state
    if (state?.phase === 'done' && !isReset) {
      return NextResponse.json({ phase: 'done' });
    }

    // ── Phase: start ──────────────────────────────────────────────
    if (!state) {
      const entry = await getCountryContributions(cc);
      if (!entry || entry.contributions.length === 0) {
        return NextResponse.json({ error: 'No contributions to consolidate' }, { status: 400 });
      }

      console.log(`[CONSOLIDATE] Starting for ${cc} with ${entry.contributions.length} contributions`);

      const runId = `${Date.now()}`;
      const consolidatedTable = `master_${cc.toLowerCase()}_${runId}`;
      const { createTableSQL, ctasSQL, contribTableName } = buildConsolidationSQL(cc, consolidatedTable);

      // Create the contributions external table
      try {
        await runQuery(createTableSQL);
        console.log(`[CONSOLIDATE] Created contributions table: ${contribTableName}`);
      } catch (e: any) {
        if (!e.message?.includes('already exists')) {
          console.warn(`[CONSOLIDATE] Warning creating contributions table:`, e.message);
        }
      }

      // Start CTAS query
      const ctasQueryId = await startQueryAsync(ctasSQL);
      console.log(`[CONSOLIDATE] CTAS started: ${ctasQueryId}`);

      state = {
        phase: 'running_ctas',
        ctasQueryId,
        tempTables: [contribTableName],
        consolidatedTable,
      };
      await putConfig(STATE_KEY(cc), state, { compact: true });

      return NextResponse.json({
        phase: 'running_ctas',
        progress: { step: 'ctas', percent: 20, message: 'Building consolidated table...' },
      });
    }

    // ── Phase: running_ctas ───────────────────────────────────────
    if (state.phase === 'running_ctas' && state.ctasQueryId) {
      try {
        const status = await checkQueryStatus(state.ctasQueryId);

        if (status.state === 'RUNNING' || status.state === 'QUEUED') {
          return NextResponse.json({
            phase: 'running_ctas',
            progress: {
              step: 'ctas',
              percent: 40,
              message: `Building consolidated table... ${status.statistics?.dataScannedBytes ? (status.statistics.dataScannedBytes / 1e9).toFixed(2) : '?'} GB scanned`,
            },
          });
        }

        if (status.state === 'FAILED' || status.state === 'CANCELLED') {
          state = { ...state, phase: 'error', error: status.error || 'CTAS failed' };
          await putConfig(STATE_KEY(cc), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

        // CTAS succeeded — start stats queries
        console.log(`[CONSOLIDATE] CTAS complete for ${cc}`);

        const statsSQL = buildStatsQuery(state.consolidatedTable!);
        const statsQid = await startQueryAsync(statsSQL);

        state = { ...state, phase: 'running_stats', statsQueryId: statsQid };
        await putConfig(STATE_KEY(cc), state, { compact: true });

        return NextResponse.json({
          phase: 'running_stats',
          progress: { step: 'stats', percent: 70, message: 'Computing statistics...' },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'error', error: 'Query expired — please retry' };
          await putConfig(STATE_KEY(cc), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        throw err;
      }
    }

    // ── Phase: running_stats ──────────────────────────────────────
    if (state.phase === 'running_stats' && state.statsQueryId) {
      try {
        const status = await checkQueryStatus(state.statsQueryId);

        if (status.state === 'RUNNING' || status.state === 'QUEUED') {
          return NextResponse.json({
            phase: 'running_stats',
            progress: { step: 'stats', percent: 80, message: 'Computing statistics...' },
          });
        }

        if (status.state === 'FAILED' || status.state === 'CANCELLED') {
          state = { ...state, phase: 'error', error: status.error || 'Stats query failed' };
          await putConfig(STATE_KEY(cc), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

        // Stats succeeded — parse results
        console.log(`[CONSOLIDATE] Stats query complete for ${cc}`);
        const result = await runQuery(buildStatsQuery(state.consolidatedTable!));

        const byAttribute: AttributeStat[] = result.rows.map(row => ({
          attributeType: String(row.attr_type),
          attributeValue: String(row.attr_value),
          maidCount: parseInt(String(row.maid_count)) || 0,
          oldestData: '',
          newestData: '',
          avgDwell: parseFloat(String(row.avg_dwell)) || 0,
          medianDwell: parseFloat(String(row.median_dwell)) || 0,
        }));

        // Get total unique MAIDs
        const totalResult = await runQuery(buildTotalQuery(state.consolidatedTable!));
        const totalMaids = parseInt(String(totalResult.rows[0]?.total)) || 0;

        // Save stats
        await saveConsolidationStats(cc, totalMaids, byAttribute);

        // Clean up temp tables (fire-and-forget)
        for (const table of state.tempTables || []) {
          dropTempTable(table).catch(() => {});
        }
        // Don't drop consolidated table — it backs the Parquet files for download

        state = { ...state, phase: 'done' };
        await putConfig(STATE_KEY(cc), state, { compact: true });

        return NextResponse.json({
          phase: 'done',
          totalMaids,
          byAttribute,
          progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} unique MAIDs consolidated` },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'error', error: 'Query expired — please retry' };
          await putConfig(STATE_KEY(cc), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        throw err;
      }
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[CONSOLIDATE] Error for ${cc}:`, error.message);
    try {
      await putConfig(STATE_KEY(cc), { phase: 'error', error: error.message });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
