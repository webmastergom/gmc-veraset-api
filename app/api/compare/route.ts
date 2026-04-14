import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
  runQuery,
} from '@/lib/athena';
import { getConfig, putConfig, s3Client, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

interface DatasetSide {
  name: string;
  source: 'all' | 'category-export' | 'nse-export';
  minDwell?: number;
  maxDwell?: number;
  hourFrom?: number;
  hourTo?: number;
  exportFile?: string;
}

interface CompareState {
  phase: 'starting' | 'polling' | 'reading' | 'done' | 'error';
  stateId: string;
  datasetA: DatasetSide;
  datasetB: DatasetSide;
  // 4 parallel queries: countA, countB, countOverlap, exportOverlap
  queryIds: {
    countA?: string;
    countB?: string;
    countOverlap?: string;
    exportOverlap?: string;
  };
  done: {
    countA?: boolean;
    countB?: boolean;
    countOverlap?: boolean;
    exportOverlap?: boolean;
  };
  // For CSV-export sides: temp table names to clean up
  tempTables?: string[];
  error?: string;
  result?: {
    totalA: number;
    totalB: number;
    overlap: number;
    overlapPctA: number;
    overlapPctB: number;
    downloadKey: string;
  };
}

const STATE_KEY = (id: string) => `compare-state/${id}`;

/**
 * Build a SQL subquery that returns DISTINCT ad_id for a dataset side.
 * Supports dwell interval (min/max) and hour-of-visit (from/to) filters.
 */
function maidSubquery(side: DatasetSide, tableName: string): string {
  if (side.source === 'all') {
    const minDwell = side.minDwell || 0;
    const maxDwell = side.maxDwell || 0;
    const hourFrom = side.hourFrom ?? 0;
    const hourTo = side.hourTo ?? 23;
    const hasHourFilter = hourFrom > 0 || hourTo < 23;
    const hasDwellFilter = minDwell > 0 || maxDwell > 0;

    if (hasDwellFilter || hasHourFilter) {
      // Build hour WHERE clause (supports cross-midnight ranges like 22→06)
      let hourWhere = '';
      if (hasHourFilter) {
        if (hourFrom <= hourTo) {
          hourWhere = `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
        } else {
          hourWhere = `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
        }
      }

      // Build dwell HAVING clauses
      const dwellHavings: string[] = [];
      if (minDwell > 0) dwellHavings.push(`dwell >= ${minDwell}`);
      if (maxDwell > 0) dwellHavings.push(`dwell <= ${maxDwell}`);
      const havingClause = dwellHavings.length > 0 ? `WHERE ${dwellHavings.join(' AND ')}` : '';

      return `(
        SELECT DISTINCT ad_id FROM (
          SELECT ad_id, date,
            DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell
          FROM ${tableName}
          CROSS JOIN UNNEST(poi_ids) AS t2(poi_id)
          WHERE t2.poi_id IS NOT NULL AND t2.poi_id != ''
            AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
            ${hourWhere}
          GROUP BY ad_id, date, t2.poi_id
        ) ${havingClause}
      )`;
    }
    return `(SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '')`;
  }
  // CSV export: uses temp external table
  return `(SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '')`;
}

/**
 * Create a temporary Athena external table from a CSV export in S3.
 * The CSV has a header "ad_id" and one MAID per line.
 */
async function createTempTableFromCsv(exportFile: string, stateId: string, side: string): Promise<string> {
  const tempName = `compare_tmp_${side}_${Date.now()}`;
  const s3Key = `exports/${exportFile}`;
  const tempFolder = `compare-temp/${tempName}/`;

  // Copy CSV to a dedicated folder (Athena LOCATION needs a directory)
  const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
  await s3Client.send(new CopyObjectCommand({
    Bucket: BUCKET,
    CopySource: `${BUCKET}/${s3Key}`,
    Key: `${tempFolder}data.csv`,
  }));

  const createSql = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tempName} (
      ad_id STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    LOCATION 's3://${BUCKET}/${tempFolder}'
    TBLPROPERTIES ('skip.header.line.count'='1')
  `;
  await runQuery(createSql);
  console.log(`[COMPARE] Created temp table ${tempName} from ${exportFile}`);
  return tempName;
}

/** Clean up temp tables and their S3 data */
async function cleanupTempTables(tables: string[]): Promise<void> {
  for (const t of tables) {
    try {
      await runQuery(`DROP TABLE IF EXISTS ${t}`);
      const { DeleteObjectCommand } = await import('@aws-sdk/client-s3');
      await s3Client.send(new DeleteObjectCommand({
        Bucket: BUCKET,
        Key: `compare-temp/${t}/data.csv`,
      })).catch(() => {});
    } catch (e: any) {
      console.warn(`[COMPARE] Cleanup ${t}:`, e.message);
    }
  }
}

/**
 * POST /api/compare
 *
 * Multi-phase Athena-native comparison. No large CSV downloads into memory.
 *
 * Phase 1 (start): Ensure tables, fire 4 Athena queries in parallel:
 *   - COUNT(A), COUNT(B), COUNT(A∩B), SELECT A∩B (for CSV download)
 * Phase 2 (polling): Poll all 4 queries
 * Phase 3 (reading): Fetch the 3 count results (1 row each), build response
 *   - The overlap export CSV stays in Athena output — linked for download
 */
export async function POST(request: NextRequest) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    let body: any;
    try { body = await request.json(); } catch { body = {}; }

    const isNewRequest = !!body.datasetA;
    const stateId = body.stateId || '';

    let state: CompareState | null = null;
    if (!isNewRequest && stateId) {
      state = await getConfig<CompareState>(STATE_KEY(stateId));

      if (state?.phase === 'done' && state.result) {
        return NextResponse.json({
          phase: 'done', stateId: state.stateId, result: state.result,
          progress: { step: 'done', percent: 100, message: 'Comparison complete' },
        });
      }
    }

    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: start ─────────────────────────────────────────────
    if (!state) {
      const datasetA: DatasetSide = body.datasetA;
      const datasetB: DatasetSide = body.datasetB;

      if (!datasetA?.name || !datasetB?.name) {
        return NextResponse.json({ error: 'Both datasetA and datasetB required' }, { status: 400 });
      }

      const newStateId = `${datasetA.name}-${datasetB.name}-${Date.now()}`;
      console.log(`[COMPARE] Starting: ${datasetA.name} (${datasetA.source}) vs ${datasetB.name} (${datasetB.source})`);

      const tempTables: string[] = [];
      let tableA: string;
      let tableB: string;

      if (datasetA.source === 'all') {
        await ensureTableForDataset(datasetA.name);
        tableA = getTableName(datasetA.name);
      } else {
        tableA = await createTempTableFromCsv(datasetA.exportFile!, newStateId, 'a');
        tempTables.push(tableA);
      }

      if (datasetB.source === 'all') {
        await ensureTableForDataset(datasetB.name);
        tableB = getTableName(datasetB.name);
      } else {
        tableB = await createTempTableFromCsv(datasetB.exportFile!, newStateId, 'b');
        tempTables.push(tableB);
      }

      const subA = maidSubquery(datasetA, tableA);
      const subB = maidSubquery(datasetB, tableB);

      // Fire 4 queries in parallel
      const [countAId, countBId, countOverlapId, exportOverlapId] = await Promise.all([
        startQueryAsync(`SELECT COUNT(*) as total FROM ${subA} t`),
        startQueryAsync(`SELECT COUNT(*) as total FROM ${subB} t`),
        startQueryAsync(`SELECT COUNT(*) as total FROM ${subA} a INNER JOIN ${subB} b ON a.ad_id = b.ad_id`),
        startQueryAsync(`SELECT a.ad_id FROM ${subA} a INNER JOIN ${subB} b ON a.ad_id = b.ad_id`),
      ]);

      console.log(`[COMPARE] 4 queries started — countA:${countAId} countB:${countBId} countOvl:${countOverlapId} export:${exportOverlapId}`);

      state = {
        phase: 'polling', stateId: newStateId, datasetA, datasetB,
        queryIds: { countA: countAId, countB: countBId, countOverlap: countOverlapId, exportOverlap: exportOverlapId },
        done: {},
        tempTables: tempTables.length > 0 ? tempTables : undefined,
      };
      await putConfig(STATE_KEY(newStateId), state, { compact: true });

      return NextResponse.json({
        phase: 'polling', stateId: newStateId,
        progress: { step: 'queries_started', percent: 10, message: 'Running 4 Athena queries...' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    if (state.phase === 'polling') {
      let allDone = true;
      let anyFailed = false;
      let errorMsg = '';
      let scannedGB = 0;

      for (const [key, qId] of Object.entries(state.queryIds)) {
        if (!qId || state.done[key as keyof typeof state.done]) continue;
        try {
          const status = await checkQueryStatus(qId);
          if (status.state === 'RUNNING' || status.state === 'QUEUED') {
            allDone = false;
            scannedGB += (status.statistics?.dataScannedBytes || 0) / 1e9;
          } else if (status.state === 'FAILED' || status.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `${key} failed: ${status.error || 'unknown'}`;
          } else {
            (state.done as any)[key] = true;
          }
        } catch (err: any) {
          if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
            anyFailed = true;
            errorMsg = `${key} expired — please retry`;
          } else { throw err; }
        }
      }

      if (anyFailed) {
        if (state.tempTables?.length) await cleanupTempTables(state.tempTables).catch(() => {});
        state = { ...state, phase: 'error', error: errorMsg };
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        return NextResponse.json({ phase: 'error', stateId: state.stateId, error: errorMsg });
      }

      if (!allDone) {
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        const doneCount = Object.values(state.done).filter(Boolean).length;
        const pct = 10 + Math.round((doneCount / 4) * 60);
        return NextResponse.json({
          phase: 'polling', stateId: state.stateId,
          progress: { step: 'polling', percent: pct, message: `Queries running (${doneCount}/4 done, ${scannedGB.toFixed(1)}GB scanned)` },
        });
      }

      // All done → advance to reading
      state = { ...state, phase: 'reading' };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });

      return NextResponse.json({
        phase: 'reading', stateId: state.stateId,
        progress: { step: 'reading', percent: 80, message: 'Queries complete, reading results...' },
      });
    }

    // ── Phase: reading (lightweight — only fetches 3 single-row results) ──
    if (state.phase === 'reading') {
      const [countARes, countBRes, countOvlRes] = await Promise.all([
        fetchQueryResults(state.queryIds.countA!),
        fetchQueryResults(state.queryIds.countB!),
        fetchQueryResults(state.queryIds.countOverlap!),
      ]);

      const totalA = parseInt(countARes.rows[0]?.total, 10) || 0;
      const totalB = parseInt(countBRes.rows[0]?.total, 10) || 0;
      const overlapCount = parseInt(countOvlRes.rows[0]?.total, 10) || 0;

      // The export overlap CSV is the Athena output of the 4th query
      const downloadKey = overlapCount > 0
        ? `/api/compare/download?queryId=${state.queryIds.exportOverlap}`
        : '';

      // Clean up temp tables
      if (state.tempTables?.length) await cleanupTempTables(state.tempTables).catch(() => {});

      const result = {
        totalA,
        totalB,
        overlap: overlapCount,
        overlapPctA: totalA > 0 ? Math.round((overlapCount / totalA) * 10000) / 100 : 0,
        overlapPctB: totalB > 0 ? Math.round((overlapCount / totalB) * 10000) / 100 : 0,
        downloadKey,
      };

      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });

      console.log(`[COMPARE] Done: A=${totalA}, B=${totalB}, overlap=${overlapCount} (${result.overlapPctA}%/${result.overlapPctB}%)`);

      return NextResponse.json({
        phase: 'done', stateId: state.stateId, result,
        progress: { step: 'done', percent: 100, message: 'Comparison complete' },
      });
    }

    return NextResponse.json({ phase: 'error', stateId: stateId || '', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[COMPARE] Error:`, error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
