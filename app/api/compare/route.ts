import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

interface DatasetSide {
  name: string;
  source: 'all' | 'category-export' | 'nse-export';
  minDwell?: number;
  exportFile?: string;
}

interface CompareState {
  phase: 'querying' | 'polling' | 'processing' | 'done' | 'error';
  stateId: string;
  datasetA: DatasetSide;
  datasetB: DatasetSide;
  queryIdA?: string;
  queryIdB?: string;
  queryDoneA?: boolean;
  queryDoneB?: boolean;
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

function needsAthena(side: DatasetSide): boolean {
  return side.source === 'all';
}

function buildMaidQuery(tableName: string, minDwell: number): string {
  if (minDwell > 0) {
    return `
      WITH visits AS (
        SELECT ad_id, date, t2.poi_id,
          DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell
        FROM ${tableName}
        CROSS JOIN UNNEST(poi_ids) AS t2(poi_id)
        WHERE t2.poi_id IS NOT NULL AND t2.poi_id != ''
          AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        GROUP BY ad_id, date, t2.poi_id
      )
      SELECT DISTINCT ad_id FROM visits WHERE dwell >= ${minDwell}
    `;
  }
  return `SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''`;
}

async function loadMaidsFromCsv(fileName: string): Promise<Set<string>> {
  const key = `exports/${fileName}`;
  const obj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
  const text = await obj.Body!.transformToString('utf-8');
  const lines = text.split('\n');
  const maids = new Set<string>();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim().replace(/"/g, '');
    if (line) maids.add(line);
  }
  return maids;
}

async function loadMaidsFromAthenaResult(queryId: string): Promise<Set<string>> {
  const csvKey = `athena-results/${queryId}.csv`;
  const obj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: csvKey }));
  const text = await obj.Body!.transformToString('utf-8');
  const lines = text.split('\n');
  const maids = new Set<string>();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim().replace(/"/g, '');
    if (line) maids.add(line);
  }
  return maids;
}

/**
 * POST /api/compare
 *
 * Multi-phase polling endpoint for MAID comparison between two datasets.
 *
 * First call body: { datasetA: DatasetSide, datasetB: DatasetSide }
 * Subsequent calls: { stateId: string } (reads state from S3)
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

    // Read existing state if polling
    let state: CompareState | null = null;
    if (!isNewRequest && stateId) {
      state = await getConfig<CompareState>(STATE_KEY(stateId));

      // Return cached results
      if (state?.phase === 'done' && state.result) {
        return NextResponse.json({
          phase: 'done',
          stateId: state.stateId,
          result: state.result,
          progress: { step: 'done', percent: 100, message: 'Comparison complete' },
        });
      }
    }

    // Reset on error or new request
    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: start ─────────────────────────────────────────────
    if (!state) {
      const datasetA: DatasetSide = body.datasetA;
      const datasetB: DatasetSide = body.datasetB;

      if (!datasetA?.name || !datasetB?.name) {
        return NextResponse.json({ error: 'Both datasetA and datasetB required' }, { status: 400 });
      }

      const newStateId = `${datasetA.name}-${datasetB.name}-${Date.now()}`;
      console.log(`[COMPARE] Starting comparison: ${datasetA.name} (${datasetA.source}) vs ${datasetB.name} (${datasetB.source})`);

      // Fast path: both are CSV exports → no Athena needed
      if (!needsAthena(datasetA) && !needsAthena(datasetB)) {
        console.log(`[COMPARE] Fast path: both from CSV exports`);

        const [maidsA, maidsB] = await Promise.all([
          loadMaidsFromCsv(datasetA.exportFile!),
          loadMaidsFromCsv(datasetB.exportFile!),
        ]);

        const overlap = new Set<string>();
        for (const id of maidsA) {
          if (maidsB.has(id)) overlap.add(id);
        }

        // Save overlap CSV
        const fileName = `compare-${datasetA.name}-${datasetB.name}-${Date.now()}.csv`;
        const csvContent = 'ad_id\n' + Array.from(overlap).join('\n');
        await s3Client.send(new PutObjectCommand({
          Bucket: BUCKET,
          Key: `exports/${fileName}`,
          Body: csvContent,
          ContentType: 'text/csv',
        }));

        const result = {
          totalA: maidsA.size,
          totalB: maidsB.size,
          overlap: overlap.size,
          overlapPctA: maidsA.size > 0 ? Math.round((overlap.size / maidsA.size) * 10000) / 100 : 0,
          overlapPctB: maidsB.size > 0 ? Math.round((overlap.size / maidsB.size) * 10000) / 100 : 0,
          downloadKey: `/api/datasets/${datasetA.name}/export/download?file=${encodeURIComponent(fileName)}`,
        };

        state = { phase: 'done', stateId: newStateId, datasetA, datasetB, result };
        await putConfig(STATE_KEY(newStateId), state, { compact: true });

        return NextResponse.json({ phase: 'done', stateId: newStateId, result, progress: { step: 'done', percent: 100, message: 'Comparison complete' } });
      }

      // Athena path: start queries for sides that need it
      let queryIdA: string | undefined;
      let queryIdB: string | undefined;
      let queryDoneA = !needsAthena(datasetA);
      let queryDoneB = !needsAthena(datasetB);

      if (needsAthena(datasetA)) {
        await ensureTableForDataset(datasetA.name);
        const table = getTableName(datasetA.name);
        const sql = buildMaidQuery(table, datasetA.minDwell || 0);
        queryIdA = await startQueryAsync(sql);
        console.log(`[COMPARE] Athena query A started: ${queryIdA}`);
      }

      if (needsAthena(datasetB)) {
        await ensureTableForDataset(datasetB.name);
        const table = getTableName(datasetB.name);
        const sql = buildMaidQuery(table, datasetB.minDwell || 0);
        queryIdB = await startQueryAsync(sql);
        console.log(`[COMPARE] Athena query B started: ${queryIdB}`);
      }

      state = {
        phase: 'polling', stateId: newStateId, datasetA, datasetB,
        queryIdA, queryIdB, queryDoneA, queryDoneB,
      };
      await putConfig(STATE_KEY(newStateId), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        stateId: newStateId,
        progress: { step: 'queries_started', percent: 10, message: 'Running Athena queries...' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    if (state.phase === 'polling') {
      let allDone = true;
      let anyFailed = false;
      let errorMsg = '';
      let scannedInfo = '';

      // Check query A
      if (state.queryIdA && !state.queryDoneA) {
        try {
          const status = await checkQueryStatus(state.queryIdA);
          if (status.state === 'RUNNING' || status.state === 'QUEUED') {
            allDone = false;
            const gb = status.statistics?.dataScannedBytes ? (status.statistics.dataScannedBytes / 1e9).toFixed(1) : '0';
            scannedInfo += `A: ${gb}GB `;
          } else if (status.state === 'FAILED' || status.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `Query A failed: ${status.error || 'unknown'}`;
          } else {
            state.queryDoneA = true;
          }
        } catch (err: any) {
          if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
            anyFailed = true;
            errorMsg = 'Query A expired — please retry';
          } else { throw err; }
        }
      }

      // Check query B
      if (state.queryIdB && !state.queryDoneB) {
        try {
          const status = await checkQueryStatus(state.queryIdB);
          if (status.state === 'RUNNING' || status.state === 'QUEUED') {
            allDone = false;
            const gb = status.statistics?.dataScannedBytes ? (status.statistics.dataScannedBytes / 1e9).toFixed(1) : '0';
            scannedInfo += `B: ${gb}GB`;
          } else if (status.state === 'FAILED' || status.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `Query B failed: ${status.error || 'unknown'}`;
          } else {
            state.queryDoneB = true;
          }
        } catch (err: any) {
          if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
            anyFailed = true;
            errorMsg = 'Query B expired — please retry';
          } else { throw err; }
        }
      }

      if (anyFailed) {
        state = { ...state, phase: 'error', error: errorMsg };
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        return NextResponse.json({ phase: 'error', stateId: state.stateId, error: errorMsg });
      }

      if (!allDone || !state.queryDoneA || !state.queryDoneB) {
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        return NextResponse.json({
          phase: 'polling',
          stateId: state.stateId,
          progress: { step: 'polling', percent: 40, message: `Queries running... ${scannedInfo}`.trim() },
        });
      }

      // Both done → move to processing
      state = { ...state, phase: 'processing' };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });

      return NextResponse.json({
        phase: 'processing',
        stateId: state.stateId,
        progress: { step: 'processing', percent: 70, message: 'Queries complete, computing overlap...' },
      });
    }

    // ── Phase: processing ────────────────────────────────────────
    if (state.phase === 'processing') {
      console.log(`[COMPARE] Processing: loading MAIDs from both sides`);

      // Load MAIDs from each side
      const [maidsA, maidsB] = await Promise.all([
        state.queryIdA
          ? loadMaidsFromAthenaResult(state.queryIdA)
          : loadMaidsFromCsv(state.datasetA.exportFile!),
        state.queryIdB
          ? loadMaidsFromAthenaResult(state.queryIdB)
          : loadMaidsFromCsv(state.datasetB.exportFile!),
      ]);

      console.log(`[COMPARE] A: ${maidsA.size} MAIDs, B: ${maidsB.size} MAIDs`);

      // Compute intersection
      const overlap = new Set<string>();
      const smaller = maidsA.size <= maidsB.size ? maidsA : maidsB;
      const larger = maidsA.size <= maidsB.size ? maidsB : maidsA;
      for (const id of smaller) {
        if (larger.has(id)) overlap.add(id);
      }

      console.log(`[COMPARE] Overlap: ${overlap.size} MAIDs`);

      // Save overlap CSV
      const fileName = `compare-${state.datasetA.name}-${state.datasetB.name}-${Date.now()}.csv`;
      if (overlap.size > 0) {
        const csvContent = 'ad_id\n' + Array.from(overlap).join('\n');
        await s3Client.send(new PutObjectCommand({
          Bucket: BUCKET,
          Key: `exports/${fileName}`,
          Body: csvContent,
          ContentType: 'text/csv',
        }));
      }

      const result = {
        totalA: maidsA.size,
        totalB: maidsB.size,
        overlap: overlap.size,
        overlapPctA: maidsA.size > 0 ? Math.round((overlap.size / maidsA.size) * 10000) / 100 : 0,
        overlapPctB: maidsB.size > 0 ? Math.round((overlap.size / maidsB.size) * 10000) / 100 : 0,
        downloadKey: overlap.size > 0
          ? `/api/datasets/${state.datasetA.name}/export/download?file=${encodeURIComponent(fileName)}`
          : '',
      };

      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        stateId: state.stateId,
        result,
        progress: { step: 'done', percent: 100, message: 'Comparison complete' },
      });
    }

    return NextResponse.json({ phase: 'error', stateId: stateId || '', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[COMPARE] Error:`, error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
