/**
 * Multi-phase dataset analysis that works within 60s Vercel Hobby timeout.
 * Splits the analysis into: partition prep → start queries → poll → process results.
 */

import {
  createTableForDataset,
  tableExists,
  getTableName,
  discoverPartitionsFromS3,
  getPartitionsFromCatalog,
  addPartitionsManually,
  startQueryAsync,
  checkQueryStatus,
  runQuery,
} from './athena';
import { s3Client, BUCKET } from './s3-config';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import type { AnalysisResult, DailyData, VisitByPoi } from './dataset-analysis';

// ── State ────────────────────────────────────────────────────────────

export interface AnalysisState {
  status: 'preparing' | 'queries_running' | 'processing' | 'completed' | 'error';
  datasetName: string;
  progress: { step: string; percent: number; message: string };
  /** Athena query IDs for the three parallel queries */
  queryIds?: { daily: string; summary: string; visits: string };
  /** Resolved date range */
  dateFrom?: string;
  dateTo?: string;
  allDates?: string[];
  expectedDates?: string[];
  jobMetadata?: { radius?: number; requestedRadius?: number; radiusMismatch?: boolean };
  result?: AnalysisResult;
  error?: string;
  startedAt: string;
  updatedAt: string;
}

const STATE_PREFIX = 'config/analysis-state';

async function getState(datasetName: string): Promise<AnalysisState | null> {
  try {
    const res = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: `${STATE_PREFIX}/${datasetName}.json`,
    }));
    const body = await res.Body?.transformToString();
    return body ? JSON.parse(body) : null;
  } catch {
    return null;
  }
}

async function saveState(state: AnalysisState): Promise<void> {
  state.updatedAt = new Date().toISOString();
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${STATE_PREFIX}/${state.datasetName}.json`,
    Body: JSON.stringify(state),
    ContentType: 'application/json',
  }));
}

// ── Main entry ───────────────────────────────────────────────────────

/**
 * Start or advance a multi-phase analysis.
 * Each call completes within ~50s.
 */
export async function analyzeMultiPhase(datasetName: string): Promise<AnalysisState> {
  let state = await getState(datasetName);

  // Reset if previous run is done
  if (state && (state.status === 'completed' || state.status === 'error')) {
    state = null;
  }

  // ── Phase 1: Prepare partitions + start queries ──────────────
  if (!state) {
    const tableName = getTableName(datasetName);

    if (!(await tableExists(datasetName))) {
      await createTableForDataset(datasetName);
    }

    // Discover partitions from S3 (source of truth)
    const s3Partitions = await discoverPartitionsFromS3(datasetName);
    if (s3Partitions.length === 0) {
      const errState: AnalysisState = {
        status: 'error',
        datasetName,
        progress: { step: 'error', percent: 0, message: `No partitions found for ${datasetName}` },
        error: `No partitions found for ${datasetName}`,
        startedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      await saveState(errState);
      return errState;
    }

    // Quick catalog sync (no retries — just add missing)
    const catalogPartitions = await getPartitionsFromCatalog(tableName).catch(() => [] as string[]);
    const missing = s3Partitions.filter(p => !catalogPartitions.includes(p));
    if (missing.length > 0) {
      try {
        await runQuery(`MSCK REPAIR TABLE ${tableName}`);
      } catch {
        await addPartitionsManually(tableName, datasetName, missing);
      }
    }

    // Get job metadata (expected dates, radius, POI names)
    let expectedDates: string[] = [];
    let jobMetadata: { radius?: number; requestedRadius?: number; radiusMismatch?: boolean } | undefined;
    try {
      const { getAllJobs } = await import('./jobs');
      const { calculateDaysInclusive } = await import('./s3');
      const allJobs = await getAllJobs();
      const job = allJobs.find(j => {
        if (!j.s3DestPath) return false;
        const path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
        const folder = path.split('/').filter(Boolean)[0] || path.replace(/\/$/, '');
        return folder === datasetName;
      });

      if (job?.verasetPayload?.date_range?.from_date && job?.verasetPayload?.date_range?.to_date) {
        const from = job.verasetPayload.date_range.from_date;
        const to = job.verasetPayload.date_range.to_date;
        const fromDate = new Date(from + 'T00:00:00Z');
        const toDate = new Date(to + 'T00:00:00Z');
        for (let d = new Date(fromDate); d <= toDate; d.setDate(d.getDate() + 1)) {
          expectedDates.push(d.toISOString().split('T')[0]);
        }
      }

      if (job) {
        const reqRadius = job.auditTrail?.userInput?.radius;
        jobMetadata = {
          radius: job.radius,
          requestedRadius: reqRadius,
          radiusMismatch: reqRadius !== undefined && job.radius !== undefined && reqRadius !== job.radius,
        };
      }
    } catch { /* ignore job metadata errors */ }

    const allDates = s3Partitions.sort();
    const dateFrom = allDates[0];
    const dateTo = allDates[allDates.length - 1];

    // Start the 3 analysis queries in parallel
    const dailySql = `
      SELECT date, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
      FROM ${tableName}
      WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      GROUP BY date ORDER BY date ASC
    `;

    const summarySql = `
      SELECT COUNT(*) as total_pings, COUNT(DISTINCT ad_id) as unique_devices,
        COUNT(DISTINCT poi_id) as unique_pois
      FROM ${tableName} CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND poi_id IS NOT NULL AND poi_id != ''
    `;

    const visitsByPoiSql = `
      SELECT poi_id, COUNT(*) as visits, COUNT(DISTINCT ad_id) as devices
      FROM ${tableName} CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
        AND poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      GROUP BY poi_id ORDER BY visits DESC
    `;

    const [dailyId, summaryId, visitsId] = await Promise.all([
      startQueryAsync(dailySql),
      startQueryAsync(summarySql),
      startQueryAsync(visitsByPoiSql),
    ]);

    state = {
      status: 'queries_running',
      datasetName,
      queryIds: { daily: dailyId, summary: summaryId, visits: visitsId },
      dateFrom,
      dateTo,
      allDates,
      expectedDates: expectedDates.length > 0 ? expectedDates : undefined,
      jobMetadata,
      progress: { step: 'queries', percent: 20, message: `Queries Athena iniciadas (${allDates.length} días)...` },
      startedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await saveState(state);
    console.log(`[ANALYSIS] ${datasetName}: started 3 queries, ${allDates.length} partitions`);
    return state;
  }

  // ── Phase 2: Poll Athena queries ────────────────────────────
  if (state.status === 'queries_running') {
    const ids = state.queryIds!;
    const [dailyS, summaryS, visitsS] = await Promise.all([
      checkQueryStatus(ids.daily),
      checkQueryStatus(ids.summary),
      checkQueryStatus(ids.visits),
    ]);

    // Check for failures
    for (const [name, s] of [['daily', dailyS], ['summary', summaryS], ['visits', visitsS]] as const) {
      if (s.state === 'FAILED') {
        state.status = 'error';
        state.error = `${name} query failed: ${s.error}`;
        state.progress = { step: 'error', percent: 0, message: state.error };
        await saveState(state);
        return state;
      }
    }

    const allDone = dailyS.state === 'SUCCEEDED' && summaryS.state === 'SUCCEEDED' && visitsS.state === 'SUCCEEDED';

    if (!allDone) {
      const statusMsg = `Daily: ${dailyS.state}, Summary: ${summaryS.state}, Visits: ${visitsS.state}`;
      state.progress = { step: 'queries', percent: 40, message: `Esperando queries... (${statusMsg})` };
      await saveState(state);
      return state;
    }

    // All done — advance to processing
    state.status = 'processing';
    state.progress = { step: 'queries', percent: 60, message: 'Queries completadas. Procesando resultados...' };
    await saveState(state);
    // Fall through to processing
  }

  // ── Phase 3: Process results ────────────────────────────────
  if (state.status === 'processing') {
    const ids = state.queryIds!;

    // Download results from Athena output CSVs
    const fetchCsv = async (queryId: string) => {
      const res = await s3Client.send(new GetObjectCommand({
        Bucket: BUCKET,
        Key: `athena-results/${queryId}.csv`,
      }));
      const text = await res.Body?.transformToString() || '';
      const lines = text.split('\n');
      const headers = lines[0]?.replace(/"/g, '').split(',').map(h => h.trim()) || [];
      const rows: Record<string, string>[] = [];
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        const values = line.replace(/"/g, '').split(',');
        const row: Record<string, string> = {};
        headers.forEach((h, idx) => { row[h] = values[idx] || ''; });
        rows.push(row);
      }
      return rows;
    };

    const [dailyRows, summaryRows, visitsRows] = await Promise.all([
      fetchCsv(ids.daily),
      fetchCsv(ids.summary),
      fetchCsv(ids.visits),
    ]);

    state.progress = { step: 'processing', percent: 80, message: 'Construyendo resultado...' };

    // Build dailyData
    const dailyDataMap = new Map<string, DailyData>();
    for (const r of dailyRows) {
      const date = (r.date || '').trim();
      if (date) {
        dailyDataMap.set(date, {
          date,
          pings: Number(r.pings) || 0,
          devices: Number(r.devices) || 0,
        });
      }
    }

    // Fill expected dates with zeros
    if (state.expectedDates) {
      for (const d of state.expectedDates) {
        if (!dailyDataMap.has(d)) {
          dailyDataMap.set(d, { date: d, pings: 0, devices: 0 });
        }
      }
    }

    const dailyData = Array.from(dailyDataMap.values()).sort((a, b) => a.date.localeCompare(b.date));
    const summaryRow = summaryRows[0] || {};

    // Resolve POI names
    let poiNamesByVerasetId: Record<string, string> = {};
    const poiNamesByOriginalId: Record<string, string> = {};
    try {
      const { getAllJobs } = await import('./jobs');
      const jobs = await getAllJobs();
      const job = jobs.find(j => {
        if (!j.s3DestPath) return false;
        const path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
        const folder = path.split('/').filter(Boolean)[0] || path.replace(/\/$/, '');
        return folder === datasetName;
      });

      if (job?.poiNames) {
        poiNamesByVerasetId = { ...job.poiNames };
        if (job?.poiMapping) {
          for (const [verasetId, originalId] of Object.entries(job.poiMapping)) {
            const name = poiNamesByVerasetId[verasetId];
            if (name) poiNamesByOriginalId[originalId as string] = name;
          }
        }
      }
      if (job?.externalPois) {
        for (const p of job.externalPois) {
          if (p.name && p.id) poiNamesByOriginalId[p.id] = p.name;
        }
      }
      if (job?.poiCollectionId && job?.poiMapping) {
        try {
          const { getPOICollection } = await import('./poi-storage');
          const geojson = await getPOICollection(job.poiCollectionId);
          if (geojson?.features) {
            for (const [verasetPoiId, originalPoiId] of Object.entries(job.poiMapping)) {
              if (poiNamesByVerasetId[verasetPoiId]) continue;
              const f = geojson.features.find((feat: any) => {
                const id = feat.id ?? feat.properties?.id ?? feat.properties?.poi_id ?? feat.properties?.identifier;
                return String(id) === String(originalPoiId);
              });
              const name = f?.properties?.name;
              if (name) {
                poiNamesByVerasetId[verasetPoiId] = name;
                poiNamesByOriginalId[originalPoiId as string] = name;
              }
            }
          }
        } catch { /* ignore */ }
      }
    } catch { /* ignore */ }

    const visitsByPoi: VisitByPoi[] = visitsRows.map(r => {
      const poiId = (r.poi_id || '').trim();
      return {
        poiId,
        name: poiNamesByVerasetId[poiId] || poiNamesByOriginalId[poiId] || undefined,
        visits: Number(r.visits) || 0,
        devices: Number(r.devices) || 0,
      };
    });

    const result: AnalysisResult = {
      dataset: datasetName,
      analyzedAt: new Date().toISOString(),
      summary: {
        totalPings: Number(summaryRow.total_pings) || 0,
        uniqueDevices: Number(summaryRow.unique_devices) || 0,
        uniquePois: Number(summaryRow.unique_pois) || 0,
        dateRange: { from: state.dateFrom!, to: state.dateTo! },
        daysAnalyzed: dailyData.length,
      },
      dailyData,
      visitsByPoi,
      jobMetadata: state.jobMetadata,
    };

    state.status = 'completed';
    state.result = result;
    state.progress = {
      step: 'done',
      percent: 100,
      message: `Análisis completo: ${result.summary.uniqueDevices.toLocaleString()} devices, ${result.summary.daysAnalyzed} días`,
    };
    await saveState(state);
    console.log(`[ANALYSIS] ${datasetName}: completed — ${result.summary.uniqueDevices} devices, ${dailyData.length} days`);
    return state;
  }

  return state;
}

export async function resetAnalysisState(datasetName: string): Promise<void> {
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${STATE_PREFIX}/${datasetName}.json`,
      Body: '',
    }));
  } catch { /* ignore */ }
}
