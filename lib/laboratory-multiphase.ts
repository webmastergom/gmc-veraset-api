/**
 * Laboratory Multi-Phase Analysis Engine.
 *
 * Replaces the single-request SSE flow with a state machine persisted to S3.
 * Each invocation completes within ~50s (safe for Vercel 60s timeout).
 *
 * Phases:
 *   starting → spatial_running → origins_running → processing → completed
 *
 * The frontend polls POST /api/laboratory/analyze/poll every 2.5s,
 * sending the full LabConfig. Each call advances the state machine.
 */

import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  startCTASAsync,
  runQuery,
  runQueryViaS3,
  ensureTableForDataset,
  dropTempTable,
  cleanupTempS3,
} from './athena';
import {
  buildSpatialJoinQueries,
  buildOriginsCTASQuery,
  processVisitsForRecipe,
  geocodeOrigins,
} from './laboratory-analyzer';
import type { ParsedVisit } from './laboratory-analyzer';
import type { LabConfig, LabAnalysisResult, PoiCategory } from './laboratory-types';
import { toIsoCountry } from './country-inference';

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
const LAB_STATE_PREFIX = 'config/laboratory-state';
const STALE_TIMEOUT_MS = 10 * 60 * 1000; // 10 min

// ── Batching constants ──────────────────────────────────────────────────
// When POI count exceeds this, split into geographic batches to avoid
// Athena "Query exhausted resources" on the spatial join CTAS.
const POI_BATCH_THRESHOLD = 5000;
const MAX_POI_BATCHES = 4;

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

// ── State interface ──────────────────────────────────────────────────────

export interface LaboratoryState {
  status: 'starting' | 'spatial_running' | 'origins_running' | 'processing' | 'completed' | 'error';
  datasetName: string;
  config: LabConfig;
  runId: string;
  // Query tracking (arrays for batched spatial join)
  spatialCTASIds?: string[];
  totalDevicesQueryId?: string;
  originsCTASId?: string;
  // Temp table names (arrays for batched spatial join)
  spatialTableNames?: string[];
  originsTableName?: string;
  // Batching info
  poiBatchCount?: number;
  // Intermediate values
  totalDevices?: number;
  visitCount?: number;
  // Progress + result
  progress: { step: string; percent: number; message: string };
  result?: LabAnalysisResult;
  error?: string;
  startedAt: string;
  updatedAt: string;
}

// ── S3 state persistence ─────────────────────────────────────────────────

async function getLabState(datasetName: string): Promise<LaboratoryState | null> {
  try {
    const res = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: `${LAB_STATE_PREFIX}/${datasetName}.json`,
    }));
    const body = await res.Body?.transformToString();
    return body ? JSON.parse(body) : null;
  } catch {
    return null;
  }
}

async function saveLabState(state: LaboratoryState): Promise<void> {
  state.updatedAt = new Date().toISOString();
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${LAB_STATE_PREFIX}/${state.datasetName}.json`,
    Body: JSON.stringify(state),
    ContentType: 'application/json',
  }));
}

export async function resetLabState(datasetName: string): Promise<void> {
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${LAB_STATE_PREFIX}/${datasetName}.json`,
      Body: '{}',
      ContentType: 'application/json',
    }));
  } catch { /* ignore */ }
}

// ── Empty result helper ──────────────────────────────────────────────────

function buildEmptyResult(config: LabConfig, totalDevices = 0): LabAnalysisResult {
  return {
    config,
    analyzedAt: new Date().toISOString(),
    segment: { totalDevices: 0, devices: [] },
    records: [],
    profiles: [],
    stats: {
      totalPingsAnalyzed: 0,
      totalDevicesInDataset: totalDevices,
      segmentSize: 0,
      segmentPercent: 0,
      totalPostalCodes: 0,
      categoriesAnalyzed: 0,
      avgAffinityIndex: 0,
      avgDwellMinutes: 0,
      categoryBreakdown: [],
      topHotspots: [],
    },
  };
}

// ── Main state machine ───────────────────────────────────────────────────

export async function analyzeLaboratoryMultiPhase(
  config: LabConfig,
): Promise<LaboratoryState> {
  const datasetName = config.datasetId;
  let state = await getLabState(datasetName);

  // Reset completed/error states
  if (state && (state.status === 'completed' || state.status === 'error')) {
    state = null;
  }

  // Reset old format (no runId)
  if (state && !state.runId) {
    state = null;
  }

  // Reset stale states (>10 min without update)
  if (state && state.updatedAt) {
    const ageMs = Date.now() - new Date(state.updatedAt).getTime();
    if (ageMs > STALE_TIMEOUT_MS) {
      console.log(`[LAB-MP] ${datasetName}: stale state (${Math.round(ageMs / 60000)}min old, status=${state.status}), resetting`);
      state = null;
    }
  }

  try {
    if (!state) {
      // ── Phase 1: Starting ─────────────────────────────────────────
      state = await phaseStarting(config);
    } else if (state.status === 'spatial_running') {
      // ── Phase 2: Poll spatial + total queries ─────────────────────
      state = await phaseSpatialRunning(state);
    } else if (state.status === 'origins_running') {
      // ── Phase 3: Poll origins query ───────────────────────────────
      state = await phaseOriginsRunning(state);
    } else if (state.status === 'processing') {
      // ── Phase 4: Download, geocode, process ───────────────────────
      state = await phaseProcessing(state);
    }

    await saveLabState(state);
    return state;
  } catch (error: any) {
    console.error(`[LAB-MP] ${datasetName} error:`, error.message);
    // Clean up any partial temp data on failure
    if (state?.spatialTableNames) {
      for (const tbl of state.spatialTableNames) {
        dropTempTable(tbl).catch(() => {});
        cleanupTempS3(tbl).catch(() => {});
      }
    }
    if (state?.originsTableName) {
      dropTempTable(state.originsTableName).catch(() => {});
      cleanupTempS3(state.originsTableName).catch(() => {});
    }
    const errorState: LaboratoryState = {
      ...(state || {
        datasetName,
        config,
        runId: '',
        startedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      }),
      status: 'error',
      progress: { step: 'error', percent: 0, message: error.message || 'Analysis failed' },
      error: error.message,
    } as LaboratoryState;
    await saveLabState(errorState);
    return errorState;
  }
}

// ── Phase 1: Starting ────────────────────────────────────────────────────

async function phaseStarting(config: LabConfig): Promise<LaboratoryState> {
  const datasetName = config.datasetId;
  const runId = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
  const originsTableName = `temp_lab_origins_${runId}`;

  console.log(`[LAB-MP] ${datasetName}: Phase 1 — Starting (runId=${runId})`);

  // Ensure dataset table exists
  try {
    await ensureTableForDataset(datasetName);
  } catch (e: any) {
    if (!e.message?.includes('already exists')) throw e;
  }

  // Ensure POI table exists
  try {
    await runQuery(`
      CREATE EXTERNAL TABLE IF NOT EXISTS lab_pois_gmc (
        id STRING, name STRING, category STRING, city STRING,
        postal_code STRING, country STRING, latitude DOUBLE, longitude DOUBLE
      )
      STORED AS PARQUET
      LOCATION 's3://${BUCKET}/pois_gmc/'
    `);
  } catch (e: any) {
    if (!e.message?.includes('already exists')) {
      console.warn(`[LAB-MP] Warning creating POI table:`, e.message);
    }
  }

  // Collect all categories from recipe
  const allCategories = new Set<PoiCategory>();
  for (const step of config.recipe.steps) {
    for (const cat of step.categories) allCategories.add(cat);
  }
  const categoryList = Array.from(allCategories);
  const spatialRadius = config.spatialJoinRadiusMeters || 200;
  const country = config.country;
  const isoCountry = country ? toIsoCountry(country) : '';
  const catSql = categoryList.map(c => `'${c}'`).join(',');

  // Count POIs + get latitude range to decide if batching is needed
  const poiCountSql = `
    SELECT COUNT(*) as cnt, MIN(latitude) as min_lat, MAX(latitude) as max_lat
    FROM lab_pois_gmc
    WHERE category IS NOT NULL
      AND category IN (${catSql})
      ${isoCountry ? `AND country = '${isoCountry}'` : ''}
  `;
  const poiCountRes = await runQuery(poiCountSql);
  const poiCount = parseInt(String(poiCountRes.rows[0]?.cnt)) || 0;
  const minLat = parseFloat(String(poiCountRes.rows[0]?.min_lat)) || 0;
  const maxLat = parseFloat(String(poiCountRes.rows[0]?.max_lat)) || 0;

  // Determine batch count based on POI count
  const batchCount = poiCount > POI_BATCH_THRESHOLD
    ? Math.min(Math.ceil(poiCount / POI_BATCH_THRESHOLD), MAX_POI_BATCHES)
    : 1;

  console.log(`[LAB-MP] ${datasetName}: ${poiCount} POIs (lat ${minLat.toFixed(1)}–${maxLat.toFixed(1)}), batchCount=${batchCount}`);

  // Build geographic batch filters (contiguous latitude bands)
  const batchFilters: string[] = [];
  if (batchCount > 1) {
    const bandHeight = (maxLat - minLat) / batchCount;
    for (let i = 0; i < batchCount; i++) {
      const bandMin = minLat + i * bandHeight;
      // Last band uses open-ended upper bound to catch any rounding edge cases
      if (i === batchCount - 1) {
        batchFilters.push(`AND p.latitude >= ${bandMin}`);
      } else {
        const bandMax = minLat + (i + 1) * bandHeight;
        batchFilters.push(`AND p.latitude >= ${bandMin} AND p.latitude < ${bandMax}`);
      }
    }
  } else {
    batchFilters.push(''); // single batch, no extra filter
  }

  // Create table names and drop any leftovers
  const spatialTableNames = batchFilters.map((_, i) =>
    batchCount > 1 ? `temp_lab_spatial_${runId}_b${i}` : `temp_lab_spatial_${runId}`
  );
  await Promise.all([
    ...spatialTableNames.map(t => dropTempTable(t).catch(() => {})),
    dropTempTable(originsTableName).catch(() => {}),
  ]);

  // Fire spatial CTAS per batch + one total devices query
  const { totalDevicesQuery } = buildSpatialJoinQueries(
    datasetName, categoryList, config.country,
    config.dateFrom, config.dateTo, spatialRadius,
  );

  const spatialCTASIds: string[] = [];
  for (let i = 0; i < batchCount; i++) {
    const { spatialSelectForCTAS } = buildSpatialJoinQueries(
      datasetName, categoryList, config.country,
      config.dateFrom, config.dateTo, spatialRadius,
      batchFilters[i],
    );
    const limitedSpatial = `${spatialSelectForCTAS} LIMIT ${Math.ceil(500000 / batchCount)}`;
    const ctasId = await startCTASAsync(limitedSpatial, spatialTableNames[i]);
    spatialCTASIds.push(ctasId);
  }
  const totalDevicesQueryId = await startQueryAsync(totalDevicesQuery);

  console.log(`[LAB-MP] ${datasetName}: Fired ${batchCount} spatial CTAS + total devices query`);

  return {
    status: 'spatial_running',
    datasetName,
    config,
    runId,
    spatialCTASIds,
    totalDevicesQueryId,
    spatialTableNames,
    originsTableName,
    poiBatchCount: batchCount,
    progress: { step: 'spatial_join', percent: 10, message: `Running spatial join${batchCount > 1 ? ` (${batchCount} batches)` : ''}...` },
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase 2: Spatial Running ─────────────────────────────────────────────

async function phaseSpatialRunning(state: LaboratoryState): Promise<LaboratoryState> {
  const { datasetName, spatialCTASIds, totalDevicesQueryId, spatialTableNames } = state;
  const batchCount = state.poiBatchCount || 1;

  // Poll all spatial batch queries + total devices query
  const spatialStatuses = await Promise.all(
    (spatialCTASIds || []).map(id => checkQueryStatus(id)),
  );
  const totalStatus = await checkQueryStatus(totalDevicesQueryId!);

  const spatialStates = spatialStatuses.map(s => s.state);
  console.log(`[LAB-MP] ${datasetName}: spatial=[${spatialStates.join(',')}], total=${totalStatus.state}`);

  // Check for failures — clean up partial S3 data from failed CTAS batches
  for (let i = 0; i < spatialStatuses.length; i++) {
    if (spatialStatuses[i].state === 'FAILED') {
      // Clean up all batch tables
      for (const tbl of (spatialTableNames || [])) {
        dropTempTable(tbl).catch(() => {});
        cleanupTempS3(tbl).catch(() => {});
      }
      throw new Error(`Spatial join query failed: ${spatialStatuses[i].error}`);
    }
  }
  if (totalStatus.state === 'FAILED') {
    throw new Error(`Total devices query failed: ${totalStatus.error}`);
  }

  // Extract total devices when ready
  if (totalStatus.state === 'SUCCEEDED' && state.totalDevices === undefined) {
    const totalRes = await fetchQueryResults(totalDevicesQueryId!);
    state.totalDevices = parseInt(String(totalRes.rows[0]?.total)) || 0;
    console.log(`[LAB-MP] ${datasetName}: totalDevices = ${state.totalDevices}`);
  }

  // All spatial batches + total devices done?
  const allSpatialDone = spatialStatuses.every(s => s.state === 'SUCCEEDED');
  if (allSpatialDone && totalStatus.state === 'SUCCEEDED') {
    // Count visits across all batch tables
    const countParts = (spatialTableNames || []).map(t => `SELECT COUNT(*) as cnt FROM ${t}`);
    const countSql = batchCount > 1
      ? `SELECT SUM(cnt) as cnt FROM (${countParts.join(' UNION ALL ')})`
      : `SELECT COUNT(*) as cnt FROM ${spatialTableNames![0]}`;
    const countRes = await runQuery(countSql);
    const visitCount = parseInt(String(countRes.rows[0]?.cnt)) || 0;
    state.visitCount = visitCount;
    console.log(`[LAB-MP] ${datasetName}: ${visitCount} spatial visits (${batchCount} batch${batchCount > 1 ? 'es' : ''})`);

    if (visitCount === 0) {
      console.log(`[LAB-MP] ${datasetName}: No visits found, completing with empty result`);
      state.status = 'completed';
      state.result = buildEmptyResult(state.config, state.totalDevices);
      state.progress = { step: 'completed', percent: 100, message: 'No visits found matching criteria' };
      for (const tbl of (spatialTableNames || [])) dropTempTable(tbl).catch(() => {});
      return state;
    }

    // Build ad_id source from all batch tables for origins query
    const adIdSource = batchCount > 1
      ? `(${(spatialTableNames || []).map(t => `SELECT DISTINCT ad_id FROM ${t}`).join(' UNION ')})`
      : spatialTableNames![0];

    // Fire origins CTAS
    const originsSelect = buildOriginsCTASQuery(
      state.config.datasetId,
      adIdSource,
      state.config.dateFrom,
      state.config.dateTo,
    );
    state.originsCTASId = await startCTASAsync(originsSelect, state.originsTableName!);
    state.status = 'origins_running';
    state.progress = { step: 'geocoding', percent: 50, message: 'Resolving device origins...' };
    console.log(`[LAB-MP] ${datasetName}: Origins CTAS started (${state.originsCTASId})`);
  } else {
    // Still running — update progress
    const completedBatches = spatialStatuses.filter(s => s.state === 'SUCCEEDED').length;
    const pct = 10 + Math.round((completedBatches / batchCount) * 30);
    state.progress = {
      step: 'spatial_join',
      percent: pct,
      message: batchCount > 1
        ? `Spatial join: ${completedBatches}/${batchCount} batches complete...`
        : 'Spatial join running...',
    };
  }

  return state;
}

// ── Phase 3: Origins Running ─────────────────────────────────────────────

async function phaseOriginsRunning(state: LaboratoryState): Promise<LaboratoryState> {
  const { datasetName, originsCTASId } = state;

  const originsStatus = await checkQueryStatus(originsCTASId!);
  console.log(`[LAB-MP] ${datasetName}: origins=${originsStatus.state}`);

  if (originsStatus.state === 'FAILED') {
    if (state.originsTableName) {
      dropTempTable(state.originsTableName).catch(() => {});
      cleanupTempS3(state.originsTableName).catch(() => {});
    }
    throw new Error(`Origins query failed: ${originsStatus.error}`);
  }

  if (originsStatus.state === 'SUCCEEDED') {
    state.status = 'processing';
    state.progress = { step: 'geocoding', percent: 65, message: 'Processing results...' };
    console.log(`[LAB-MP] ${datasetName}: Origins done, advancing to processing`);
  } else {
    state.progress = { step: 'geocoding', percent: 55, message: 'Resolving device origins...' };
  }

  return state;
}

// ── Phase 4: Processing ──────────────────────────────────────────────────

async function phaseProcessing(state: LaboratoryState): Promise<LaboratoryState> {
  const { datasetName, spatialTableNames, originsTableName, config } = state;
  const batchCount = state.poiBatchCount || 1;

  console.log(`[LAB-MP] ${datasetName}: Phase 4 — Processing (${batchCount} batch${batchCount > 1 ? 'es' : ''})`);
  state.progress = { step: 'geocoding', percent: 70, message: 'Downloading results...' };
  await saveLabState(state);

  // Build spatial source: UNION ALL for multi-batch, direct table for single
  const spatialSource = batchCount > 1
    ? `(${(spatialTableNames || []).map(t => `SELECT * FROM ${t}`).join(' UNION ALL ')})`
    : spatialTableNames![0];

  // Read spatial visits + origins in a single joined query
  const joinedSql = `
    SELECT
      v.ad_id, v.date, v.poi_id, v.category, v.dwell_minutes, v.visit_hour,
      COALESCE(o.origin_lat, 0) as origin_lat,
      COALESCE(o.origin_lng, 0) as origin_lng
    FROM ${spatialSource} v
    LEFT JOIN ${originsTableName} o ON v.ad_id = o.ad_id AND v.date = o.date
  `;

  const joinedResult = await runQueryViaS3(joinedSql);
  console.log(`[LAB-MP] ${datasetName}: ${joinedResult.rows.length} joined rows downloaded`);

  state.progress = { step: 'geocoding', percent: 78, message: 'Parsing visits...' };
  await saveLabState(state);

  // Parse visits (origins already assigned via SQL JOIN)
  const visits: ParsedVisit[] = joinedResult.rows.map(row => ({
    adId: String(row.ad_id),
    date: String(row.date),
    poiId: String(row.poi_id),
    category: String(row.category) as PoiCategory,
    dwellMinutes: parseFloat(String(row.dwell_minutes)) || 0,
    visitHour: parseInt(String(row.visit_hour)) || 0,
    originLat: parseFloat(String(row.origin_lat)) || 0,
    originLng: parseFloat(String(row.origin_lng)) || 0,
  }));

  // Build originMap from parsed visits
  const originMap = new Map<string, { lat: number; lng: number }>();
  for (const v of visits) {
    if (v.originLat !== 0 || v.originLng !== 0) {
      originMap.set(`${v.adId}|${v.date}`, { lat: v.originLat, lng: v.originLng });
    }
  }

  // Geocode unique origin coordinates
  state.progress = { step: 'geocoding', percent: 82, message: 'Geocoding device origins...' };
  await saveLabState(state);

  const isoCountry = config.country ? toIsoCountry(config.country) : undefined;
  const coordToZip = await geocodeOrigins(visits, isoCountry);
  console.log(`[LAB-MP] ${datasetName}: ${coordToZip.size} coordinates geocoded`);

  // Process visits with recipe
  state.progress = { step: 'computing_affinity', percent: 88, message: 'Computing affinity indices...' };
  await saveLabState(state);

  const result = processVisitsForRecipe({
    config,
    allVisits: visits,
    totalDevicesInDataset: state.totalDevices || 0,
    originMap,
    coordToZip,
  });

  console.log(`[LAB-MP] ${datasetName}: Analysis complete — ${result.segment.totalDevices} devices in segment, ${result.profiles.length} postal codes`);

  // Clean up temp tables (fire and forget)
  for (const tbl of (spatialTableNames || [])) dropTempTable(tbl).catch(() => {});
  dropTempTable(originsTableName!).catch(() => {});

  state.status = 'completed';
  state.result = result;
  state.progress = { step: 'completed', percent: 100, message: 'Analysis complete' };
  return state;
}
