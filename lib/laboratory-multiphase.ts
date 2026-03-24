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
  // Query tracking
  spatialCTASId?: string;
  totalDevicesQueryId?: string;
  originsCTASId?: string;
  // Temp table names
  spatialTableName?: string;
  originsTableName?: string;
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
    if (state?.spatialTableName) {
      dropTempTable(state.spatialTableName).catch(() => {});
      cleanupTempS3(state.spatialTableName).catch(() => {});
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
  const spatialTableName = `temp_lab_spatial_${runId}`;
  const originsTableName = `temp_lab_origins_${runId}`;

  console.log(`[LAB-MP] ${datasetName}: Phase 1 — Starting (runId=${runId})`);

  // Drop temp tables if they exist from a previous failed run
  await Promise.all([
    dropTempTable(spatialTableName).catch(() => {}),
    dropTempTable(originsTableName).catch(() => {}),
  ]);

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

  // Build queries
  const { spatialSelectForCTAS, totalDevicesQuery } = buildSpatialJoinQueries(
    datasetName, categoryList, config.country,
    config.dateFrom, config.dateTo, spatialRadius,
  );

  // Fire spatial CTAS with LIMIT 500000 (matches current behavior)
  const limitedSpatial = `${spatialSelectForCTAS} LIMIT 500000`;
  const [spatialCTASId, totalDevicesQueryId] = await Promise.all([
    startCTASAsync(limitedSpatial, spatialTableName),
    startQueryAsync(totalDevicesQuery),
  ]);

  console.log(`[LAB-MP] ${datasetName}: Spatial CTAS started (${spatialCTASId}), Total devices query (${totalDevicesQueryId})`);

  return {
    status: 'spatial_running',
    datasetName,
    config,
    runId,
    spatialCTASId,
    totalDevicesQueryId,
    spatialTableName,
    originsTableName,
    progress: { step: 'spatial_join', percent: 10, message: 'Running spatial join...' },
    startedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}

// ── Phase 2: Spatial Running ─────────────────────────────────────────────

async function phaseSpatialRunning(state: LaboratoryState): Promise<LaboratoryState> {
  const { datasetName, spatialCTASId, totalDevicesQueryId } = state;

  // Poll both queries
  const [spatialStatus, totalStatus] = await Promise.all([
    checkQueryStatus(spatialCTASId!),
    checkQueryStatus(totalDevicesQueryId!),
  ]);

  console.log(`[LAB-MP] ${datasetName}: spatial=${spatialStatus.state}, total=${totalStatus.state}`);

  // Check for failures — clean up partial S3 data from failed CTAS
  if (spatialStatus.state === 'FAILED') {
    if (state.spatialTableName) {
      dropTempTable(state.spatialTableName).catch(() => {});
      cleanupTempS3(state.spatialTableName).catch(() => {});
    }
    throw new Error(`Spatial join query failed: ${spatialStatus.error}`);
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

  // Both done? Check visit count and fire origins
  if (spatialStatus.state === 'SUCCEEDED' && totalStatus.state === 'SUCCEEDED') {
    // Quick COUNT to check if spatial join returned results
    const countRes = await runQuery(`SELECT COUNT(*) as cnt FROM ${state.spatialTableName}`);
    const visitCount = parseInt(String(countRes.rows[0]?.cnt)) || 0;
    state.visitCount = visitCount;
    console.log(`[LAB-MP] ${datasetName}: ${visitCount} spatial visits`);

    if (visitCount === 0) {
      // No visits — return empty result
      console.log(`[LAB-MP] ${datasetName}: No visits found, completing with empty result`);
      state.status = 'completed';
      state.result = buildEmptyResult(state.config, state.totalDevices);
      state.progress = { step: 'completed', percent: 100, message: 'No visits found matching criteria' };
      // Clean up
      dropTempTable(state.spatialTableName!).catch(() => {});
      return state;
    }

    // Fire origins CTAS
    const originsSelect = buildOriginsCTASQuery(
      state.config.datasetId,
      state.spatialTableName!,
      state.config.dateFrom,
      state.config.dateTo,
    );
    state.originsCTASId = await startCTASAsync(originsSelect, state.originsTableName!);
    state.status = 'origins_running';
    state.progress = { step: 'geocoding', percent: 50, message: 'Resolving device origins...' };
    console.log(`[LAB-MP] ${datasetName}: Origins CTAS started (${state.originsCTASId})`);
  } else {
    // Still running — update progress
    const spatialRunning = spatialStatus.state === 'RUNNING' || spatialStatus.state === 'QUEUED';
    state.progress = {
      step: 'spatial_join',
      percent: spatialRunning ? 20 : 40,
      message: spatialRunning ? 'Spatial join running...' : 'Waiting for queries...',
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
  const { datasetName, spatialTableName, originsTableName, config } = state;

  console.log(`[LAB-MP] ${datasetName}: Phase 4 — Processing`);
  state.progress = { step: 'geocoding', percent: 70, message: 'Downloading results...' };
  await saveLabState(state);

  // Read spatial visits + origins in a single joined query
  const joinedSql = `
    SELECT
      v.ad_id, v.date, v.poi_id, v.category, v.dwell_minutes, v.visit_hour,
      COALESCE(o.origin_lat, 0) as origin_lat,
      COALESCE(o.origin_lng, 0) as origin_lng
    FROM ${spatialTableName} v
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
  dropTempTable(spatialTableName!).catch(() => {});
  dropTempTable(originsTableName!).catch(() => {});

  state.status = 'completed';
  state.result = result;
  state.progress = { step: 'completed', percent: 100, message: 'Analysis complete' };
  return state;
}
