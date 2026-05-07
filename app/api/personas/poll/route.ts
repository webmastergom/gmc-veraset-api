/**
 * POST /api/personas/poll
 *
 * Multi-phase polling endpoint that runs the personas pipeline:
 *   1. starting → resolve POIs + brand mapping per megajob, materialize a
 *      (poi_id, brand) external Athena table, kick off feature-vector CTAS.
 *   2. feature_polling → wait for CTAS to SUCCEED.
 *   3. download → stream feature rows from S3 into memory.
 *   4. clustering → two-stage k-means: top-50k for centroides, full pop for
 *      nearest-centroid assignment.
 *   5. aggregation → per-cluster aggregates + RFM + insights.
 *   6. master_maids_export → write labels CSV → external table → one CTAS
 *      per cluster (parallel).
 *   7. export_polling → wait for CTAS, register as Master MAIDs persona contributions.
 *   8. done → return cached PersonaReport.
 *
 * State key: `persona-state/{runId}`. Run identification = hash of
 * (sorted megaJobIds + filters). Idempotent: re-runs hit cache.
 */

import { NextRequest, NextResponse } from 'next/server';
import { createHash } from 'crypto';
import {
  startQueryAsync,
  startCTASAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
  runQueryViaS3,
  runQuery,
  tempTableName,
  dropTempTable,
} from '@/lib/athena';
import {
  getConfig,
  putConfig,
  invalidateCache,
  s3Client,
  BUCKET,
} from '@/lib/s3-config';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import { getPOICollection } from '@/lib/poi-storage';
import { resolveBrand } from '@/lib/persona-brand-lookup';
import { discoverBrands, type PoiNameLookup } from '@/lib/persona-brand-discovery';
import { buildFeatureCTAS, extractPoiCoords } from '@/lib/persona-feature-query';
import { runClusteringPipeline } from '@/lib/persona-clusterer';
import { computeRfm } from '@/lib/persona-rfm';
import { computeCohabitation } from '@/lib/persona-cohabitation';
import { computeZipAffinityPerSource } from '@/lib/persona-zip-affinity';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { generateInsights } from '@/lib/persona-insights';
import { registerAthenaContribution } from '@/lib/master-maids';
import {
  type PersonaState,
  type PersonaRunConfig,
  type PersonaReport,
  type DeviceFeatures,
  type PersonaPhase,
} from '@/lib/persona-types';
import { PutObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

const STATE_KEY = (runId: string) => `persona-state/${runId}`;
const REPORT_KEY = (runId: string) => `persona-reports/${runId}`;

// Hash a config to a stable runId.
function configToRunId(cfg: PersonaRunConfig): string {
  const norm = {
    megaJobIds: [...cfg.megaJobIds].sort(),
    filters: cfg.filters || {},
  };
  return createHash('sha1').update(JSON.stringify(norm)).digest('hex').slice(0, 16);
}

/**
 * Force a fresh runId for the same config. Used when the user explicitly
 * asks to re-run (e.g. data was synced after the last run, or to compare
 * results before/after a tweak). Past runs are preserved on S3 — the new
 * runId is just `<configHash>-<timestamp>` so it sorts next to its
 * siblings in the runs list.
 */
function rerunRunId(cfg: PersonaRunConfig): string {
  const baseHash = configToRunId(cfg);
  const stamp = Date.now().toString(36).slice(-6);
  return `${baseHash}-${stamp}`;
}

// ─── Phase: starting ──────────────────────────────────────────────────

/**
 * One "source" in the pipeline = one feature CTAS. A megajob is a
 * collection of sub-jobs unioned together; a single (standalone) job is
 * treated as a synthetic megajob of one. Everything downstream uses
 * `sourceId` as the key (in featureCtas, in the per-row `source_megajob_id`,
 * in cross-dataset analysis).
 */
interface ResolvedSource {
  sourceId: string;
  syncedJobs: any[]; // Job[]
  collectionIds: string[];
  rangeTo: string;
  label: string;
}

async function resolveMegaJobSource(megaJobId: string): Promise<ResolvedSource> {
  const megaJob = await getMegaJob(megaJobId);
  if (!megaJob) throw new Error(`Mega-job ${megaJobId} not found`);
  if (!megaJob.subJobIds?.length) throw new Error(`Mega-job ${megaJobId} has no sub-jobs`);
  const subJobs = (
    await Promise.all(megaJob.subJobIds.map((j) => getJob(j)))
  ).filter((j): j is NonNullable<typeof j> => j !== null);
  const syncedJobs = subJobs.filter((j) => j.status === 'SUCCESS' && j.syncedAt);
  if (syncedJobs.length === 0) {
    throw new Error(`Mega-job "${megaJob.name}" has no synced sub-jobs yet`);
  }
  const collectionIds = megaJob.sourceScope?.poiCollectionIds || [];
  const rangeTo =
    megaJob.sourceScope?.dateRange?.to ||
    syncedJobs.reduce((max, j) => (j.dateRange?.to && j.dateRange.to > max ? j.dateRange.to : max), '');
  if (!rangeTo) throw new Error(`Cannot determine date range for mega-job ${megaJobId}`);
  return { sourceId: megaJobId, syncedJobs, collectionIds, rangeTo, label: megaJob.name };
}

async function resolveJobSource(jobId: string): Promise<ResolvedSource> {
  const job = await getJob(jobId);
  if (!job) throw new Error(`Job ${jobId} not found`);
  if (job.status !== 'SUCCESS' || !job.syncedAt) {
    throw new Error(`Job "${job.name}" not synced yet (status=${job.status})`);
  }
  // Single-job sources are treated as a 1-element syncedJobs array.
  const syncedJobs = [job];
  const colIds: string[] = [];
  // Job may have legacy single poiCollectionId or none (external POIs).
  if ((job as any).poiCollectionId) colIds.push((job as any).poiCollectionId);
  const rangeTo = job.dateRange?.to || '';
  if (!rangeTo) throw new Error(`Job ${jobId} has no dateRange.to`);
  return { sourceId: jobId, syncedJobs, collectionIds: colIds, rangeTo, label: job.name || jobId };
}

async function fireFeatureCtasForSource(args: {
  source: ResolvedSource;
  runId: string;
  filters: any;
}): Promise<{ queryId: string; tableName: string }> {
  const { source, runId, filters } = args;

  // Ensure all dataset tables exist.
  await Promise.all(
    source.syncedJobs.map(async (j) => {
      const ds = j.s3DestPath?.replace(/\/$/, '').split('/').pop();
      if (ds) await ensureTableForDataset(ds);
    })
  );

  // Extract POI coords (for spatial join).
  const poiCoords = extractPoiCoords(source.syncedJobs);
  if (poiCoords.length === 0) {
    throw new Error(`No POI coords for source ${source.label}`);
  }

  // Resolve brand for each POI using a 3-layer strategy:
  //   1. GeoJSON properties (brand / chain / cadena / marca / concesionaria / …)
  //      — explicit user override. Match is case-insensitive.
  //   2. Auto-discovery from POI name frequency (handles arbitrary
  //      datasets like Auto Dealerships, retail competitors, …).
  //   3. BRAND_RULES hardcoded fast-food / common chains — final fallback
  //      for POIs the discovery missed.
  // Common GeoJSON property names that carry a brand/chain. Case-insensitive
  // match — typed by analysts in EN/ES so "Cadena", "MARCA", "Brand" all work.
  const BRAND_PROP_KEYS = [
    'brand', 'chain',
    'cadena', 'marca', 'franquicia',
    'concesionaria', 'concesionario',
    'operador', 'operator',
    'enseigne', // FR — used in some Carrefour / fashion catalogues
  ];
  const pickBrandProp = (props: Record<string, any> | undefined): string => {
    if (!props) return '';
    // Build a case-insensitive lookup once per feature.
    const lower: Record<string, string> = {};
    for (const k of Object.keys(props)) {
      const v = props[k];
      if (typeof v === 'string' && v.trim()) lower[k.toLowerCase()] = v.trim();
    }
    for (const key of BRAND_PROP_KEYS) {
      if (lower[key]) return lower[key];
    }
    return '';
  };
  const poiInputs: PoiNameLookup[] = [];
  for (const colId of source.collectionIds) {
    const geo = await getPOICollection(colId);
    if (!geo?.features) continue;
    for (const f of geo.features as any[]) {
      const id = f.properties?.id || f.id;
      const name = f.properties?.name || f.properties?.label || '';
      const override = pickBrandProp(f.properties);
      if (!id) continue;
      poiInputs.push({ poiId: String(id), name, brandOverride: override || undefined });
    }
  }
  // If we still have no named POIs, synth from verasetPayload.geo_radius.
  if (poiInputs.length === 0) {
    console.warn(`[PERSONAS ${runId}] No named POIs for ${source.label}; falling back to verasetPayload.geo_radius.`);
    for (const job of source.syncedJobs) {
      for (const g of job.verasetPayload?.geo_radius || []) {
        if (g.poi_id) poiInputs.push({ poiId: String(g.poi_id), name: '' });
      }
    }
  }
  if (poiInputs.length === 0) {
    throw new Error(`No POIs found for source ${source.label}`);
  }

  // Layer 2: auto-discovery (handles overrides via brandOverride field).
  const discovery = discoverBrands(poiInputs);
  console.log(
    `[PERSONAS ${runId}] Brand discovery for ${source.label}: ` +
    `${discovery.source.override} explicit + ${discovery.source.discovered} discovered + ${discovery.source.other} other. ` +
    `Top candidates: ${discovery.candidates.slice(0, 8).map((c) => `${c.brand}(${c.count})`).join(', ')}`
  );

  // Layer 3: for POIs left as 'other', try the hardcoded BRAND_RULES
  // (fast-food chains etc. that auto-discovery may not pick up if the
  // dataset is a mix of brands with low individual counts).
  const poiToBrand: Array<{ poiId: string; brand: string }> = [];
  let layer3Hits = 0;
  for (const p of poiInputs) {
    let brand = discovery.poiToBrand.get(p.poiId) || 'other';
    if (brand === 'other' && p.name) {
      const ruleBrand = resolveBrand(p.name);
      if (ruleBrand !== 'other') {
        brand = ruleBrand;
        layer3Hits++;
      }
    }
    poiToBrand.push({ poiId: p.poiId, brand });
  }
  if (layer3Hits > 0) {
    console.log(`[PERSONAS ${runId}] BRAND_RULES recovered ${layer3Hits} additional POIs from 'other'.`);
  }

  // Brand mapping is inlined as a VALUES CTE inside the feature CTAS
  // (see lib/persona-feature-query.ts). No external CSV-backed Athena
  // table is created — that path was fragile across Athena engine
  // versions (Hive `CREATE EXTERNAL TABLE` is rejected by Trino-strict
  // workgroups; Trino's `CREATE TABLE WITH (external_location=...)`
  // is rejected by some Athena DDL parsers as `WITH (` after a column
  // list with no `AS`). Inlining sidesteps both grammars.
  console.log(`[PERSONAS ${runId}] Brand mapping (${poiToBrand.length} POIs) will be inlined for source ${source.label}`);

  // Feature CTAS uses a per-attempt timestamp suffix so two concurrent
  // phaseStarting calls (frontend retries during a long Vercel cycle)
  // don't collide on table creation. The state machine persists the
  // chosen tableName so downstream phases reuse it.
  const safeId = source.sourceId.replace(/-/g, '_').slice(0, 24);
  const attemptTs = Math.floor(Date.now() / 1000).toString(36); // ~7 chars
  const featureTableBase = `persona_features_${safeId}_${runId.slice(0, 8)}_${attemptTs}`;
  const featureTable = featureTableBase.length > 60 ? featureTableBase.slice(0, 60) : featureTableBase;
  try { await runQuery(`DROP TABLE IF EXISTS ${featureTable}`); } catch {}
  const featureS3 = `s3://${BUCKET}/athena-temp/${featureTable}/`;

  const sql = buildFeatureCTAS({
    ctasTable: featureTable,
    ctasS3Path: featureS3,
    syncedJobs: source.syncedJobs,
    poiCoords,
    poiToBrand,
    dateRangeTo: source.rangeTo,
    sourceMegajobId: source.sourceId,
    filters,
  });

  const queryId = await startQueryAsync(sql);
  console.log(`[PERSONAS ${runId}] Feature CTAS started for ${source.label}: queryId=${queryId}`);
  return { queryId, tableName: featureTable };
}

async function phaseStarting(state: PersonaState): Promise<PersonaState> {
  const { config, runId } = state;
  console.log(`[PERSONAS ${runId}] starting: megaJobs=${config.megaJobIds.join(',')} jobs=${(config.jobIds || []).join(',')}`);

  const featureCtas: Record<string, { queryId: string; tableName: string }> = {};

  for (const megaJobId of config.megaJobIds) {
    const source = await resolveMegaJobSource(megaJobId);
    featureCtas[source.sourceId] = await fireFeatureCtasForSource({ source, runId, filters: config.filters as any });
  }
  for (const jobId of config.jobIds || []) {
    const source = await resolveJobSource(jobId);
    featureCtas[source.sourceId] = await fireFeatureCtasForSource({ source, runId, filters: config.filters as any });
  }

  if (Object.keys(featureCtas).length === 0) {
    throw new Error('No sources resolved (megaJobIds + jobIds both empty?)');
  }

  return {
    ...state,
    phase: 'feature_polling',
    featureCtas,
    updatedAt: new Date().toISOString(),
  };
}

// ─── Phase: feature_polling ──────────────────────────────────────────

async function phaseFeaturePolling(state: PersonaState): Promise<PersonaState> {
  if (!state.featureCtas) throw new Error('No featureCtas in state');
  const queryIds = Object.values(state.featureCtas).map((f) => f.queryId);
  const statuses = await Promise.all(queryIds.map((q) => checkQueryStatus(q)));
  const anyFailed = statuses.find((s) => s.state === 'FAILED' || s.state === 'CANCELLED');
  if (anyFailed) throw new Error(`Feature CTAS failed: ${anyFailed.error || 'unknown'}`);
  const allDone = statuses.every((s) => s.state === 'SUCCEEDED');

  // Surface granular progress: GB scanned + runtime per query.
  const totalScannedGB = statuses.reduce((s, x) => s + ((x.statistics?.dataScannedBytes || 0) / 1e9), 0);
  const doneCount = statuses.filter((s) => s.state === 'SUCCEEDED').length;
  const perSource: Record<string, string> = {};
  Object.entries(state.featureCtas).forEach(([src, { queryId }], i) => {
    const st = statuses[i];
    perSource[src] = `${st.state} · ${((st.statistics?.dataScannedBytes || 0) / 1e9).toFixed(1)} GB scanned`;
  });
  const subProgress = {
    label: allDone ? 'All feature CTAS finished' : `Athena CTAS: ${doneCount}/${statuses.length} sources done`,
    ratio: doneCount / Math.max(1, statuses.length),
    details: `${totalScannedGB.toFixed(1)} GB scanned across ${statuses.length} source${statuses.length > 1 ? 's' : ''}`,
    perSource,
  };

  if (!allDone) {
    return { ...state, subProgress, updatedAt: new Date().toISOString() };
  }
  return { ...state, phase: 'download_query', subProgress, updatedAt: new Date().toISOString() };
}

// ─── Phase: download_query ───────────────────────────────────────────

async function phaseDownloadQuery(state: PersonaState): Promise<PersonaState> {
  if (!state.featureCtas) throw new Error('No featureCtas in state');
  const downloadQueries: Record<string, { queryId: string; csvKey: string }> = {};
  for (const [src, { tableName }] of Object.entries(state.featureCtas)) {
    // Project only the columns we need for clustering + aggregation.
    // Filter out devices with <2 visits — those are noise (one-off
    // visitors with unstable features) and they bloat the CSV by 2-5×.
    // The cluster centroids derive from real repeat visitors anyway.
    const sql = `
      SELECT ad_id, total_visits, total_dwell_min, recency_days, avg_dwell_min,
        morning_share, midday_share, evening_share, night_share, weekend_share,
        friday_evening_share, gyration_km, unique_h3_cells,
        home_zip, home_region, home_lat, home_lng,
        brand_visits_json, brand_loyalty_hhi, tier_high_quality
      FROM ${tableName}
      WHERE total_visits >= 2
    `;
    const queryId = await startQueryAsync(sql);
    downloadQueries[src] = { queryId, csvKey: `athena-results/${queryId}.csv` };
    console.log(`[PERSONAS ${state.runId}] Download SELECT started for ${src}: queryId=${queryId}`);
  }
  return {
    ...state,
    phase: 'download_polling',
    downloadQueries,
    subProgress: {
      label: 'Streaming feature vectors',
      details: `${Object.keys(downloadQueries).length} parallel SELECT queries launched (devices with 2+ visits)`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ─── Phase: download_polling ─────────────────────────────────────────

async function phaseDownloadPolling(state: PersonaState): Promise<PersonaState> {
  if (!state.downloadQueries) throw new Error('No downloadQueries');
  const queryIds = Object.values(state.downloadQueries).map((d) => d.queryId);
  const statuses = await Promise.all(queryIds.map((q) => checkQueryStatus(q)));
  const anyFailed = statuses.find((s) => s.state === 'FAILED' || s.state === 'CANCELLED');
  if (anyFailed) throw new Error(`Download SELECT failed: ${anyFailed.error || 'unknown'}`);
  const allDone = statuses.every((s) => s.state === 'SUCCEEDED');

  const totalScannedGB = statuses.reduce((s, x) => s + ((x.statistics?.dataScannedBytes || 0) / 1e9), 0);
  const doneCount = statuses.filter((s) => s.state === 'SUCCEEDED').length;
  const perSource: Record<string, string> = {};
  Object.entries(state.downloadQueries).forEach(([src, { queryId }], i) => {
    const st = statuses[i];
    perSource[src] = `${st.state} · ${((st.statistics?.dataScannedBytes || 0) / 1e9).toFixed(2)} GB`;
  });
  const subProgress = {
    label: allDone ? 'Feature vectors ready' : `Athena: ${doneCount}/${statuses.length} downloads done`,
    ratio: doneCount / Math.max(1, statuses.length),
    details: `${totalScannedGB.toFixed(2)} GB processed`,
    perSource,
  };

  if (!allDone) {
    return { ...state, subProgress, updatedAt: new Date().toISOString() };
  }
  return { ...state, phase: 'download_read', subProgress, updatedAt: new Date().toISOString() };
}

// ─── Phase: download_read ────────────────────────────────────────────

function parseBrandJson(s: string | null | undefined): Record<string, number> {
  if (!s) return {};
  try {
    // Athena CAST(MAP AS JSON) returns a JSON object string.
    const obj = JSON.parse(s);
    if (obj && typeof obj === 'object') {
      const out: Record<string, number> = {};
      for (const [k, v] of Object.entries(obj)) {
        const n = Number(v);
        if (Number.isFinite(n)) out[k] = n;
      }
      return out;
    }
  } catch {}
  return {};
}

/**
 * Parse a single CSV line respecting double-quote escaping (Athena uses
 * RFC 4180 style: '"value with, comma"', '""' = literal '"' inside quoted).
 */
function parseCsvLine(line: string): string[] {
  const cells: string[] = [];
  let cur = '';
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQuote) {
      if (c === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }
        else { inQuote = false; }
      } else {
        cur += c;
      }
    } else {
      if (c === ',') { cells.push(cur); cur = ''; }
      else if (c === '"') { inQuote = true; }
      else { cur += c; }
    }
  }
  cells.push(cur);
  return cells;
}

/**
 * Stream the Athena result CSV from S3 (athena-results/{queryId}.csv).
 * Avoids loading the full 200-300 MB string into memory by parsing
 * line-by-line off the S3 body stream.
 */
async function streamAthenaCsv<T>(
  queryId: string,
  parseRow: (record: Record<string, string>) => T,
): Promise<{ rows: T[]; lineCount: number }> {
  const { GetObjectCommand } = await import('@aws-sdk/client-s3');
  const obj = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: `athena-results/${queryId}.csv`,
  }));
  if (!obj.Body) throw new Error(`Empty body for athena-results/${queryId}.csv`);
  const body = obj.Body as any; // Node.js Readable stream
  let buffer = '';
  let header: string[] | null = null;
  const out: T[] = [];
  let lineCount = 0;

  const handleLine = (line: string) => {
    if (!line) return;
    if (!header) {
      header = parseCsvLine(line);
      return;
    }
    const cells = parseCsvLine(line);
    const rec: Record<string, string> = {};
    for (let i = 0; i < header.length; i++) rec[header[i]] = cells[i] ?? '';
    out.push(parseRow(rec));
    lineCount++;
  };

  for await (const chunk of body) {
    buffer += chunk.toString('utf-8');
    let nl = buffer.indexOf('\n');
    while (nl >= 0) {
      handleLine(buffer.slice(0, nl).replace(/\r$/, ''));
      buffer = buffer.slice(nl + 1);
      nl = buffer.indexOf('\n');
    }
  }
  if (buffer.trim()) handleLine(buffer.trim());
  return { rows: out, lineCount };
}

async function phaseDownloadRead(state: PersonaState): Promise<{ state: PersonaState; features: DeviceFeatures[] }> {
  if (!state.downloadQueries) throw new Error('No downloadQueries');
  const all: DeviceFeatures[] = [];
  for (const [src, { queryId }] of Object.entries(state.downloadQueries)) {
    const { rows } = await streamAthenaCsv(queryId, (r): DeviceFeatures => ({
      ad_id: String(r.ad_id || ''),
      total_visits: parseInt(r.total_visits, 10) || 0,
      total_dwell_min: parseFloat(r.total_dwell_min) || 0,
      recency_days: parseInt(r.recency_days, 10) || 0,
      avg_dwell_min: parseFloat(r.avg_dwell_min) || 0,
      morning_share: parseFloat(r.morning_share) || 0,
      midday_share: parseFloat(r.midday_share) || 0,
      afternoon_share: parseFloat(r.afternoon_share) || 0,
      evening_share: parseFloat(r.evening_share) || 0,
      night_share: parseFloat(r.night_share) || 0,
      weekend_share: parseFloat(r.weekend_share) || 0,
      friday_evening_share: parseFloat(r.friday_evening_share) || 0,
      gyration_km: parseFloat(r.gyration_km) || 0,
      unique_h3_cells: parseInt(r.unique_h3_cells, 10) || 0,
      home_zip: String(r.home_zip || '').trim(),
      home_region: String(r.home_region || '').trim(),
      home_lat: r.home_lat ? parseFloat(r.home_lat) : null,
      home_lng: r.home_lng ? parseFloat(r.home_lng) : null,
      gps_share: 0,
      avg_circle_score: 0,
      brand_visits: parseBrandJson(r.brand_visits_json),
      brand_loyalty_hhi: parseFloat(r.brand_loyalty_hhi) || 0,
      nearby_categories_top5: [],
      tier_high_quality: r.tier_high_quality === 'true',
      source_megajob_id: src,
    }));
    for (const f of rows) all.push(f);
    console.log(`[PERSONAS ${state.runId}] Streamed ${rows.length} rows for ${src} (queryId=${queryId})`);
  }

  // Reverse-geocode fallback for devices missing home_zip (BASIC schema, or
  // FULL with sparse zipcode coverage). We use the home_lat/home_lng pair
  // we materialized in the feature CTAS — these are night-mode (with
  // any-hour fallback) averages of the device's location, so they
  // approximate the residential address. Reverse-geocoding through
  // lib/reverse-geocode resolves them to local postal codes.
  const missing = all.filter(
    (f) => (!f.home_zip || !f.home_zip.trim())
      && f.home_lat != null && f.home_lng != null
      && Number.isFinite(f.home_lat) && Number.isFinite(f.home_lng)
  );
  if (missing.length > 0) {
    try {
      const country = await guessCountry(state);
      if (country) setCountryFilter([country]);
      // Round to 1 decimal (~11km cells) so we batch-geocode at most a
      // few thousand unique points instead of millions of devices.
      const cellMap = new Map<string, { lat: number; lng: number; ads: string[] }>();
      for (const f of missing) {
        const key = `${(f.home_lat as number).toFixed(1)},${(f.home_lng as number).toFixed(1)}`;
        let cell = cellMap.get(key);
        if (!cell) {
          cell = { lat: f.home_lat as number, lng: f.home_lng as number, ads: [] };
          cellMap.set(key, cell);
        }
        cell.ads.push(f.ad_id);
      }
      const points = Array.from(cellMap.values()).map((c) => ({
        lat: c.lat, lng: c.lng, deviceCount: c.ads.length,
      }));
      const results = await batchReverseGeocode(points);
      const adZip = new Map<string, string>();
      const cells = Array.from(cellMap.values());
      for (let i = 0; i < cells.length; i++) {
        const r = results[i];
        if (r.type === 'geojson_local' && r.postcode) {
          for (const ad of cells[i].ads) adZip.set(ad, r.postcode);
        }
      }
      let resolvedCount = 0;
      for (const f of all) {
        if ((!f.home_zip || !f.home_zip.trim()) && adZip.has(f.ad_id)) {
          f.home_zip = adZip.get(f.ad_id) || '';
          if (f.home_zip) resolvedCount++;
        }
      }
      console.log(
        `[PERSONAS ${state.runId}] Reverse-geocoded ${resolvedCount}/${missing.length} ` +
        `missing home_zip via ${cellMap.size} unique cells (country=${country || 'auto'})`
      );
    } catch (e: any) {
      console.warn(`[PERSONAS ${state.runId}] Reverse-geocode fallback failed: ${e?.message || e}`);
    }
  }

  const subProgress = {
    label: `Loaded ${all.length.toLocaleString()} feature rows`,
    details: 'Ready for clustering',
  };
  return {
    state: { ...state, phase: 'clustering', subProgress, updatedAt: new Date().toISOString() },
    features: all,
  };
}

// ─── Phase: clustering + aggregation ─────────────────────────────────

async function phaseClusterAndAggregate(
  state: PersonaState,
  features: DeviceFeatures[]
): Promise<{ state: PersonaState; report: PersonaReport; clusterAssignments: Map<string, number>; clusterNames: string[] }> {
  const { runId, config } = state;
  console.log(`[PERSONAS ${runId}] clustering ${features.length} devices`);

  const clusteringResult = runClusteringPipeline(features);

  const rfm = computeRfm(features);
  const cohab =
    config.megaJobIds.length >= 2
      ? computeCohabitation(features, config.megaJobIds)
      : undefined;

  // Per-source ZIP affinity. Resolve source labels (megajob name / job
  // name) so the UI tab and CSV download can use the human label.
  const sourceLabels: Record<string, string> = {};
  for (const id of config.megaJobIds) {
    try {
      const mj = await getMegaJob(id);
      sourceLabels[id] = mj?.name || id.slice(0, 8);
    } catch {
      sourceLabels[id] = id.slice(0, 8);
    }
  }
  for (const id of config.jobIds || []) {
    try {
      const j = await getJob(id);
      sourceLabels[id] = (j as any)?.name || id.slice(0, 8);
    } catch {
      sourceLabels[id] = id.slice(0, 8);
    }
  }
  const zipAffinity = computeZipAffinityPerSource(features, sourceLabels);
  if (zipAffinity.length > 0) {
    const zipsTotal = zipAffinity.reduce((s, x) => s + x.rows.length, 0);
    console.log(
      `[PERSONAS ${runId}] zip-affinity: ${zipAffinity.length} source(s), ${zipsTotal} ZIP rows total`
    );
  }

  // Scorecard
  const totalDevices = features.length;
  const highQ = features.filter((f) => f.tier_high_quality).length;
  const heavyCount = features.filter((f) => f.total_visits >= 8).length;
  const regularCount = features.filter((f) => f.total_visits >= 3 && f.total_visits < 8).length;
  const lightCount = totalDevices - heavyCount - regularCount;
  const longCount = features.filter((f) => f.avg_dwell_min >= 30).length;
  const medCount = features.filter((f) => f.avg_dwell_min >= 5 && f.avg_dwell_min < 30).length;
  const shortCount = totalDevices - longCount - medCount;

  const sortedWeekend = features.map((f) => f.weekend_share).sort((a, b) => a - b);
  const sortedGyr = features.map((f) => f.gyration_km).sort((a, b) => a - b);
  const p = (arr: number[], q: number) => (arr.length ? arr[Math.min(arr.length - 1, Math.floor(arr.length * q))] : 0);

  const avgHourBuckets = ['morning', 'midday', 'afternoon', 'evening', 'night'].map((bucket) => {
    const k = `${bucket}_share` as keyof DeviceFeatures;
    const sum = features.reduce((s, f) => s + (f[k] as number), 0);
    return { bucket: bucket as any, share: totalDevices > 0 ? sum / totalDevices : 0 };
  });

  const nseCrosstab = clusteringResult.personas.map((p) => ({
    personaId: p.id,
    personaName: p.name,
    distribution: p.nseHistogram,
  }));

  const insights = generateInsights({
    features,
    personas: clusteringResult.personas,
    rfm,
    cohabitation: cohab,
    config,
  });

  const report: PersonaReport = {
    runId,
    generatedAt: new Date().toISOString(),
    config,
    scorecard: {
      totalDevices,
      highQualityDevices: highQ,
      freqTiers: [
        { tier: 'heavy', count: heavyCount, percent: totalDevices ? (heavyCount / totalDevices) * 100 : 0 },
        { tier: 'regular', count: regularCount, percent: totalDevices ? (regularCount / totalDevices) * 100 : 0 },
        { tier: 'light', count: lightCount, percent: totalDevices ? (lightCount / totalDevices) * 100 : 0 },
      ],
      dwellTiers: [
        { tier: 'short', count: shortCount, percent: totalDevices ? (shortCount / totalDevices) * 100 : 0 },
        { tier: 'medium', count: medCount, percent: totalDevices ? (medCount / totalDevices) * 100 : 0 },
        { tier: 'long', count: longCount, percent: totalDevices ? (longCount / totalDevices) * 100 : 0 },
      ],
      hourBuckets: avgHourBuckets,
      weekendShareMedian: p(sortedWeekend, 0.5),
      weekendShareP90: p(sortedWeekend, 0.9),
      gyrationKmP50: p(sortedGyr, 0.5),
      gyrationKmP90: p(sortedGyr, 0.9),
    },
    personas: clusteringResult.personas,
    rfm,
    cohabitation: cohab,
    zipAffinity: zipAffinity.length > 0 ? zipAffinity : undefined,
    nseCrosstab,
    insights,
    exports: [],
  };

  return {
    state: { ...state, phase: 'master_maids_export', updatedAt: new Date().toISOString() },
    report,
    clusterAssignments: clusteringResult.assignments,
    clusterNames: clusteringResult.personas.map((p) => p.name),
  };
}

// ─── Phase: master_maids_export ──────────────────────────────────────

async function phaseMasterMaidsExport(
  state: PersonaState,
  report: PersonaReport,
  clusterAssignments: Map<string, number>,
  clusterNames: string[]
): Promise<PersonaState> {
  const { runId } = state;

  // Materialize labels CSV: (ad_id, persona_name).
  // Use timestamp suffix on table name to avoid collisions if exports race.
  const exportTs = Math.floor(Date.now() / 1000).toString(36);
  const labelsKey = `athena-temp/persona-labels/${runId}_${exportTs}/labels.csv`;
  const labelsTable = `persona_labels_${runId.replace(/-/g, '_').slice(0, 16)}_${exportTs}`;
  const lines = ['ad_id,persona_name'];
  for (const [adId, clusterId] of clusterAssignments.entries()) {
    const name = clusterNames[clusterId] || `cluster_${clusterId}`;
    const safeName = name.replace(/"/g, '""');
    lines.push(`"${adId}","${safeName}"`);
  }
  await s3Client.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: labelsKey,
      Body: lines.join('\n'),
      ContentType: 'text/csv',
    })
  );
  try { await runQuery(`DROP TABLE IF EXISTS ${labelsTable}`); } catch {}
  try {
    // Hive-style external table backed by the labels CSV. Same syntax used
    // by nse-poll and lib/athena.ts for the base parquet tables — known to
    // work on the project's Athena setup. (Earlier "Trino-style" attempt
    // failed with "no viable alternative at input '...WITH ('" — Athena
    // DDL parser doesn't support `WITH (external_location=...)` here.)
    await runQuery(`
      CREATE EXTERNAL TABLE IF NOT EXISTS ${labelsTable} (
        ad_id STRING,
        persona_name STRING
      )
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
      STORED AS TEXTFILE
      LOCATION 's3://${BUCKET}/athena-temp/persona-labels/${runId}_${exportTs}/'
      TBLPROPERTIES ('skip.header.line.count' = '1')
    `);
  } catch (e: any) {
    if (!/already exist/i.test(e?.message || '')) throw e;
  }
  console.log(`[PERSONAS ${runId}] Labels table ${labelsTable} ready`);

  // Per cluster CTAS in parallel — table names include the same timestamp.
  const exportQueryIds: Record<string, string> = {};
  const exportTables: Record<string, string> = {};
  for (const persona of report.personas) {
    if (persona.deviceCount === 0) continue;
    const slug = persona.name.replace(/[^a-z0-9]+/gi, '_').toLowerCase().slice(0, 24);
    const table = `master_persona_${slug}_${runId.slice(0, 6)}_${exportTs}`;
    const s3Path = `s3://${BUCKET}/athena-temp/${table}/`;
    try { await runQuery(`DROP TABLE IF EXISTS ${table}`); } catch {}
    const sql = `
      CREATE TABLE ${table}
      WITH (format='PARQUET', parquet_compression='SNAPPY',
            external_location='${s3Path}')
      AS SELECT DISTINCT ad_id FROM ${labelsTable}
      WHERE persona_name = '${persona.name.replace(/'/g, "''")}'
    `;
    const qid = await startQueryAsync(sql);
    exportQueryIds[String(persona.id)] = qid;
    exportTables[String(persona.id)] = table;
    console.log(`[PERSONAS ${runId}] Export CTAS for ${persona.name} (id=${persona.id}): ${qid}`);
  }

  return {
    ...state,
    phase: 'export_polling',
    exportQueryIds,
    exportTables,
    report,
    updatedAt: new Date().toISOString(),
  };
}

// ─── Phase: export_polling + register ────────────────────────────────

async function phaseExportPolling(state: PersonaState): Promise<PersonaState> {
  if (!state.exportQueryIds || !state.report) throw new Error('No export state');

  const queryIds = Object.values(state.exportQueryIds);
  const statuses = await Promise.all(queryIds.map((q) => checkQueryStatus(q)));
  const anyFailed = statuses.find((s) => s.state === 'FAILED' || s.state === 'CANCELLED');
  if (anyFailed) {
    console.warn(`[PERSONAS ${state.runId}] Export CTAS failed: ${anyFailed.error}`);
    // continue but mark exports as failed
  }
  const allDone = statuses.every((s) => s.state === 'SUCCEEDED' || s.state === 'FAILED' || s.state === 'CANCELLED');
  if (!allDone) return { ...state, updatedAt: new Date().toISOString() };

  // Register successful exports.
  const country = await guessCountry(state);
  const dateRange = await guessDateRange(state);
  const sourceDisplayName = await resolveSourceDisplayName(state);
  const exports: PersonaReport['exports'] = [];
  for (const persona of state.report.personas) {
    if (persona.deviceCount === 0) continue;
    const qid = state.exportQueryIds[String(persona.id)];
    const tbl = state.exportTables?.[String(persona.id)];
    if (!qid || !tbl) continue;
    const status = await checkQueryStatus(qid);
    if (status.state !== 'SUCCEEDED') continue;
    try {
      if (country) {
        // attr_value is what shows up everywhere a persona is referenced
        // (Master MAIDs UI, audience pickers, exports). Prefix with the
        // source display name so the brand / study is obvious without
        // having to dig into sourceDataset.
        const attributeValue = `${sourceDisplayName} · ${persona.name}`;
        await registerAthenaContribution(
          country,
          sourceDisplayName,
          'persona' as any, // 'persona' added to AttributeType in master-maids.ts
          attributeValue,
          tbl,
          `s3://${BUCKET}/athena-temp/${tbl}/`,
          persona.deviceCount,
          dateRange,
        );
      }
      exports.push({
        personaId: persona.id,
        personaName: persona.name,
        athenaTable: tbl,
        s3Prefix: `athena-temp/${tbl}/`,
        maidCount: persona.deviceCount,
      });
    } catch (e: any) {
      console.error(`[PERSONAS ${state.runId}] Failed to register persona ${persona.name}:`, e.message);
    }
  }

  const updatedReport: PersonaReport = { ...state.report, exports };
  await putConfig(REPORT_KEY(state.runId), updatedReport, { compact: true });
  return {
    ...state,
    phase: 'done',
    report: updatedReport,
    updatedAt: new Date().toISOString(),
  };
}

/**
 * Build a human-readable label that names the source megajobs / jobs the
 * persona run was generated from. Used both as `sourceDataset` and as the
 * prefix of `attributeValue` when registering Master MAIDs contributions
 * — so an analyst opening /master-maids sees "BK History · Heavy Lunchtime"
 * instead of an opaque persona name with no context.
 */
async function resolveSourceDisplayName(state: PersonaState): Promise<string> {
  const names: string[] = [];
  for (const id of state.config.megaJobIds) {
    const mj = await getMegaJob(id);
    if (mj?.name) names.push(mj.name);
    else names.push(id.slice(0, 8));
  }
  for (const id of state.config.jobIds || []) {
    const j = await getJob(id);
    if ((j as any)?.name) names.push((j as any).name);
    else names.push(id.slice(0, 8));
  }
  return names.length > 0 ? names.join(' + ') : 'persona-run';
}

async function guessCountry(state: PersonaState): Promise<string | null> {
  for (const mjId of state.config.megaJobIds) {
    const mj = await getMegaJob(mjId);
    if (mj?.country) return mj.country;
  }
  for (const jId of state.config.jobIds || []) {
    const j = await getJob(jId);
    if ((j as any)?.country) return (j as any).country as string;
  }
  return null;
}

async function guessDateRange(state: PersonaState): Promise<{ from: string; to: string }> {
  let from = '';
  let to = '';
  for (const mjId of state.config.megaJobIds) {
    const mj = await getMegaJob(mjId);
    const r = mj?.sourceScope?.dateRange;
    if (r?.from && (!from || r.from < from)) from = r.from;
    if (r?.to && (!to || r.to > to)) to = r.to;
  }
  for (const jId of state.config.jobIds || []) {
    const j = await getJob(jId);
    const r = (j as any)?.dateRange;
    if (r?.from && (!from || r.from < from)) from = r.from;
    if (r?.to && (!to || r.to > to)) to = r.to;
  }
  return { from: from || '', to: to || '' };
}

// ─── Route handler ───────────────────────────────────────────────────

export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    let body: any = {};
    try { body = await request.json(); } catch {}

    // ── Re-run shortcut ────────────────────────────────────────────
    // Body: { srcRunId: '<id>', rerun: true }
    // Loads the previous run's config from S3, kicks off a brand-new run
    // with a unique runId. The past run is preserved as history.
    if (body.srcRunId && body.rerun === true) {
      invalidateCache(STATE_KEY(body.srcRunId));
      const src = await getConfig<PersonaState>(STATE_KEY(body.srcRunId));
      if (!src) {
        return NextResponse.json({ error: `Source run ${body.srcRunId} not found` }, { status: 404 });
      }
      const cfg = src.config;
      const newRunId = rerunRunId(cfg);
      const initialState: PersonaState = {
        phase: 'starting',
        runId: newRunId,
        config: cfg,
        updatedAt: new Date().toISOString(),
      };
      await putConfig(STATE_KEY(newRunId), initialState, { compact: true });
      console.log(`[PERSONAS] Re-run: ${body.srcRunId} → ${newRunId}`);
      // Return the new runId immediately. The client's polling loop will
      // drive the state machine on the new run.
      return NextResponse.json({
        phase: 'starting',
        runId: newRunId,
        progress: { step: 'starting', percent: 0, message: 'Re-running with fresh state...' },
      });
    }

    const hasMega = Array.isArray(body.megaJobIds) && body.megaJobIds.length > 0;
    const hasJobs = Array.isArray(body.jobIds) && body.jobIds.length > 0;
    const isNewRequest = hasMega || hasJobs;
    const forceRerun = body.rerun === true;
    let runId: string = body.runId || '';
    if (isNewRequest) {
      const cfg: PersonaRunConfig = {
        megaJobIds: (hasMega ? body.megaJobIds : []).filter((s: any) => typeof s === 'string'),
        jobIds: (hasJobs ? body.jobIds : []).filter((s: any) => typeof s === 'string'),
        filters: body.filters || {},
      };
      // forceRerun: bypass config-hash idempotency by salting with a timestamp.
      runId = forceRerun ? rerunRunId(cfg) : configToRunId(cfg);
      // Idempotency: if state exists and is done, return cached (only when not re-running).
      invalidateCache(STATE_KEY(runId));
      if (!forceRerun) {
        const existing = await getConfig<PersonaState>(STATE_KEY(runId));
        if (existing?.phase === 'done' && existing.report) {
          return NextResponse.json({ phase: 'done', runId, report: existing.report });
        }
      }
      // Reset on error or first time (or re-run)
      const initialState: PersonaState = {
        phase: 'starting',
        runId,
        config: cfg,
        updatedAt: new Date().toISOString(),
      };
      await putConfig(STATE_KEY(runId), initialState, { compact: true });
      if (forceRerun) console.log(`[PERSONAS] Force re-run with new runId=${runId}`);
    }

    if (!runId) {
      return NextResponse.json({ error: 'runId or megaJobIds required' }, { status: 400 });
    }

    invalidateCache(STATE_KEY(runId));
    let state = await getConfig<PersonaState>(STATE_KEY(runId));
    if (!state) {
      return NextResponse.json({ error: 'State not found' }, { status: 404 });
    }

    // Drive the state machine forward by ONE phase.
    try {
      switch (state.phase as PersonaPhase) {
        case 'starting':
          state = await phaseStarting(state);
          break;
        case 'feature_polling':
          state = await phaseFeaturePolling(state);
          break;
        // Legacy state for runs created before the download split — auto-promote.
        case 'feature_ctas' as any:
          state = { ...state, phase: 'feature_polling' };
          break;
        case 'download_query':
          state = await phaseDownloadQuery(state);
          break;
        case 'download_polling':
          state = await phaseDownloadPolling(state);
          break;
        // Legacy state name 'download' from earlier deploys → restart from query.
        case 'download' as any:
          state = await phaseDownloadQuery(state);
          break;
        case 'download_read': {
          const { state: ns, features } = await phaseDownloadRead(state);
          const { state: ns2, report, clusterAssignments, clusterNames } = await phaseClusterAndAggregate(ns, features);
          // Save the report early so frontend can render scorecard while exports finish.
          await putConfig(REPORT_KEY(state.runId), report, { compact: true });
          state = await phaseMasterMaidsExport(ns2, report, clusterAssignments, clusterNames);
          break;
        }
        case 'clustering':
        case 'aggregation':
          // These are intermediate states that should never persist (the
          // download_read handler does both inline). If we land here, it
          // means a previous run was killed mid-flight — restart from
          // download_query so the SELECT fires again.
          state = { ...state, phase: 'download_query' };
          break;
        case 'master_maids_export':
        case 'export_polling':
          state = await phaseExportPolling(state);
          break;
        case 'done':
          // Nothing to do
          break;
        case 'error':
          return NextResponse.json({ phase: 'error', runId, error: state.error });
      }
    } catch (e: any) {
      console.error(`[PERSONAS ${runId}] Phase ${state.phase} error:`, e.message, e.stack);
      state = { ...state, phase: 'error', error: e.message, updatedAt: new Date().toISOString() };
    }

    await putConfig(STATE_KEY(runId), state, { compact: true });

    return NextResponse.json({
      phase: state.phase,
      runId,
      report: state.report,
      error: state.error,
      progress: {
        step: state.phase,
        percent: phasePercent(state.phase),
        message: state.subProgress?.label || phaseMessage(state.phase),
        details: state.subProgress?.details,
        ratio: state.subProgress?.ratio,
        perSource: state.subProgress?.perSource,
        phaseLabel: phaseMessage(state.phase),
      },
    });
  } catch (e: any) {
    console.error('[PERSONAS] error:', e?.message, e?.stack);
    return NextResponse.json({ error: e?.message || String(e) }, { status: 500 });
  }
}

function phasePercent(phase: PersonaPhase): number {
  return ({
    starting: 5,
    feature_ctas: 10,
    feature_polling: 25,
    enrichment_ctas: 30,
    enrichment_polling: 35,
    download_query: 45,
    download_polling: 55,
    download_read: 65,
    clustering: 75,
    aggregation: 82,
    master_maids_export: 88,
    export_polling: 95,
    done: 100,
    error: 0,
  } as const)[phase];
}

function phaseMessage(phase: PersonaPhase): string {
  return ({
    starting: 'Resolving POIs and brands…',
    feature_ctas: 'Launching feature CTAS in Athena…',
    feature_polling: 'Aggregating per-device features in Athena',
    enrichment_ctas: 'Building enrichment tables…',
    enrichment_polling: 'Waiting for enrichment…',
    download_query: 'Launching feature-vector download…',
    download_polling: 'Streaming feature vectors from Athena',
    download_read: 'Loading feature vectors into memory',
    clustering: 'Discovering clusters with k-means',
    aggregation: 'Aggregating per-persona stats + insights',
    master_maids_export: 'Exporting personas to Master MAIDs',
    export_polling: 'Finalizing Master MAIDs contributions',
    done: 'Done',
    error: 'Error',
  } as const)[phase];
}
