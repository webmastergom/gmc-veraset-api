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
import { buildFeatureCTAS, buildVisitorPingsCTAS, extractPoiCoords } from '@/lib/persona-feature-query';
import { runClusteringPipeline } from '@/lib/persona-clusterer';
import { computeRfm } from '@/lib/persona-rfm';
import { computeCohabitation } from '@/lib/persona-cohabitation';
import { computeZipAffinityPerSource } from '@/lib/persona-zip-affinity';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { tzForCountry } from '@/lib/timezones';
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
// Bumped from 120 → 300 (Vercel Pro max) because the clustering phase combines
// phaseDownloadRead (~30-40s) + phaseClusterAndAggregate (~60-90s) and was
// hitting the timeout on large megajobs. 300s gives comfortable headroom.
export const maxDuration = 300;

const STATE_KEY = (runId: string) => `persona-state/${runId}`;
const REPORT_KEY = (runId: string) => `persona-reports/${runId}`;
// Inter-phase persistence: cluster assignments + names, handed off from
// clustering → master_maids_export. (We don't persist the parsed features
// array — JSON.stringify on a 500k-row DeviceFeatures[] OOMs the function.
// Features live in memory only for the duration of the clustering phase.)
const CLUSTER_DATA_KEY = (runId: string) => `persona-cluster-data/${runId}`;
// Multi-phase geocoding:
//   geocode_cells stream-aggregates unique 0.1° lat/lng cells from devices
//   with missing home_zip and persists the cellMap (small) here.
//   geocode_lookup loads the cellMap, reverse-geocodes the unique cells, and
//   persists the cell→zip map here. clustering re-streams the CSV and uses
//   the cell→zip map to fill home_zip for devices that lacked it. Splitting
//   keeps the 600k-device cellMap + the country GeoJSON from co-existing in
//   memory (which OOMs the Vercel function even at 3008 MB).
const CELLS_KEY = (runId: string) => `persona-cells/${runId}`;
const CELL_ZIP_KEY = (runId: string) => `persona-cell-zip/${runId}`;

// Hash a config to a stable runId.
//
// CRITICAL: must include jobIds AND megaJobIds AND filters. Earlier
// versions of this function only hashed megaJobIds + filters, which
// caused runs with the same megaJobIds but different jobIds to collide
// on the same runId — selecting [DatasetA] would serve a cached run
// of [DatasetA, DatasetB] from S3. Real bug observed in MX dealerships.
function configToRunId(cfg: PersonaRunConfig): string {
  const norm = {
    megaJobIds: [...cfg.megaJobIds].sort(),
    jobIds: [...(cfg.jobIds || [])].sort(),
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
  /** ISO2 country code, used to derive local timezone for hour buckets. */
  country?: string;
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
  const country = megaJob.country
    || (syncedJobs.find((j: any) => j.country) as any)?.country
    || '';
  return { sourceId: megaJobId, syncedJobs, collectionIds, rangeTo, label: megaJob.name, country };
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
  const country = (job as any).country || '';
  return { sourceId: jobId, syncedJobs, collectionIds: colIds, rangeTo, label: job.name || jobId, country };
}

/**
 * Per-source resolved metadata needed by Stage 2 (feature CTAS). We save
 * this in PersonaState during Stage 1 launch so Stage 2 doesn't have to
 * re-resolve POI collections + brand discovery + timezone (which can be
 * 10s of seconds for large megajobs and would slow the stage transition).
 */
interface PerSourceMeta {
  sourceId: string;
  label: string;
  rangeTo: string;
  country: string;
  poiToBrand: Array<{ poiId: string; brand: string }>;
  /**
   * Mean of all POI lat/lng for this source — used downstream as the
   * "reference point" for the Lift sub-index in zip affinity (distance-
   * weighted expected visitor share). For sources where POIs are spread
   * across a country, this centroid is a coarse summary; if Lift looks
   * wrong, upgrade to nearest-POI distance per zip.
   */
  poiCentroid?: { lat: number; lng: number };
}

async function fireFeatureCtasForSource(args: {
  source: ResolvedSource;
  runId: string;
  filters: any;
}): Promise<{ queryId: string; tableName: string; meta: PerSourceMeta }> {
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
  // Precompute the simple mean centroid — used downstream as the reference
  // point for the Lift sub-index in zip affinity.
  const poiCentroid = {
    lat: poiCoords.reduce((s, p) => s + p.lat, 0) / poiCoords.length,
    lng: poiCoords.reduce((s, p) => s + p.lng, 0) / poiCoords.length,
  };

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

  // STAGE 1: visitor_pings CTAS. Materializes the per-source pings
  // table that Stage 2 reads from. Light query plan (single UNION ALL
  // scan + spatial-grid JOIN + ad_id filter to visitors) — fits Athena
  // resource budget for any megajob the original feature CTAS would
  // have OOM'd on.
  const safeId = source.sourceId.replace(/-/g, '_').slice(0, 24);
  const attemptTs = Math.floor(Date.now() / 1000).toString(36); // ~7 chars
  const visitorPingsBase = `persona_visitor_pings_${safeId}_${runId.slice(0, 8)}_${attemptTs}`;
  const visitorPingsTable = visitorPingsBase.length > 60 ? visitorPingsBase.slice(0, 60) : visitorPingsBase;
  try { await runQuery(`DROP TABLE IF EXISTS ${visitorPingsTable}`); } catch {}
  const visitorPingsS3 = `s3://${BUCKET}/athena-temp/${visitorPingsTable}/`;

  // Resolve local timezone from the source's country so that hour
  // buckets + DAY_OF_WEEK reflect local time (not UTC). Without this,
  // MX afternoon visits (UTC 22:00+) get bucketed as "Night" and ALL
  // personas show peak-hour = Night. Used in Stage 1 (filters) AND
  // Stage 2 (bucket aggregation).
  const localTz = tzForCountry(source.country);
  if (localTz !== 'UTC') {
    console.log(`[PERSONAS ${runId}] Source ${source.label} (country=${source.country}) → tz=${localTz}`);
  }

  const sql = buildVisitorPingsCTAS({
    ctasTable: visitorPingsTable,
    ctasS3Path: visitorPingsS3,
    syncedJobs: source.syncedJobs,
    poiCoords,
    localTz,
    filters,
  });

  const queryId = await startQueryAsync(sql);
  console.log(`[PERSONAS ${runId}] Stage 1 (visitor_pings) CTAS started for ${source.label}: queryId=${queryId}, table=${visitorPingsTable}`);
  return {
    queryId,
    tableName: visitorPingsTable,
    meta: {
      sourceId: source.sourceId,
      label: source.label,
      rangeTo: source.rangeTo,
      country: source.country || '',
      poiToBrand,
      poiCentroid,
    },
  };
}

/**
 * STAGE 2 launcher: builds + fires the feature CTAS for ONE source,
 * reading from the pre-materialized visitor_pings table. Called from
 * phaseVisitorPingsPolling once Stage 1 has finished for all sources.
 */
async function fireFeatureCtasFromVisitorPings(args: {
  visitorPingsTable: string;
  meta: PerSourceMeta;
  runId: string;
  filters: any;
}): Promise<{ queryId: string; tableName: string }> {
  const { visitorPingsTable, meta, runId, filters } = args;
  const safeId = meta.sourceId.replace(/-/g, '_').slice(0, 24);
  const attemptTs = Math.floor(Date.now() / 1000).toString(36);
  const featureTableBase = `persona_features_${safeId}_${runId.slice(0, 8)}_${attemptTs}`;
  const featureTable = featureTableBase.length > 60 ? featureTableBase.slice(0, 60) : featureTableBase;
  try { await runQuery(`DROP TABLE IF EXISTS ${featureTable}`); } catch {}
  const featureS3 = `s3://${BUCKET}/athena-temp/${featureTable}/`;

  const localTz = tzForCountry(meta.country);
  const sql = buildFeatureCTAS({
    ctasTable: featureTable,
    ctasS3Path: featureS3,
    visitorPingsTable,
    poiToBrand: meta.poiToBrand,
    dateRangeTo: meta.rangeTo,
    sourceMegajobId: meta.sourceId,
    localTz,
    filters,
  });

  const queryId = await startQueryAsync(sql);
  console.log(`[PERSONAS ${runId}] Stage 2 (feature) CTAS started for ${meta.label}: queryId=${queryId}, table=${featureTable}, reads from ${visitorPingsTable}`);
  return { queryId, tableName: featureTable };
}

async function phaseStarting(state: PersonaState): Promise<PersonaState> {
  const { config, runId } = state;
  console.log(`[PERSONAS ${runId}] starting: megaJobs=${config.megaJobIds.join(',')} jobs=${(config.jobIds || []).join(',')}`);

  // STAGE 1 launch — fire visitor_pings CTAS per source. Each one is a
  // single UNION ALL scan + spatial-grid JOIN + visitor filter, MUCH
  // simpler than the original 20-CTE feature query and well within
  // Athena's resource budget at any scale.
  const visitorPingsCtas: Record<string, { queryId: string; tableName: string }> = {};
  const sourceMeta: NonNullable<PersonaState['sourceMeta']> = {};

  for (const megaJobId of config.megaJobIds) {
    const source = await resolveMegaJobSource(megaJobId);
    const fired = await fireFeatureCtasForSource({ source, runId, filters: config.filters as any });
    visitorPingsCtas[source.sourceId] = { queryId: fired.queryId, tableName: fired.tableName };
    sourceMeta[source.sourceId] = fired.meta;
  }
  for (const jobId of config.jobIds || []) {
    const source = await resolveJobSource(jobId);
    const fired = await fireFeatureCtasForSource({ source, runId, filters: config.filters as any });
    visitorPingsCtas[source.sourceId] = { queryId: fired.queryId, tableName: fired.tableName };
    sourceMeta[source.sourceId] = fired.meta;
  }

  if (Object.keys(visitorPingsCtas).length === 0) {
    throw new Error('No sources resolved (megaJobIds + jobIds both empty?)');
  }

  return {
    ...state,
    phase: 'visitor_pings_polling',
    visitorPingsCtas,
    sourceMeta,
    updatedAt: new Date().toISOString(),
  };
}

// ─── Phase: feature_polling ──────────────────────────────────────────

/**
 * Stage 1 polling: wait for all visitor_pings CTASes to finish, then
 * fan out Stage 2 (feature CTAS reading from each visitor_pings table).
 * The split exists so each Athena query is simple enough to stay within
 * the workgroup's per-query resource budget — the original monolithic
 * feature CTAS exhausted resources at ~120 GB scanned per source.
 */
async function phaseVisitorPingsPolling(state: PersonaState): Promise<PersonaState> {
  if (!state.visitorPingsCtas) throw new Error('No visitorPingsCtas in state');
  const entries = Object.entries(state.visitorPingsCtas);
  const queryIds = entries.map(([, v]) => v.queryId);
  const statuses = await Promise.all(queryIds.map((q) => checkQueryStatus(q)));
  const anyFailed = statuses.find((s) => s.state === 'FAILED' || s.state === 'CANCELLED');
  if (anyFailed) throw new Error(`Stage 1 (visitor_pings) CTAS failed: ${anyFailed.error || 'unknown'}`);
  const allDone = statuses.every((s) => s.state === 'SUCCEEDED');

  const totalScannedGB = statuses.reduce((s, x) => s + ((x.statistics?.dataScannedBytes || 0) / 1e9), 0);
  const doneCount = statuses.filter((s) => s.state === 'SUCCEEDED').length;
  const perSource: Record<string, string> = {};
  entries.forEach(([src], i) => {
    const st = statuses[i];
    perSource[src] = `${st.state} · ${((st.statistics?.dataScannedBytes || 0) / 1e9).toFixed(1)} GB scanned`;
  });
  const subProgress = {
    label: allDone ? 'Stage 1 done · firing feature CTAS' : `Stage 1 (visitor pings): ${doneCount}/${statuses.length} sources done`,
    ratio: doneCount / Math.max(1, statuses.length),
    details: `${totalScannedGB.toFixed(1)} GB scanned · 2-stage pipeline avoids the resource-exhaust failure at this scale`,
    perSource,
  };

  if (!allDone) {
    return { ...state, subProgress, updatedAt: new Date().toISOString() };
  }

  // ── Fan out Stage 2: one feature CTAS per source, reading from the
  //    pre-materialized visitor_pings table.
  if (!state.sourceMeta) throw new Error('Stage 1 finished but sourceMeta missing — cannot fire Stage 2');
  const featureCtas: Record<string, { queryId: string; tableName: string }> = {};
  for (const [sourceId, vp] of entries) {
    const meta = state.sourceMeta[sourceId];
    if (!meta) {
      console.warn(`[PERSONAS ${state.runId}] No sourceMeta for ${sourceId} — skipping Stage 2`);
      continue;
    }
    featureCtas[sourceId] = await fireFeatureCtasFromVisitorPings({
      visitorPingsTable: vp.tableName,
      meta,
      runId: state.runId,
      filters: state.config.filters as any,
    });
  }

  return {
    ...state,
    phase: 'feature_polling',
    featureCtas,
    subProgress: {
      ...subProgress,
      label: 'Stage 2 (feature CTAS) launched',
    },
    updatedAt: new Date().toISOString(),
  };
}

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
        morning_share, midday_share, afternoon_share, evening_share, night_share,
        weekend_share, friday_evening_share, gyration_km, unique_h3_cells,
        home_zip, home_region, home_lat, home_lng,
        gps_share, avg_circle_score,
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
 * Uses Node's readline over the S3 body stream — properly handles
 * backpressure and never accumulates more than one line worth of bytes
 * outside the output array. The previous string-buffer implementation
 * appears to OOM-kill the Vercel function on 150+ MB CSVs even at 3008
 * MB allocation (likely V8 heap fragmentation from the repeated
 * `buffer = buffer.slice(...)` cycle).
 */
async function streamAthenaCsv<T>(
  queryId: string,
  parseRow: (record: Record<string, string>) => T,
): Promise<{ rows: T[]; lineCount: number }> {
  const { GetObjectCommand } = await import('@aws-sdk/client-s3');
  const { createInterface } = await import('readline');
  const obj = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: `athena-results/${queryId}.csv`,
  }));
  if (!obj.Body) throw new Error(`Empty body for athena-results/${queryId}.csv`);
  // AWS SDK v3 returns a Node Readable in the Lambda/Vercel runtime.
  const stream = obj.Body as unknown as NodeJS.ReadableStream;
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let header: string[] | null = null;
  const out: T[] = [];
  let lineCount = 0;

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = parseCsvLine(line);
      continue;
    }
    const cells = parseCsvLine(line);
    const rec: Record<string, string> = {};
    for (let i = 0; i < header.length; i++) rec[header[i]] = cells[i] ?? '';
    out.push(parseRow(rec));
    lineCount++;
  }
  return { rows: out, lineCount };
}

/** Round lat/lng to 1 decimal (~11 km cells) for cell-key dedupe. */
function cellKey(lat: number, lng: number): string {
  return `${lat.toFixed(1)},${lng.toFixed(1)}`;
}

async function phaseDownloadRead(
  state: PersonaState,
  cellToZip: Map<string, string> | null,
  progress?: (step: string) => Promise<void>
): Promise<{ state: PersonaState; features: DeviceFeatures[] }> {
  if (!state.downloadQueries) throw new Error('No downloadQueries');
  const all: DeviceFeatures[] = [];
  let cellLookupHits = 0;
  for (const [src, { queryId }] of Object.entries(state.downloadQueries)) {
    await progress?.(`1a/3 streamAthenaCsv start (src=${src.slice(0,8)})`);
    const { rows } = await streamAthenaCsv(queryId, (r): DeviceFeatures => {
      // Resolve home_zip in one pass: prefer the CTAS-native value, fall
      // back to the pre-computed cell→zip map when missing. No second
      // pass over the features array, no 600k-string adZip map.
      let homeZip = String(r.home_zip || '').trim();
      const lat = r.home_lat ? parseFloat(r.home_lat) : null;
      const lng = r.home_lng ? parseFloat(r.home_lng) : null;
      if (!homeZip && cellToZip && lat != null && lng != null
          && Number.isFinite(lat) && Number.isFinite(lng)) {
        const zipFromCell = cellToZip.get(cellKey(lat, lng));
        if (zipFromCell) {
          homeZip = zipFromCell;
          cellLookupHits++;
        }
      }
      return {
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
        home_zip: homeZip,
        home_region: String(r.home_region || '').trim(),
        home_lat: lat,
        home_lng: lng,
        gps_share: parseFloat(r.gps_share) || 0,
        avg_circle_score: parseFloat(r.avg_circle_score) || 0,
        brand_visits: parseBrandJson(r.brand_visits_json),
        brand_loyalty_hhi: parseFloat(r.brand_loyalty_hhi) || 0,
        tier_high_quality: r.tier_high_quality === 'true',
        source_megajob_id: src,
      };
    });
    for (const f of rows) all.push(f);
    await progress?.(`1b/3 streamAthenaCsv done: ${rows.length} rows (${cellLookupHits} zips resolved via cellMap)`);
    console.log(`[PERSONAS ${state.runId}] Streamed ${rows.length} rows for ${src} (queryId=${queryId}, cellLookupHits=${cellLookupHits})`);
  }
  await progress?.(`1c/3 phaseDownloadRead done: ${all.length} features ready`);

  const subProgress = {
    label: `Loaded ${all.length.toLocaleString()} feature rows`,
    details: cellToZip
      ? `${cellLookupHits.toLocaleString()} home_zips resolved via reverse-geocode cellMap`
      : 'Ready for clustering',
  };
  return {
    state: { ...state, phase: 'clustering', subProgress, updatedAt: new Date().toISOString() },
    features: all,
  };
}

// ─── Phase: geocode_cells (multi-phase reverse-geocoding, step 1/2) ──
//
// Stream the SELECT * CSV for each source with a lightweight parser that
// only extracts home_zip + home_lat + home_lng. Aggregate the unique
// 0.1° lat/lng cells of devices that lack home_zip into a single Map
// keyed by `lat.toFixed(1),lng.toFixed(1)`. Persist the small cellMap
// to S3 for geocode_lookup to consume.
//
// Memory budget: ~50 MB (stream buffer + Map of a few thousand cells).
// Never materializes the full DeviceFeatures array.

interface CellsArtifact {
  /** Total rows seen across all sources. */
  totalRows: number;
  /** Devices that need reverse-geocode (no native zip, valid lat/lng). */
  missingRows: number;
  /** Unique 0.1° cells with at least one missing-zip device. */
  cells: Array<{ key: string; lat: number; lng: number; deviceCount: number }>;
}

async function phaseGeocodeCells(state: PersonaState): Promise<PersonaState> {
  if (!state.downloadQueries) throw new Error('No downloadQueries');
  const cellMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
  let totalRows = 0;
  let missingRows = 0;
  for (const [, { queryId }] of Object.entries(state.downloadQueries)) {
    await streamAthenaCsv(queryId, (r): null => {
      totalRows++;
      const homeZip = String(r.home_zip || '').trim();
      if (homeZip) return null;
      const lat = r.home_lat ? parseFloat(r.home_lat) : NaN;
      const lng = r.home_lng ? parseFloat(r.home_lng) : NaN;
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
      missingRows++;
      const roundedLat = Math.round(lat * 10) / 10;
      const roundedLng = Math.round(lng * 10) / 10;
      const key = `${roundedLat.toFixed(1)},${roundedLng.toFixed(1)}`;
      const cell = cellMap.get(key);
      if (cell) {
        cell.deviceCount++;
      } else {
        cellMap.set(key, { lat: roundedLat, lng: roundedLng, deviceCount: 1 });
      }
      return null;
    });
  }
  const artifact: CellsArtifact = {
    totalRows,
    missingRows,
    cells: Array.from(cellMap.entries()).map(([key, c]) => ({
      key, lat: c.lat, lng: c.lng, deviceCount: c.deviceCount,
    })),
  };
  await putConfig(CELLS_KEY(state.runId), artifact, { compact: true });
  console.log(
    `[PERSONAS ${state.runId}] geocode_cells: ${totalRows} rows, ` +
    `${missingRows} missing zips → ${artifact.cells.length} unique 0.1° cells`,
  );
  return {
    ...state,
    phase: 'geocode_lookup',
    subProgress: {
      label: `Aggregated ${artifact.cells.length.toLocaleString()} unique cells`,
      details: `${missingRows.toLocaleString()} devices need reverse-geocode of ${totalRows.toLocaleString()} total`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ─── Phase: geocode_lookup (multi-phase reverse-geocoding, step 2/2) ─
//
// Load the cellMap, reverse-geocode the unique cells (small input —
// usually a few thousand points), and persist the cell→zip map for
// clustering to consume during its CSV re-stream.

async function phaseGeocodeLookup(state: PersonaState): Promise<PersonaState> {
  const artifact = await getConfig<CellsArtifact>(CELLS_KEY(state.runId));
  if (!artifact) throw new Error('Cell artifact not found — re-run from geocode_cells');
  const cellToZip: Record<string, string> = {};
  if (artifact.cells.length === 0) {
    // All devices already had home_zip from the CTAS — nothing to do.
    await putConfig(CELL_ZIP_KEY(state.runId), cellToZip, { compact: true });
    return {
      ...state,
      phase: 'clustering',
      subProgress: { label: 'No reverse-geocoding needed', details: 'All devices have home_zip from CTAS' },
      updatedAt: new Date().toISOString(),
    };
  }
  const country = await guessCountry(state);
  if (country) setCountryFilter([country]);
  try {
    const points = artifact.cells.map((c) => ({
      lat: c.lat, lng: c.lng, deviceCount: c.deviceCount,
    }));
    const results = await batchReverseGeocode(points);
    let resolved = 0;
    for (let i = 0; i < artifact.cells.length; i++) {
      const r = results[i];
      if ((r.type === 'geojson_local' || r.type === 'nominatim_match') && r.postcode) {
        cellToZip[artifact.cells[i].key] = r.postcode;
        resolved++;
      }
    }
    console.log(
      `[PERSONAS ${state.runId}] geocode_lookup: ${resolved}/${artifact.cells.length} cells resolved ` +
      `(country=${country || 'auto'})`,
    );
  } finally {
    if (country) setCountryFilter(null);
  }
  await putConfig(CELL_ZIP_KEY(state.runId), cellToZip, { compact: true });
  return {
    ...state,
    phase: 'clustering',
    subProgress: {
      label: `Resolved ${Object.keys(cellToZip).length.toLocaleString()} cell→zip mappings`,
      details: `Country: ${country || 'auto'}`,
    },
    updatedAt: new Date().toISOString(),
  };
}

// ─── Phase: clustering + aggregation ─────────────────────────────────

async function phaseClusterAndAggregate(
  state: PersonaState,
  features: DeviceFeatures[]
): Promise<{ state: PersonaState; report: PersonaReport; clusterAssignments: Map<string, number>; clusterNames: string[] }> {
  const { runId, config } = state;

  // ── Resolve source labels + countries (used by NSE load + zipAffinity) ───
  const sourceLabels: Record<string, string> = {};
  const sourceCountries: Record<string, string> = {};
  for (const id of config.megaJobIds) {
    try {
      const mj = await getMegaJob(id);
      sourceLabels[id] = mj?.name || id.slice(0, 8);
      if (mj?.country) sourceCountries[id] = mj.country.toUpperCase();
    } catch {
      sourceLabels[id] = id.slice(0, 8);
    }
  }
  for (const id of config.jobIds || []) {
    try {
      const j = await getJob(id);
      sourceLabels[id] = (j as any)?.name || id.slice(0, 8);
      if ((j as any)?.country) sourceCountries[id] = String((j as any).country).toUpperCase();
    } catch {
      sourceLabels[id] = id.slice(0, 8);
    }
  }

  // ── Load NSE records per country (powers BOTH population lookup AND
  //    nse_bracket enrichment per device). Loaded once, reused below. ─────
  type NseRecord = { postal_code: string; population: number; nse: number };
  const nseByCountry = new Map<string, NseRecord[]>();
  const popMapByCountry = new Map<string, Map<string, number>>();
  const nseScoreByCountry = new Map<string, Map<string, number>>();
  for (const country of new Set(Object.values(sourceCountries))) {
    if (!country) continue;
    try {
      const records = await getConfig<NseRecord[]>(`nse/${country}`);
      const popMap = new Map<string, number>();
      const nseMap = new Map<string, number>();
      if (records && Array.isArray(records)) {
        for (const r of records) {
          if (!r?.postal_code) continue;
          const zip = String(r.postal_code).trim();
          if (Number.isFinite(r.population) && r.population > 0) popMap.set(zip, r.population);
          if (Number.isFinite(r.nse)) nseMap.set(zip, r.nse);
        }
      }
      nseByCountry.set(country, records || []);
      popMapByCountry.set(country, popMap);
      nseScoreByCountry.set(country, nseMap);
      console.log(`[PERSONAS ${runId}] NSE for ${country}: ${popMap.size} ZIPs with pop, ${nseMap.size} with NSE`);
    } catch (e: any) {
      console.warn(`[PERSONAS ${runId}] No NSE data for ${country} (${e?.message || e})`);
      popMapByCountry.set(country, new Map());
      nseScoreByCountry.set(country, new Map());
    }
  }

  // ── Enrich features with nse_bracket BEFORE clustering. Brackets
  //    match the rest of the platform: 0-19, 20-39, 40-59, 60-79, 80-100.
  const bracketize = (score: number): string => {
    if (!Number.isFinite(score)) return '';
    if (score < 20) return '0-19';
    if (score < 40) return '20-39';
    if (score < 60) return '40-59';
    if (score < 80) return '60-79';
    return '80-100';
  };
  let nseEnriched = 0;
  for (const f of features) {
    const country = sourceCountries[f.source_megajob_id] || '';
    const nseMap = nseScoreByCountry.get(country);
    if (!nseMap || !f.home_zip) continue;
    const score = nseMap.get(f.home_zip);
    if (score == null) continue;
    f.nse_bracket = bracketize(score);
    nseEnriched++;
  }
  if (nseEnriched > 0) {
    console.log(`[PERSONAS ${runId}] Enriched ${nseEnriched}/${features.length} devices with NSE bracket`);
  }

  console.log(`[PERSONAS ${runId}] clustering ${features.length} devices`);
  const clusteringResult = runClusteringPipeline(features);

  const rfm = computeRfm(features);
  const cohab =
    config.megaJobIds.length >= 2
      ? computeCohabitation(features, config.megaJobIds)
      : undefined;

  // Build the populationLookup expected by computeZipAffinityPerSource
  // (sourceId → zip-pop map). Uses the per-country pop maps loaded above.
  const populationLookup = { bySource: new Map<string, Map<string, number>>() };
  for (const [sourceId, country] of Object.entries(sourceCountries)) {
    const popMap = popMapByCountry.get(country);
    if (popMap && popMap.size > 0) populationLookup.bySource.set(sourceId, popMap);
  }
  // POI centroids per source — needed for the Lift sub-index distance
  // decay. Pulled from sourceMeta (persisted during Stage 1).
  const poiCentroidLookup = { bySource: new Map<string, { lat: number; lng: number }>() };
  for (const [sourceId, meta] of Object.entries(state.sourceMeta || {})) {
    if (meta?.poiCentroid) poiCentroidLookup.bySource.set(sourceId, meta.poiCentroid);
  }
  const zipAffinity = computeZipAffinityPerSource(
    features, sourceLabels, populationLookup, sourceCountries, poiCentroidLookup,
  );
  if (zipAffinity.length > 0) {
    const zipsTotal = zipAffinity.reduce((s, x) => s + x.rows.length, 0);
    const withPop = zipAffinity.filter((s) => s.hasPopulation).length;
    console.log(
      `[PERSONAS ${runId}] zip-affinity: ${zipAffinity.length} source(s) (${withPop} with pop data), ${zipsTotal} ZIP rows total`
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
        case 'visitor_pings_polling':
          state = await phaseVisitorPingsPolling(state);
          break;
        case 'feature_polling':
          state = await phaseFeaturePolling(state);
          break;
        // Legacy state for runs created before the download split — auto-promote.
        case 'feature_ctas' as any:
          state = { ...state, phase: 'feature_polling' };
          break;
        // Legacy state for runs created before the visitor_pings split.
        // The new pipeline launches Stage 1 from `starting` and uses
        // `visitor_pings_polling` for what was previously `feature_ctas`.
        case 'visitor_pings_ctas' as any:
          state = { ...state, phase: 'visitor_pings_polling' };
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
        case 'download_read':
          // No-op transition: route to multi-phase geocoding. Stage 1
          // (geocode_cells) re-streams the CSV with a lightweight parser
          // that only extracts home_zip + lat/lng, then aggregates unique
          // 0.1° cells of devices missing home_zip. Splitting the
          // reverse-geocode out of the heavy clustering function gives the
          // country GeoJSON + cellMap their own memory budget.
          state = { ...state, phase: 'geocode_cells', updatedAt: new Date().toISOString() };
          break;
        case 'geocode_cells':
          state = await phaseGeocodeCells(state);
          break;
        case 'geocode_lookup':
          state = await phaseGeocodeLookup(state);
          break;
        case 'clustering': {
          // Read CSV from S3, parse to DeviceFeatures[] (with pre-computed
          // cell→zip map for missing zips from the geocoding phases), then
          // run k-means + RFM + cohabitation + zip affinity + insights.
          // Diagnostic step tracking: write each step to state.subProgress
          // so a hard crash (OOM, Vercel kill) leaves a breadcrumb in S3.
          let currentStep = 'init';
          const stateRunId = state.runId;
          const baseState = state;
          const writeStep = async (step: string) => {
            currentStep = step;
            console.log(`[PERSONAS ${stateRunId}] clustering step: ${step}`);
            try {
              await putConfig(STATE_KEY(stateRunId), {
                ...baseState,
                subProgress: { label: `clustering: ${step}`, details: new Date().toISOString() },
                updatedAt: new Date().toISOString(),
              } as any, { compact: true });
            } catch {}
          };
          try {
            await writeStep('0/4 load cell→zip map');
            // Tolerate a missing cell-zip artifact (e.g. resumed from older
            // state). Without it, devices without native home_zip will just
            // have empty home_zip — clustering still works.
            const cellToZipObj = (await getConfig<Record<string, string>>(CELL_ZIP_KEY(state.runId))) || {};
            const cellToZip = new Map(Object.entries(cellToZipObj));
            await writeStep(`1/4 phaseDownloadRead start (cellMap: ${cellToZip.size} entries)`);
            const { state: ns, features } = await phaseDownloadRead(state, cellToZip, writeStep);
            await writeStep(`2/4 phaseClusterAndAggregate start (${features.length} features)`);
            const { state: ns2, report, clusterAssignments, clusterNames } =
              await phaseClusterAndAggregate(ns, features);
            await writeStep(`3/4 saving report + cluster data (${clusterAssignments.size} assignments, ${clusterNames.length} clusters)`);
            await putConfig(REPORT_KEY(state.runId), report, { compact: true });
            await putConfig(CLUSTER_DATA_KEY(state.runId), {
              clusterAssignmentsObj: Object.fromEntries(clusterAssignments),
              clusterNames,
            }, { compact: true });
            await writeStep('4/4 done');
            state = {
              ...ns2,
              phase: 'master_maids_export',
              report,
              updatedAt: new Date().toISOString(),
            };
          } catch (e: any) {
            const errMsg = `[clustering crashed at ${currentStep}] ${e?.message || String(e)}\n${(e?.stack || '').slice(0, 500)}`;
            console.error(errMsg);
            try {
              await putConfig(STATE_KEY(state.runId), {
                ...state,
                phase: 'error',
                error: errMsg,
                updatedAt: new Date().toISOString(),
              } as any, { compact: true });
            } catch {}
            throw e;
          }
          break;
        }
        case 'aggregation':
          // Legacy intermediate phase from before the split — restart from
          // download_query if any old runs land here.
          state = { ...state, phase: 'download_query' };
          break;
        case 'master_maids_export': {
          // Kick off the per-persona CTAS exports. phaseMasterMaidsExport
          // returns state with phase='export_polling'.
          if (!state.report) throw new Error('No report in state — re-run from clustering');
          const clusterData = await getConfig<{
            clusterAssignmentsObj: Record<string, number>;
            clusterNames: string[];
          }>(CLUSTER_DATA_KEY(state.runId));
          if (!clusterData) throw new Error('Cluster data not found — re-run from clustering');
          const clusterAssignments = new Map(Object.entries(clusterData.clusterAssignmentsObj));
          state = await phaseMasterMaidsExport(
            state,
            state.report,
            clusterAssignments,
            clusterData.clusterNames,
          );
          break;
        }
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
    visitor_pings_ctas: 7,
    visitor_pings_polling: 18,
    feature_ctas: 22,
    feature_polling: 32,
    enrichment_ctas: 38,
    enrichment_polling: 42,
    download_query: 48,
    download_polling: 56,
    download_read: 60,
    geocode_cells: 64,
    geocode_lookup: 70,
    clustering: 78,
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
    visitor_pings_ctas: 'Launching Stage 1 (visitor pings) in Athena…',
    visitor_pings_polling: 'Stage 1: materializing per-source visitor pings',
    feature_ctas: 'Launching Stage 2 (feature CTAS) in Athena…',
    feature_polling: 'Stage 2: aggregating per-device features in Athena',
    enrichment_ctas: 'Building enrichment tables…',
    enrichment_polling: 'Waiting for enrichment…',
    download_query: 'Launching feature-vector download…',
    download_polling: 'Streaming feature vectors from Athena',
    download_read: 'Preparing feature vectors…',
    geocode_cells: 'Aggregating geographic cells from devices',
    geocode_lookup: 'Reverse-geocoding cells to postal codes',
    clustering: 'Discovering clusters with k-means',
    aggregation: 'Aggregating per-persona stats + insights',
    master_maids_export: 'Exporting personas to Master MAIDs',
    export_polling: 'Finalizing Master MAIDs contributions',
    done: 'Done',
    error: 'Error',
  } as const)[phase];
}
