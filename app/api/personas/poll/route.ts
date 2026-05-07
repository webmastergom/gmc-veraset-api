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
import { buildFeatureCTAS, extractPoiCoords } from '@/lib/persona-feature-query';
import { runClusteringPipeline } from '@/lib/persona-clusterer';
import { computeRfm } from '@/lib/persona-rfm';
import { computeCohabitation } from '@/lib/persona-cohabitation';
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

// ─── Phase: starting ──────────────────────────────────────────────────

async function phaseStarting(state: PersonaState): Promise<PersonaState> {
  const { config, runId } = state;
  console.log(`[PERSONAS ${runId}] starting: megaJobs=${config.megaJobIds.join(',')}`);

  const featureCtas: Record<string, { queryId: string; tableName: string }> = {};

  for (const megaJobId of config.megaJobIds) {
    const megaJob = await getMegaJob(megaJobId);
    if (!megaJob) throw new Error(`Mega-job ${megaJobId} not found`);
    if (!megaJob.subJobIds?.length) throw new Error(`Mega-job ${megaJobId} has no sub-jobs`);

    // Load all sub-jobs
    const subJobs = (
      await Promise.all(megaJob.subJobIds.map((j) => getJob(j)))
    ).filter((j): j is NonNullable<typeof j> => j !== null);

    const syncedJobs = subJobs.filter((j) => j.status === 'SUCCESS' && j.syncedAt);
    if (syncedJobs.length === 0) {
      throw new Error(`Mega-job ${megaJobId} has no synced sub-jobs yet`);
    }

    // Ensure all dataset tables exist.
    await Promise.all(
      syncedJobs.map(async (j) => {
        const ds = j.s3DestPath?.replace(/\/$/, '').split('/').pop();
        if (ds) await ensureTableForDataset(ds);
      })
    );

    // Extract POI coords (for spatial join).
    const poiCoords = extractPoiCoords(syncedJobs);
    if (poiCoords.length === 0) throw new Error(`No POI coords for mega-job ${megaJobId}`);

    // Resolve brand for each POI from the source collection's GeoJSON.
    const collectionIds = megaJob.sourceScope?.poiCollectionIds || [];
    const poiToBrand: Array<{ poiId: string; brand: string }> = [];
    for (const colId of collectionIds) {
      const geo = await getPOICollection(colId);
      if (!geo?.features) continue;
      for (const f of geo.features as any[]) {
        const id = f.properties?.id || f.id;
        const name = f.properties?.name || f.properties?.label || '';
        if (!id) continue;
        poiToBrand.push({ poiId: String(id), brand: resolveBrand(name) });
      }
    }
    if (poiToBrand.length === 0) {
      throw new Error(`No POIs with names found for mega-job ${megaJobId} (collections=${collectionIds.join(',')})`);
    }

    // Materialize as external CSV table.
    const safeMjId = megaJobId.replace(/-/g, '_');
    const brandTableBase = `persona_brands_${safeMjId}_${runId}`;
    const brandTable = brandTableBase.length > 60 ? brandTableBase.slice(0, 60) : brandTableBase;
    const brandS3Key = `athena-temp/${brandTable}/data.csv`;
    const csvBody = ['poi_id,brand']
      .concat(poiToBrand.map((b) => `${b.poiId},${b.brand}`))
      .join('\n');

    await s3Client.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: brandS3Key,
        Body: csvBody,
        ContentType: 'text/csv',
      })
    );

    // Drop existing brand table before recreating.
    try { await runQuery(`DROP TABLE IF EXISTS ${brandTable}`); } catch {}

    const createBrandTableSql = `
      CREATE EXTERNAL TABLE ${brandTable} (
        poi_id STRING,
        brand STRING
      )
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
      STORED AS TEXTFILE
      LOCATION 's3://${BUCKET}/athena-temp/${brandTable}/'
      TBLPROPERTIES ('skip.header.line.count' = '1')
    `;
    await runQuery(createBrandTableSql);
    console.log(`[PERSONAS ${runId}] Created brand table ${brandTable} (${poiToBrand.length} POIs)`);

    // Build & launch feature CTAS.
    const featureTableBase = `persona_features_${safeMjId}_${runId}`;
    const featureTable = featureTableBase.length > 60 ? featureTableBase.slice(0, 60) : featureTableBase;
    try { await runQuery(`DROP TABLE IF EXISTS ${featureTable}`); } catch {}
    const featureS3 = `s3://${BUCKET}/athena-temp/${featureTable}/`;

    // dateRangeTo from megaJob's sourceScope or syncedJobs max
    const rangeTo =
      megaJob.sourceScope?.dateRange?.to ||
      syncedJobs.reduce((max, j) => (j.dateRange?.to && j.dateRange.to > max ? j.dateRange.to : max), '');
    if (!rangeTo) throw new Error(`Cannot determine date range for mega-job ${megaJobId}`);

    const sql = buildFeatureCTAS({
      ctasTable: featureTable,
      ctasS3Path: featureS3,
      syncedJobs,
      poiCoords,
      brandTableRef: brandTable,
      dateRangeTo: rangeTo,
      sourceMegajobId: megaJobId,
      filters: config.filters as any,
    });

    const queryId = await startQueryAsync(sql);
    console.log(`[PERSONAS ${runId}] Feature CTAS started for ${megaJobId}: queryId=${queryId}`);
    featureCtas[megaJobId] = { queryId, tableName: featureTable };
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
  if (!allDone) {
    return { ...state, updatedAt: new Date().toISOString() };
  }
  return { ...state, phase: 'download', updatedAt: new Date().toISOString() };
}

// ─── Phase: download ─────────────────────────────────────────────────

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

async function phaseDownload(state: PersonaState): Promise<{ state: PersonaState; features: DeviceFeatures[] }> {
  if (!state.featureCtas) throw new Error('No featureCtas in state');
  const all: DeviceFeatures[] = [];
  for (const [megaJobId, { tableName }] of Object.entries(state.featureCtas)) {
    const result = await runQueryViaS3(`SELECT * FROM ${tableName}`);
    for (const r of result.rows as any[]) {
      const brandVisits = parseBrandJson(r.brand_visits_json);
      all.push({
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
        gps_share: parseFloat(r.gps_share) || 0,
        avg_circle_score: parseFloat(r.avg_circle_score) || 0,
        brand_visits: brandVisits,
        brand_loyalty_hhi: parseFloat(r.brand_loyalty_hhi) || 0,
        nearby_categories_top5: [], // not yet computed in v1
        tier_high_quality: r.tier_high_quality === true || r.tier_high_quality === 'true',
        source_megajob_id: megaJobId,
      });
    }
    console.log(`[PERSONAS ${state.runId}] Downloaded ${result.rows.length} feature rows for ${megaJobId}`);
  }
  return {
    state: { ...state, phase: 'clustering', updatedAt: new Date().toISOString() },
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

  // Materialize labels CSV: (ad_id, persona_name)
  const labelsKey = `athena-temp/persona-labels/${runId}/labels.csv`;
  const labelsTable = `persona_labels_${runId.replace(/-/g, '_')}`;
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
  await runQuery(`
    CREATE EXTERNAL TABLE ${labelsTable} (
      ad_id STRING,
      persona_name STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    STORED AS TEXTFILE
    LOCATION 's3://${BUCKET}/athena-temp/persona-labels/${runId}/'
    TBLPROPERTIES ('skip.header.line.count' = '1')
  `);
  console.log(`[PERSONAS ${runId}] Labels table ${labelsTable} ready`);

  // Per cluster CTAS in parallel.
  const exportQueryIds: Record<string, string> = {};
  const exportTables: Record<string, string> = {};
  for (const persona of report.personas) {
    if (persona.deviceCount === 0) continue;
    const slug = persona.name.replace(/[^a-z0-9]+/gi, '_').toLowerCase().slice(0, 30);
    const table = `master_persona_${slug}_${runId.slice(0, 8)}`;
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
        await registerAthenaContribution(
          country,
          state.config.megaJobIds.join('+'),
          'persona' as any, // 'persona' added to AttributeType in master-maids.ts
          persona.name,
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

async function guessCountry(state: PersonaState): Promise<string | null> {
  for (const mjId of state.config.megaJobIds) {
    const mj = await getMegaJob(mjId);
    if (mj?.country) return mj.country;
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
  return { from: from || '', to: to || '' };
}

// ─── Route handler ───────────────────────────────────────────────────

export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    let body: any = {};
    try { body = await request.json(); } catch {}

    const isNewRequest = Array.isArray(body.megaJobIds) && body.megaJobIds.length > 0;
    let runId: string = body.runId || '';
    if (isNewRequest) {
      const cfg: PersonaRunConfig = {
        megaJobIds: body.megaJobIds.filter((s: any) => typeof s === 'string'),
        filters: body.filters || {},
      };
      runId = configToRunId(cfg);
      // Idempotency: if state exists and is done, return cached
      invalidateCache(STATE_KEY(runId));
      const existing = await getConfig<PersonaState>(STATE_KEY(runId));
      if (existing?.phase === 'done' && existing.report) {
        return NextResponse.json({ phase: 'done', runId, report: existing.report });
      }
      // Reset on error or first time
      const initialState: PersonaState = {
        phase: 'starting',
        runId,
        config: cfg,
        updatedAt: new Date().toISOString(),
      };
      await putConfig(STATE_KEY(runId), initialState, { compact: true });
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
        case 'download': {
          const { state: ns, features } = await phaseDownload(state);
          const { state: ns2, report, clusterAssignments, clusterNames } = await phaseClusterAndAggregate(ns, features);
          // Save the report early so frontend can render scorecard while exports finish.
          await putConfig(REPORT_KEY(state.runId), report, { compact: true });
          state = await phaseMasterMaidsExport(ns2, report, clusterAssignments, clusterNames);
          break;
        }
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
        message: phaseMessage(state.phase),
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
    feature_polling: 30,
    enrichment_ctas: 35,
    enrichment_polling: 45,
    download: 55,
    clustering: 70,
    aggregation: 80,
    master_maids_export: 88,
    export_polling: 95,
    done: 100,
    error: 0,
  } as const)[phase];
}

function phaseMessage(phase: PersonaPhase): string {
  return ({
    starting: 'Resolving POIs and brands…',
    feature_ctas: 'Launching feature CTAS…',
    feature_polling: 'Running per-device feature aggregation in Athena…',
    enrichment_ctas: 'Building enrichment tables…',
    enrichment_polling: 'Waiting for enrichment…',
    download: 'Downloading feature vectors…',
    clustering: 'Discovering clusters with k-means…',
    aggregation: 'Aggregating per-persona stats and insights…',
    master_maids_export: 'Exporting personas to Master MAIDs…',
    export_polling: 'Finalizing Master MAIDs contributions…',
    done: 'Done',
    error: 'Error',
  } as const)[phase];
}
