/**
 * Audience Agent — Runner.
 *
 * Orchestrates audience analysis: single runs and optimized batch processing.
 *
 * Single run: wraps analyzeLaboratory() with S3 persistence.
 * Batch run: ONE spatial join for ALL audiences, in-memory processing per audience.
 */

import { PutObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET, listObjects } from './s3-config';
import { analyzeLaboratory, runSpatialJoin, buildSpatialJoinQueries, buildOriginsCTASQuery, resolveOrigins, geocodeOrigins, processVisitsForRecipe } from './laboratory-analyzer';
import type { ParsedVisit, GeoInfo } from './laboratory-analyzer';
import type { LabAnalysisResult, LabProgressCallback, PoiCategory } from './laboratory-types';
import { startQueryAsync, fetchQueryResults, ensureTableForDataset, runQuery, startCTASAsync, tempTableName, dropTempTable, cleanupTempS3 } from './athena';
import {
  AUDIENCE_CATALOG,
  audienceToLabConfig,
  collectAllCategories,
  type AudienceDefinition,
  type AudienceRunResult,
} from './audience-catalog';
import type { AudienceRunStatus } from './audience-run-status';

// ── Background mode hooks ────────────────────────────────────────────────

export interface BatchRunOptions {
  /** Called between audience iterations to check if user requested stop. */
  checkCancelled?: () => Promise<boolean>;
  /** Called after each phase/audience to persist progress to S3. */
  saveStatus?: (update: Partial<AudienceRunStatus>) => Promise<void>;
}

// ── Single audience run ──────────────────────────────────────────────────

/**
 * Run a single audience analysis and persist results to S3.
 */
export async function runAudienceAnalysis(
  audienceId: string,
  dataset: { id: string; name: string; jobId: string },
  country: string,
  dateFrom?: string,
  dateTo?: string,
  onProgress?: LabProgressCallback,
): Promise<AudienceRunResult> {
  const audience = AUDIENCE_CATALOG.find(a => a.id === audienceId);
  if (!audience) throw new Error(`Unknown audience: ${audienceId}`);

  const config = audienceToLabConfig(audience, dataset, country, dateFrom, dateTo);
  const startedAt = new Date().toISOString();

  try {
    const result = await analyzeLaboratory(config, onProgress);
    return await persistAudienceResult(audience, dataset.id, country, result, startedAt);
  } catch (error: any) {
    return {
      audienceId,
      datasetId: dataset.id,
      country,
      status: 'failed',
      startedAt,
      completedAt: new Date().toISOString(),
      error: error.message,
    };
  }
}

// ── Batch audience run (optimized) ───────────────────────────────────────

/**
 * Run multiple audiences in a single batch with shared spatial join.
 *
 * Flow:
 *   1. Collect ALL categories across all audiences
 *   2. Run ONE spatial join (Athena) with all categories
 *   3. Collect ALL matched ad_ids across all audiences
 *   4. Run ONE origin resolution batch
 *   5. Run ONE geocoding pass
 *   6. For each audience: process visits in-memory, persist to S3
 */
export async function runBatchAudienceAnalysis(
  audienceIds: string[],
  dataset: { id: string; name: string; jobId: string },
  country: string,
  dateFrom?: string,
  dateTo?: string,
  onProgress?: (progress: {
    phase: 'spatial_join' | 'origins' | 'geocoding' | 'processing';
    audienceId?: string;
    audienceName?: string;
    current: number;
    total: number;
    percent: number;
    message: string;
  }) => void,
  options?: BatchRunOptions,
): Promise<Record<string, AudienceRunResult>> {
  const report = onProgress || (() => {});
  const startedAt = new Date().toISOString();
  const results: Record<string, AudienceRunResult> = {};

  // Resolve audience definitions
  const audiences: AudienceDefinition[] = [];
  for (const id of audienceIds) {
    const a = AUDIENCE_CATALOG.find(x => x.id === id);
    if (a) audiences.push(a);
  }

  if (audiences.length === 0) return results;

  // 1. Collect all categories
  const allCategories = collectAllCategories(audiences);
  console.log(`[AUDIENCE-BATCH] ${audiences.length} audiences, ${allCategories.length} unique categories`);

  // 2. Run ONE spatial join
  report({ phase: 'spatial_join', current: 0, total: audiences.length, percent: 5, message: `Running spatial join for ${allCategories.length} categories...` });

  let spatialResult;
  try {
    spatialResult = await runSpatialJoin(
      dataset.id, allCategories, country,
      dateFrom, dateTo, 200, // default radius
    );
  } catch (error: any) {
    console.error(`[AUDIENCE-BATCH] Spatial join failed:`, error.message);
    for (const a of audiences) {
      results[a.id] = {
        audienceId: a.id,
        datasetId: dataset.id,
        country,
        status: 'failed',
        startedAt,
        completedAt: new Date().toISOString(),
        error: `Spatial join failed: ${error.message}`,
      };
    }
    return results;
  }

  const { visits, totalDevicesInDataset } = spatialResult;
  console.log(`[AUDIENCE-BATCH] Spatial join: ${visits.length} visits, ${totalDevicesInDataset} total devices`);

  // Save status: spatial join complete
  if (options?.saveStatus) {
    await options.saveStatus({ phase: 'origins', percent: 40, message: `Spatial join done: ${visits.length} visits` });
  }

  if (visits.length === 0) {
    for (const a of audiences) {
      results[a.id] = {
        audienceId: a.id,
        datasetId: dataset.id,
        country,
        status: 'completed',
        startedAt,
        completedAt: new Date().toISOString(),
        segmentSize: 0,
        segmentPercent: 0,
        totalDevicesInDataset,
        totalPostalCodes: 0,
        avgAffinityIndex: 0,
        avgDwellMinutes: 0,
        topHotspots: [],
      };
    }
    return results;
  }

  // 3. Collect ALL matched ad_ids across all audiences (for shared origin resolution)
  report({ phase: 'origins', current: 0, total: audiences.length, percent: 40, message: 'Identifying matched devices...' });

  const allMatchedAdIds = new Set<string>();
  // Quick pre-pass: for each audience, find matching ad_ids from visits
  const deviceVisitsMap = new Map<string, ParsedVisit[]>();
  for (const v of visits) {
    const list = deviceVisitsMap.get(v.adId) || [];
    list.push(v);
    deviceVisitsMap.set(v.adId, list);
  }

  for (const audience of audiences) {
    const audienceCats = new Set(audience.categories);
    for (const [adId, devVisits] of deviceVisitsMap.entries()) {
      const relevantVisits = devVisits.filter(v => audienceCats.has(v.category));
      if (relevantVisits.length > 0) {
        // Check basic filters (simplified — full check happens in processVisitsForRecipe)
        allMatchedAdIds.add(adId);
      }
    }
  }

  console.log(`[AUDIENCE-BATCH] ${allMatchedAdIds.size} unique devices across all audiences`);

  // 4. Run ONE origin resolution
  report({ phase: 'origins', current: 0, total: audiences.length, percent: 50, message: `Resolving origins for ${allMatchedAdIds.size} devices...` });

  const originMap = await resolveOrigins(
    Array.from(allMatchedAdIds), dataset.id,
    dateFrom, dateTo,
  );

  // Assign origins to visits
  for (const v of visits) {
    const origin = originMap.get(`${v.adId}|${v.date}`);
    if (origin) {
      v.originLat = origin.lat;
      v.originLng = origin.lng;
    }
  }

  // Save status: origins complete
  if (options?.saveStatus) {
    await options.saveStatus({ phase: 'geocoding', percent: 60, message: `Origins resolved for ${allMatchedAdIds.size} devices` });
  }

  // 5. Run ONE geocoding pass
  report({ phase: 'geocoding', current: 0, total: audiences.length, percent: 60, message: 'Geocoding device origins...' });

  const coordToZip = await geocodeOrigins(visits);
  console.log(`[AUDIENCE-BATCH] Geocoded ${coordToZip.size} unique coordinates`);

  // Save status: geocoding complete
  if (options?.saveStatus) {
    await options.saveStatus({ phase: 'processing', percent: 65, message: `Geocoded ${coordToZip.size} coordinates` });
  }

  // 6. Process each audience in-memory
  for (let i = 0; i < audiences.length; i++) {
    // Check cancellation before each audience
    if (options?.checkCancelled) {
      const cancelled = await options.checkCancelled();
      if (cancelled) {
        console.log(`[AUDIENCE-BATCH] Cancellation requested after ${i} audiences`);
        for (let j = i; j < audiences.length; j++) {
          results[audiences[j].id] = {
            audienceId: audiences[j].id,
            datasetId: dataset.id,
            country,
            status: 'failed',
            startedAt,
            completedAt: new Date().toISOString(),
            error: 'Cancelled by user',
          };
        }
        break;
      }
    }

    const audience = audiences[i];
    const pct = 65 + Math.round(((i + 1) / audiences.length) * 30);

    report({
      phase: 'processing',
      audienceId: audience.id,
      audienceName: audience.name,
      current: i + 1,
      total: audiences.length,
      percent: pct,
      message: `Processing ${audience.name} (${i + 1}/${audiences.length})...`,
    });

    try {
      const config = audienceToLabConfig(audience, dataset, country, dateFrom, dateTo);
      const labResult = processVisitsForRecipe({
        config,
        allVisits: visits,
        totalDevicesInDataset,
        originMap,
        coordToZip,
      });

      results[audience.id] = await persistAudienceResult(audience, dataset.id, country, labResult, startedAt);
      console.log(`[AUDIENCE-BATCH] ${audience.name}: ${labResult.stats.segmentSize} devices, AI=${labResult.stats.avgAffinityIndex}`);
    } catch (error: any) {
      console.error(`[AUDIENCE-BATCH] ${audience.name} failed:`, error.message);
      results[audience.id] = {
        audienceId: audience.id,
        datasetId: dataset.id,
        country,
        status: 'failed',
        startedAt,
        completedAt: new Date().toISOString(),
        error: error.message,
      };
    }

    // Save status after each audience
    if (options?.saveStatus) {
      await options.saveStatus({
        current: i + 1,
        currentAudienceName: audience.name,
        percent: pct,
        message: `Completed ${audience.name} (${i + 1}/${audiences.length})`,
        completedAudiences: Object.keys(results).filter(id => results[id].status === 'completed'),
      });
    }
  }

  report({ phase: 'processing', current: audiences.length, total: audiences.length, percent: 100, message: `Batch complete: ${audiences.length} audiences processed` });

  return results;
}

// ── Async batch start (fire-and-forget Athena CTAS) ─────────────────────

/**
 * Phase 1 of the async CTAS pipeline.
 * Ensures tables exist, builds SQL, fires Athena queries, returns immediately.
 *
 * Q1: CTAS spatial join → temp_visits_{runId}  (materializes ~200MB Parquet)
 * Q2: COUNT DISTINCT total devices             (lightweight, returns 1 number)
 *
 * The queries continue running in Athena — poll with checkQueryStatus().
 */
export async function startBatchAsync(
  audienceIds: string[],
  dataset: { id: string; name: string; jobId: string },
  country: string,
  runId: string,
  dateFrom?: string,
  dateTo?: string,
): Promise<{ spatialQueryId: string; totalDevicesQueryId: string; visitsTableName: string }> {
  // Resolve audience definitions, collect all categories
  const audiences: AudienceDefinition[] = [];
  for (const id of audienceIds) {
    const a = AUDIENCE_CATALOG.find(x => x.id === id);
    if (a) audiences.push(a);
  }
  if (audiences.length === 0) throw new Error('No valid audiences');

  const allCategories = collectAllCategories(audiences);
  console.log(`[AUDIENCE-BATCH-ASYNC] ${audiences.length} audiences, ${allCategories.length} unique categories`);

  // Ensure tables exist (fast — skips if already set up)
  await ensureTableForDataset(dataset.id);
  try {
    await runQuery(`
      CREATE EXTERNAL TABLE IF NOT EXISTS lab_pois_gmc (
        id STRING, name STRING, category STRING, city STRING,
        postal_code STRING, country STRING, latitude DOUBLE, longitude DOUBLE
      )
      STORED AS PARQUET
      LOCATION 's3://${BUCKET}/pois_gmc/'
    `);
  } catch (error: any) {
    if (!error.message?.includes('already exists')) {
      console.warn(`[AUDIENCE-BATCH-ASYNC] Warning creating POI table:`, error.message);
    }
  }

  // Build SQL queries
  const { spatialSelectForCTAS, totalDevicesQuery } = buildSpatialJoinQueries(
    dataset.id, allCategories, country, dateFrom, dateTo, 200,
  );

  // Generate temp table name for CTAS spatial join
  const visitsTable = tempTableName('visits', runId);

  // Fire both queries asynchronously:
  // - Q1 as CTAS → creates temp_visits_{runId} table backed by Parquet in S3
  // - Q2 as regular query → just returns a count
  const [spatialQueryId, totalDevicesQueryId] = await Promise.all([
    startCTASAsync(spatialSelectForCTAS, visitsTable),
    startQueryAsync(totalDevicesQuery),
  ]);

  console.log(`[AUDIENCE-BATCH-ASYNC] Fired CTAS queries: spatial=${spatialQueryId} (→ ${visitsTable}), totalDevices=${totalDevicesQueryId}`);
  return { spatialQueryId, totalDevicesQueryId, visitsTableName: visitsTable };
}

// ── Async origins start (Phase 2 — triggered by /status when Q1 succeeds) ──

/**
 * Fire Q3: CTAS origins query.
 * Called from the /status endpoint once the spatial CTAS (Q1) has SUCCEEDED.
 * Joins the full dataset against the small materialized visits table to resolve
 * first-ping-of-day origins for ALL devices — ONE query instead of N/500.
 */
export async function startOriginsAsync(
  datasetId: string,
  runId: string,
  visitsTableName: string,
  dateFrom?: string,
  dateTo?: string,
): Promise<{ originsQueryId: string; originsTableName: string }> {
  const originsTable = tempTableName('origins', runId);
  const selectSql = buildOriginsCTASQuery(datasetId, visitsTableName, dateFrom, dateTo);

  const originsQueryId = await startCTASAsync(selectSql, originsTable);
  console.log(`[AUDIENCE-BATCH-ASYNC] Fired CTAS origins: ${originsQueryId} (→ ${originsTable})`);

  return { originsQueryId, originsTableName: originsTable };
}

// ── Async batch continue (Phase 3: read temp tables + process) ───────────

/**
 * Phase 3 of the async CTAS pipeline.
 * Called after ALL Athena queries (Q1 spatial CTAS, Q2 total devices, Q3 origins CTAS)
 * are SUCCEEDED. Reads from the small materialized temp tables — no more N/500 queries.
 *
 * Data flow:
 *   SELECT * FROM temp_visits_{runId}   (~200MB, fast)
 *   SELECT * FROM temp_origins_{runId}  (~50MB, fast)
 *   + fetchQueryResults(totalDevicesQueryId) for the count
 *   → geocode origins (local, ~30s)
 *   → process each audience (in-memory, ~5s/audience)
 *   → persist results to S3
 *   → cleanup temp tables + S3 (fire-and-forget)
 */
export async function continueBatchProcessing(
  audienceIds: string[],
  dataset: { id: string; name: string; jobId: string },
  country: string,
  totalDevicesQueryId: string,
  visitsTableName: string,
  originsTableName: string,
  dateFrom?: string,
  dateTo?: string,
  options?: BatchRunOptions,
): Promise<Record<string, AudienceRunResult>> {
  const startedAt = new Date().toISOString();
  const results: Record<string, AudienceRunResult> = {};

  // Resolve audiences
  const audiences = audienceIds
    .map(id => AUDIENCE_CATALOG.find(x => x.id === id))
    .filter(Boolean) as AudienceDefinition[];

  if (audiences.length === 0) return results;

  // 1. Read from temp tables + fetch total devices count
  console.log(`[AUDIENCE-BATCH-CONTINUE] Reading from temp tables: ${visitsTableName}, ${originsTableName}`);
  if (options?.saveStatus) {
    await options.saveStatus({ phase: 'processing', percent: 70, message: 'Reading materialized results from temp tables...' });
  }

  const [visitsRes, originsRes, totalRes] = await Promise.all([
    runQuery(`SELECT ad_id, date, poi_id, category, dwell_minutes, visit_hour, ping_count FROM ${visitsTableName}`),
    runQuery(`SELECT ad_id, date, origin_lat, origin_lng FROM ${originsTableName}`),
    fetchQueryResults(totalDevicesQueryId),
  ]);

  const totalDevicesInDataset = parseInt(String(totalRes.rows[0]?.total)) || 0;

  // 2. Parse visits
  const visits: ParsedVisit[] = visitsRes.rows.map(row => ({
    adId: String(row.ad_id),
    date: String(row.date),
    poiId: String(row.poi_id),
    category: String(row.category) as PoiCategory,
    dwellMinutes: parseFloat(String(row.dwell_minutes)) || 0,
    visitHour: parseInt(String(row.visit_hour)) || 0,
    originLat: 0,
    originLng: 0,
  }));

  console.log(`[AUDIENCE-BATCH-CONTINUE] Visits: ${visits.length}, Origins: ${originsRes.rows.length}, Total devices: ${totalDevicesInDataset}`);

  // 3. Build origin map from temp_origins table (replaces resolveOrigins N/500 queries)
  const originMap = new Map<string, { lat: number; lng: number }>();
  for (const row of originsRes.rows) {
    const lat = parseFloat(String(row.origin_lat));
    const lng = parseFloat(String(row.origin_lng));
    if (!isNaN(lat) && !isNaN(lng) && lat !== 0 && lng !== 0) {
      originMap.set(`${row.ad_id}|${row.date}`, { lat, lng });
    }
  }

  // Assign origins to visits
  for (const v of visits) {
    const origin = originMap.get(`${v.adId}|${v.date}`);
    if (origin) { v.originLat = origin.lat; v.originLng = origin.lng; }
  }

  console.log(`[AUDIENCE-BATCH-CONTINUE] Origin map: ${originMap.size} device-day pairs`);

  if (options?.saveStatus) {
    await options.saveStatus({ phase: 'processing', percent: 75, message: `Loaded ${visits.length} visits, ${originMap.size} origins` });
  }

  if (visits.length === 0) {
    for (const a of audiences) {
      results[a.id] = {
        audienceId: a.id, datasetId: dataset.id, country,
        status: 'completed', startedAt, completedAt: new Date().toISOString(),
        segmentSize: 0, segmentPercent: 0, totalDevicesInDataset,
        totalPostalCodes: 0, avgAffinityIndex: 0, avgDwellMinutes: 0, topHotspots: [],
      };
    }
    // Cleanup temp tables even on empty results
    cleanupTempTables(visitsTableName, originsTableName);
    return results;
  }

  // 4. Geocode origins (local GeoJSON — very fast, ~30s)
  if (options?.saveStatus) {
    await options.saveStatus({ phase: 'processing', percent: 78, message: 'Geocoding device origins...' });
  }

  const coordToZip = await geocodeOrigins(visits);
  console.log(`[AUDIENCE-BATCH-CONTINUE] Geocoded ${coordToZip.size} unique coordinates`);

  if (options?.saveStatus) {
    await options.saveStatus({ phase: 'processing', percent: 80, message: `Geocoded ${coordToZip.size} coordinates` });
  }

  // 5. Process each audience in-memory
  for (let i = 0; i < audiences.length; i++) {
    // Check cancellation
    if (options?.checkCancelled) {
      const cancelled = await options.checkCancelled();
      if (cancelled) {
        console.log(`[AUDIENCE-BATCH-CONTINUE] Cancellation requested after ${i} audiences`);
        for (let j = i; j < audiences.length; j++) {
          results[audiences[j].id] = {
            audienceId: audiences[j].id, datasetId: dataset.id, country,
            status: 'failed', startedAt, completedAt: new Date().toISOString(),
            error: 'Cancelled by user',
          };
        }
        break;
      }
    }

    const audience = audiences[i];
    const pct = 80 + Math.round(((i + 1) / audiences.length) * 18); // 80-98%

    if (options?.saveStatus) {
      await options.saveStatus({
        current: i + 1,
        currentAudienceName: audience.name,
        percent: pct,
        message: `Processing ${audience.name} (${i + 1}/${audiences.length})...`,
      });
    }

    try {
      const config = audienceToLabConfig(audience, dataset, country, dateFrom, dateTo);
      const labResult = processVisitsForRecipe({
        config, allVisits: visits, totalDevicesInDataset, originMap, coordToZip,
      });

      results[audience.id] = await persistAudienceResult(audience, dataset.id, country, labResult, startedAt);
      console.log(`[AUDIENCE-BATCH-CONTINUE] ${audience.name}: ${labResult.stats.segmentSize} devices, AI=${labResult.stats.avgAffinityIndex}`);
    } catch (error: any) {
      console.error(`[AUDIENCE-BATCH-CONTINUE] ${audience.name} failed:`, error.message);
      results[audience.id] = {
        audienceId: audience.id, datasetId: dataset.id, country,
        status: 'failed', startedAt, completedAt: new Date().toISOString(),
        error: error.message,
      };
    }

    if (options?.saveStatus) {
      await options.saveStatus({
        current: i + 1,
        currentAudienceName: audience.name,
        percent: pct,
        message: `Completed ${audience.name} (${i + 1}/${audiences.length})`,
        completedAudiences: Object.keys(results).filter(id => results[id].status === 'completed'),
      });
    }
  }

  // 6. Cleanup temp tables + S3 (fire-and-forget — don't block the response)
  cleanupTempTables(visitsTableName, originsTableName);

  return results;
}

/**
 * Fire-and-forget cleanup of CTAS temp tables and their S3 data.
 * Drops Glue catalog entries and deletes Parquet files from athena-temp/.
 */
function cleanupTempTables(visitsTableName: string, originsTableName: string): void {
  Promise.all([
    dropTempTable(visitsTableName),
    dropTempTable(originsTableName),
    cleanupTempS3(visitsTableName),
    cleanupTempS3(originsTableName),
  ]).then(() => {
    console.log(`[CLEANUP] Temp tables cleaned: ${visitsTableName}, ${originsTableName}`);
  }).catch(err => {
    console.warn(`[CLEANUP] Some cleanup failed (non-fatal):`, err.message);
  });
}

// ── Persist results to S3 ────────────────────────────────────────────────

async function persistAudienceResult(
  audience: AudienceDefinition,
  datasetId: string,
  country: string,
  result: LabAnalysisResult,
  startedAt: string,
): Promise<AudienceRunResult> {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const prefix = `audiences/${datasetId}/${country.toLowerCase()}/${audience.id}`;

  // Use allSegmentDevices (full list) for CSV, fallback to devices (truncated)
  const allDevices = result.segment.allSegmentDevices || result.segment.devices;

  // Store full result JSON
  const resultKey = `${prefix}/${timestamp}-result.json`;
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: resultKey,
    Body: JSON.stringify(result),
    ContentType: 'application/json',
  }));

  // Store segment CSV (all ad_ids)
  const csvKey = `${prefix}/${timestamp}-segment.csv`;
  const csvContent = 'ad_id,matched_steps,total_visits,avg_dwell_minutes,categories\n' +
    allDevices.map(d =>
      `${d.adId},${d.matchedSteps},${d.totalVisits},${d.avgDwellMinutes},"${d.categories.join(';')}"`
    ).join('\n');
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: csvKey,
    Body: csvContent,
    ContentType: 'text/csv',
  }));

  // Build summary
  const summary: AudienceRunResult = {
    audienceId: audience.id,
    datasetId,
    country,
    status: 'completed',
    startedAt,
    completedAt: new Date().toISOString(),
    segmentSize: result.stats.segmentSize,
    segmentPercent: result.stats.segmentPercent,
    totalDevicesInDataset: result.stats.totalDevicesInDataset,
    totalPostalCodes: result.stats.totalPostalCodes,
    avgAffinityIndex: result.stats.avgAffinityIndex,
    avgDwellMinutes: result.stats.avgDwellMinutes,
    topHotspots: result.stats.topHotspots.slice(0, 10).map(h => ({
      zipcode: h.zipcode,
      city: h.city,
      category: h.category,
      affinityIndex: h.affinityIndex,
      visits: h.visits,
    })),
    s3ResultPath: `s3://${BUCKET}/${resultKey}`,
    s3SegmentCsvPath: `s3://${BUCKET}/${csvKey}`,
  };

  // Store latest pointer (overwrite each run)
  const latestKey = `${prefix}/latest.json`;
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: latestKey,
    Body: JSON.stringify(summary, null, 2),
    ContentType: 'application/json',
  }));

  return summary;
}

// ── Load results from S3 ─────────────────────────────────────────────────

/**
 * Load all audience results for a dataset+country from S3.
 */
export async function loadAudienceResults(
  datasetId: string,
  country: string,
): Promise<AudienceRunResult[]> {
  const prefix = `audiences/${datasetId}/${country.toLowerCase()}/`;
  const keys = await listObjects(prefix);

  const latestKeys = keys.filter(k => k.endsWith('/latest.json'));
  const results: AudienceRunResult[] = [];

  for (const key of latestKeys) {
    try {
      const { GetObjectCommand } = await import('@aws-sdk/client-s3');
      const response = await s3Client.send(new GetObjectCommand({
        Bucket: BUCKET,
        Key: key,
      }));
      const body = await response.Body?.transformToString();
      if (body) {
        results.push(JSON.parse(body));
      }
    } catch {
      // Skip failed reads
    }
  }

  return results;
}
