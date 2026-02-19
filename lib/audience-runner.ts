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
import { analyzeLaboratory, runSpatialJoin, resolveOrigins, geocodeOrigins, processVisitsForRecipe } from './laboratory-analyzer';
import type { ParsedVisit, GeoInfo } from './laboratory-analyzer';
import type { LabAnalysisResult, LabProgressCallback, PoiCategory } from './laboratory-types';
import {
  AUDIENCE_CATALOG,
  audienceToLabConfig,
  collectAllCategories,
  type AudienceDefinition,
  type AudienceRunResult,
} from './audience-catalog';

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

  // 5. Run ONE geocoding pass
  report({ phase: 'geocoding', current: 0, total: audiences.length, percent: 60, message: 'Geocoding device origins...' });

  const coordToZip = await geocodeOrigins(visits);
  console.log(`[AUDIENCE-BATCH] Geocoded ${coordToZip.size} unique coordinates`);

  // 6. Process each audience in-memory
  for (let i = 0; i < audiences.length; i++) {
    const audience = audiences[i];
    const pct = 65 + Math.round((i / audiences.length) * 30);

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
  }

  report({ phase: 'processing', current: audiences.length, total: audiences.length, percent: 100, message: `Batch complete: ${audiences.length} audiences processed` });

  return results;
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
