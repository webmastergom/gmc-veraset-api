/**
 * Mega-Jobs: orchestration layer for jobs that exceed Veraset limits.
 * - Max 31 days per job  → auto-split into date chunks
 * - Max 25,000 POIs per job → auto-split into POI chunks
 * - Manual grouping of existing jobs for consolidated reports
 *
 * Storage: config/mega-jobs/{megaJobId}.json  (per-file, ~1-10 KB)
 *          config/mega-jobs-index.json        (lightweight index)
 */

import { randomUUID } from 'crypto';
import { getConfig, putConfig, initConfigIfNeeded, invalidateCache } from './s3-config';

// ── Types ─────────────────────────────────────────────────────────────

export type MegaJobMode = 'auto-split' | 'manual-group';

export type MegaJobStatus =
  | 'planning'       // Split plan computed, awaiting user confirmation
  | 'creating'       // Sub-jobs being created via Veraset API (one per poll)
  | 'running'        // All sub-jobs created, waiting for Veraset to process + sync
  | 'consolidating'  // All sub-jobs synced, generating consolidated reports
  | 'completed'      // All reports ready
  | 'partial'        // Some sub-jobs failed, partial results available
  | 'error';         // Fatal error

export interface DateChunk {
  from: string; // YYYY-MM-DD
  to: string;   // YYYY-MM-DD
}

export interface PoiChunk {
  startIndex: number;
  endIndex: number;   // exclusive
  count: number;
}

export interface SplitPlan {
  dateChunks: DateChunk[];
  poiChunks: PoiChunk[];
  totalSubJobs: number; // dateChunks.length * poiChunks.length
}

export interface MegaJobProgress {
  created: number;   // sub-jobs successfully created via Veraset API
  synced: number;    // sub-jobs with SUCCESS status + synced data
  failed: number;    // sub-jobs that failed
  total: number;     // total sub-jobs expected
}

export interface ConsolidatedReports {
  visitsByPoi?: string;     // S3 key: config/mega-reports/{id}/visits.json
  catchment?: string;       // S3 key: config/mega-reports/{id}/catchment.json
  temporalTrends?: string;  // S3 key: config/mega-reports/{id}/temporal.json
  od?: string;              // S3 key: config/mega-reports/{id}/od.json
  hourly?: string;          // S3 key: config/mega-reports/{id}/hourly.json
  mobility?: string;        // S3 key: config/mega-reports/{id}/mobility.json
  maids?: string;           // S3 key: athena-results/{queryId}.csv
  [key: string]: string | undefined;
}

export interface MegaJob {
  megaJobId: string;
  name: string;
  description?: string;
  country?: string;
  mode: MegaJobMode;

  // Auto-split: the "big" scope the user defined
  sourceScope?: {
    poiCollectionId: string;
    dateRange: { from: string; to: string };
    radius: number;
    schema: 'BASIC' | 'ENHANCED' | 'FULL';
    type: 'pings' | 'aggregate' | 'devices' | 'cohort' | 'pings_by_device';
  };

  // Computed split plan (auto-split mode)
  splits?: SplitPlan;

  // Sub-job references (ordered: [date0-poi0, date0-poi1, date1-poi0, date1-poi1, ...])
  subJobIds: string[];

  // Status tracking
  status: MegaJobStatus;
  progress: MegaJobProgress;

  // Consolidated report S3 keys
  consolidatedReports?: ConsolidatedReports;

  // Error info
  error?: string;

  createdAt: string;
  updatedAt: string;
}

// ── S3 key helpers ────────────────────────────────────────────────────

type MegaJobsIndexData = Record<string, Partial<MegaJob>>;

function megaJobKey(id: string): string {
  return `mega-jobs/${id}`;
}

const HEAVY_FIELDS: (keyof MegaJob)[] = [
  'sourceScope',
  'splits',
  'consolidatedReports',
];

function toIndexEntry(mj: MegaJob): Partial<MegaJob> {
  const entry: Record<string, any> = {};
  for (const key of Object.keys(mj) as (keyof MegaJob)[]) {
    if (!HEAVY_FIELDS.includes(key)) {
      entry[key] = mj[key];
    }
  }
  return entry as Partial<MegaJob>;
}

// ── Index management ──────────────────────────────────────────────────

async function getIndex(): Promise<MegaJobsIndexData> {
  return await initConfigIfNeeded<MegaJobsIndexData>('mega-jobs-index', {});
}

async function upsertIndex(id: string, entry: Partial<MegaJob>): Promise<void> {
  invalidateCache('mega-jobs-index');
  const index = await getIndex();
  index[id] = entry;
  await putConfig('mega-jobs-index', index);
}

async function removeFromIndex(id: string): Promise<void> {
  invalidateCache('mega-jobs-index');
  const index = await getIndex();
  delete index[id];
  await putConfig('mega-jobs-index', index);
}

// ── Public CRUD ───────────────────────────────────────────────────────

/**
 * Get all mega-jobs sorted by creation date (newest first).
 * Reads from lightweight index only.
 */
export async function getAllMegaJobs(): Promise<Partial<MegaJob>[]> {
  const index = await getIndex();
  return Object.values(index).sort((a, b) =>
    new Date((b.createdAt as string) || '').getTime() -
    new Date((a.createdAt as string) || '').getTime()
  );
}

/**
 * Get a specific mega-job by ID (full data).
 */
export async function getMegaJob(id: string): Promise<MegaJob | null> {
  return await getConfig<MegaJob>(megaJobKey(id));
}

/**
 * Create a new mega-job. Returns the saved record.
 */
export async function createMegaJob(
  input: Omit<MegaJob, 'megaJobId' | 'createdAt' | 'updatedAt'>
): Promise<MegaJob> {
  const megaJob: MegaJob = {
    ...input,
    megaJobId: randomUUID(),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  await putConfig(megaJobKey(megaJob.megaJobId), megaJob);
  await upsertIndex(megaJob.megaJobId, toIndexEntry(megaJob));
  console.log(`[MEGA-JOB] Created: ${megaJob.megaJobId} - ${megaJob.name} (${megaJob.mode})`);
  return megaJob;
}

/**
 * Update an existing mega-job (partial merge).
 */
export async function updateMegaJob(
  id: string,
  updates: Partial<MegaJob>
): Promise<MegaJob | null> {
  invalidateCache(megaJobKey(id));
  const current = await getConfig<MegaJob>(megaJobKey(id));
  if (!current) return null;

  const updated: MegaJob = {
    ...current,
    ...updates,
    updatedAt: new Date().toISOString(),
  };

  await putConfig(megaJobKey(id), updated);
  await upsertIndex(id, toIndexEntry(updated));
  return updated;
}

// ── Split algorithm ───────────────────────────────────────────────────

const MAX_DAYS = 31;
const MAX_POIS = 25_000;

/**
 * Compute how to split a large scope into sub-jobs that respect Veraset limits.
 * Returns the split plan with date chunks and POI chunks.
 */
export function computeSplitPlan(
  dateRange: { from: string; to: string },
  totalPois: number
): SplitPlan {
  // Date chunks
  const dateChunks: DateChunk[] = [];
  const startDate = new Date(dateRange.from + 'T00:00:00Z');
  const endDate = new Date(dateRange.to + 'T00:00:00Z');

  let chunkStart = new Date(startDate);
  while (chunkStart <= endDate) {
    const chunkEnd = new Date(chunkStart);
    chunkEnd.setUTCDate(chunkEnd.getUTCDate() + MAX_DAYS - 1);
    if (chunkEnd > endDate) chunkEnd.setTime(endDate.getTime());

    dateChunks.push({
      from: chunkStart.toISOString().split('T')[0],
      to: chunkEnd.toISOString().split('T')[0],
    });

    // Next chunk starts the day after this chunk ends
    chunkStart = new Date(chunkEnd);
    chunkStart.setUTCDate(chunkStart.getUTCDate() + 1);
  }

  // POI chunks
  const poiChunks: PoiChunk[] = [];
  let startIndex = 0;
  while (startIndex < totalPois) {
    const endIndex = Math.min(startIndex + MAX_POIS, totalPois);
    poiChunks.push({
      startIndex,
      endIndex,
      count: endIndex - startIndex,
    });
    startIndex = endIndex;
  }

  // Edge case: 0 POIs means 1 chunk (shouldn't happen, but be safe)
  if (poiChunks.length === 0) {
    poiChunks.push({ startIndex: 0, endIndex: 0, count: 0 });
  }

  return {
    dateChunks,
    poiChunks,
    totalSubJobs: dateChunks.length * poiChunks.length,
  };
}

/**
 * Get the sub-job index for a given (dateChunkIdx, poiChunkIdx) pair.
 * Order: iterate date chunks first, then POI chunks within each date.
 * [date0-poi0, date0-poi1, date1-poi0, date1-poi1, ...]
 */
export function getSubJobIndex(
  dateChunkIdx: number,
  poiChunkIdx: number,
  poiChunkCount: number
): number {
  return dateChunkIdx * poiChunkCount + poiChunkIdx;
}

/**
 * Given a sub-job linear index, return the (dateChunkIdx, poiChunkIdx) pair.
 */
export function getChunkIndices(
  subJobIndex: number,
  poiChunkCount: number
): { dateChunkIdx: number; poiChunkIdx: number } {
  return {
    dateChunkIdx: Math.floor(subJobIndex / poiChunkCount),
    poiChunkIdx: subJobIndex % poiChunkCount,
  };
}

// ── Report S3 keys ────────────────────────────────────────────────────

export function megaReportKey(megaJobId: string, reportType: string): string {
  return `mega-reports/${megaJobId}/${reportType}`;
}
