import { getConfig, putConfig, initConfigIfNeeded, invalidateCache } from './s3-config';
import { inferCountryFromName } from './country-inference';

export interface Job {
  jobId: string;
  name: string;
  type: 'pings' | 'aggregate' | 'devices' | 'cohort' | 'pings_by_device';
  poiCount: number;
  poiCollectionId?: string;
  dateRange: { from: string; to: string };
  radius: number;
  schema: 'BASIC' | 'FULL' | 'ENHANCED' | 'N/A';
  status: 'QUEUED' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'SCHEDULED';
  s3SourcePath?: string;
  s3DestPath?: string | null;
  objectCount?: number; // Number of objects copied (progress)
  totalBytes?: number; // Total bytes copied (progress)
  expectedObjectCount?: number; // Total objects expected (set at sync start)
  expectedTotalBytes?: number; // Total bytes expected (set at sync start)
  syncedAt?: string | null;
  syncCancelledAt?: string | null;
  /** ISO timestamp when current sync was last started (for stall detection). */
  syncStartedAt?: string | null;
  /** ISO timestamp when current sync started; used as lock to prevent concurrent syncs. */
  syncLock?: string | null;
  errorMessage?: string;
  createdAt: string;
  updatedAt?: string;
  summaryMetrics?: string;
  external?: boolean; // true = created outside app, doesn't count toward quota
  apiKeyId?: string; // ID of the API key used to create this job (external jobs only)
  apiKeyName?: string; // Name/label of the API key used (e.g., "Cinema Republica")
  poiMapping?: Record<string, string>; // Maps Veraset POI IDs (geo_radius_X) to original GeoJSON IDs
  poiNames?: Record<string, string>; // Maps Veraset POI IDs (geo_radius_X) to human-readable names from GeoJSON properties.name
  country?: string; // ISO 2-letter country code (e.g., "ES")
  webhookUrl?: string; // URL to notify when job status changes
  externalPois?: Array<{ id: string; name?: string; latitude: number; longitude: number }>; // Original POI data from external API
  /** Exact payload sent to Veraset API (for verification) */
  verasetPayload?: {
    date_range: { from_date: string; to_date: string };
    schema_type: string;
    geo_radius?: Array<{ poi_id: string; latitude: number; longitude: number; distance_in_meters: number }>;
    place_key?: Array<{ poi_id: string; placekey: string }>;
  };
  /** Actual date range received in data (from partitions after sync) */
  actualDateRange?: { from: string; to: string; days: number };
  /** Requested days vs actual days for verification */
  dateRangeDiscrepancy?: {
    requestedDays: number;
    actualDays: number;
    missingDays: number;
  };
  /** Complete audit trail: user input, payload sent to Veraset, Veraset response, and verification results */
  auditTrail?: {
    userInput: {
      name: string;
      type: string;
      dateRange: { from: string; to: string; raw?: any };
      schema: string;
      poiCount?: number;
      radius?: number;
      verasetConfig?: any;
      pois?: any[];
    };
    verasetPayload: {
      date_range: { from_date: string; to_date: string };
      schema_type: string;
      geo_radius?: Array<{ poi_id: string; latitude: number; longitude: number; distance_in_meters?: number }>;
      place_key?: Array<{ poi_id: string; placekey: string }>;
    };
    verasetResponse?: any;
    verificationPassed: boolean;
    verificationIssues: string[];
    responseVerificationPassed?: boolean;
    responseVerificationIssues?: string[];
    timestamp: string;
  };
  /** Full verification result from post-sync checks */
  verificationResult?: {
    countMatch: boolean;
    sourceCount: number;
    destCount: number;
    integrityPassed: boolean;
    etagSampleSize: number;
    etagMismatches: number;
    multipartSkipped: number;
    sourcePartitionDates: string[];
    destPartitionDates: string[];
    missingPartitionsInDest: string[];
    verasetMissingDays: number;
    timestamp: string;
  };
  /** Whether this dataset is enabled for the Audience Agent */
  audienceAgentEnabled?: boolean;
  /** If this job belongs to a mega-job, references the parent */
  megaJobId?: string;
  /** Position within a mega-job (0-based, for ordering) */
  megaJobIndex?: number;
  /** Detailed sync progress by day (for professional loader) */
  syncProgress?: {
    currentDay?: string; // Date being copied (YYYY-MM-DD)
    currentFile?: number; // File number within current day
    totalFilesInCurrentDay?: number; // Total files in current day
    dayProgress?: Record<string, {
      date: string;
      totalFiles: number;
      copiedFiles: number;
      failedFiles: number;
      totalBytes: number;
      copiedBytes: number;
      status: 'pending' | 'copying' | 'completed' | 'failed';
      errors?: Array<{ file: string; error: string }>;
    }>;
    lastUpdated?: string; // ISO timestamp
  };
}

type JobsData = Record<string, Job>;
type JobIndexData = Record<string, Partial<Job>>;

const DEFAULT_JOBS: JobsData = {};

/** Fields stripped from the index / list endpoint to reduce payload size. */
const HEAVY_FIELDS: (keyof Job)[] = [
  'verasetPayload',
  'auditTrail',
  'verificationResult',
  'syncProgress',
  'externalPois',
  'poiMapping',
  'poiNames',
];

// ── Per-job file helpers ────────────────────────────────────────────────
// Each job is stored as config/jobs/{jobId}.json (~0.5–5 MB).
// A lightweight index at config/jobs-index.json (~100 KB) powers the list view.
// This replaces the monolithic config/jobs.json (258 MB) that was too large
// for Vercel's 10s serverless function limit.

function jobKey(jobId: string): string {
  return `jobs/${jobId}`;
}

/** Strip heavy fields to create an index entry */
function toIndexEntry(job: Job): Partial<Job> & { hasSyncProgress?: boolean; hasVerification?: boolean } {
  const entry: Record<string, any> = {};
  for (const key of Object.keys(job) as (keyof Job)[]) {
    if (!HEAVY_FIELDS.includes(key)) {
      entry[key] = job[key];
    }
  }
  entry.hasSyncProgress = !!job.syncProgress?.dayProgress;
  entry.hasVerification = !!job.verificationResult;
  return entry as Partial<Job>;
}

// ── Index management ────────────────────────────────────────────────────

async function getIndex(): Promise<JobIndexData> {
  return await initConfigIfNeeded<JobIndexData>('jobs-index', {});
}

async function upsertIndex(jobId: string, entry: Partial<Job>): Promise<void> {
  invalidateCache('jobs-index');
  const index = await getIndex();
  index[jobId] = entry;
  await putConfig('jobs-index', index);
}

// ── Public API ──────────────────────────────────────────────────────────

/**
 * Get all jobs sorted by creation date (newest first).
 * Reads individual per-job files in parallel.
 */
export async function getAllJobs(): Promise<Job[]> {
  const index = await getIndex();
  const ids = Object.keys(index);

  if (ids.length === 0) {
    // Fallback: pre-migration monolithic file
    const data = await getConfig<JobsData>('jobs');
    if (data) {
      return Object.values(data).sort((a, b) =>
        new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
      );
    }
    return [];
  }

  // Read individual job files in parallel
  const jobs = await Promise.all(ids.map(id => getConfig<Job>(jobKey(id))));
  return jobs
    .filter((j): j is Job => j !== null)
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
}

/**
 * Get all jobs for the list view — reads from the lightweight index only.
 * Strips heavy nested fields that are only needed on the detail page.
 * Reduces response from ~500KB+ to ~20KB for a typical 20-job account.
 */
export async function getAllJobsSummary(): Promise<Partial<Job>[]> {
  const index = await getIndex();
  const entries = Object.values(index);

  if (entries.length === 0) {
    // Fallback: pre-migration monolithic file
    const data = await getConfig<JobsData>('jobs');
    if (data) {
      return Object.values(data)
        .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
        .map((job) => {
          const summary: Record<string, any> = {};
          for (const key of Object.keys(job) as (keyof Job)[]) {
            if (!HEAVY_FIELDS.includes(key)) {
              summary[key] = job[key];
            }
          }
          summary.hasSyncProgress = !!job.syncProgress?.dayProgress;
          summary.hasVerification = !!job.verificationResult;
          return summary as Partial<Job>;
        });
    }
    return [];
  }

  return entries.sort((a, b) =>
    new Date((b.createdAt as string) || '').getTime() - new Date((a.createdAt as string) || '').getTime()
  );
}

/**
 * Get jobs enabled for the Audience Agent (SUCCESS status + audienceAgentEnabled)
 */
export async function getAudienceEnabledJobs(): Promise<Job[]> {
  const index = await getIndex();
  const eligibleIds = Object.entries(index)
    .filter(([_, j]) => j.status === 'SUCCESS' && j.audienceAgentEnabled === true)
    .map(([id]) => id);

  if (eligibleIds.length === 0) return [];

  const jobs = await Promise.all(eligibleIds.map(id => getConfig<Job>(jobKey(id))));
  return jobs.filter((j): j is Job => j !== null);
}

/**
 * Get a specific job by ID.
 * Reads from per-job file (fast: ~1 MB) with fallback to monolithic file.
 */
export async function getJob(jobId: string): Promise<Job | null> {
  // Try per-job file first (fast: single file read)
  const job = await getConfig<Job>(jobKey(jobId));
  if (job) return job;

  // Fallback: monolithic file (pre-migration)
  const data = await getConfig<JobsData>('jobs');
  return data?.[jobId] || null;
}

/**
 * Create a new job.
 * Writes per-job file + updates index. Does NOT touch the monolithic jobs.json.
 */
export async function createJob(job: Omit<Job, 'createdAt' | 'updatedAt'>): Promise<Job> {
  const newJob: Job = {
    ...job,
    country: job.country || inferCountryFromName(job.name) || undefined,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  try {
    // Write individual job file (compact JSON, ~0.5–2 MB)
    await putConfig(jobKey(job.jobId), newJob, { compact: true });
    // Update lightweight index (~100 KB)
    await upsertIndex(job.jobId, toIndexEntry(newJob));
    console.log(`✅ Job created: ${job.jobId} - ${job.name}`);
  } catch (error) {
    console.error(`❌ Failed to save job ${job.jobId}:`, error);
    throw error;
  }

  return newJob;
}

/**
 * Update an existing job.
 * Reads and writes only the per-job file (~1 MB). Updates index only for
 * state-level changes (status, sync completion), not for frequent progress updates.
 *
 * CRITICAL: Performs atomic updates — reads, merges, and writes in one operation.
 * Multiple concurrent calls may still race, but each call sees the latest state.
 */
export async function updateJob(jobId: string, updates: Partial<Job>): Promise<Job | null> {
  // Invalidate per-job cache to ensure fresh read from S3
  invalidateCache(jobKey(jobId));

  // Read individual job file (fast: ~1 MB)
  let currentJob = await getConfig<Job>(jobKey(jobId));

  if (!currentJob) {
    // Fallback: try monolithic file (pre-migration)
    const data = await getConfig<JobsData>('jobs');
    currentJob = data?.[jobId] || null;
    if (!currentJob) {
      console.error(`❌ Job not found: ${jobId}`);
      return null;
    }
  }

  // CRITICAL: Preserve syncedAt if sync is complete and update is just progress
  // This prevents late progress callbacks from overwriting completion status
  const isSyncComplete = !!currentJob.syncedAt;
  const isProgressUpdate = 'objectCount' in updates || 'totalBytes' in updates || 'syncProgress' in updates;

  if (isSyncComplete && isProgressUpdate && !('syncedAt' in updates)) {
    console.log(`[UPDATE JOB] Preserving syncedAt=${currentJob.syncedAt} for completed sync`);
  }

  // Merge updates atomically
  const updatedJob: Job = {
    ...currentJob,
    ...updates,
    updatedAt: new Date().toISOString(),
  };

  try {
    // Write individual job file (compact JSON)
    await putConfig(jobKey(jobId), updatedJob, { compact: true });

    // Update index only for state-level changes (skip frequent progress updates)
    const isProgressOnly = isProgressUpdate
      && !('status' in updates)
      && !('syncedAt' in updates)
      && !('syncCancelledAt' in updates)
      && !('errorMessage' in updates)
      && !('s3DestPath' in updates)
      && !('name' in updates)
      && !('country' in updates)
      && !('audienceAgentEnabled' in updates);

    if (!isProgressOnly) {
      await upsertIndex(jobId, toIndexEntry(updatedJob));
    }

    // Only log if it's not a frequent progress update to reduce noise
    if (!isProgressUpdate || updates.syncedAt) {
      console.log(`✅ Job updated: ${jobId}${updates.syncedAt ? ' (sync completed)' : ''}`);
    }
  } catch (error) {
    console.error(`❌ Failed to update job ${jobId}:`, error);
    throw error;
  }

  return updatedJob;
}

/**
 * Valid job status transitions. Terminal states (SUCCESS) have no outgoing transitions.
 * FAILED allows retry back to QUEUED.
 */
const VALID_TRANSITIONS: Record<Job['status'], Job['status'][]> = {
  QUEUED: ['RUNNING', 'SUCCESS', 'FAILED', 'SCHEDULED'],
  SCHEDULED: ['QUEUED', 'RUNNING', 'SUCCESS', 'FAILED'],
  RUNNING: ['SUCCESS', 'FAILED'],
  SUCCESS: [],  // Terminal
  FAILED: ['QUEUED'],  // Allow retry
};

/**
 * Update job status with state machine validation.
 * Invalid transitions are logged and silently ignored to prevent
 * stale Veraset responses from corrupting local state.
 */
export async function updateJobStatus(
  jobId: string,
  status: Job['status'],
  errorMessage?: string
): Promise<Job | null> {
  const currentJob = await getJob(jobId);
  if (!currentJob) {
    console.error(`[updateJobStatus] Job not found: ${jobId}`);
    return null;
  }

  const currentStatus = currentJob.status;

  // Same status — no-op, just update errorMessage if changed
  if (currentStatus === status) {
    if (errorMessage !== undefined && errorMessage !== currentJob.errorMessage) {
      return updateJob(jobId, { errorMessage });
    }
    return currentJob;
  }

  // Validate transition
  const allowedNext = VALID_TRANSITIONS[currentStatus] || [];
  if (!allowedNext.includes(status)) {
    console.warn(
      `[updateJobStatus] Invalid transition for ${jobId}: ${currentStatus} -> ${status}. ` +
      `Allowed: [${allowedNext.join(', ') || 'terminal'}]`
    );
    return currentJob;
  }

  console.log(`[updateJobStatus] ${jobId}: ${currentStatus} -> ${status}`);
  return updateJob(jobId, { status, errorMessage });
}

/**
 * Mark job as synced with S3 details
 */
export async function markJobSynced(
  jobId: string,
  s3DestPath: string,
  objectCount: number,
  totalBytes: number
): Promise<Job | null> {
  const job = await getJob(jobId);
  if (!job) return null;

  return updateJob(jobId, {
    s3DestPath,
    objectCount,
    totalBytes,
    // Preserve expected totals if already set, otherwise use final values
    expectedObjectCount: job.expectedObjectCount || objectCount,
    expectedTotalBytes: job.expectedTotalBytes || totalBytes,
    syncedAt: new Date().toISOString(),
  });
}

const SYNC_LOCK_TTL_MS = 600_000; // 10 min

/**
 * Try to acquire sync lock. Returns true if acquired, false if another sync
 * is in progress or lock not expired.
 * Reads/writes only the per-job file (fast).
 */
export async function tryAcquireSyncLock(jobId: string): Promise<boolean> {
  invalidateCache(jobKey(jobId));

  const job = await getConfig<Job>(jobKey(jobId));
  if (!job) {
    // Fallback: try monolithic file
    const data = await getConfig<JobsData>('jobs');
    if (!data?.[jobId]) return false;
    // If found in monolithic, migrate to per-job file
    const migrated = data[jobId];
    migrated.syncLock = new Date().toISOString();
    migrated.updatedAt = new Date().toISOString();
    await putConfig(jobKey(jobId), migrated, { compact: true });
    return true;
  }

  const lock = job.syncLock;
  if (lock) {
    const age = Date.now() - new Date(lock).getTime();
    if (age < SYNC_LOCK_TTL_MS) return false;
  }

  const updatedJob: Job = {
    ...job,
    syncLock: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  await putConfig(jobKey(jobId), updatedJob, { compact: true });
  return true;
}

/**
 * Release sync lock (call when sync finishes or fails).
 * Reads/writes only the per-job file (fast).
 */
export async function releaseSyncLock(jobId: string): Promise<void> {
  invalidateCache(jobKey(jobId));

  const job = await getConfig<Job>(jobKey(jobId));
  if (!job) return;

  const updatedJob: Job = {
    ...job,
    syncLock: null,
    updatedAt: new Date().toISOString(),
  };

  await putConfig(jobKey(jobId), updatedJob, { compact: true });
}

/**
 * Initialize sync with expected totals
 */
export async function initializeSync(
  jobId: string,
  destPath: string,
  expectedObjectCount: number,
  expectedTotalBytes: number
): Promise<Job | null> {
  // NOTE: Do NOT reset objectCount/totalBytes to 0 here.
  // The orchestrator will re-list source & dest and set accurate counts.
  // Resetting to 0 causes a brief flash of "0 progress" and, worse,
  // if the function dies before the first flushProgress, the job is left
  // at 0/N with no way to detect that it stalled.
  return updateJob(jobId, {
    s3DestPath: destPath,
    expectedObjectCount,
    expectedTotalBytes,
    syncedAt: null,
    syncCancelledAt: null, // Clear cancellation when starting new sync
    errorMessage: '', // Clear previous errors when starting new sync
    syncStartedAt: new Date().toISOString(), // Track when sync was last started
  });
}

/**
 * Cancel sync or force-complete if data is already there (fixes stuck jobs)
 */
export async function cancelSyncJob(jobId: string): Promise<{ action: 'cancelled' | 'completed'; job: Job | null }> {
  const job = await getJob(jobId);
  if (!job) return { action: 'cancelled', job: null };

  const copied = job.objectCount || 0;
  const expected = job.expectedObjectCount || 0;

  // If sync appears complete (copied >= expected), set syncedAt to fix stuck state
  if (copied >= expected && expected > 0) {
    const updated = await updateJob(jobId, {
      syncedAt: new Date().toISOString(),
      syncCancelledAt: null,
    });
    return { action: 'completed', job: updated };
  }

  // Otherwise mark as cancelled
  const updated = await updateJob(jobId, {
    syncCancelledAt: new Date().toISOString(),
  });
  return { action: 'cancelled', job: updated };
}

/**
 * Update sync progress in real-time
 */
export async function updateSyncProgress(
  jobId: string,
  copied: number,
  total: number,
  copiedBytes: number,
  totalBytes: number
): Promise<Job | null> {
  const job = await getJob(jobId);
  if (!job) return null;

  // CRITICAL: Never overwrite syncedAt - once sync is complete, progress updates must not touch it.
  // A late progress callback can run after markJobSynced due to async ordering.
  // By omitting syncedAt from the update, we preserve the completion status.
  const updates: Partial<Job> = {
    objectCount: copied,
    totalBytes: copiedBytes,
    expectedObjectCount: job.expectedObjectCount || total,
    expectedTotalBytes: job.expectedTotalBytes || totalBytes,
  };

  const updated = await updateJob(jobId, updates);

  // Log progress update for debugging
  console.log(`[UPDATE SYNC PROGRESS] Job ${jobId}: copied=${copied}/${total}, bytes=${copiedBytes}/${totalBytes}, syncedAt=${job.syncedAt || 'null'}`);

  return updated;
}
