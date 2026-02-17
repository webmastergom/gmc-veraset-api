import { getConfig, putConfig, initConfigIfNeeded } from './s3-config';

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

const DEFAULT_JOBS: JobsData = {};

/**
 * Get all jobs sorted by creation date (newest first)
 */
export async function getAllJobs(): Promise<Job[]> {
  const data = await initConfigIfNeeded<JobsData>('jobs', DEFAULT_JOBS);
  return Object.values(data).sort((a, b) => 
    new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
  );
}

/**
 * Get a specific job by ID
 */
export async function getJob(jobId: string): Promise<Job | null> {
  const data = await initConfigIfNeeded<JobsData>('jobs', DEFAULT_JOBS);
  return data[jobId] || null;
}

/**
 * Create a new job
 */
export async function createJob(job: Omit<Job, 'createdAt' | 'updatedAt'>): Promise<Job> {
  const data = await initConfigIfNeeded<JobsData>('jobs', DEFAULT_JOBS);
  
  const newJob: Job = {
    ...job,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
  
  data[job.jobId] = newJob;
  
  try {
    await putConfig('jobs', data);
    console.log(`✅ Job created: ${job.jobId} - ${job.name}`);
  } catch (error) {
    console.error(`❌ Failed to save job ${job.jobId}:`, error);
    throw error;
  }
  
  return newJob;
}

/**
 * Update an existing job
 * CRITICAL: This function performs atomic updates - it reads, merges, and writes in one operation.
 * Multiple concurrent calls may still race, but each call sees the latest state before updating.
 */
export async function updateJob(jobId: string, updates: Partial<Job>): Promise<Job | null> {
  // Read current state
  const data = await initConfigIfNeeded<JobsData>('jobs', DEFAULT_JOBS);
  
  if (!data[jobId]) {
    console.error(`❌ Job not found: ${jobId}`);
    return null;
  }
  
  // CRITICAL: Preserve syncedAt if sync is complete and update is just progress
  // This prevents late progress callbacks from overwriting completion status
  const currentJob = data[jobId];
  const isSyncComplete = !!currentJob.syncedAt;
  const isProgressUpdate = 'objectCount' in updates || 'totalBytes' in updates || 'syncProgress' in updates;
  
  // If sync is complete and this is just a progress update, preserve syncedAt
  if (isSyncComplete && isProgressUpdate && !('syncedAt' in updates)) {
    // Don't allow progress updates to overwrite completion
    // But allow explicit syncedAt updates (e.g., from markJobSynced)
    console.log(`[UPDATE JOB] Preserving syncedAt=${currentJob.syncedAt} for completed sync`);
  }
  
  // Merge updates atomically
  data[jobId] = {
    ...currentJob,
    ...updates,
    updatedAt: new Date().toISOString(),
  };
  
  try {
    await putConfig('jobs', data);
    // Only log if it's not a frequent progress update to reduce noise
    if (!isProgressUpdate || updates.syncedAt) {
      console.log(`✅ Job updated: ${jobId}${updates.syncedAt ? ' (sync completed)' : ''}`);
    }
  } catch (error) {
    console.error(`❌ Failed to update job ${jobId}:`, error);
    throw error;
  }
  
  return data[jobId];
}

/**
 * Update job status
 */
export async function updateJobStatus(
  jobId: string,
  status: Job['status'],
  errorMessage?: string
): Promise<Job | null> {
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
 * Try to acquire sync lock. Returns true if acquired, false if another sync is in progress or lock not expired.
 */
export async function tryAcquireSyncLock(jobId: string): Promise<boolean> {
  const data = await initConfigIfNeeded<JobsData>('jobs', DEFAULT_JOBS);
  const job = data[jobId];
  if (!job) return false;
  const lock = job.syncLock;
  if (lock) {
    const age = Date.now() - new Date(lock).getTime();
    if (age < SYNC_LOCK_TTL_MS) return false;
  }
  data[jobId] = {
    ...job,
    syncLock: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
  await putConfig('jobs', data);
  return true;
}

/**
 * Release sync lock (call when sync finishes or fails).
 */
export async function releaseSyncLock(jobId: string): Promise<void> {
  const data = await initConfigIfNeeded<JobsData>('jobs', DEFAULT_JOBS);
  if (!data[jobId]) return;
  data[jobId] = {
    ...data[jobId],
    syncLock: null,
    updatedAt: new Date().toISOString(),
  };
  await putConfig('jobs', data);
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
