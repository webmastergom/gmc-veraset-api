import { getConfig, putConfig, initConfigIfNeeded } from './s3-config';

export interface Job {
  jobId: string;
  name: string;
  type: 'pings' | 'aggregate' | 'devices' | 'cohort';
  poiCount: number;
  poiCollectionId?: string;
  dateRange: { from: string; to: string };
  radius: number;
  schema: 'BASIC' | 'FULL' | 'ENHANCED' | 'N/A';
  status: 'QUEUED' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'SCHEDULED';
  s3SourcePath?: string;
  s3DestPath?: string | null;
  objectCount?: number;
  totalBytes?: number;
  syncedAt?: string | null;
  errorMessage?: string;
  createdAt: string;
  updatedAt?: string;
  summaryMetrics?: string;
  external?: boolean; // true = created outside app, doesn't count toward quota
  poiMapping?: Record<string, string>; // Maps Veraset POI IDs (geo_radius_X) to original GeoJSON IDs
  poiNames?: Record<string, string>; // Maps Veraset POI IDs (geo_radius_X) to human-readable names from GeoJSON properties.name
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
 */
export async function updateJob(jobId: string, updates: Partial<Job>): Promise<Job | null> {
  const data = await initConfigIfNeeded<JobsData>('jobs', DEFAULT_JOBS);
  
  if (!data[jobId]) {
    console.error(`❌ Job not found: ${jobId}`);
    return null;
  }
  
  data[jobId] = {
    ...data[jobId],
    ...updates,
    updatedAt: new Date().toISOString(),
  };
  
  try {
    await putConfig('jobs', data);
    console.log(`✅ Job updated: ${jobId}`);
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
  return updateJob(jobId, {
    s3DestPath,
    objectCount,
    totalBytes,
    syncedAt: new Date().toISOString(),
  });
}
