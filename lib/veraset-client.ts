const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';
// Use relative URL for client-side, absolute for server-side
const API_BASE_URL = typeof window === 'undefined' 
  ? (process.env.NEXT_PUBLIC_API_URL || 'https://gmc-mobility-api.vercel.app')
  : '';

export interface VerasetJobConfig {
  name: string;
  type: 'pings' | 'aggregate' | 'devices' | 'cohort' | 'pings_by_device';
  poiCollection: string;
  dateRange: {
    from: string; // YYYY-MM-DD
    to: string;
  };
  radius: number;
  schema: 'BASIC' | 'FULL';
}

export interface VerasetJobResponse {
  job_id: string;
  status: string;
}

export interface VerasetJobStatus {
  job_id: string;
  status: 'QUEUED' | 'RUNNING' | 'SUCCESS' | 'FAILED' | 'SCHEDULED';
  created_at?: string;
  updated_at?: string;
  error_message?: string;
}

export async function createVerasetJob(config: VerasetJobConfig, pois: any[]): Promise<VerasetJobResponse> {
  const url = API_BASE_URL ? `${API_BASE_URL}/api/veraset/movement` : '/api/veraset/movement'
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      type: config.type,
      date_range: {
        from: config.dateRange.from,
        to: config.dateRange.to,
      },
      pois: pois.map(poi => ({
        latitude: poi.latitude,
        longitude: poi.longitude,
        radius: config.radius,
      })),
      schema: config.schema,
    }),
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.message || 'Failed to create job');
  }

  return response.json();
}

export async function getJobStatus(jobId: string): Promise<VerasetJobStatus> {
  // Use our internal API route which handles S3 storage and Veraset updates
  const url = `/api/jobs/${jobId}`;
  
  try {
    // Add timestamp to prevent caching
    const cacheBuster = `?t=${Date.now()}`;
    const response = await fetch(url + cacheBuster, {
      cache: 'no-store', // Always fetch fresh data
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
      },
    });
    
    if (!response.ok) {
      let errorMessage = 'Failed to fetch job status';
      try {
        const errorData = await response.json();
        errorMessage = errorData.error || errorData.details || errorMessage;
      } catch {
        // If response is not JSON, use status text
        errorMessage = response.statusText || errorMessage;
      }
      throw new Error(errorMessage);
    }
    
    const job = await response.json();
    
    // Transform our Job format to VerasetJobStatus format
    return {
      job_id: job.jobId || job.job_id,
      status: job.status as 'QUEUED' | 'RUNNING' | 'SUCCESS' | 'FAILED',
      created_at: job.createdAt,
      updated_at: job.updatedAt,
      error_message: job.errorMessage || job.error_message,
    };
  } catch (error) {
    console.error(`Error fetching job status for ${jobId}:`, error);
    throw error;
  }
}

export async function getCategories() {
  const url = API_BASE_URL ? `${API_BASE_URL}/api/veraset/categories` : '/api/veraset/categories'
  const response = await fetch(url);
  
  if (!response.ok) {
    throw new Error('Failed to fetch categories');
  }
  
  return response.json();
}
