/**
 * Utilities for mapping Veraset POI IDs to original GeoJSON IDs
 */

import { getJob } from './jobs';

/**
 * Get the original POI ID from GeoJSON for a Veraset-generated POI ID
 * Veraset generates IDs as geo_radius_0, geo_radius_1, etc. based on array index
 */
export async function getOriginalPoiId(
  jobId: string,
  verasetPoiId: string
): Promise<string | null> {
  try {
    const job = await getJob(jobId);
    if (!job || !job.poiMapping) {
      return null;
    }
    
    return job.poiMapping[verasetPoiId] || null;
  } catch (error) {
    console.error(`Error getting original POI ID for ${verasetPoiId}:`, error);
    return null;
  }
}

/**
 * Map an array of Veraset POI IDs to their original GeoJSON IDs
 */
export async function mapPoiIds(
  jobId: string,
  verasetPoiIds: string[]
): Promise<Record<string, string>> {
  try {
    const job = await getJob(jobId);
    if (!job || !job.poiMapping) {
      // Return identity mapping if no mapping available
      return Object.fromEntries(verasetPoiIds.map(id => [id, id]));
    }
    
    const mapping: Record<string, string> = {};
    for (const verasetId of verasetPoiIds) {
      mapping[verasetId] = job.poiMapping[verasetId] || verasetId;
    }
    
    return mapping;
  } catch (error) {
    console.error(`Error mapping POI IDs for job ${jobId}:`, error);
    // Return identity mapping on error
    return Object.fromEntries(verasetPoiIds.map(id => [id, id]));
  }
}

/**
 * Get display name for a POI ID
 * Returns human-readable name if available, otherwise original ID, otherwise Veraset ID
 */
export function getPoiDisplayName(
  verasetPoiId: string,
  originalPoiId: string | null,
  poiName: string | null = null
): string {
  if (poiName) {
    return poiName;
  }
  if (originalPoiId && originalPoiId !== verasetPoiId) {
    return originalPoiId;
  }
  return verasetPoiId;
}

/**
 * Get POI names mapping for a job
 */
export async function getPoiNames(
  jobId: string
): Promise<Record<string, string> | null> {
  try {
    const job = await getJob(jobId);
    if (!job || !job.poiNames) {
      return null;
    }
    
    return job.poiNames;
  } catch (error) {
    console.error(`Error getting POI names for job ${jobId}:`, error);
    return null;
  }
}
