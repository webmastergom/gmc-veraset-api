/**
 * Audit utilities to verify data integrity
 */

import { getPOICollection } from './s3-config';
import { getAllJobs } from './jobs';
import { ListObjectsV2Command } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from './s3-config';

export interface POIAuditResult {
  collectionId: string;
  collectionName: string;
  expectedCount: number;
  actualCount: number;
  validPoints: number;
  invalidFeatures: number;
  discrepancy: number;
  issues: string[];
}

export interface JobAuditResult {
  jobId: string;
  jobName: string;
  expectedDays: number;
  actualPartitions: number;
  partitionDates: string[];
  expectedDateRange: { from: string; to: string };
  discrepancy: number;
  issues: string[];
}

/**
 * Audit a POI collection to verify POI count matches
 */
export async function auditPOICollection(collectionId: string): Promise<POIAuditResult> {
  const issues: string[] = [];
  
  try {
    // Get collection metadata
    const { getConfig } = await import('./s3-config');
    const collections = await getConfig<Record<string, any>>('poi-collections') || {};
    const collection = collections[collectionId];
    
    if (!collection) {
      return {
        collectionId,
        collectionName: 'Unknown',
        expectedCount: 0,
        actualCount: 0,
        validPoints: 0,
        invalidFeatures: 0,
        discrepancy: 0,
        issues: [`Collection ${collectionId} not found`],
      };
    }
    
    const expectedCount = collection.poiCount || 0;
    
    // Load GeoJSON
    const geojson = await getPOICollection(collectionId);
    
    if (!geojson) {
      return {
        collectionId,
        collectionName: collection.name,
        expectedCount,
        actualCount: 0,
        validPoints: 0,
        invalidFeatures: 0,
        discrepancy: expectedCount,
        issues: [`GeoJSON file not found for collection ${collectionId}`],
      };
    }
    
    const totalFeatures = geojson.features?.length || 0;
    
    // Count valid Point features
    const validPoints = (geojson.features || []).filter((f: any) => {
      return (
        f.geometry &&
        f.geometry.type === 'Point' &&
        Array.isArray(f.geometry.coordinates) &&
        f.geometry.coordinates.length >= 2 &&
        typeof f.geometry.coordinates[0] === 'number' &&
        typeof f.geometry.coordinates[1] === 'number' &&
        !isNaN(f.geometry.coordinates[0]) &&
        !isNaN(f.geometry.coordinates[1]) &&
        f.geometry.coordinates[0] >= -180 &&
        f.geometry.coordinates[0] <= 180 &&
        f.geometry.coordinates[1] >= -90 &&
        f.geometry.coordinates[1] <= 90
      );
    }).length;
    
    const invalidFeatures = totalFeatures - validPoints;
    const discrepancy = expectedCount - validPoints;
    
    if (discrepancy !== 0) {
      issues.push(
        `POI count mismatch: Expected ${expectedCount}, found ${validPoints} valid points (${totalFeatures} total features, ${invalidFeatures} invalid)`
      );
    }
    
    if (invalidFeatures > 0) {
      issues.push(
        `${invalidFeatures} invalid features found (non-Point geometry or invalid coordinates)`
      );
    }
    
    return {
      collectionId,
      collectionName: collection.name,
      expectedCount,
      actualCount: totalFeatures,
      validPoints,
      invalidFeatures,
      discrepancy,
      issues,
    };
  } catch (error: any) {
    return {
      collectionId,
      collectionName: 'Unknown',
      expectedCount: 0,
      actualCount: 0,
      validPoints: 0,
      invalidFeatures: 0,
      discrepancy: 0,
      issues: [`Error auditing collection: ${error.message}`],
    };
  }
}

/**
 * Audit a job to verify date partitions match expected date range
 */
export async function auditJob(jobId: string): Promise<JobAuditResult> {
  const issues: string[] = [];
  
  try {
    const jobs = await getAllJobs();
    const job = jobs.find(j => j.jobId === jobId);
    
    if (!job) {
      return {
        jobId,
        jobName: 'Unknown',
        expectedDays: 0,
        actualPartitions: 0,
        partitionDates: [],
        expectedDateRange: { from: '', to: '' },
        discrepancy: 0,
        issues: [`Job ${jobId} not found`],
      };
    }
    
    const dateRange = job.dateRange;
    if (!dateRange || !dateRange.from || !dateRange.to) {
      return {
        jobId,
        jobName: job.name,
        expectedDays: 0,
        actualPartitions: 0,
        partitionDates: [],
        expectedDateRange: { from: '', to: '' },
        discrepancy: 0,
        issues: [`Job ${jobId} has no date range`],
      };
    }
    
    // Use consistent date calculation
    const { calculateDaysInclusive } = await import('./s3');
    const expectedDays = calculateDaysInclusive(dateRange.from, dateRange.to);
    
    // Discover partitions from S3
    const datasetName = jobId;
    const prefix = `${datasetName}/`;
    const partitions = new Set<string>();
    
    try {
      let continuationToken: string | undefined;
      
      do {
        const listRes = await s3Client.send(new ListObjectsV2Command({
          Bucket: BUCKET,
          Prefix: prefix,
          Delimiter: '/',
        }));
        
        // Extract partition dates from common prefixes
        if (listRes.CommonPrefixes) {
          for (const prefixObj of listRes.CommonPrefixes) {
            const prefixPath = prefixObj.Prefix || '';
            const match = prefixPath.match(/date=(\d{4}-\d{2}-\d{2})/);
            if (match) {
              partitions.add(match[1]);
            }
          }
        }
        
        // Also check individual objects for date patterns
        if (listRes.Contents) {
          for (const obj of listRes.Contents) {
            const key = obj.Key || '';
            const match = key.match(/date=(\d{4}-\d{2}-\d{2})/);
            if (match) {
              partitions.add(match[1]);
            }
          }
        }
        
        continuationToken = listRes.NextContinuationToken;
      } while (continuationToken);
    } catch (error: any) {
      issues.push(`Error discovering partitions from S3: ${error.message}`);
    }
    
    const partitionDates = Array.from(partitions).sort();
    const actualPartitions = partitionDates.length;
    const discrepancy = expectedDays - actualPartitions;
    
    if (discrepancy !== 0) {
      issues.push(
        `Date range mismatch: Expected ${expectedDays} days (${dateRange.from} to ${dateRange.to}), found ${actualPartitions} partitions`
      );
    }
    
    // Check if partition dates match expected range
    if (partitionDates.length > 0) {
      const firstPartition = partitionDates[0];
      const lastPartition = partitionDates[partitionDates.length - 1];
      
      if (firstPartition !== dateRange.from) {
        issues.push(
          `First partition date (${firstPartition}) does not match start date (${dateRange.from})`
        );
      }
      
      if (lastPartition !== dateRange.to) {
        issues.push(
          `Last partition date (${lastPartition}) does not match end date (${dateRange.to})`
        );
      }
    }
    
    return {
      jobId,
      jobName: job.name,
      expectedDays,
      actualPartitions,
      partitionDates,
      expectedDateRange: dateRange,
      discrepancy,
      issues,
    };
  } catch (error: any) {
    return {
      jobId,
      jobName: 'Unknown',
      expectedDays: 0,
      actualPartitions: 0,
      partitionDates: [],
      expectedDateRange: { from: '', to: '' },
      discrepancy: 0,
      issues: [`Error auditing job: ${error.message}`],
    };
  }
}

/**
 * Audit all POI collections
 */
export async function auditAllPOICollections(): Promise<POIAuditResult[]> {
  const { getConfig } = await import('./s3-config');
  const collections = await getConfig<Record<string, any>>('poi-collections') || {};
  const collectionIds = Object.keys(collections);
  
  const results = await Promise.all(
    collectionIds.map(id => auditPOICollection(id))
  );
  
  return results;
}

/**
 * Audit all jobs
 */
export async function auditAllJobs(): Promise<JobAuditResult[]> {
  const jobs = await getAllJobs();
  
  const results = await Promise.all(
    jobs.map(job => auditJob(job.jobId))
  );
  
  return results;
}
