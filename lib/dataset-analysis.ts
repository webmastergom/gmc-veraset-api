/**
 * Functions for analyzing datasets and calculating statistics
 */

import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { readParquetFilesFromS3 } from './parquet-reader';
import { getAllJobs } from './jobs';

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

export interface DatasetStats {
  name: string;
  displayName: string;
  totalPings: number;
  uniqueDevices: number;
  uniquePOIs: number;
  dateRange: {
    from: string;
    to: string;
  };
  objectCount: number;
  size: string;
}

/**
 * Get dataset statistics by analyzing Parquet files
 */
export async function getDatasetStats(datasetName: string): Promise<DatasetStats | null> {
  try {
    // Find job for this dataset
    const jobs = await getAllJobs();
    const job = jobs.find(j => {
      const destPath = j.s3DestPath || '';
      const pathName = destPath.split('/').filter(Boolean).pop();
      return pathName === datasetName || destPath.includes(datasetName);
    });

    if (!job || !job.s3DestPath) {
      return null;
    }

    // Parse S3 path
    const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
    const prefix = s3Path.endsWith('/') ? s3Path : `${s3Path}/`;

    // List parquet files
    const listRes = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
    }));

    const parquetFiles = (listRes.Contents || [])
      .filter(obj => obj.Key?.endsWith('.parquet'))
      .map(obj => obj.Key!)
      .sort();

    if (parquetFiles.length === 0) {
      // Return basic info from job if no files
      return {
        name: datasetName,
        displayName: job.name,
        totalPings: 0,
        uniqueDevices: 0,
        uniquePOIs: 0,
        dateRange: job.dateRange,
        objectCount: job.objectCount || 0,
        size: job.totalBytes ? `${(job.totalBytes / 1024 / 1024 / 1024).toFixed(2)} GB` : '0 GB',
      };
    }

    // Sample files for statistics (process first 10 files for performance)
    const sampleFiles = parquetFiles.slice(0, Math.min(10, parquetFiles.length));
    const allRows: any[] = [];

    try {
      const rows = await readParquetFilesFromS3(sampleFiles);
      allRows.push(...rows);
    } catch (error) {
      console.warn('Error reading sample files, using job metadata:', error);
    }

    // Calculate statistics
    const deviceSet = new Set<string>();
    const poiSet = new Set<string>();
    let totalPings = 0;
    const dates: Date[] = [];

    for (const row of allRows) {
      const adId = row.ad_id as string;
      if (adId) {
        deviceSet.add(adId);
      }

      const poiIds = (row.poi_ids as string[]) || [];
      poiIds.forEach((poiId: string) => {
        if (poiId) poiSet.add(poiId);
      });

      const timestampStr = row.utc_timestamp || row.timestamp || row.date;
      if (timestampStr) {
        const date = new Date(timestampStr);
        if (!isNaN(date.getTime())) {
          dates.push(date);
        }
      }

      totalPings++;
    }

    // Estimate total pings based on sample
    const sampleRatio = parquetFiles.length > 0 ? allRows.length / sampleFiles.length : 0;
    const estimatedTotalPings = sampleRatio > 0 
      ? Math.round(totalPings * (parquetFiles.length / sampleFiles.length))
      : totalPings;

    // Get date range
    let dateFrom = job.dateRange.from;
    let dateTo = job.dateRange.to;
    
    if (dates.length > 0) {
      const sortedDates = dates.sort((a, b) => a.getTime() - b.getTime());
      dateFrom = sortedDates[0].toISOString().split('T')[0];
      dateTo = sortedDates[sortedDates.length - 1].toISOString().split('T')[0];
    }

    return {
      name: datasetName,
      displayName: job.name,
      totalPings: estimatedTotalPings,
      uniqueDevices: deviceSet.size,
      uniquePOIs: poiSet.size,
      dateRange: {
        from: dateFrom,
        to: dateTo,
      },
      objectCount: job.objectCount || parquetFiles.length,
      size: job.totalBytes ? `${(job.totalBytes / 1024 / 1024 / 1024).toFixed(2)} GB` : '0 GB',
    };

  } catch (error) {
    console.error(`Error getting dataset stats for ${datasetName}:`, error);
    return null;
  }
}
