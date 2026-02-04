/**
 * Utility functions for reading Parquet files from S3
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import * as parquet from 'parquetjs-lite';

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/**
 * Read a Parquet file from S3 and return rows
 */
export async function readParquetFromS3(key: string): Promise<any[]> {
  try {
    // Download file from S3
    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });
    
    const response = await s3Client.send(command);
    
    if (!response.Body) {
      throw new Error(`Empty response body for ${key}`);
    }
    
    // Convert stream to buffer
    const chunks: Uint8Array[] = [];
    const stream = response.Body as Readable;
    
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    
    const buffer = Buffer.concat(chunks);
    
    // Read Parquet file
    const reader = await parquet.ParquetReader.openBuffer(buffer);
    const cursor = reader.getCursor();
    
    const rows: any[] = [];
    let row = await cursor.next();
    
    while (row) {
      rows.push(row);
      row = await cursor.next();
    }
    
    await reader.close();
    
    return rows;
  } catch (error: any) {
    console.error(`Error reading Parquet file ${key}:`, error.message);
    throw error;
  }
}

/**
 * Read multiple Parquet files from S3
 */
export async function readParquetFilesFromS3(keys: string[]): Promise<any[]> {
  const allRows: any[] = [];
  
  for (const key of keys) {
    try {
      const rows = await readParquetFromS3(key);
      allRows.push(...rows);
    } catch (error) {
      console.warn(`Skipping file ${key} due to error:`, error);
      // Continue with other files
    }
  }
  
  return allRows;
}
