import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import * as fs from 'fs';
import * as path from 'path';

export const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
  // Disable automatic CRC32 checksum in presigned URLs — AWS SDK v3.654+
  // adds x-amz-checksum-crc32 params by default, which causes CORS preflight
  // failures when browsers PUT directly to S3 via presigned URLs.
  requestChecksumCalculation: 'WHEN_REQUIRED',
  responseChecksumValidation: 'WHEN_REQUIRED',
});

export const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/** S3 config for health checks */
export async function getS3Config(): Promise<{ bucket: string; region: string } | null> {
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    return null;
  }
  return {
    bucket: BUCKET,
    region: process.env.AWS_REGION || 'us-west-2',
  };
}

/**
 * Check if config file exists in S3
 */
export async function configExists(key: string): Promise<boolean> {
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      return false;
    }
    
    await s3Client.send(new HeadObjectCommand({
      Bucket: BUCKET,
      Key: `config/${key}.json`,
    }));
    return true;
  } catch (error: any) {
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      return false;
    }
    // Other errors (permissions, network) - assume doesn't exist
    return false;
  }
}

/**
 * Read a JSON config file from S3 with proper error handling
 */
export async function getConfig<T>(key: string): Promise<T | null> {
  try {
    // Check if AWS credentials are configured
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      console.warn(`AWS credentials not configured. Returning null for config/${key}.json`);
      return null;
    }

    const command = new GetObjectCommand({
      Bucket: BUCKET,
      Key: `config/${key}.json`,
    });
    
    const response = await s3Client.send(command);
    const body = await response.Body?.transformToString();
    
    if (!body) {
      return null;
    }
    
    return JSON.parse(body) as T;
  } catch (error: any) {
    // File doesn't exist
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      console.warn(`Config file not found: config/${key}.json`);
      return null;
    }
    
    // Don't throw errors in development - just log and return null
    if (process.env.NODE_ENV === 'development') {
      console.warn(`Error reading config/${key}.json:`, error.message || error);
      return null;
    }
    
    console.error(`Error reading config/${key}.json:`, error);
    throw error;
  }
}

/**
 * Write a JSON config file to S3 with validation
 */
export async function putConfig<T>(key: string, data: T): Promise<void> {
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      throw new Error('AWS credentials not configured');
    }

    const json = JSON.stringify(data, null, 2);
    
    // Validate JSON before saving
    JSON.parse(json);
    
    const command = new PutObjectCommand({
      Bucket: BUCKET,
      Key: `config/${key}.json`,
      Body: json,
      ContentType: 'application/json',
    });
    
    await s3Client.send(command);
    console.log(`✅ Saved config/${key}.json`);
  } catch (error: any) {
    console.error(`❌ Error saving config/${key}.json:`, error.message || error);
    throw error;
  }
}

/**
 * Initialize config file if it doesn't exist
 */
export async function initConfigIfNeeded<T>(key: string, defaultData: T): Promise<T> {
  const exists = await configExists(key);
  
  if (!exists) {
    console.log(`📝 Initializing config/${key}.json with defaults`);
    try {
      await putConfig(key, defaultData);
      return defaultData;
    } catch (error) {
      console.warn(`Could not initialize config/${key}.json, using in-memory default`);
      return defaultData;
    }
  }
  
  const existing = await getConfig<T>(key);
  return existing || defaultData;
}

/**
 * Update a JSON config file (read, modify, write)
 */
export async function updateConfig<T extends Record<string, any>>(
  key: string,
  updater: (data: T | null) => T
): Promise<T> {
  const existing = await getConfig<T>(key);
  const updated = updater(existing);
  await putConfig(key, updated);
  return updated;
}

/**
 * List all objects in a prefix
 */
export async function listObjects(prefix: string): Promise<string[]> {
  try {
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      return [];
    }

    const { ListObjectsV2Command } = await import('@aws-sdk/client-s3');
    const command = new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
    });
    
    const response = await s3Client.send(command);
    return (response.Contents || []).map(obj => obj.Key || '').filter(Boolean);
  } catch (error) {
    console.warn(`Error listing objects with prefix ${prefix}:`, error);
    return [];
  }
}

/**
 * Get POI collection GeoJSON from S3, with fallback to local files
 */
export async function getPOICollection(collectionId: string): Promise<any | null> {
  try {
    // Get collection metadata to find the correct GeoJSON path
    const collections = await getConfig<Record<string, any>>('poi-collections') || {};
    const collection = collections[collectionId];
    
    // Use geojsonPath from collection metadata if available, otherwise construct from ID
    const geojsonKey = collection?.geojsonPath || `pois/${collectionId}.geojson`;

    // Try S3 first if credentials are configured
    console.log(`[getPOICollection] id=${collectionId}, key=${geojsonKey}, hasConfig=${!!collection}, bucket=${BUCKET}`);
    if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
      try {
        const command = new GetObjectCommand({
          Bucket: BUCKET,
          Key: geojsonKey,
        });

        console.log(`[getPOICollection] Fetching S3: ${BUCKET}/${geojsonKey}`);
        const response = await s3Client.send(command);
        const body = await response.Body?.transformToString();

        if (body) {
          console.log(`[getPOICollection] Success: ${body.length} bytes from S3`);
          return JSON.parse(body);
        }
        console.warn(`[getPOICollection] Empty body from S3`);
      } catch (s3Error: any) {
        console.error(`[getPOICollection] S3 error: name=${s3Error.name}, msg=${s3Error.message}, status=${s3Error.$metadata?.httpStatusCode}`);
        // Fall through to local file check
      }
    } else {
      console.warn(`[getPOICollection] No AWS credentials`);
    }

    // Fallback: Try to read from local POIs directory
    // Map collection IDs to local file names (with underscores)
    const localFileMap: Record<string, string> = {
      'spain-ecigarette-combined': 'spain_ecigarette_combined.geojson',
      'spain-tobacco-combined': 'spain_tobacco_combined.geojson',
      'spain-nicotine-full': 'overture_spain_nicotine_clean.geojson', // Use the combined nicotine file
    };

    const localFileName = localFileMap[collectionId] || `${collectionId.replace(/-/g, '_')}.geojson`;
    const localPath = path.join(process.cwd(), 'POIs', localFileName);

    if (fs.existsSync(localPath)) {
      console.log(`📁 Using local GeoJSON file: ${localPath}`);
      const fileContent = fs.readFileSync(localPath, 'utf-8');
      return JSON.parse(fileContent);
    }

    if (!collection) {
      console.warn(`POI collection ${collectionId} not found in config and no local file found`);
    } else {
      console.warn(`GeoJSON file not found in S3 or locally for collection ${collectionId}`);
    }
    return null;
  } catch (error: any) {
    console.error(`Error reading GeoJSON for collection ${collectionId}:`, error);
    throw error;
  }
}

/**
 * Save POI collection GeoJSON to S3
 */
export async function putPOICollection(collectionId: string, geojson: any): Promise<void> {
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials not configured');
  }

  const command = new PutObjectCommand({
    Bucket: BUCKET,
    Key: `pois/${collectionId}.geojson`,
    Body: JSON.stringify(geojson, null, 2),
    ContentType: 'application/json',
  });
  
  await s3Client.send(command);
}
