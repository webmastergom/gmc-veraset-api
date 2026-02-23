/**
 * POI Collection storage — local file fallback + S3.
 *
 * Extracted from s3-config.ts so that importing s3-config.ts does NOT pull in
 * the `fs` / `path` modules.  Node File Tracing (NFT) follows static imports
 * of `fs` and traces every file that `fs.existsSync()` / `fsp.readFile()` could
 * touch — including the large GeoJSON files under `./POIs/`.  By isolating
 * these imports here, only routes that actually need local-file POI access pay
 * the bundle-size cost.
 */

import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';
import { s3Client, BUCKET, getConfig } from './s3-config';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';

/**
 * Get POI collection GeoJSON from S3, with fallback to local files.
 * Local fallback uses async fs.readFile instead of blocking readFileSync.
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
      const fileContent = await fsp.readFile(localPath, 'utf-8');
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
