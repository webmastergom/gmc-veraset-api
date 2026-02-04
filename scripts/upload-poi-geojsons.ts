/**
 * Script to upload POI GeoJSON files from local POIs/ directory to S3
 */

import * as dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

dotenv.config();

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

// Mapping of local file names to S3 keys
const fileMappings: Record<string, string> = {
  'spain_ecigarette_combined.geojson': 'pois/spain-ecigarette-combined.geojson',
  'spain_tobacco_combined.geojson': 'pois/spain-tobacco-combined.geojson',
  // Note: spain-nicotine-full might need to be created by combining the other two
};

async function uploadFile(localPath: string, s3Key: string): Promise<void> {
  try {
    const fileContent = fs.readFileSync(localPath);
    
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: s3Key,
      Body: fileContent,
      ContentType: 'application/geo+json',
    }));
    
    console.log(`✅ Uploaded ${path.basename(localPath)} → s3://${BUCKET}/${s3Key}`);
  } catch (error: any) {
    console.error(`❌ Failed to upload ${localPath}:`, error.message);
    throw error;
  }
}

async function main() {
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    console.error('❌ AWS credentials not configured');
    process.exit(1);
  }

  const poisDir = path.join(process.cwd(), 'POIs');
  
  if (!fs.existsSync(poisDir)) {
    console.error(`❌ POIs directory not found: ${poisDir}`);
    process.exit(1);
  }

  console.log('📤 Uploading POI GeoJSON files to S3...\n');

  const files = fs.readdirSync(poisDir);
  const geojsonFiles = files.filter(f => f.endsWith('.geojson'));

  let uploaded = 0;
  let skipped = 0;

  for (const file of geojsonFiles) {
    const localPath = path.join(poisDir, file);
    const s3Key = fileMappings[file];

    if (!s3Key) {
      console.log(`⏭️  Skipping ${file} (no mapping defined)`);
      skipped++;
      continue;
    }

    try {
      await uploadFile(localPath, s3Key);
      uploaded++;
    } catch (error) {
      console.error(`Failed to upload ${file}`);
    }
  }

  console.log(`\n✅ Upload complete: ${uploaded} uploaded, ${skipped} skipped`);
}

main().catch(console.error);
