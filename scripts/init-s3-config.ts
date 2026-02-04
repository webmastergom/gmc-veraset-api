/**
 * Initialize S3 config files with seed data
 * Run with: npx tsx scripts/init-s3-config.ts
 */

import { config } from 'dotenv';
import { resolve } from 'path';

// Load .env file
config({ path: resolve(process.cwd(), '.env') });

import { S3Client, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { initialJobsData, initialUsageData, initialPOICollectionsData } from '../lib/seed-jobs';

const s3 = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

const INITIAL_DATA = {
  usage: initialUsageData,
  jobs: initialJobsData,
  'poi-collections': initialPOICollectionsData,
};

async function init() {
  console.log('🚀 Initializing S3 config files...\n');
  
  // Check credentials
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    console.error('❌ AWS credentials not configured');
    console.log('Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables\n');
    process.exit(1);
  }
  
  // Force overwrite if FORCE_OVERWRITE=true
  const forceOverwrite = process.env.FORCE_OVERWRITE === 'true';
  
  for (const [name, data] of Object.entries(INITIAL_DATA)) {
    const key = `config/${name}.json`;
    
    // Check if exists (unless forcing overwrite)
    if (!forceOverwrite) {
      try {
        await s3.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
        console.log(`⏭️  ${key} already exists, skipping (use FORCE_OVERWRITE=true to overwrite)`);
        continue;
      } catch {
        // Doesn't exist, create it
      }
    }
    
    try {
      await s3.send(new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: JSON.stringify(data, null, 2),
        ContentType: 'application/json',
      }));
      
      console.log(`✅ ${forceOverwrite ? 'Overwritten' : 'Created'} ${key}`);
    } catch (error: any) {
      console.error(`❌ Failed to ${forceOverwrite ? 'overwrite' : 'create'} ${key}:`, error.message);
    }
  }
  
  console.log('\n✨ Done!');
}

init().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
