/**
 * Verification script for S3 config files
 * Run with: npx tsx scripts/verify-s3-config.ts
 */

import { config } from 'dotenv';
import { resolve } from 'path';

// Load .env file
config({ path: resolve(process.cwd(), '.env') });

import { S3Client, GetObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';

const s3 = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

async function verify() {
  console.log('🔍 Verifying S3 config files...\n');
  
  // Check credentials
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    console.error('❌ AWS credentials not configured');
    console.log('Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables\n');
    process.exit(1);
  }
  
  const files = ['usage', 'jobs', 'poi-collections'];
  
  for (const file of files) {
    try {
      const res = await s3.send(new GetObjectCommand({
        Bucket: BUCKET,
        Key: `config/${file}.json`,
      }));
      const body = await res.Body?.transformToString();
      const data = JSON.parse(body || '{}');
      
      console.log(`✅ config/${file}.json`);
      console.log(`   Keys: ${Object.keys(data).length}`);
      const preview = JSON.stringify(data).slice(0, 100);
      console.log(`   Preview: ${preview}...\n`);
      
    } catch (error: any) {
      if (error.name === 'NoSuchKey') {
        console.log(`❌ config/${file}.json - NOT FOUND`);
        console.log(`   Run: npx tsx scripts/init-s3-config.ts\n`);
      } else {
        console.log(`❌ config/${file}.json - ERROR: ${error.message}\n`);
      }
    }
  }
  
  console.log('✨ Verification complete!');
}

verify().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
