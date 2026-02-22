/**
 * Production Diagnostic Script
 * Run: npx tsx scripts/diagnose-production.ts
 *
 * Checks all critical production dependencies:
 * - Environment variables
 * - S3 config bucket (our bucket)
 * - Veraset source bucket (for sync)
 * - Veraset API connectivity
 */

import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(process.cwd(), '.env') });
config({ path: resolve(process.cwd(), '.env.local') });

import { S3Client, ListObjectsV2Command, HeadBucketCommand } from '@aws-sdk/client-s3';

const OUR_BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
const VERASET_BUCKET = 'veraset-prd-platform-us-west-2';
const VERASET_API = 'https://platform.prd.veraset.tech/v1/job/test';

async function check(name: string, fn: () => Promise<boolean | string>): Promise<void> {
  process.stdout.write(`  ${name}... `);
  try {
    const result = await fn();
    if (result === true || result === '') {
      console.log('✅ OK');
    } else {
      console.log(`✅ ${result}`);
    }
  } catch (err: any) {
    console.log(`❌ ${err.message || err}`);
  }
}

async function main() {
  console.log('\n🔍 PRODUCTION DIAGNOSTIC\n');
  console.log('═'.repeat(50));

  // 1. Environment variables
  console.log('\n1. ENVIRONMENT VARIABLES');
  const required = [
    'VERASET_API_KEY',
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_REGION',
    'S3_BUCKET',
    'AUTH_SECRET',
  ];
  for (const v of required) {
    const val = process.env[v];
    const ok = !!val && val.length > 0;
    console.log(`  ${v}: ${ok ? '✅ Set' : '❌ MISSING'}`);
  }

  // 2. Our S3 bucket (config + data)
  console.log('\n2. OUR S3 BUCKET (config & data)');
  const s3 = new S3Client({
    region: process.env.AWS_REGION || 'us-west-2',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
  });

  await check('Bucket exists', async () => {
    await s3.send(new HeadBucketCommand({ Bucket: OUR_BUCKET }));
    return true;
  });

  await check('config/jobs.json', async () => {
    const { ListObjectsV2Command } = await import('@aws-sdk/client-s3');
    const r = await s3.send(new ListObjectsV2Command({
      Bucket: OUR_BUCKET,
      Prefix: 'config/jobs.json',
      MaxKeys: 1,
    }));
    return r.KeyCount && r.KeyCount > 0 ? 'exists' : 'missing (run init-s3-config)';
  });

  // 3. Veraset source bucket (CRITICAL for sync)
  console.log('\n3. VERASET S3 BUCKET (source for sync)');
  console.log('   Path: s3://veraset-prd-platform-us-west-2/output/garritz/');
  await check('Can list Veraset bucket', async () => {
    const r = await s3.send(new ListObjectsV2Command({
      Bucket: VERASET_BUCKET,
      Prefix: 'output/garritz/',
      MaxKeys: 1,
    }));
    return r.KeyCount !== undefined ? `${r.KeyCount} objects found` : 'no access';
  });

  // 4. Veraset API
  console.log('\n4. VERASET API');
  const apiKey = process.env.VERASET_API_KEY;
  await check('API key configured', async () => (!!apiKey ? 'yes' : 'no'));

  if (apiKey) {
    await check('API connectivity', async () => {
      const res = await fetch(VERASET_API, {
        method: 'GET',
        headers: { 'X-API-Key': apiKey },
      });
      // 404 is OK (test job doesn't exist) - means API is reachable
      if (res.status === 404 || res.status === 401) return 'reachable';
      if (res.ok) return 'reachable';
      throw new Error(`HTTP ${res.status}`);
    });
  }

  console.log('\n' + '═'.repeat(50));
  console.log('\n📋 RESUMEN');
  console.log('  - Si Veraset bucket falla: Las credenciales AWS necesitan acceso');
  console.log('    cross-account al bucket de Veraset. Contacta a Veraset.');
  console.log('  - Si Veraset API falla: Verifica VERASET_API_KEY en Vercel.');
  console.log('  - Después de cambiar env vars en Vercel: REDEPLOY obligatorio.');
  console.log('\n');
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
