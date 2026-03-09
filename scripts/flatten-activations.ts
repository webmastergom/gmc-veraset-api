/**
 * One-time migration: move activation CSVs to staging/ for Karlsgate.
 *
 * Before: staging/activations/{name}.csv
 * After:  staging/{name}.csv + staging/{name}.csv.spec.yml
 *
 * The Karlsgate node watches staging/ for paired .csv + .spec.yml files.
 *
 * Usage: npx tsx scripts/flatten-activations.ts
 */

import { S3Client, ListObjectsV2Command, CopyObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
const REGION = process.env.AWS_REGION || 'us-west-2';
const PREFIX = 'staging/activations/';

const s3 = new S3Client({ region: REGION });

const SPEC_CONTENT = 'identifiers:\n  - "*"\nattributes:\n  - "*"\n';

async function main() {
  console.log(`Listing objects in s3://${BUCKET}/${PREFIX}...`);

  const res = await s3.send(new ListObjectsV2Command({
    Bucket: BUCKET,
    Prefix: PREFIX,
    MaxKeys: 1000,
  }));

  const keys = (res.Contents || []).map(o => o.Key!).filter(Boolean);
  console.log(`Found ${keys.length} objects total.`);

  // Find .csv files in staging/activations/
  const csvFiles = keys.filter(k => k.endsWith('.csv'));

  if (csvFiles.length === 0) {
    console.log('No CSV files to migrate.');
    return;
  }

  console.log(`\nMigrating ${csvFiles.length} activation file(s) to staging/:\n`);

  for (const oldKey of csvFiles) {
    // Extract handle: staging/activations/{name}.csv → {name}
    const handle = oldKey.slice(PREFIX.length).replace(/\.csv$/, '');
    if (!handle) continue;

    const newCsvKey = `staging/${handle}.csv`;
    const newSpecKey = `staging/${handle}.csv.spec.yml`;

    console.log(`  ${oldKey}`);
    console.log(`  → ${newCsvKey}`);
    console.log(`  → ${newSpecKey}`);

    // Copy CSV to staging/
    await s3.send(new CopyObjectCommand({
      Bucket: BUCKET,
      CopySource: `${BUCKET}/${oldKey}`,
      Key: newCsvKey,
    }));

    // Create spec file
    await s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: newSpecKey,
      Body: SPEC_CONTENT,
      ContentType: 'text/yaml',
    }));

    // Delete old file
    await s3.send(new DeleteObjectCommand({
      Bucket: BUCKET,
      Key: oldKey,
    }));

    console.log(`  ✓ done\n`);
  }

  console.log(`Migrated ${csvFiles.length} activation file(s) to staging/.`);
}

main().catch(err => {
  console.error('Migration failed:', err);
  process.exit(1);
});
