/**
 * One-time migration: flatten activation folders.
 *
 * Before: staging/activations/{name}/maids.csv
 * After:  staging/activations/{name}.csv
 *
 * Usage: npx tsx scripts/flatten-activations.ts
 */

import { S3Client, ListObjectsV2Command, CopyObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
const REGION = process.env.AWS_REGION || 'us-west-2';
const PREFIX = 'staging/activations/';

const s3 = new S3Client({ region: REGION });

async function main() {
  console.log(`Listing objects in s3://${BUCKET}/${PREFIX}...`);

  const res = await s3.send(new ListObjectsV2Command({
    Bucket: BUCKET,
    Prefix: PREFIX,
    MaxKeys: 1000,
  }));

  const keys = (res.Contents || []).map(o => o.Key!).filter(Boolean);
  console.log(`Found ${keys.length} objects total.`);

  // Find keys that match the old pattern: staging/activations/{folder}/maids.csv
  const oldPattern = keys.filter(k => k.endsWith('/maids.csv') && k.startsWith(PREFIX));

  if (oldPattern.length === 0) {
    console.log('No folders to flatten. All good!');
    return;
  }

  console.log(`\nFound ${oldPattern.length} folder(s) to flatten:\n`);

  for (const oldKey of oldPattern) {
    // Extract folder name: staging/activations/{folderName}/maids.csv → folderName
    const relativePath = oldKey.slice(PREFIX.length); // "{folderName}/maids.csv"
    const folderName = relativePath.split('/')[0];
    const newKey = `${PREFIX}${folderName}.csv`;

    console.log(`  ${oldKey}`);
    console.log(`  → ${newKey}`);

    // Copy to flat path
    await s3.send(new CopyObjectCommand({
      Bucket: BUCKET,
      CopySource: `${BUCKET}/${oldKey}`,
      Key: newKey,
    }));

    // Delete the old nested file
    await s3.send(new DeleteObjectCommand({
      Bucket: BUCKET,
      Key: oldKey,
    }));

    console.log(`  ✓ done\n`);
  }

  console.log(`Flattened ${oldPattern.length} activation file(s).`);
}

main().catch(err => {
  console.error('Migration failed:', err);
  process.exit(1);
});
