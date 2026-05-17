/**
 * Nuke every derived artifact for a dataset, leaving only the
 * Veraset-synced parquets and the job/UI config untouched.
 *
 *   npx tsx scripts/reset-dataset.ts <datasetName> [--yes]
 *
 * What gets deleted (all in S3 garritz-veraset-data-us-west-2):
 *   1. Home table: Glue catalog entry + `home-locations/{ds}/` parquet
 *   2. All dataset reports: `config/dataset-reports/{ds}/**`
 *      (visits, temporal, hourly, od, catchment*, affinity*, mobility,
 *      and any subdirectories like category-affinity/*)
 *   3. Per-dataset basic analysis: `config/dataset-analysis/{ds}.json`
 *   4. All polling/state files keyed by the dataset:
 *        - `config/dataset-report-state/{ds}.json`
 *        - `config/catchment-state/{ds}.json`
 *        - `config/analysis-state/{ds}.json`
 *
 * What gets KEPT (intentional):
 *   • The raw parquet data under `{datasetName}/...` (the actual sync)
 *   • The job config in `config/jobs.json` / mega-jobs index
 *   • Geocode caches and POI collections (shared across datasets)
 *   • Exports under `exports/{ds}/` (user-triggered, not analysis)
 *
 * Use case: a previous run produced a biased home table or wrong
 * report and you want to re-run analysis from scratch without
 * re-syncing the parquets from Veraset.
 */

import 'dotenv/config';
import {
  ListObjectsV2Command,
  DeleteObjectsCommand,
} from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '../lib/s3-config';
import { dropHomeTable } from '../lib/home-detector';

async function listKeys(prefix: string): Promise<string[]> {
  const out: string[] = [];
  let continuationToken: string | undefined;
  do {
    const r = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    }));
    for (const o of r.Contents || []) if (o.Key) out.push(o.Key);
    continuationToken = r.IsTruncated ? r.NextContinuationToken : undefined;
  } while (continuationToken);
  return out;
}

async function deleteKeys(keys: string[]): Promise<number> {
  let deleted = 0;
  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000).map((Key) => ({ Key }));
    const r = await s3Client.send(new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: { Objects: batch, Quiet: true },
    }));
    deleted += batch.length - (r.Errors?.length || 0);
    if (r.Errors?.length) {
      for (const e of r.Errors) {
        console.error(`  delete error  ${e.Key}: ${e.Code} ${e.Message}`);
      }
    }
  }
  return deleted;
}

async function main() {
  const ds = process.argv[2];
  const yes = process.argv.includes('--yes');

  if (!ds) {
    console.error('Usage: npx tsx scripts/reset-dataset.ts <datasetName> [--yes]');
    process.exit(1);
  }

  console.log(`\n=== Reset dataset: ${ds} ===\n`);
  console.log(`Bucket: ${BUCKET}\n`);

  // ── 1. Plan ─────────────────────────────────────────────────────
  const reportsPrefix = `config/dataset-reports/${ds}/`;
  const homePrefix = `home-locations/${ds}/`;
  const singleStateKeys = [
    `config/dataset-analysis/${ds}.json`,
    `config/dataset-report-state/${ds}.json`,
    `config/catchment-state/${ds}.json`,
    `config/analysis-state/${ds}.json`,
  ];

  console.log('Listing what would be deleted...');
  const [reportKeys, homeKeys] = await Promise.all([
    listKeys(reportsPrefix),
    listKeys(homePrefix),
  ]);

  console.log(`  reports under ${reportsPrefix}     ${reportKeys.length} keys`);
  console.log(`  home parquets under ${homePrefix}  ${homeKeys.length} keys`);
  console.log(`  state files                            ${singleStateKeys.length} potential keys`);

  const total = reportKeys.length + homeKeys.length + singleStateKeys.length;
  if (total === 0) {
    console.log('\nNothing to delete. Dataset is already clean.');
    return;
  }

  if (!yes) {
    console.log('\nDry run. Re-run with --yes to actually delete.');
    return;
  }

  // ── 2. Execute ──────────────────────────────────────────────────
  console.log('\nDeleting…');

  console.log('  1/4  drop home table (Glue + S3 parquet)');
  await dropHomeTable(ds);

  if (reportKeys.length > 0) {
    console.log(`  2/4  wipe ${reportKeys.length} report file(s)`);
    const n = await deleteKeys(reportKeys);
    console.log(`       ${n} deleted`);
  } else {
    console.log('  2/4  no report files');
  }

  console.log('  3/4  delete state files (idempotent)');
  await deleteKeys(singleStateKeys);

  console.log('  4/4  done');
  console.log(`\n=== ${ds} reset. Next /reports/poll click will re-run home detection + reports from scratch. ===\n`);
}

main().catch((e) => {
  console.error('FATAL:', e);
  process.exit(1);
});
