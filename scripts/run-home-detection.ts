/**
 * Manual smoke-test for the TC-WK-19-7 home-detection pipeline.
 *
 *   npx tsx scripts/run-home-detection.ts <datasetName>
 *
 * Runs:
 *   1. startHomeDetection(ds) — kicks off the CTAS
 *   2. pollHomeDetection(queryId) in a loop until done or error
 *   3. SELECT COUNT(*) on the output table for a quick sanity check
 *   4. A small per-home-confidence histogram
 *   5. Top-20 home buckets by population (sanity-check: are dense
 *      buckets in actually-populated places?)
 *
 * No code paths in production touch the home table yet — the rewrite
 * of catchment to JOIN against this output is Phase 2.B per
 * docs/METHODOLOGY.md §7. This script is a manual gate before
 * unleashing the new query on every dataset.
 */

import 'dotenv/config';
import { startHomeDetection, pollHomeDetection, buildHomeDetectionSQL } from '../lib/home-detector';
import { runQuery, getTableName, ensureTableForDataset } from '../lib/athena';

async function main() {
  const ds = process.argv[2];
  const dryRun = process.argv.includes('--dry-run');

  if (!ds) {
    console.error('Usage: npx tsx scripts/run-home-detection.ts <datasetName> [--dry-run]');
    console.error('       --dry-run prints the SQL without executing.');
    process.exit(1);
  }

  if (dryRun) {
    await ensureTableForDataset(ds);
    const sourceTable = getTableName(ds);
    const sql = buildHomeDetectionSQL(sourceTable, 'home_DRY_RUN', `home-locations/${ds}`);
    console.log(sql);
    return;
  }

  console.log(`\n=== Home detection: ${ds} ===\n`);
  console.log('Methodology: TC-WK-19-7 (Pappalardo et al. 2023). See docs/METHODOLOGY.md §2.3.');
  console.log('Filters: nighttime 19h-07h, weekdays, GPS quality, ≥3 distinct nights.\n');

  const t0 = Date.now();
  const { queryId, outputTable, outputS3Prefix } = await startHomeDetection(ds);
  console.log(`Athena queryId : ${queryId}`);
  console.log(`Output table   : ${outputTable}`);
  console.log(`Output prefix  : s3://.../${outputS3Prefix}/\n`);

  // Poll loop. Athena queries can take 10s-30min depending on dataset
  // size and partition count.
  let lastState = '';
  for (let i = 0; i < 600; i++) {
    const { state, error } = await pollHomeDetection(queryId);
    if (state !== lastState) {
      lastState = state;
      console.log(`[${(Math.round((Date.now() - t0) / 1000)).toString().padStart(4)}s] state: ${state}`);
    }
    if (state === 'done') break;
    if (state === 'error') {
      console.error(`FAILED: ${error}`);
      process.exit(1);
    }
    await new Promise((r) => setTimeout(r, 3000));
  }

  const elapsedSec = Math.round((Date.now() - t0) / 1000);
  console.log(`\nQuery finished in ${elapsedSec}s.\n`);

  // ── Sanity checks against the output ─────────────────────────────
  console.log('--- Output sanity checks ---\n');

  const total = await runQuery(`SELECT COUNT(*) AS n FROM ${outputTable}`);
  const nHomes = Number(total.rows[0]?.n || 0);
  console.log(`Total homes detected      : ${nHomes.toLocaleString()}`);

  // For context, count distinct MAIDs in the source — what fraction
  // got a confident home?
  const sourceTable = getTableName(ds);
  const allMaids = await runQuery(`SELECT COUNT(DISTINCT ad_id) AS n FROM ${sourceTable}`);
  const nMaids = Number(allMaids.rows[0]?.n || 0);
  console.log(`Total distinct ad_ids     : ${nMaids.toLocaleString()}`);
  if (nMaids > 0) {
    console.log(`Home-coverage rate        : ${((nHomes / nMaids) * 100).toFixed(1)}% of MAIDs got a stable home`);
  }

  // Histogram of n_nights — quick read on stability distribution.
  const histo = await runQuery(`
    SELECT
      CASE
        WHEN n_nights <= 3 THEN '03 nights'
        WHEN n_nights <= 5 THEN '04-05 nights'
        WHEN n_nights <= 10 THEN '06-10 nights'
        WHEN n_nights <= 20 THEN '11-20 nights'
        ELSE '21+ nights'
      END AS bucket,
      COUNT(*) AS n
    FROM ${outputTable}
    GROUP BY 1
    ORDER BY 1
  `);
  console.log(`\nNights-at-home histogram:`);
  histo.rows.forEach((r: any) => {
    console.log(`  ${r.bucket.padEnd(14)} ${Number(r.n).toLocaleString()}`);
  });

  // Confidence distribution.
  const conf = await runQuery(`
    SELECT
      CASE
        WHEN home_confidence < 0.3 THEN 'a) <30%   (noisy)'
        WHEN home_confidence < 0.5 THEN 'b) 30-50%'
        WHEN home_confidence < 0.7 THEN 'c) 50-70%'
        WHEN home_confidence < 0.9 THEN 'd) 70-90%'
        ELSE 'e) >=90% (very stable)'
      END AS bucket,
      COUNT(*) AS n
    FROM ${outputTable}
    GROUP BY 1
    ORDER BY 1
  `);
  console.log(`\nHome-confidence histogram:`);
  conf.rows.forEach((r: any) => {
    console.log(`  ${r.bucket.padEnd(22)} ${Number(r.n).toLocaleString()}`);
  });

  // Top home buckets — these should land on actual residential areas.
  const top = await runQuery(`
    SELECT
      ROUND(home_lat, 2) AS lat,
      ROUND(home_lng, 2) AS lng,
      ARBITRARY(home_zip) AS zip,
      ARBITRARY(home_city) AS city,
      ARBITRARY(home_region) AS region,
      COUNT(*) AS n_homes
    FROM ${outputTable}
    GROUP BY ROUND(home_lat, 2), ROUND(home_lng, 2)
    ORDER BY n_homes DESC
    LIMIT 20
  `);
  console.log(`\nTop-20 home buckets:`);
  console.log(`  ${'#'.padEnd(2)} ${'n_homes'.padEnd(9)} ${'lat'.padEnd(8)} ${'lng'.padEnd(9)} ${'zip'.padEnd(9)} ${'city'.padEnd(28)} region`);
  top.rows.forEach((r: any, i: number) => {
    console.log(
      `  ${String(i + 1).padStart(2)} ${String(Number(r.n_homes).toLocaleString()).padEnd(9)} ` +
      `${String(r.lat).padEnd(8)} ${String(r.lng).padEnd(9)} ` +
      `${String(r.zip || '—').padEnd(9)} ` +
      `${String(r.city || '—').slice(0, 27).padEnd(28)} ` +
      `${r.region || '—'}`
    );
  });

  console.log(`\n=== Done. Output table: ${outputTable} ===\n`);
}

main().catch((e) => {
  console.error('FATAL:', e);
  process.exit(1);
});
