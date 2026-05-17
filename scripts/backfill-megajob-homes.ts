/**
 * Backfill home-location tables for every sub-job of a megajob.
 *
 *   npx tsx scripts/backfill-megajob-homes.ts <megaJobId>
 *
 * Discovers the megajob's synced sub-jobs, then for each one that's
 * missing its home table at config/home-locations/{ds}/, kicks off
 * the TC-WK-19-7 CTAS (METHODOLOGY.md §2.3). Sub-jobs are processed
 * sequentially so we don't blast Athena's concurrent-DML quota.
 *
 * Use this once per megajob (typically right after consolidation
 * finishes). Once the home tables exist, Phase 2.B-ii's mega catchment
 * query will pick them up automatically on its next run.
 */

import 'dotenv/config';
import { getMegaJob } from '../lib/mega-jobs';
import { getJob } from '../lib/jobs';
import {
  startHomeDetection,
  pollHomeDetection,
  homeTableExists,
  homeTableName,
} from '../lib/home-detector';

async function backfillOne(ds: string): Promise<void> {
  if (await homeTableExists(ds)) {
    console.log(`  ✓ ${ds}: already has home table (${homeTableName(ds)}); skipping.`);
    return;
  }

  console.log(`  → ${ds}: starting TC-WK-19-7 CTAS…`);
  const t0 = Date.now();
  const { queryId } = await startHomeDetection(ds);
  let lastState = '';
  for (let i = 0; i < 600; i++) {  // up to 30 min per sub-job
    const { state, error } = await pollHomeDetection(queryId);
    if (state !== lastState) {
      lastState = state;
      const elapsed = Math.round((Date.now() - t0) / 1000);
      console.log(`    [${String(elapsed).padStart(4)}s] ${state}${error ? `: ${error}` : ''}`);
    }
    if (state === 'done') {
      const elapsed = Math.round((Date.now() - t0) / 1000);
      console.log(`  ✓ ${ds}: completed in ${elapsed}s`);
      return;
    }
    if (state === 'error') {
      throw new Error(`Home detection failed for ${ds}: ${error}`);
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
  throw new Error(`Home detection for ${ds} timed out after 30 min`);
}

async function main() {
  const megaJobId = process.argv[2];
  if (!megaJobId) {
    console.error('Usage: npx tsx scripts/backfill-megajob-homes.ts <megaJobId>');
    process.exit(1);
  }

  const mj = await getMegaJob(megaJobId);
  if (!mj) {
    console.error(`Mega-job not found: ${megaJobId}`);
    process.exit(1);
  }

  console.log(`\n=== Backfill home tables for ${mj.name} ===\n`);
  console.log(`Mega-job ID: ${mj.megaJobId}`);
  console.log(`Sub-jobs   : ${mj.subJobIds.length}\n`);

  // Resolve sub-job dataset names.
  const subDsNames: string[] = [];
  for (const jid of mj.subJobIds) {
    const j = await getJob(jid);
    if (!j) {
      console.warn(`  ! ${jid}: not found, skipping`);
      continue;
    }
    if (!j.s3DestPath || !j.syncedAt) {
      console.warn(`  ! ${jid}: not synced (${j.status}), skipping`);
      continue;
    }
    const ds = j.s3DestPath.replace(/\/$/, '').split('/').pop()!;
    subDsNames.push(ds);
  }
  console.log(`Synced sub-job datasets: ${subDsNames.length}\n`);

  // Process sequentially.
  let okCount = 0;
  let failCount = 0;
  for (const ds of subDsNames) {
    try {
      await backfillOne(ds);
      okCount++;
    } catch (e: any) {
      console.error(`  ✗ ${ds}: ${e.message}`);
      failCount++;
    }
  }

  console.log(`\n=== Done: ${okCount} succeeded, ${failCount} failed ===\n`);
  if (failCount > 0) process.exit(1);
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
