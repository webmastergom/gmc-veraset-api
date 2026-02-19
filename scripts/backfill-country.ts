#!/usr/bin/env npx tsx
/**
 * Backfill country field on jobs that have audienceAgentEnabled but no country.
 * Infers country from job name using pattern matching.
 *
 * Usage:
 *   npx tsx scripts/backfill-country.ts          # dry-run (shows what would change)
 *   npx tsx scripts/backfill-country.ts --apply   # actually writes to S3
 */

import { getAllJobs, updateJob } from '../lib/jobs';
import { inferCountryFromName } from '../lib/country-inference';

async function main() {
  const apply = process.argv.includes('--apply');

  console.log(apply ? '🔧 APPLYING changes to S3...' : '👀 DRY RUN (pass --apply to write)');
  console.log('');

  const jobs = await getAllJobs();
  const needsBackfill = jobs.filter(j => j.audienceAgentEnabled && !j.country);

  if (needsBackfill.length === 0) {
    console.log('✅ All audience-enabled jobs already have a country set.');
    return;
  }

  console.log(`Found ${needsBackfill.length} jobs with audienceAgentEnabled but no country:\n`);

  let updated = 0;
  let skipped = 0;

  for (const job of needsBackfill) {
    const inferred = inferCountryFromName(job.name);
    if (inferred) {
      console.log(`  ✅ "${job.name}" → ${inferred}`);
      if (apply) {
        await updateJob(job.jobId, { country: inferred });
      }
      updated++;
    } else {
      console.log(`  ⚠️  "${job.name}" → UNKNOWN (cannot infer country)`);
      skipped++;
    }
  }

  console.log('');
  console.log(`Results: ${updated} updated, ${skipped} skipped (unknown country)`);
  if (!apply && updated > 0) {
    console.log('\nRun with --apply to persist changes to S3.');
  }
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
