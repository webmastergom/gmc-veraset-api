#!/usr/bin/env npx tsx
/**
 * Reprocess existing master-maids contributions with the bot filter
 * introduced in lib/bot-filter.ts.
 *
 * For each registered `plain` / `catchment` Athena CTAS table:
 *   1. DROP the existing table
 *   2. Wipe its athena-temp/ S3 prefix
 *   3. Re-run CTAS with the new qualified_ad_ids CTE (≥ 2 distinct
 *      visit-days per ad_id) against the source pings table
 *   4. Read the new row count and update master-maids-index.json
 *
 * After running this, trigger the per-country consolidation from the
 * /master-maids UI (or POST /api/master-maids/{cc}/consolidate) so
 * country-level totalMaids reflects the new filtered tables.
 *
 * Scope: only handles contributions whose sourceDataset is a single
 * job (e.g. "job-26ff1993") — the pings table is unambiguous. Mega
 * contributions (sourceDataset starts with "mega-" or contains " + ")
 * are skipped; reprocess those via the mega-job consolidation flow,
 * which already uses the bot-filter floor.
 *
 * Usage:
 *   # Dry run for all countries (shows what would be reprocessed)
 *   npx tsx scripts/reprocess-master-maids.ts
 *
 *   # Reprocess a single country
 *   npx tsx scripts/reprocess-master-maids.ts --apply MX
 *
 *   # Reprocess multiple countries
 *   npx tsx scripts/reprocess-master-maids.ts --apply MX ES FR
 *
 *   # Reprocess all calibrated countries
 *   npx tsx scripts/reprocess-master-maids.ts --apply --all
 *
 * Cost note: each plain CTAS scans the full source pings table for the
 * dataset's date window. Mexico Cities POIs April 2026 = 748 GB scan
 * ≈ $3.74 per Athena run. Budget ~$15-25 to reprocess all MX
 * contributions, ~$2-5 per other country.
 */

import {
  runQuery,
  startQueryAsync,
  checkQueryStatus,
  getTableName,
  ensureTableForDataset,
  dropTempTable,
  cleanupTempS3,
} from '../lib/athena';
import {
  getConfig,
  putConfig,
  invalidateCache,
  BUCKET,
} from '../lib/s3-config';
import { qualifiedAdIdsCTE } from '../lib/bot-filter';
import type {
  MasterMaidsIndex,
  Contribution,
} from '../lib/master-maids';

const INDEX_KEY = 'master-maids-index';

interface ReprocessResult {
  cc: string;
  contribution: Contribution;
  oldMaidCount: number;
  newMaidCount: number | null;
  durationSec: number;
  bytesScanned: number;
  error?: string;
}

/** Source-dataset name → Athena table identifier. Mirrors getTableName. */
function pingsTableFor(sourceDataset: string): string {
  return getTableName(sourceDataset);
}

/**
 * True iff this contribution maps to a single-job pings table we can
 * re-CTAS from. Skips mega aggregates and "X + Y" combined sources.
 */
function isReprocessable(c: Contribution): boolean {
  if (c.attributeType !== 'plain' && c.attributeType !== 'catchment') return false;
  if (!c.athenaTable || c.athenaTable.trim() === '') return false;
  if (!c.sourceDataset) return false;
  if (c.sourceDataset.startsWith('mega-')) return false;
  if (c.sourceDataset.includes(' + ')) return false;
  if (!c.dateRange?.from || !c.dateRange?.to) return false;
  return true;
}

function buildPlainCtas(c: Contribution): string {
  const pingsTable = pingsTableFor(c.sourceDataset);
  return `
    CREATE TABLE ${c.athenaTable}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='s3://${BUCKET}/${c.s3Prefix.replace(/\/$/, '')}/')
    AS
    WITH ${qualifiedAdIdsCTE(pingsTable, c.dateRange.from, c.dateRange.to)}
    SELECT ad_id FROM qualified_ad_ids
  `;
}

function buildCatchmentCtas(c: Contribution): string {
  const pingsTable = pingsTableFor(c.sourceDataset);
  return `
    CREATE TABLE ${c.athenaTable}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='s3://${BUCKET}/${c.s3Prefix.replace(/\/$/, '')}/')
    AS
    WITH ${qualifiedAdIdsCTE(pingsTable, c.dateRange.from, c.dateRange.to)},
    poi_visitors AS (
      SELECT t.ad_id, t.date FROM ${pingsTable} t
      INNER JOIN qualified_ad_ids q ON t.ad_id = q.ad_id
      WHERE CARDINALITY(t.poi_ids) > 0
        AND t.date >= '${c.dateRange.from}' AND t.date <= '${c.dateRange.to}'
      GROUP BY t.ad_id, t.date
    ),
    origins AS (
      SELECT pv.ad_id,
        ROUND(MIN_BY(TRY_CAST(t.latitude AS DOUBLE), t.utc_timestamp), 1) as origin_lat,
        ROUND(MIN_BY(TRY_CAST(t.longitude AS DOUBLE), t.utc_timestamp), 1) as origin_lng
      FROM poi_visitors pv
      INNER JOIN ${pingsTable} t ON pv.ad_id = t.ad_id AND pv.date = t.date
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
      GROUP BY pv.ad_id
    )
    SELECT DISTINCT ad_id, origin_lat, origin_lng
    FROM origins WHERE origin_lat IS NOT NULL
  `;
}

/** Poll a query to terminal state. Returns final state + scan bytes. */
async function waitForQuery(queryId: string, label: string): Promise<{ state: string; bytesScanned: number; error?: string }> {
  let elapsed = 0;
  const POLL_INTERVAL_MS = 5_000;
  const MAX_WAIT_MS = 30 * 60 * 1000;
  while (elapsed < MAX_WAIT_MS) {
    const status = await checkQueryStatus(queryId);
    if (status.state === 'SUCCEEDED' || status.state === 'FAILED' || status.state === 'CANCELLED') {
      return {
        state: status.state,
        bytesScanned: status.statistics?.dataScannedBytes ?? 0,
        error: status.state !== 'SUCCEEDED' ? (status.error ?? 'unknown') : undefined,
      };
    }
    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
    elapsed += POLL_INTERVAL_MS;
    if (elapsed % 30_000 === 0) {
      console.log(`    [${label}] still ${status.state} after ${elapsed / 1000}s...`);
    }
  }
  return { state: 'TIMEOUT', bytesScanned: 0, error: `>${MAX_WAIT_MS / 1000}s without finishing` };
}

async function reprocessContribution(cc: string, c: Contribution): Promise<ReprocessResult> {
  const start = Date.now();
  const sourceDataset = c.sourceDataset;
  const oldMaidCount = c.maidCount || 0;
  const label = `${cc}/${c.attributeType}/${sourceDataset}`;

  try {
    console.log(`  → ${label}: ensure pings table exists`);
    await ensureTableForDataset(sourceDataset);

    console.log(`  → ${label}: drop ${c.athenaTable}`);
    await dropTempTable(c.athenaTable);

    console.log(`  → ${label}: wipe S3 prefix ${c.s3Prefix}`);
    // s3Prefix is stored without the leading "athena-temp/" but the
    // cleanup helper prepends it. Normalize either form.
    const prefixForCleanup = c.s3Prefix.replace(/^athena-temp\//, '').replace(/\/$/, '');
    await cleanupTempS3(prefixForCleanup);

    const sql = c.attributeType === 'plain' ? buildPlainCtas(c) : buildCatchmentCtas(c);
    console.log(`  → ${label}: start CTAS`);
    const qid = await startQueryAsync(sql);
    const result = await waitForQuery(qid, label);
    if (result.state !== 'SUCCEEDED') {
      return {
        cc, contribution: c, oldMaidCount, newMaidCount: null,
        durationSec: (Date.now() - start) / 1000,
        bytesScanned: result.bytesScanned,
        error: `CTAS ${result.state}: ${result.error}`,
      };
    }

    console.log(`  → ${label}: count rows`);
    const cntResult = await runQuery(`SELECT COUNT(*) as cnt FROM ${c.athenaTable}`);
    const newMaidCount = parseInt(cntResult.rows[0]?.cnt as string, 10) || 0;

    return {
      cc, contribution: c, oldMaidCount, newMaidCount,
      durationSec: (Date.now() - start) / 1000,
      bytesScanned: result.bytesScanned,
    };
  } catch (e: any) {
    return {
      cc, contribution: c, oldMaidCount, newMaidCount: null,
      durationSec: (Date.now() - start) / 1000,
      bytesScanned: 0,
      error: e.message ?? String(e),
    };
  }
}

async function main() {
  const args = process.argv.slice(2);
  const apply = args.includes('--apply');
  const allFlag = args.includes('--all');
  const cliCountries = args.filter(a => !a.startsWith('--')).map(s => s.toUpperCase());

  console.log(apply ? '🔧 APPLY mode — will DROP and re-CTAS Athena tables' : '👀 DRY RUN (pass --apply to actually reprocess)');
  console.log('');

  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY);
  if (!index) {
    console.error(`No ${INDEX_KEY} found in S3.`);
    process.exit(1);
  }

  const allCountries = Object.keys(index).sort();
  const targetCountries = allFlag ? allCountries : (cliCountries.length > 0 ? cliCountries : allCountries);

  console.log(`Master MAIDs index has ${allCountries.length} countries: ${allCountries.join(', ')}`);
  console.log(`Targeting: ${targetCountries.join(', ')}`);
  console.log('');

  const todo: Array<{ cc: string; c: Contribution }> = [];
  const skipped: Array<{ cc: string; c: Contribution; reason: string }> = [];
  for (const cc of targetCountries) {
    const entry = index[cc];
    if (!entry) continue;
    for (const c of entry.contributions) {
      if (isReprocessable(c)) {
        todo.push({ cc, c });
      } else {
        const reason = c.attributeType !== 'plain' && c.attributeType !== 'catchment'
          ? `attr=${c.attributeType}`
          : !c.athenaTable ? 'legacy CSV (no Athena table)'
            : c.sourceDataset?.startsWith('mega-') ? 'mega-job source'
              : c.sourceDataset?.includes(' + ') ? 'combined source'
                : !c.dateRange?.from ? 'no date range'
                  : 'unknown';
        skipped.push({ cc, c, reason });
      }
    }
  }

  console.log(`Plan: reprocess ${todo.length} contributions, skip ${skipped.length}`);
  console.log('');

  if (skipped.length > 0) {
    console.log('Skipped (cannot reprocess from a single pings table):');
    const skippedByReason: Record<string, number> = {};
    for (const s of skipped) skippedByReason[s.reason] = (skippedByReason[s.reason] || 0) + 1;
    for (const [r, n] of Object.entries(skippedByReason)) console.log(`  ${r}: ${n}`);
    console.log('');
  }

  if (todo.length === 0) {
    console.log('Nothing to do.');
    return;
  }

  console.log('Reprocess plan (top 20 by current maidCount):');
  const top20 = [...todo].sort((a, b) => (b.c.maidCount || 0) - (a.c.maidCount || 0)).slice(0, 20);
  for (const { cc, c } of top20) {
    console.log(`  ${cc}  ${c.attributeType.padEnd(9)}  ${(c.maidCount || 0).toLocaleString().padStart(12)} MAIDs  ${c.sourceDataset}  [${c.dateRange?.from}→${c.dateRange?.to}]`);
  }
  if (todo.length > 20) console.log(`  ... + ${todo.length - 20} more`);
  console.log('');

  const totalCurrentMaids = todo.reduce((s, t) => s + (t.c.maidCount || 0), 0);
  console.log(`Total MAIDs across reprocess plan: ${totalCurrentMaids.toLocaleString()}`);
  console.log('');

  if (!apply) {
    console.log('Dry run complete. Add --apply to execute.');
    return;
  }

  // ── Apply: reprocess contributions sequentially per country, parallel across countries ─
  console.log('Reprocessing…');
  const byCountry: Record<string, Array<{ cc: string; c: Contribution }>> = {};
  for (const t of todo) {
    if (!byCountry[t.cc]) byCountry[t.cc] = [];
    byCountry[t.cc].push(t);
  }

  const results: ReprocessResult[] = [];
  // Run one country at a time to stay safely under Athena's concurrent
  // query limit (default 20 DML + 20 DDL per workgroup). Within a
  // country, fire two contributions in parallel (plain + catchment of
  // the same job often share scan plans).
  for (const cc of Object.keys(byCountry)) {
    const items = byCountry[cc]!;
    console.log(`\n=== ${cc}: ${items.length} contributions ===`);
    for (let i = 0; i < items.length; i += 2) {
      const batch = items.slice(i, i + 2);
      const batchResults = await Promise.all(batch.map(({ cc, c }) => reprocessContribution(cc, c)));
      results.push(...batchResults);
      for (const r of batchResults) {
        if (r.error) {
          console.log(`  ✗ ${r.cc}/${r.contribution.attributeType}/${r.contribution.sourceDataset}: ${r.error}`);
        } else {
          const delta = (r.newMaidCount! - r.oldMaidCount);
          const pct = r.oldMaidCount > 0 ? ((r.newMaidCount! / r.oldMaidCount) * 100).toFixed(1) : 'n/a';
          console.log(`  ✓ ${r.cc}/${r.contribution.attributeType}/${r.contribution.sourceDataset}: ${r.oldMaidCount.toLocaleString()} → ${r.newMaidCount!.toLocaleString()} (${pct}% kept, Δ=${delta.toLocaleString()}, ${(r.bytesScanned / 1e9).toFixed(1)} GB scanned, ${r.durationSec.toFixed(0)}s)`);
        }
      }
    }
  }

  // ── Update master-maids-index with new counts ───────────────────
  console.log('\nUpdating master-maids-index.json…');
  invalidateCache(INDEX_KEY);
  const freshIndex = (await getConfig<MasterMaidsIndex>(INDEX_KEY))!;
  let updated = 0;
  for (const r of results) {
    if (r.error || r.newMaidCount === null) continue;
    const ccEntry = freshIndex[r.cc];
    if (!ccEntry) continue;
    const target = ccEntry.contributions.find(c => c.athenaTable === r.contribution.athenaTable);
    if (!target) continue;
    target.maidCount = r.newMaidCount;
    updated++;
  }
  await putConfig(INDEX_KEY, freshIndex);
  invalidateCache(INDEX_KEY);
  console.log(`Updated ${updated} contribution maidCount values.`);

  // ── Summary ─────────────────────────────────────────────────────
  const successes = results.filter(r => !r.error);
  const failures = results.filter(r => r.error);
  const totalOld = successes.reduce((s, r) => s + r.oldMaidCount, 0);
  const totalNew = successes.reduce((s, r) => s + (r.newMaidCount || 0), 0);
  const totalGB = results.reduce((s, r) => s + r.bytesScanned / 1e9, 0);
  console.log('\n═══ Summary ═══');
  console.log(`Reprocessed: ${successes.length} / ${results.length}`);
  console.log(`Failed:      ${failures.length}`);
  console.log(`MAIDs (sum of per-contrib raw counts, not deduped):`);
  console.log(`  Before: ${totalOld.toLocaleString()}`);
  console.log(`  After:  ${totalNew.toLocaleString()}`);
  console.log(`  Kept:   ${totalOld > 0 ? ((totalNew / totalOld) * 100).toFixed(1) : 'n/a'}%`);
  console.log(`Total scanned: ${totalGB.toFixed(1)} GB (~$${(totalGB / 1000 * 5).toFixed(2)} Athena cost)`);
  if (failures.length > 0) {
    console.log('\nFailures:');
    for (const f of failures) {
      console.log(`  ${f.cc}/${f.contribution.attributeType}/${f.contribution.sourceDataset}: ${f.error}`);
    }
  }
  console.log('\n→ Next: trigger per-country consolidation to refresh stats:');
  for (const cc of Object.keys(byCountry)) {
    console.log(`  curl -X POST -b "auth-token=$AUTH_TOKEN" https://gmc-mobility-api.vercel.app/api/master-maids/${cc}/consolidate`);
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
