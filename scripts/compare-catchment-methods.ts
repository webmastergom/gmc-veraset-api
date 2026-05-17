/**
 * Compare OLD (first-ping-of-day) vs NEW (TC-WK-19-7 JOIN) catchment
 * for a given dataset. Runs both queries against the same data and
 * shows the top-30 origin grid cells for each, plus aggregate stats.
 *
 *    npx tsx scripts/compare-catchment-methods.ts <datasetName>
 *
 * This is the empirical validation called for by METHODOLOGY.md §5
 * (Validation plan). Use after running scripts/run-home-detection.ts
 * to produce the home table.
 */

import 'dotenv/config';
import { runQuery, getTableName, ensureTableForDataset } from '../lib/athena';
import { homeTableExists, homeTableName } from '../lib/home-detector';

const ACCURACY_THRESHOLD_METERS = 500;
const COORDINATE_PRECISION = 4;

function buildLegacyOriginsQuery(table: string): string {
  return `
    WITH
    poi_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    ),
    valid_pings AS (
      SELECT t.ad_id, t.date, t.utc_timestamp,
             TRY_CAST(t.latitude AS DOUBLE) as lat,
             TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${table} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
    ),
    first_pings AS (
      SELECT ad_id, date,
             MIN_BY(lat, utc_timestamp) as origin_lat,
             MIN_BY(lng, utc_timestamp) as origin_lng
      FROM valid_pings
      GROUP BY ad_id, date
    ),
    device_homes AS (
      SELECT ad_id,
             ROUND(origin_lat, ${COORDINATE_PRECISION}) as home_lat,
             ROUND(origin_lng, ${COORDINATE_PRECISION}) as home_lng,
             COUNT(DISTINCT date) as days_at_loc
      FROM first_pings
      GROUP BY ad_id,
               ROUND(origin_lat, ${COORDINATE_PRECISION}),
               ROUND(origin_lng, ${COORDINATE_PRECISION})
      HAVING COUNT(DISTINCT date) >= 3
    )
    SELECT
      ROUND(home_lat, 2) AS lat,
      ROUND(home_lng, 2) AS lng,
      COUNT(*) as device_days
    FROM device_homes
    GROUP BY ROUND(home_lat, 2), ROUND(home_lng, 2)
    ORDER BY device_days DESC
    LIMIT 30
  `;
}

function buildNewOriginsQuery(table: string, homeTbl: string): string {
  return `
    WITH poi_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    )
    SELECT
      ROUND(h.home_lat, 2) AS lat,
      ROUND(h.home_lng, 2) AS lng,
      COUNT(*) as device_days
    FROM ${homeTbl} h
    INNER JOIN poi_visitors v ON h.ad_id = v.ad_id
    GROUP BY ROUND(h.home_lat, 2), ROUND(h.home_lng, 2)
    ORDER BY device_days DESC
    LIMIT 30
  `;
}

async function main() {
  const ds = process.argv[2];
  if (!ds) {
    console.error('Usage: npx tsx scripts/compare-catchment-methods.ts <datasetName>');
    process.exit(1);
  }

  await ensureTableForDataset(ds);
  const sourceTable = getTableName(ds);
  const hasHome = await homeTableExists(ds);
  if (!hasHome) {
    console.error(`No home table for ${ds}. Run scripts/run-home-detection.ts first.`);
    process.exit(1);
  }
  const homeTbl = homeTableName(ds);

  console.log(`\n=== Catchment-method comparison: ${ds} ===\n`);
  console.log(`Source table : ${sourceTable}`);
  console.log(`Home table   : ${homeTbl}\n`);

  // Run both in parallel — they hit different parquet patterns and
  // benefit from Athena's concurrency.
  const t0 = Date.now();
  console.log('Running both queries (in parallel)…\n');
  const [legacyRes, newRes] = await Promise.all([
    runQuery(buildLegacyOriginsQuery(sourceTable)),
    runQuery(buildNewOriginsQuery(sourceTable, homeTbl)),
  ]);
  const elapsed = Math.round((Date.now() - t0) / 1000);
  console.log(`Both queries finished in ${elapsed}s.\n`);

  const fmt = (rows: any[], label: string) => {
    console.log(`--- TOP 30 origins: ${label} ---`);
    let sum = 0;
    rows.forEach((r: any) => sum += Number(r.device_days));
    console.log(`  Σ device_days (top 30): ${sum.toLocaleString()}\n`);
    console.log(`  ${'rank'.padEnd(4)} ${'device_days'.padEnd(13)} ${'lat'.padEnd(8)} ${'lng'.padEnd(9)}`);
    rows.forEach((r: any, i: number) => {
      console.log(
        `  ${String(i + 1).padStart(4)} ${String(Number(r.device_days).toLocaleString()).padEnd(13)} ` +
        `${String(r.lat).padEnd(8)} ${String(r.lng).padEnd(9)}`
      );
    });
    console.log();
  };

  fmt(legacyRes.rows, 'LEGACY (first-ping-of-day)');
  fmt(newRes.rows, 'NEW (TC-WK-19-7 home JOIN)');

  // Overlap analysis — how many of the top-30 grid cells are shared?
  const legacySet = new Set(legacyRes.rows.map((r: any) => `${r.lat},${r.lng}`));
  const newSet = new Set(newRes.rows.map((r: any) => `${r.lat},${r.lng}`));
  const shared = [...legacySet].filter((k) => newSet.has(k as string));
  console.log(`--- Overlap of top-30 grid cells ---`);
  console.log(`  Shared: ${shared.length} / 30`);
  console.log(`  Only LEGACY: ${legacySet.size - shared.length}`);
  console.log(`  Only NEW:    ${newSet.size - shared.length}`);
  console.log();
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
