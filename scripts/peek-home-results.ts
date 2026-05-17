/** One-off: query the home table after a successful run for quick stats. */
import 'dotenv/config';
import { runQuery } from '../lib/athena';

async function main() {
  const table = process.argv[2];
  if (!table) {
    console.error('Usage: npx tsx scripts/peek-home-results.ts <homeTableName>');
    process.exit(1);
  }

  const total = await runQuery(`SELECT COUNT(*) AS n FROM ${table}`);
  console.log(`Total homes detected: ${Number(total.rows[0]?.n || 0).toLocaleString()}`);

  const histo = await runQuery(`
    SELECT
      CASE
        WHEN n_nights = 3 THEN '03 nights      '
        WHEN n_nights <= 5 THEN '04-05 nights   '
        WHEN n_nights <= 10 THEN '06-10 nights   '
        WHEN n_nights <= 20 THEN '11-20 nights   '
        ELSE '21+ nights     '
      END AS bucket,
      COUNT(*) AS n
    FROM ${table}
    GROUP BY 1
    ORDER BY 1
  `);
  console.log(`\nNights-at-home histogram:`);
  histo.rows.forEach((r: any) => {
    console.log(`  ${r.bucket} ${Number(r.n).toLocaleString().padStart(12)}`);
  });

  const top = await runQuery(`
    SELECT
      ROUND(home_lat, 2) AS lat,
      ROUND(home_lng, 2) AS lng,
      ARBITRARY(home_zip) AS zip,
      ARBITRARY(home_city) AS city,
      ARBITRARY(home_region) AS region,
      COUNT(*) AS n_homes
    FROM ${table}
    GROUP BY ROUND(home_lat, 2), ROUND(home_lng, 2)
    ORDER BY n_homes DESC
    LIMIT 30
  `);
  console.log(`\nTop-30 home buckets (each ~1.1km grid cell):`);
  console.log(`  ${'rank'.padEnd(4)} ${'n_homes'.padEnd(10)} ${'lat'.padEnd(8)} ${'lng'.padEnd(9)} ${'zip'.padEnd(9)} ${'city'.padEnd(35)} region`);
  top.rows.forEach((r: any, i: number) => {
    console.log(
      `  ${String(i + 1).padStart(4)} ${String(Number(r.n_homes).toLocaleString()).padEnd(10)} ` +
      `${String(r.lat).padEnd(8)} ${String(r.lng).padEnd(9)} ` +
      `${String(r.zip || '—').padEnd(9)} ` +
      `${String(r.city || '—').slice(0, 34).padEnd(35)} ` +
      `${r.region || '—'}`
    );
  });
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
