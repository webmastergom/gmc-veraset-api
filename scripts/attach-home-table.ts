/** Attach an existing home-locations parquet as an external Glue table.
 *  Avoids re-running the (12 min) CTAS when parquet already exists.
 *
 *    npx tsx scripts/attach-home-table.ts <datasetName>
 */
import 'dotenv/config';
import { attachHomeTable, homeTableName, homeTableExists } from '../lib/home-detector';

async function main() {
  const ds = process.argv[2];
  if (!ds) {
    console.error('Usage: npx tsx scripts/attach-home-table.ts <datasetName>');
    process.exit(1);
  }
  const before = await homeTableExists(ds);
  console.log(`Before: home table ${homeTableName(ds)} ${before ? 'EXISTS' : 'absent'}.`);
  if (before) {
    console.log('Nothing to do.');
    return;
  }
  await attachHomeTable(ds);
  console.log(`Attached: ${homeTableName(ds)} now queryable.`);
}

main().catch((e) => { console.error('FATAL:', e); process.exit(1); });
