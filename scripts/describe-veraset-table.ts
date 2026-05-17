/**
 * Veraset schema discovery script.
 *
 * Run: npx tsx scripts/describe-veraset-table.ts [datasetName]
 *
 * Purpose: confirm empirically whether Veraset's delivered parquets carry
 * any home-related columns we haven't been reading. We declare an Athena
 * EXTERNAL TABLE with a fixed column list (lib/athena.ts → ensureTableForDataset);
 * Athena reads parquet columns BY NAME. If Veraset adds extra columns to
 * the parquets, they're invisible to our queries unless we declare them.
 *
 * To check, we use:
 *   1. DESCRIBE on the registered Athena table — shows what WE declared.
 *   2. A SHOW CREATE TABLE — same info, alternate form.
 *   3. Reading the parquet schema directly via S3 (requires pyarrow or
 *      parquet-tools) is more authoritative but outside the scope here.
 *
 * For (3), the user can also run from CLI on a synced dataset:
 *      parquet-tools schema s3://garritz-veraset-data-us-west-2/{dataset}/{date}/file.parquet
 */

import 'dotenv/config';
import { runQuery, ensureTableForDataset, getTableName } from '../lib/athena';
import { s3Client, BUCKET } from '../lib/s3-config';
import { ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';

async function main() {
  const ds = process.argv[2];
  if (!ds) {
    console.error('Usage: npx tsx scripts/describe-veraset-table.ts <datasetName>');
    console.error('       e.g. job-e055fcf7');
    // Pick the most recent dataset automatically if none provided.
    console.log('\nSampling recent datasets in S3 for convenience…');
    const list = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: '',
      Delimiter: '/',
      MaxKeys: 100,
    }));
    const candidates = (list.CommonPrefixes || [])
      .map((p) => p.Prefix?.replace(/\/$/, ''))
      .filter((p): p is string =>
        !!p && (p.startsWith('job-') || p.match(/^[a-z0-9-]+_\d{4}/) !== null),
      )
      .slice(0, 20);
    console.log('Candidates:\n', candidates.map((c) => '  ' + c).join('\n'));
    process.exit(1);
  }

  console.log(`\n=== DESCRIBE for dataset: ${ds} ===\n`);

  await ensureTableForDataset(ds);
  const table = getTableName(ds);
  console.log(`Athena table name: ${table}`);

  // 1) DESCRIBE — what we DECLARED to Athena
  console.log('\n--- DESCRIBE (declared columns) ---');
  const desc = await runQuery(`DESCRIBE ${table}`);
  desc.rows.forEach((r: any, i: number) => {
    console.log(`  ${String(i + 1).padStart(2)} ${r.col_name || r.name || ''}\t${r.data_type || r.type || ''}`);
  });

  // 2) Sample a single parquet object — list keys under the dataset prefix
  console.log('\n--- Sample S3 keys (first 3 parquet files) ---');
  const sample = await s3Client.send(new ListObjectsV2Command({
    Bucket: BUCKET,
    Prefix: `${ds}/`,
    MaxKeys: 200,
  }));
  const parquets = (sample.Contents || [])
    .map((o) => o.Key)
    .filter((k): k is string => !!k && k.endsWith('.parquet') || k?.endsWith('.snappy.parquet') === true)
    .slice(0, 3);
  parquets.forEach((k) => console.log(`  s3://${BUCKET}/${k}`));

  if (parquets.length === 0) {
    console.log(`  (no parquets found under ${ds}/)`);
    return;
  }

  // 3) Use SELECT * LIMIT 1 to see ACTUAL columns from parquet metadata
  //    (Athena projects only declared columns, but a SHOW CREATE TABLE
  //    + checking $path may not reveal hidden columns).
  console.log('\n--- SELECT * LIMIT 1 (sample row, declared columns only) ---');
  try {
    const sampleQ = await runQuery(`SELECT * FROM ${table} LIMIT 1`);
    if (sampleQ.rows.length === 0) {
      console.log('  (no rows in table)');
    } else {
      const row = sampleQ.rows[0];
      Object.entries(row).forEach(([k, v]) => {
        const display = typeof v === 'string' && v.length > 80 ? v.slice(0, 80) + '…' : v;
        console.log(`  ${k}: ${JSON.stringify(display)}`);
      });
    }
  } catch (e: any) {
    console.log(`  SELECT failed: ${e.message}`);
  }

  // 4) Try probing for fields the schema doc HINTED at (home_zip, home_geohash, etc).
  //    If they exist in parquet, we can ALTER TABLE to add them.
  //    If they don't, DESCRIBE returns ERROR or the query fails.
  console.log('\n--- Probing for hypothetical home-* fields (each query returns 0 rows if absent) ---');
  const probes = [
    'home_zip', 'home_zipcode', 'home_geohash', 'home_h3',
    'home_lat', 'home_lng', 'home_country',
    'work_zip', 'work_geohash',
    'residential_zip', 'residence_zip',
    'inferred_home', 'home_location',
  ];
  for (const col of probes) {
    try {
      // Tries to read the column directly from a single ping. If parquet
      // has it (even though we didn't declare it), this would fail BUT
      // we can detect it from the error message.
      const probe = `SELECT TRY("${col}") as v FROM ${table} LIMIT 1`;
      const r = await runQuery(probe);
      if (r.rows.length > 0) {
        console.log(`  ✓ ${col}: column exists, sample value = ${JSON.stringify(r.rows[0].v)}`);
      } else {
        console.log(`  • ${col}: query succeeded with 0 rows`);
      }
    } catch (e: any) {
      const msg = String(e.message || '');
      if (msg.includes('cannot be resolved') || msg.includes('not found') || msg.includes('does not exist')) {
        console.log(`  ✗ ${col}: not in our table declaration`);
      } else {
        console.log(`  ? ${col}: error → ${msg.split('\n')[0]}`);
      }
    }
  }

  // 5) Read the actual parquet schema via S3 byte-range probing.
  //    Parquet footer holds the schema. We download the last 4KB (footer
  //    + magic) and try to grep for column names. This sidesteps Athena
  //    and reveals columns the parquet has that we never declared.
  console.log('\n--- Parquet footer schema probe (raw S3, bypassing Athena) ---');
  if (parquets[0]) {
    try {
      const obj = await s3Client.send(new GetObjectCommand({
        Bucket: BUCKET,
        Key: parquets[0],
        Range: 'bytes=-65536', // last 64KB — should contain the footer
      }));
      const buf = await obj.Body?.transformToByteArray();
      if (buf) {
        // Parquet schema column names are ASCII-encoded in the footer's
        // Thrift-serialized schema. We grep for likely names.
        const text = Buffer.from(buf).toString('latin1');
        const known = [
          'ad_id', 'utc_timestamp', 'latitude', 'longitude',
          'geo_fields', 'quality_fields', 'horizontal_accuracy',
          'iso_country_code', 'id_type', 'ip_address', 'poi_ids',
        ];
        const seen = new Set<string>();
        // Find candidate column names (alphanumeric / underscore tokens)
        const tokens = text.match(/[a-z][a-z0-9_]{2,30}/g) || [];
        for (const t of tokens) {
          if (t.match(/^[a-z][a-z_]/)) seen.add(t);
        }
        const declared = new Set(known);
        const unexpected: string[] = [];
        for (const t of seen) {
          if (declared.has(t)) continue;
          // Filter out obviously non-column tokens (Thrift, Parquet internals)
          if (t.match(/^(parquet|thrift|optional|required|repeated|group|primitive|int32|int64|byte_array|fixed_len|boolean|float|double|utf8|enum|date|time|timestamp|interval|json|bson|map|list|tuple|union|struct|none|null|true|false|gzip|snappy|brotli|zstd|uncompressed|plain|rle|bit_packed|delta|byte_stream|column|page|row_group|file_metadata|file_meta_data|column_metadata|column_meta_data|column_chunk|key_value|encoding|compression|num_values|total_compressed_size|total_uncompressed_size|file_path|file_offset|created_by|index_page_offset|data_page_offset|dictionary_page_offset|statistics|null_count|distinct_count|min_value|max_value|min|max|repetition_type|logical_type|converted_type|element|key|value|schema)$/)) continue;
          if (t.match(/^[0-9]/)) continue;
          unexpected.push(t);
        }
        const interesting = unexpected.filter((t) =>
          t.includes('home') || t.includes('work') || t.includes('zip') ||
          t.includes('residence') || t.includes('postal') || t.includes('geohash') ||
          t.includes('h3') || t.includes('zipcode') || t.includes('city') ||
          t.includes('country') || t.includes('region') || t.includes('precision') ||
          t.includes('score') || t.includes('source') || t.includes('ping')
        );
        console.log(`  Declared columns we expect: ${known.join(', ')}`);
        if (interesting.length > 0) {
          console.log(`  Interesting tokens in footer (subject to false positives):`);
          [...new Set(interesting)].sort().forEach((t) => console.log(`    • ${t}`));
        } else {
          console.log('  No interesting unexpected tokens found in footer.');
        }
      }
    } catch (e: any) {
      console.log(`  Footer read failed: ${e.message}`);
    }
  }

  console.log('\n=== END ===\n');
}

main().catch((e) => {
  console.error('FATAL:', e);
  process.exit(1);
});
