/**
 * NSE bracket export via Athena UNLOAD — server-side bracket assignment.
 *
 * The previous JS-side approach (download big CSV → in-memory geocode → write
 * per-bracket CSVs) doesn't scale to country-wide POI grids: the SELECT *
 * result for the FR/ES 50k-POI megajobs hits ~1 GB of CSV which can't be
 * downloaded + parsed within Vercel's 60s function limit.
 *
 * Instead we materialize the bracket assignment as a small Athena external
 * table (one row per geocode grid cell with its bracket_label) and run UNLOAD
 * queries that JOIN the NSE CTAS with the bracket map and write per-bracket
 * MAIDs CSVs directly to S3. Athena does the heavy lifting; JS only handles
 * the small bracket map + status polling.
 */

import { PutObjectCommand, ListObjectsV2Command, DeleteObjectsCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET, getConfig } from './s3-config';
import { startQueryAsync, dropTempTable, runQuery, tempTableName } from './athena';

export interface NseBracket {
  label: string;
  min: number;
  max: number;
}

export interface NseRecord {
  postal_code: string;
  population: number;
  nse: number;
}

export interface BracketUnloadHandle {
  label: string;
  unloadQueryId: string;
  unloadS3Prefix: string;
}

/**
 * Build the bracket map CSV content from the country's geocode cache + NSE config.
 * Returns CSV string with header row (lat_key,lng_key,bracket_label) and one row
 * per grid cell that maps to a postal code with a bracket assignment.
 */
function buildBracketMapCsv(
  geocodeCacheCsv: string,
  nseRecords: NseRecord[],
  brackets: NseBracket[],
): { csv: string; rowCount: number; perBracket: Record<string, number> } {
  // postal_code → bracket_label
  const cpToBracket = new Map<string, string>();
  for (const r of nseRecords) {
    for (const b of brackets) {
      if (r.nse >= b.min && r.nse <= b.max) {
        cpToBracket.set(r.postal_code, b.label);
        break;
      }
    }
  }

  const lines = ['lat_key,lng_key,bracket_label'];
  const perBracket: Record<string, number> = {};
  let rowCount = 0;

  for (const line of geocodeCacheCsv.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const parts = trimmed.split(',');
    if (parts.length < 3) continue;
    const [latKey, lngKey, postalCode] = parts;
    const bracket = cpToBracket.get(postalCode);
    if (!bracket) continue; // grid cell maps to a postal code outside any bracket
    lines.push(`${latKey},${lngKey},${bracket}`);
    perBracket[bracket] = (perBracket[bracket] || 0) + 1;
    rowCount++;
  }

  return { csv: lines.join('\n') + '\n', rowCount, perBracket };
}

/**
 * Materialize the bracket-assignment map as an Athena external table.
 *
 * Reads the country's pre-computed geocode cache (lat_key,lng_key,postal_code)
 * from S3, reads the country's NSE config from S3 (postal_code → nse score),
 * computes the bracket assignment per grid cell, uploads the result as a CSV
 * to S3, and registers it as an Athena external table.
 *
 * Returns the table name and the S3 prefix (for cleanup).
 */
export async function materializeNseBracketMap(
  megaJobId: string,
  runId: string,
  country: string,
  brackets: NseBracket[],
): Promise<{ tableName: string; s3Prefix: string; perBracket: Record<string, number> }> {
  const cc = country.toUpperCase();

  // 1. Load geocode cache CSV from S3
  const geocodeKey = `config/geocode-cache/${cc}.csv`;
  const geocodeRes = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: geocodeKey }));
  const geocodeCsv = await geocodeRes.Body!.transformToString('utf-8');

  // 2. Load NSE records from S3
  const nseRecords = await getConfig<NseRecord[]>(`nse/${cc}`);
  if (!nseRecords?.length) {
    throw new Error(`No NSE config for ${cc} — upload one before running NSE export`);
  }

  // 3. Build bracket map CSV
  const { csv, rowCount, perBracket } = buildBracketMapCsv(geocodeCsv, nseRecords, brackets);
  console.log(`[NSE-BRACKET-MAP] Built bracket map for ${cc}: ${rowCount} grid cells, per-bracket=${JSON.stringify(perBracket)}`);

  // 4. Upload to S3
  const s3Prefix = `nse-bracket-map/${megaJobId}_${runId}`;
  const csvKey = `${s3Prefix}/map.csv`;
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: csvKey,
    Body: csv,
    ContentType: 'text/csv',
  }));

  // 5. Create external Athena table
  const tableName = tempTableName('nse_bracket_map', `${megaJobId}_${runId}`);
  // Drop if exists (in case of retry)
  try { await dropTempTable(tableName); } catch {}

  const ddl = `
    CREATE EXTERNAL TABLE ${tableName} (
      lat_key BIGINT,
      lng_key BIGINT,
      bracket_label STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION 's3://${BUCKET}/${s3Prefix}/'
    TBLPROPERTIES ('skip.header.line.count'='1')
  `;
  await runQuery(ddl);
  console.log(`[NSE-BRACKET-MAP] Registered ${tableName} (s3://${BUCKET}/${s3Prefix}/)`);

  return { tableName, s3Prefix, perBracket };
}

/**
 * Build the JOIN CTE used by both UNLOAD and COUNT queries.
 * Joins the per-device NSE CTAS with the bracket map by rounded coords.
 */
function buildJoinSql(nseCtasTable: string, bracketMapTable: string): string {
  return `
    FROM ${nseCtasTable} n
    JOIN ${bracketMapTable} b
      ON CAST(ROUND(n.origin_lat * 10) AS BIGINT) = b.lat_key
      AND CAST(ROUND(n.origin_lng * 10) AS BIGINT) = b.lng_key
  `;
}

/**
 * Launch one UNLOAD query per bracket. Each writes a CSV (single column ad_id)
 * to s3://bucket/exports/mega-{id}-maids-nse-{label}-{runId}/. Multiple part
 * files may be produced for big brackets — the download endpoint concatenates.
 *
 * Returns one handle per bracket with the queryId for polling and the S3
 * prefix where the CSV files will appear.
 */
export async function startBracketUnloads(
  megaJobId: string,
  runId: string,
  nseCtasTable: string,
  bracketMapTable: string,
  brackets: NseBracket[],
): Promise<BracketUnloadHandle[]> {
  const join = buildJoinSql(nseCtasTable, bracketMapTable);
  const handles: BracketUnloadHandle[] = [];

  // Launch in sequence (Promise.all of startQueryAsync would fan out fine, but
  // we want to keep order predictable for state debugging). Each call is just
  // a fast Athena API submission.
  for (const b of brackets) {
    const safeLabel = b.label.replace(/[^a-zA-Z0-9-]/g, '_');
    const s3Prefix = `exports/mega-${megaJobId}-maids-nse-${safeLabel}-${runId}`;
    const sql = `
      UNLOAD (
        SELECT DISTINCT n.ad_id
        ${join}
        WHERE b.bracket_label = '${b.label.replace(/'/g, "''")}'
      )
      TO 's3://${BUCKET}/${s3Prefix}/'
      WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'NONE')
    `;
    const queryId = await startQueryAsync(sql);
    handles.push({ label: b.label, unloadQueryId: queryId, unloadS3Prefix: s3Prefix });
  }

  console.log(`[NSE-UNLOAD] Started ${handles.length} bracket UNLOAD queries`);
  return handles;
}

/**
 * Launch a single COUNT query that returns COUNT(DISTINCT ad_id) per bracket.
 * Returns the queryId — caller polls and reads via runQueryViaS3 when SUCCEEDED.
 */
export async function startBracketCountQuery(
  nseCtasTable: string,
  bracketMapTable: string,
): Promise<string> {
  const join = buildJoinSql(nseCtasTable, bracketMapTable);
  const sql = `
    SELECT b.bracket_label, COUNT(DISTINCT n.ad_id) as cnt
    ${join}
    GROUP BY b.bracket_label
  `;
  return startQueryAsync(sql);
}

/**
 * Drop the bracket map table + delete its CSV from S3. Best-effort.
 */
export async function dropNseBracketMap(tableName: string, s3Prefix: string): Promise<void> {
  await dropTempTable(tableName);
  try {
    const fullPrefix = `${s3Prefix}/`;
    let continuationToken: string | undefined;
    do {
      const listRes = await s3Client.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: fullPrefix,
        ContinuationToken: continuationToken,
      }));
      const objects = listRes.Contents || [];
      if (objects.length > 0) {
        await s3Client.send(new DeleteObjectsCommand({
          Bucket: BUCKET,
          Delete: { Objects: objects.map(o => ({ Key: o.Key! })), Quiet: true },
        }));
      }
      continuationToken = listRes.IsTruncated ? listRes.NextContinuationToken : undefined;
    } while (continuationToken);
  } catch (e: any) {
    console.warn(`[NSE-BRACKET-MAP] S3 cleanup failed for ${s3Prefix}: ${e.message}`);
  }
}
