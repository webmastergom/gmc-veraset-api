/**
 * Master MAID List per Country
 *
 * Zero-copy registration model: Node.js only records metadata about exports.
 * Athena handles all heavy lifting (dedup, stats, consolidation).
 *
 * Flow:
 * 1. Each MAID export (plain, NSE, category) calls registerContribution()
 * 2. registerContribution() appends metadata to config/master-maids-index.json
 * 3. User triggers "Consolidate" → Athena queries the original export CSVs
 * 4. Stats saved back to the index JSON
 */

import { getConfig, putConfig, invalidateCache } from './s3-config';
import { startQueryAsync, checkQueryStatus, startCTASAsync, dropTempTable } from './athena';

const INDEX_KEY = 'master-maids-index';
const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

// ── Types ──────────────────────────────────────────────────────────────

export type AttributeType = 'plain' | 'nse_bracket' | 'category' | 'catchment';

export interface Contribution {
  id: string;
  s3Key: string;                    // e.g. "exports/job-xxx-maids-nse-0-19-123.csv"
  attributeType: AttributeType;
  attributeValue: string;           // e.g. "0-19", "university", ""
  sourceDataset: string;            // e.g. "job-6b0f6fc4"
  dateRange: { from: string; to: string };
  registeredAt: string;
}

export interface AttributeStat {
  attributeType: string;
  attributeValue: string;
  maidCount: number;
  oldestData: string;
  newestData: string;
}

export interface CountryEntry {
  lastConsolidatedAt: string | null;
  stats: {
    totalMaids: number;
    byAttribute: AttributeStat[];
    byDataset: Record<string, number>;
  } | null;
  contributions: Contribution[];
}

export interface MasterMaidsIndex {
  [countryCode: string]: CountryEntry;
}

export interface ConsolidationState {
  phase: 'creating_tables' | 'running_ctas' | 'running_stats' | 'done' | 'error';
  ctasQueryId?: string;
  statsQueryId?: string;
  tempTables?: string[];
  consolidatedTable?: string;
  error?: string;
}

// ── Registration (lightweight, Node.js only) ────────────────────────────

/**
 * Register a MAID export as a contribution to the master list.
 * Only writes metadata (~1KB) — never touches actual MAID data.
 */
export async function registerContribution(
  country: string,
  sourceDataset: string,
  attributeType: AttributeType,
  attributeValue: string,
  s3FileName: string,
  dateRange: { from: string; to: string },
): Promise<void> {
  if (!country) {
    console.warn('[MASTER-MAIDS] Skipping registration: no country');
    return;
  }

  const cc = country.toUpperCase();
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};

  if (!index[cc]) {
    index[cc] = {
      lastConsolidatedAt: null,
      stats: null,
      contributions: [],
    };
  }

  const contribution: Contribution = {
    id: `c_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    s3Key: s3FileName.startsWith('exports/') ? s3FileName : `exports/${s3FileName}`,
    attributeType,
    attributeValue,
    sourceDataset,
    dateRange,
    registeredAt: new Date().toISOString(),
  };

  index[cc].contributions.push(contribution);
  await putConfig(INDEX_KEY, index, { compact: true });

  console.log(`[MASTER-MAIDS] Registered ${attributeType}:${attributeValue} for ${cc} from ${sourceDataset} (${contribution.id})`);
}

/**
 * Get the full master MAID index (all countries).
 */
export async function getMasterIndex(): Promise<MasterMaidsIndex> {
  return await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};
}

/**
 * Get contributions for a specific country.
 */
export async function getCountryContributions(country: string): Promise<CountryEntry | null> {
  const index = await getMasterIndex();
  return index[country.toUpperCase()] || null;
}

/**
 * Remove a contribution by ID (metadata only — doesn't delete the S3 file).
 */
export async function removeContribution(country: string, contributionId: string): Promise<boolean> {
  const cc = country.toUpperCase();
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};

  if (!index[cc]) return false;

  const before = index[cc].contributions.length;
  index[cc].contributions = index[cc].contributions.filter(c => c.id !== contributionId);

  if (index[cc].contributions.length === before) return false;

  await putConfig(INDEX_KEY, index, { compact: true });
  console.log(`[MASTER-MAIDS] Removed contribution ${contributionId} from ${cc}`);
  return true;
}

// ── Consolidation (Athena-heavy) ────────────────────────────────────────

/**
 * Build CREATE EXTERNAL TABLE SQL for a single contribution CSV.
 * Each export CSV has a single column: ad_id (with quoted header).
 */
function buildContributionTableSQL(tableName: string, s3Key: string): string {
  // Point at the specific file's parent prefix + use the file directly
  return `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName} (
      ad_id STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    STORED AS TEXTFILE
    LOCATION 's3://${BUCKET}/${s3Key.replace(/\/[^/]+$/, '/')}/'
    TBLPROPERTIES ('skip.header.line.count' = '1')
  `;
}

/**
 * Build a CTAS query that unions all contributions into one consolidated table.
 * Each contribution gets its metadata columns injected as constants.
 */
export function buildConsolidationCTAS(
  consolidatedTableName: string,
  contributions: { tableName: string; contribution: Contribution }[],
): string {
  const selects = contributions.map(({ tableName, contribution }) => {
    const attrType = contribution.attributeType.replace(/'/g, "''");
    const attrValue = contribution.attributeValue.replace(/'/g, "''");
    const source = contribution.sourceDataset.replace(/'/g, "''");
    const obsFrom = contribution.dateRange.from;
    const obsTo = contribution.dateRange.to;

    return `
      SELECT
        ad_id,
        '${attrType}' as attr_type,
        '${attrValue}' as attr_value,
        '${source}' as source_dataset,
        DATE '${obsFrom}' as obs_from,
        DATE '${obsTo}' as obs_to
      FROM ${tableName}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
    `;
  });

  const unionAll = selects.join('\nUNION ALL\n');
  const outputPath = `s3://${BUCKET}/master-maids/${consolidatedTableName}/`;

  return `
    CREATE TABLE ${consolidatedTableName}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = '${outputPath}'
    )
    AS ${unionAll}
  `;
}

/**
 * Build a stats query on the consolidated table.
 * Returns: total unique MAIDs + breakdown by attribute type/value.
 */
export function buildStatsQuery(consolidatedTableName: string): string {
  return `
    SELECT
      attr_type,
      attr_value,
      COUNT(DISTINCT ad_id) as maid_count,
      CAST(MIN(obs_from) AS VARCHAR) as oldest_data,
      CAST(MAX(obs_to) AS VARCHAR) as newest_data
    FROM ${consolidatedTableName}
    GROUP BY attr_type, attr_value
    ORDER BY maid_count DESC
  `;
}

/**
 * Build a total unique MAIDs query (separate because GROUP BY changes the count).
 */
export function buildTotalQuery(consolidatedTableName: string): string {
  return `SELECT COUNT(DISTINCT ad_id) as total FROM ${consolidatedTableName}`;
}

/**
 * Generate safe table names for contribution temp tables.
 */
export function contributionTableName(index: number, runId: string): string {
  const safeRun = runId.replace(/[^a-z0-9]/gi, '_');
  return `master_contrib_${index}_${safeRun}`;
}

export function consolidatedTableName(country: string, runId: string): string {
  const safeRun = runId.replace(/[^a-z0-9]/gi, '_');
  return `master_${country.toLowerCase()}_${safeRun}`;
}

/**
 * Start consolidation phase 1: create temp tables for each contribution CSV.
 * Returns the table creation query IDs.
 *
 * NOTE: Athena external tables can't point at a single file — they point at a prefix.
 * Since multiple exports may share the `exports/` prefix, we use a workaround:
 * create one table per unique S3 key by crafting the LOCATION carefully.
 *
 * Actually, we'll use a different approach: build ONE big query that reads
 * directly from S3 using Athena's ability to read from specific paths via
 * a single external table per file. But Athena doesn't support single-file
 * tables natively. Instead, we'll use a UNION ALL of subqueries that each
 * filter by filename.
 *
 * SIMPLEST APPROACH: One CTAS that does everything in one query, using
 * Athena's ability to read CSVs directly. We create one external table
 * per contribution, each pointing at a unique prefix we control.
 * Since all exports are in `exports/`, we need a different strategy.
 *
 * PRACTICAL APPROACH: Copy nothing. Each contribution CSV is already in S3.
 * For consolidation, we build a single query using S3 SELECT or simply
 * create individual tables with `LOCATION` set to the file's directory and
 * use TBLPROPERTIES to filter. BUT Athena doesn't support single-file LOCATION.
 *
 * ACTUAL APPROACH: We use Athena's "read_csv" or we simply create a single
 * table pointing at `exports/` prefix and filter by filename in the WHERE.
 * BUT external tables don't expose the filename.
 *
 * FINAL APPROACH: Use Athena's built-in `$path` pseudo-column to filter
 * rows by their source file. Create ONE external table pointing at `exports/`
 * and use WHERE "$path" = 's3://bucket/exports/filename.csv' in each SELECT.
 */

/**
 * Build a single consolidation query using the $path pseudo-column.
 * One external table covers all exports; each contribution filters by its file path.
 */
export function buildSingleTableConsolidation(
  consolidatedTableName: string,
  contributions: Contribution[],
): { createTableSQL: string; ctasSQL: string; exportsTableName: string } {
  const exportsTableName = `master_exports_${Date.now()}`;

  const createTableSQL = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${exportsTableName} (
      ad_id STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    STORED AS TEXTFILE
    LOCATION 's3://${BUCKET}/exports/'
    TBLPROPERTIES ('skip.header.line.count' = '1')
  `;

  const selects = contributions.map(c => {
    const attrType = c.attributeType.replace(/'/g, "''");
    const attrValue = c.attributeValue.replace(/'/g, "''");
    const source = c.sourceDataset.replace(/'/g, "''");
    const s3Path = `s3://${BUCKET}/${c.s3Key}`;

    return `
      SELECT
        ad_id,
        '${attrType}' as attr_type,
        '${attrValue}' as attr_value,
        '${source}' as source_dataset,
        DATE '${c.dateRange.from}' as obs_from,
        DATE '${c.dateRange.to}' as obs_to
      FROM ${exportsTableName}
      WHERE "$path" = '${s3Path}'
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    `;
  });

  const unionAll = selects.join('\nUNION ALL\n');
  const outputPath = `s3://${BUCKET}/master-maids/${consolidatedTableName}/`;

  const ctasSQL = `
    CREATE TABLE ${consolidatedTableName}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = '${outputPath}'
    )
    AS ${unionAll}
  `;

  return { createTableSQL, ctasSQL, exportsTableName };
}

/**
 * Save consolidation stats back to the index.
 */
export async function saveConsolidationStats(
  country: string,
  totalMaids: number,
  byAttribute: AttributeStat[],
): Promise<void> {
  const cc = country.toUpperCase();
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};

  if (!index[cc]) return;

  // Build byDataset from contributions
  const byDataset: Record<string, number> = {};
  for (const c of index[cc].contributions) {
    byDataset[c.sourceDataset] = (byDataset[c.sourceDataset] || 0) + 1;
  }

  index[cc].lastConsolidatedAt = new Date().toISOString();
  index[cc].stats = { totalMaids, byAttribute, byDataset };

  await putConfig(INDEX_KEY, index, { compact: true });
  console.log(`[MASTER-MAIDS] Saved stats for ${cc}: ${totalMaids} total MAIDs, ${byAttribute.length} attributes`);
}
