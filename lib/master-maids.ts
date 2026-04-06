/**
 * Master MAID List per Country
 *
 * Enriched contribution model: each export writes a multi-column CSV to
 * master-maids/{CC}/contributions/ with the format:
 *   ad_id,attr_type,attr_value,dwell_minutes,postal_code
 *
 * Node.js writes these CSVs + registers metadata in the index.
 * Athena handles consolidation (dedup, stats) by pointing at the contributions prefix.
 *
 * Flow:
 * 1. Each MAID export writes an enriched CSV + calls registerContribution()
 * 2. registerContribution() appends metadata to config/master-maids-index.json
 * 3. User triggers "Consolidate" → Athena queries contribution CSVs directly
 * 4. Stats saved back to the index JSON
 */

import { getConfig, putConfig, invalidateCache } from './s3-config';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from './s3-config';

const INDEX_KEY = 'master-maids-index';

/** Normalize country codes (UK → GB, etc.) */
function normalizeCC(cc: string): string {
  const upper = cc.toUpperCase();
  if (upper === 'UK') return 'GB';
  return upper;
}

// ── Types ──────────────────────────────────────────────────────────────

export type AttributeType = 'plain' | 'nse_bracket' | 'category' | 'catchment';

/** A single row in a contribution CSV */
export interface ContributionRow {
  ad_id: string;
  attr_type: AttributeType;
  attr_value: string;
  dwell_minutes: number | null;     // Only for category contributions
  postal_code: string;              // Catchment postal code (if available)
}

export interface Contribution {
  id: string;
  s3Key: string;                    // e.g. "master-maids/ES/contributions/job-xxx-nse-123.csv"
  attributeType: AttributeType;
  attributeValue: string;           // e.g. "0-19", "university", "" (summary label)
  sourceDataset: string;
  dateRange: { from: string; to: string };
  rowCount: number;
  registeredAt: string;
}

export interface AttributeStat {
  attributeType: string;
  attributeValue: string;
  maidCount: number;
  oldestData: string;
  newestData: string;
  avgDwell?: number;
  medianDwell?: number;
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

// Dwell time buckets for UI display
export const DWELL_BUCKETS = [
  { label: '2-10 min', min: 2, max: 10 },
  { label: '10-30 min', min: 10, max: 30 },
  { label: '30-60 min', min: 30, max: 60 },
  { label: '1-3 hr', min: 60, max: 180 },
  { label: '3+ hr', min: 180, max: 999999 },
];

// ── Write enriched contribution CSV ─────────────────────────────────────

/**
 * Write an enriched contribution CSV to S3 and register it in the index.
 *
 * CSV format: ad_id,attr_type,attr_value,dwell_minutes,postal_code
 *
 * For NSE: rows have attr_type=nse_bracket, attr_value=bracket label, postal_code=origin
 * For Category: rows have attr_type=category, attr_value=category name, dwell_minutes=actual dwell
 * For Plain: rows have attr_type=plain, minimal data
 */
export async function writeContribution(
  country: string,
  sourceDataset: string,
  rows: ContributionRow[],
  dateRange: { from: string; to: string },
  label: string = '',  // Summary label for the contribution (e.g. "nse_brackets", "university")
): Promise<void> {
  if (!country || rows.length === 0) {
    console.warn(`[MASTER-MAIDS] Skipping: ${!country ? 'no country' : 'no rows'}`);
    return;
  }

  const cc = normalizeCC(country);
  const timestamp = Date.now();
  const attrType = rows[0].attr_type;
  const safeLabel = (label || attrType).replace(/[^a-zA-Z0-9_-]/g, '_');
  const fileName = `${sourceDataset}-${safeLabel}-${timestamp}.csv`;
  const s3Key = `master-maids/${cc}/contributions/${fileName}`;

  // Build CSV
  const header = 'ad_id,attr_type,attr_value,dwell_minutes,postal_code';
  const csvLines = rows.map(r =>
    `${r.ad_id},${r.attr_type},${(r.attr_value || '').replace(/,/g, ' ')},${r.dwell_minutes ?? ''},${(r.postal_code || '').replace(/,/g, '')}`
  );
  const csvContent = header + '\n' + csvLines.join('\n');

  // Write to S3
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: s3Key,
    Body: csvContent,
    ContentType: 'text/csv',
  }));

  console.log(`[MASTER-MAIDS] Wrote ${rows.length} rows to ${s3Key}`);

  // Register in index
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};

  if (!index[cc]) {
    index[cc] = { lastConsolidatedAt: null, stats: null, contributions: [] };
  }

  const contribution: Contribution = {
    id: `c_${timestamp}_${Math.random().toString(36).slice(2, 8)}`,
    s3Key,
    attributeType: attrType,
    attributeValue: label || attrType,
    sourceDataset,
    dateRange,
    rowCount: rows.length,
    registeredAt: new Date().toISOString(),
  };

  index[cc].contributions.push(contribution);
  await putConfig(INDEX_KEY, index, { compact: true });

  console.log(`[MASTER-MAIDS] Registered ${attrType}:${label} for ${cc} from ${sourceDataset} (${contribution.id}, ${rows.length} rows)`);
}

// ── Registration (lightweight, kept for backward compat) ─────────────────

/**
 * Register an EXISTING export CSV. For new code, prefer writeContribution().
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

  const cc = normalizeCC(country);
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};

  if (!index[cc]) {
    index[cc] = { lastConsolidatedAt: null, stats: null, contributions: [] };
  }

  const contribution: Contribution = {
    id: `c_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    s3Key: s3FileName.startsWith('exports/') || s3FileName.startsWith('master-maids/')
      ? s3FileName
      : `exports/${s3FileName}`,
    attributeType,
    attributeValue,
    sourceDataset,
    dateRange,
    rowCount: 0,
    registeredAt: new Date().toISOString(),
  };

  index[cc].contributions.push(contribution);
  await putConfig(INDEX_KEY, index, { compact: true });

  console.log(`[MASTER-MAIDS] Registered ${attributeType}:${attributeValue} for ${cc} from ${sourceDataset} (${contribution.id})`);
}

// ── Read ────────────────────────────────────────────────────────────────

export async function getMasterIndex(): Promise<MasterMaidsIndex> {
  return await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};
}

export async function getCountryContributions(country: string): Promise<CountryEntry | null> {
  const index = await getMasterIndex();
  return index[normalizeCC(country)] || null;
}

export async function removeContribution(country: string, contributionId: string): Promise<boolean> {
  const cc = normalizeCC(country);
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};
  if (!index[cc]) return false;

  const before = index[cc].contributions.length;
  index[cc].contributions = index[cc].contributions.filter(c => c.id !== contributionId);
  if (index[cc].contributions.length === before) return false;

  await putConfig(INDEX_KEY, index, { compact: true });
  return true;
}

// ── Consolidation (Athena) ──────────────────────────────────────────────

/**
 * Build CREATE EXTERNAL TABLE + CTAS for consolidation.
 *
 * The contributions table reads ALL CSVs under master-maids/{CC}/contributions/.
 * Each CSV has the same schema: ad_id,attr_type,attr_value,dwell_minutes,postal_code.
 * No $path filtering needed — one table, one prefix, Athena reads everything.
 */
export function buildConsolidationSQL(
  country: string,
  consolidatedTableName: string,
): { createTableSQL: string; ctasSQL: string; contribTableName: string } {
  const cc = normalizeCC(country);
  const contribTableName = `master_contrib_${cc.toLowerCase()}_${Date.now()}`;
  const contribPrefix = `s3://${BUCKET}/master-maids/${cc}/contributions/`;

  const createTableSQL = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${contribTableName} (
      ad_id STRING,
      attr_type STRING,
      attr_value STRING,
      dwell_minutes STRING,
      postal_code STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    STORED AS TEXTFILE
    LOCATION '${contribPrefix}'
    TBLPROPERTIES ('skip.header.line.count' = '1')
  `;

  // Use unique path per run to avoid HIVE_PATH_ALREADY_EXISTS
  const outputPath = `s3://${BUCKET}/master-maids/${cc}/consolidated/${consolidatedTableName}/`;

  const ctasSQL = `
    CREATE TABLE ${consolidatedTableName}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = '${outputPath}'
    )
    AS
    SELECT
      ad_id,
      COALESCE(NULLIF(attr_type, ''), 'plain') as attr_type,
      COALESCE(attr_value, '') as attr_value,
      TRY_CAST(NULLIF(dwell_minutes, '') AS DOUBLE) as dwell_minutes,
      NULLIF(postal_code, '') as postal_code
    FROM ${contribTableName}
    WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
      AND ad_id != 'ad_id'
  `;

  return { createTableSQL, ctasSQL, contribTableName };
}

/**
 * Stats query: breakdown by attr_type + attr_value, plus dwell bucket distribution.
 */
export function buildStatsQuery(consolidatedTableName: string): string {
  return `
    SELECT
      attr_type,
      attr_value,
      COUNT(DISTINCT ad_id) as maid_count,
      ROUND(AVG(dwell_minutes), 1) as avg_dwell,
      ROUND(APPROX_PERCENTILE(dwell_minutes, 0.5), 1) as median_dwell
    FROM ${consolidatedTableName}
    GROUP BY attr_type, attr_value
    ORDER BY maid_count DESC
  `;
}

export function buildTotalQuery(consolidatedTableName: string): string {
  return `SELECT COUNT(DISTINCT ad_id) as total FROM ${consolidatedTableName}`;
}

/**
 * Dwell distribution query: how many MAIDs per dwell bucket for category attributes.
 */
export function buildDwellDistributionQuery(consolidatedTableName: string): string {
  return `
    SELECT
      attr_value as category,
      COUNT(DISTINCT CASE WHEN dwell_minutes >= 2 AND dwell_minutes < 10 THEN ad_id END) as dwell_2_10,
      COUNT(DISTINCT CASE WHEN dwell_minutes >= 10 AND dwell_minutes < 30 THEN ad_id END) as dwell_10_30,
      COUNT(DISTINCT CASE WHEN dwell_minutes >= 30 AND dwell_minutes < 60 THEN ad_id END) as dwell_30_60,
      COUNT(DISTINCT CASE WHEN dwell_minutes >= 60 AND dwell_minutes < 180 THEN ad_id END) as dwell_60_180,
      COUNT(DISTINCT CASE WHEN dwell_minutes >= 180 THEN ad_id END) as dwell_180_plus
    FROM ${consolidatedTableName}
    WHERE attr_type = 'category' AND dwell_minutes IS NOT NULL
    GROUP BY attr_value
    ORDER BY attr_value
  `;
}

/**
 * Postal code distribution query: top postal codes by MAID count.
 */
export function buildPostalCodeQuery(consolidatedTableName: string): string {
  return `
    SELECT
      postal_code,
      COUNT(DISTINCT ad_id) as maid_count
    FROM ${consolidatedTableName}
    WHERE postal_code IS NOT NULL AND postal_code != ''
    GROUP BY postal_code
    ORDER BY maid_count DESC
    LIMIT 100
  `;
}

// ── Save stats ──────────────────────────────────────────────────────────

export async function saveConsolidationStats(
  country: string,
  totalMaids: number,
  byAttribute: AttributeStat[],
): Promise<void> {
  const cc = normalizeCC(country);
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};
  if (!index[cc]) return;

  const byDataset: Record<string, number> = {};
  for (const c of index[cc].contributions) {
    byDataset[c.sourceDataset] = (byDataset[c.sourceDataset] || 0) + 1;
  }

  index[cc].lastConsolidatedAt = new Date().toISOString();
  index[cc].stats = { totalMaids, byAttribute, byDataset };

  await putConfig(INDEX_KEY, index, { compact: true });
  console.log(`[MASTER-MAIDS] Saved stats for ${cc}: ${totalMaids} total MAIDs, ${byAttribute.length} attributes`);
}
