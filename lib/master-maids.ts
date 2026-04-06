/**
 * Master MAID List per Country — v2 (Athena-Native)
 *
 * Architecture: Node.js NEVER touches MAID data.
 * Each contribution is an Athena table (Parquet via CTAS).
 * Consolidation uses UNION ALL of registered tables.
 *
 * Flow:
 * 1. Each export runs a CTAS query → creates a Parquet-backed Athena table
 * 2. registerAthenaContribution() records table name + metadata in the index
 * 3. Consolidation builds UNION ALL of all registered tables → deduped stats
 */

import { getConfig, putConfig, invalidateCache } from './s3-config';

const INDEX_KEY = 'master-maids-index';
const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/** Normalize country codes (UK → GB, etc.) */
function normalizeCC(cc: string): string {
  const upper = cc.toUpperCase();
  if (upper === 'UK') return 'GB';
  return upper;
}

// ── Types ──────────────────────────────────────────────────────────────

export type AttributeType = 'plain' | 'nse' | 'category' | 'catchment';

/**
 * Each contribution is an Athena table backed by Parquet.
 * The table has a known schema based on attributeType:
 *   plain:     (ad_id STRING)
 *   category:  (ad_id STRING, category STRING, dwell_minutes DOUBLE)
 *   nse:       (ad_id STRING, nse_bracket STRING, postal_code STRING)
 *   catchment: (ad_id STRING, origin_lat DOUBLE, origin_lng DOUBLE)
 */
export interface Contribution {
  id: string;
  athenaTable: string;          // Athena table name (e.g. "master_es_plain_job6b0f_123")
  s3Prefix: string;             // Where the Parquet lives (e.g. "athena-temp/master_es_plain_...")
  attributeType: AttributeType;
  attributeValue: string;       // Summary label: "", "sports", "nse", etc.
  sourceDataset: string;
  dateRange: { from: string; to: string };
  maidCount: number;            // Known at creation time from CTAS stats
  registeredAt: string;

  // Legacy fields (for backward compat with old CSV contributions)
  s3Key?: string;               // Old CSV path
  rowCount?: number;
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

export const DWELL_BUCKETS = [
  { label: '2-10 min', min: 2, max: 10 },
  { label: '10-30 min', min: 10, max: 30 },
  { label: '30-60 min', min: 30, max: 60 },
  { label: '1-3 hr', min: 60, max: 180 },
  { label: '3+ hr', min: 180, max: 999999 },
];

// ── Registration ────────────────────────────────────────────────────────

/**
 * Register an Athena CTAS table as a contribution to the master list.
 * This is the PRIMARY registration function in v2.
 *
 * Called after a CTAS query completes. The table already exists in Athena
 * and its Parquet files are in S3. Node.js never touches the data.
 */
export async function registerAthenaContribution(
  country: string,
  sourceDataset: string,
  attributeType: AttributeType,
  attributeValue: string,
  athenaTable: string,
  s3Prefix: string,
  maidCount: number,
  dateRange: { from: string; to: string },
): Promise<void> {
  if (!country) {
    console.warn('[MASTER-MAIDS] Skipping: no country');
    return;
  }

  const cc = normalizeCC(country);
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};

  if (!index[cc]) {
    index[cc] = { lastConsolidatedAt: null, stats: null, contributions: [] };
  }

  // Prevent duplicate registration of same table
  if (index[cc].contributions.some(c => c.athenaTable === athenaTable)) {
    console.log(`[MASTER-MAIDS] Table ${athenaTable} already registered for ${cc}`);
    return;
  }

  const contribution: Contribution = {
    id: `c_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    athenaTable,
    s3Prefix,
    attributeType,
    attributeValue,
    sourceDataset,
    dateRange,
    maidCount,
    registeredAt: new Date().toISOString(),
  };

  index[cc].contributions.push(contribution);
  await putConfig(INDEX_KEY, index, { compact: true });

  console.log(`[MASTER-MAIDS] Registered ${attributeType}:${attributeValue} for ${cc} — ${athenaTable} (${maidCount.toLocaleString()} MAIDs)`);
}

/**
 * Legacy registration for old CSV-based contributions.
 * Kept for backward compatibility during migration.
 */
export async function registerContribution(
  country: string,
  sourceDataset: string,
  attributeType: AttributeType | 'nse_bracket',
  attributeValue: string,
  s3FileName: string,
  dateRange: { from: string; to: string },
  maidCount?: number,
): Promise<void> {
  if (!country) return;

  const cc = normalizeCC(country);
  invalidateCache(INDEX_KEY);
  const index = await getConfig<MasterMaidsIndex>(INDEX_KEY) || {};

  if (!index[cc]) {
    index[cc] = { lastConsolidatedAt: null, stats: null, contributions: [] };
  }

  const s3Key = s3FileName.startsWith('exports/') || s3FileName.startsWith('master-maids/') || s3FileName.startsWith('athena')
    ? s3FileName : `exports/${s3FileName}`;

  // Prevent duplicate registration of same file
  if (index[cc].contributions.some(c => c.s3Key === s3Key)) {
    console.log(`[MASTER-MAIDS] CSV ${s3Key} already registered for ${cc}`);
    return;
  }

  const contribution: Contribution = {
    id: `c_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    athenaTable: '',
    s3Prefix: '',
    s3Key,
    attributeType: attributeType === 'nse_bracket' ? 'nse' : attributeType as AttributeType,
    attributeValue,
    sourceDataset,
    dateRange,
    maidCount: maidCount || 0,
    registeredAt: new Date().toISOString(),
  };

  index[cc].contributions.push(contribution);
  await putConfig(INDEX_KEY, index, { compact: true });
  console.log(`[MASTER-MAIDS] Registered legacy ${attributeType}:${attributeValue} for ${cc} — ${s3Key} (${(maidCount || 0).toLocaleString()} MAIDs)`);
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

// ── Consolidation v2: UNION ALL of Athena Tables ────────────────────────

/**
 * Build consolidation SQL from registered Athena tables.
 * Each contribution has its own table with a known schema.
 * The UNION ALL normalizes them into a common schema.
 *
 * Also handles legacy CSV-based contributions via an external CSV table.
 */
export function buildConsolidationFromTables(
  country: string,
  contributions: Contribution[],
  consolidatedTableName: string,
): { ctasSQL: string; legacyCsvTableSQL?: string; legacyCsvTableName?: string } {
  const cc = normalizeCC(country);
  const outputPath = `s3://${BUCKET}/master-maids/${cc}/consolidated/${consolidatedTableName}/`;

  const selects: string[] = [];
  let legacyCsvTableSQL: string | undefined;
  let legacyCsvTableName: string | undefined;
  const legacyContribs: Contribution[] = [];

  for (const c of contributions) {
    if (c.athenaTable) {
      // v2: Athena table contribution
      switch (c.attributeType) {
        case 'plain':
          selects.push(`
            SELECT ad_id, 'plain' as attr_type, '' as attr_value,
                   CAST(NULL AS DOUBLE) as dwell_minutes, CAST(NULL AS VARCHAR) as postal_code
            FROM ${c.athenaTable}
          `);
          break;
        case 'category':
          selects.push(`
            SELECT ad_id, 'category' as attr_type, category as attr_value,
                   dwell_minutes, CAST(NULL AS VARCHAR) as postal_code
            FROM ${c.athenaTable}
          `);
          break;
        case 'nse':
          selects.push(`
            SELECT ad_id, 'nse' as attr_type, nse_bracket as attr_value,
                   CAST(NULL AS DOUBLE) as dwell_minutes, postal_code
            FROM ${c.athenaTable}
          `);
          break;
        case 'catchment':
          selects.push(`
            SELECT ad_id, 'catchment' as attr_type, '' as attr_value,
                   CAST(NULL AS DOUBLE) as dwell_minutes, CAST(NULL AS VARCHAR) as postal_code
            FROM ${c.athenaTable}
          `);
          break;
      }
    } else if (c.s3Key) {
      // Legacy CSV contribution — collect for batch processing
      legacyContribs.push(c);
    }
  }

  // Handle legacy CSV contributions via a single external table
  if (legacyContribs.length > 0) {
    legacyCsvTableName = `master_legacy_${cc.toLowerCase()}_${Date.now()}`;
    const contribPrefix = `s3://${BUCKET}/master-maids/${cc}/contributions/`;

    legacyCsvTableSQL = `
      CREATE EXTERNAL TABLE IF NOT EXISTS ${legacyCsvTableName} (
        ad_id STRING, attr_type STRING, attr_value STRING,
        dwell_minutes STRING, postal_code STRING
      )
      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
      WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
      STORED AS TEXTFILE
      LOCATION '${contribPrefix}'
      TBLPROPERTIES ('skip.header.line.count' = '1')
    `;

    // Legacy CSVs: handle mixed formats with CASE
    selects.push(`
      SELECT
        ad_id,
        CASE
          WHEN TRY_CAST(attr_value AS DOUBLE) IS NOT NULL AND dwell_minutes IS NULL THEN 'category'
          WHEN attr_type IS NULL OR attr_type = '' THEN 'plain'
          ELSE attr_type
        END as attr_type,
        CASE
          WHEN TRY_CAST(attr_value AS DOUBLE) IS NOT NULL AND dwell_minutes IS NULL THEN attr_type
          ELSE COALESCE(attr_value, '')
        END as attr_value,
        CASE
          WHEN TRY_CAST(attr_value AS DOUBLE) IS NOT NULL AND dwell_minutes IS NULL
            THEN TRY_CAST(attr_value AS DOUBLE)
          ELSE TRY_CAST(NULLIF(dwell_minutes, '') AS DOUBLE)
        END as dwell_minutes,
        NULLIF(postal_code, '') as postal_code
      FROM ${legacyCsvTableName}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '' AND ad_id != 'ad_id'
    `);
  }

  if (selects.length === 0) {
    throw new Error('No contributions to consolidate');
  }

  const ctasSQL = `
    CREATE TABLE ${consolidatedTableName}
    WITH (
      format = 'PARQUET',
      parquet_compression = 'SNAPPY',
      external_location = '${outputPath}'
    )
    AS
    ${selects.join('\nUNION ALL\n')}
  `;

  return { ctasSQL, legacyCsvTableSQL, legacyCsvTableName };
}

/**
 * Stats query on consolidated table.
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

export function buildPostalCodeQuery(consolidatedTableName: string): string {
  return `
    SELECT postal_code, COUNT(DISTINCT ad_id) as maid_count
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

// ── CTAS helpers ────────────────────────────────────────────────────────

/** Generate safe Athena table name from components */
export function masterTableName(cc: string, type: string, dataset: string): string {
  const safe = (s: string) => s.replace(/[^a-z0-9]/gi, '_').toLowerCase();
  return `master_${safe(cc)}_${safe(type)}_${safe(dataset)}_${Date.now()}`;
}
