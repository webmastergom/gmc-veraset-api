/**
 * SQL builders for the three look-alike modes (zip / traits / brands).
 *
 * Each builder returns a single CTAS that produces one column (`ad_id`).
 * The state machine in app/api/personas/[runId]/lookalikes/poll wires
 * these together with progress polling + Master MAIDs registration.
 *
 * Conventions:
 * - All builders take `featureTables: string[]` — the persona run's feature
 *   tables across the source megajobs (sortable list, one per source).
 * - `excludeFromTable` is the cluster's already-exported ad_id table
 *   (we don't want to register devices that are already in the cluster
 *   as "lookalikes" — they're the seed).
 * - All builders sanitize literals (zips, brand names, persona name) to
 *   avoid Athena SQL injection or quoting hazards.
 */

import { BUCKET } from './s3-config';

/** SQL-quote a string literal: `O'Reilly` → `'O''Reilly'`. */
function lit(s: string): string {
  return `'${String(s).replace(/'/g, "''")}'`;
}

/** Build a UNION ALL block over multiple feature tables — used to read
 *  features across a multi-source run as one virtual table. */
function unionFeatureTables(featureTables: string[], cols: string): string {
  if (featureTables.length === 0) {
    throw new Error('No feature tables provided');
  }
  if (featureTables.length === 1) {
    return `SELECT ${cols} FROM ${featureTables[0]}`;
  }
  return featureTables
    .map((t) => `SELECT ${cols} FROM ${t}`)
    .join('\nUNION ALL\n');
}

/* ─── Mode A: ZIP look-alikes ──────────────────────────────────────────
 *
 * "Find devices with the same home neighborhoods as cluster X."
 *
 * Logic: from the persona run's feature tables, pick all devices whose
 * `home_zip` is one of the cluster's top home ZIPs but whose `ad_id` is
 * NOT in the cluster export table. The result is people who visited
 * the POIs (so we have data on them) AND live where the cluster's
 * audience lives, but landed in OTHER clusters.
 *
 * This is the cheapest mode — pure SQL filter, no new spatial join,
 * no new Athena scan over raw mobility data.
 */
export function buildZipLookalikeCTAS(args: {
  outTable: string;
  outS3Prefix: string;
  featureTables: string[];
  topZips: string[];
  /** Athena table containing the cluster's already-exported ad_ids (one column: ad_id). */
  excludeFromTable: string;
}): string {
  const { outTable, outS3Prefix, featureTables, topZips, excludeFromTable } = args;
  const zipsList = topZips.map(lit).join(', ');
  const featureUnion = unionFeatureTables(featureTables, 'ad_id, home_zip');

  return `
    CREATE TABLE ${outTable}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='${outS3Prefix}')
    AS
    SELECT DISTINCT pf.ad_id
    FROM (${featureUnion}) pf
    LEFT JOIN ${excludeFromTable} ex ON pf.ad_id = ex.ad_id
    WHERE pf.home_zip IN (${zipsList})
      AND ex.ad_id IS NULL
  `;
}

/* ─── Mode B: Mobility-trait look-alikes ──────────────────────────────
 *
 * "Find devices whose behavioral traits look like cluster X."
 *
 * Logic: range-filter the persona feature tables on the cluster's
 * representative medians ± tolerance. We use the cluster's medians (not
 * the centroid in normalized space) because:
 *   1. The medians are persisted on the PersonaCluster output, so no
 *      need to round-trip the median/MAD vectors.
 *   2. Range filters on raw values are ~100× cheaper to compute in
 *      SQL than centroid-distance scoring.
 *   3. The results are easier to explain: "weekend share within
 *      ±0.15 of cluster median, dwell within 50% of cluster median".
 *
 * Tolerance defaults are tuned to admit ~3-5× the cluster size as
 * lookalikes. Caller can tune per-feature.
 */
export interface TraitTolerances {
  /** Allowed deviation in absolute units (e.g. 0.15 = ±15 percentage points). */
  weekendShareAbs: number;
  /** Same — absolute. */
  hourShareAbs: number;
  /** Multiplicative tolerance (e.g. 0.5 = within 50% of median). */
  avgDwellRel: number;
  /** Multiplicative tolerance — gyration is long-tail so we widen this. */
  gyrationRel: number;
  /** Allowed deviation in days. */
  recencyDaysAbs: number;
}

export const DEFAULT_TRAIT_TOLERANCES: TraitTolerances = {
  weekendShareAbs: 0.15,
  hourShareAbs: 0.20,
  avgDwellRel: 0.6,
  gyrationRel: 0.7,
  recencyDaysAbs: 60,
};

export function buildTraitsLookalikeCTAS(args: {
  outTable: string;
  outS3Prefix: string;
  featureTables: string[];
  /** Cluster medians sourced from PersonaCluster.medians + dominant hour share. */
  medians: {
    avg_dwell_min: number;
    weekend_share: number;
    recency_days: number;
    gyration_km: number;
  };
  /** The cluster's dominant time-of-day bucket — we require the lookalike
   *  to over-index on the same bucket (within tolerance). */
  peakHour: {
    bucket: 'morning' | 'midday' | 'afternoon' | 'evening' | 'night';
    share: number;
  };
  excludeFromTable: string;
  tolerances?: Partial<TraitTolerances>;
}): string {
  const {
    outTable,
    outS3Prefix,
    featureTables,
    medians,
    peakHour,
    excludeFromTable,
  } = args;
  const t = { ...DEFAULT_TRAIT_TOLERANCES, ...(args.tolerances || {}) };

  // Map peakHour bucket to the matching share column on the feature table.
  const hourShareCol: Record<typeof peakHour.bucket, string> = {
    morning: 'morning_share',
    midday: 'midday_share',
    afternoon: 'afternoon_share',
    evening: 'evening_share',
    night: 'night_share',
  } as const;
  const hourCol = hourShareCol[peakHour.bucket];
  const hourLow = Math.max(0, peakHour.share - t.hourShareAbs);

  // Long-tail features: use multiplicative bounds clamped to safe ranges.
  const dwellLow = Math.max(0, medians.avg_dwell_min * (1 - t.avgDwellRel));
  const dwellHigh = medians.avg_dwell_min * (1 + t.avgDwellRel);
  const gyrLow = Math.max(0, medians.gyration_km * (1 - t.gyrationRel));
  const gyrHigh = medians.gyration_km * (1 + t.gyrationRel);
  const recLow = Math.max(0, medians.recency_days - t.recencyDaysAbs);
  const recHigh = medians.recency_days + t.recencyDaysAbs;
  const wkLow = Math.max(0, medians.weekend_share - t.weekendShareAbs);
  const wkHigh = Math.min(1, medians.weekend_share + t.weekendShareAbs);

  const cols =
    'ad_id, avg_dwell_min, weekend_share, recency_days, gyration_km, morning_share, midday_share, afternoon_share, evening_share, night_share';
  const featureUnion = unionFeatureTables(featureTables, cols);

  return `
    CREATE TABLE ${outTable}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='${outS3Prefix}')
    AS
    SELECT DISTINCT pf.ad_id
    FROM (${featureUnion}) pf
    LEFT JOIN ${excludeFromTable} ex ON pf.ad_id = ex.ad_id
    WHERE ex.ad_id IS NULL
      AND pf.avg_dwell_min BETWEEN ${dwellLow} AND ${dwellHigh}
      AND pf.weekend_share BETWEEN ${wkLow} AND ${wkHigh}
      AND pf.recency_days BETWEEN ${recLow} AND ${recHigh}
      AND pf.gyration_km BETWEEN ${gyrLow} AND ${gyrHigh}
      AND pf.${hourCol} >= ${hourLow}
  `;
}

/* ─── Mode C: Brand-affinity look-alikes ──────────────────────────────
 *
 * "Find devices that visited the same brands as cluster X."
 *
 * Logic: for each ad_id in the persona feature tables, parse the
 * brand_visits_json column (already JSON-encoded in the feature CTAS as a
 * map of brand→visit-days). A device is a brand-lookalike if it visited
 * AT LEAST K of the cluster's top brands.
 *
 * This MODE works entirely off the existing feature tables — no new
 * scan over raw poi_ids. Cheap and on-message: the brand mix is
 * already what we computed during clustering.
 */
export function buildBrandsLookalikeCTAS(args: {
  outTable: string;
  outS3Prefix: string;
  featureTables: string[];
  /** Top brands from the cluster's brandMix (most-visited first). */
  topBrands: string[];
  /** Minimum number of those brands that must appear in the device's
   *  brand_visits_json with visits >= 1. Default: ceil(N/2). */
  minBrandMatches?: number;
  excludeFromTable: string;
}): string {
  const { outTable, outS3Prefix, featureTables, topBrands, excludeFromTable } = args;
  if (topBrands.length === 0) {
    throw new Error('topBrands must not be empty for brand lookalike');
  }
  const minMatches = Math.max(
    1,
    args.minBrandMatches ?? Math.ceil(topBrands.length / 2)
  );

  // We test each top brand by JSON_EXTRACT_SCALAR(brand_visits_json, '$.brand_name').
  // Athena returns a string like "12" or NULL — we coerce to numeric and
  // count how many are > 0. A brand_visits_json missing the key (or NULL
  // string) → contributes 0.
  const matchExprs = topBrands
    .map(
      (b) =>
        `IF(TRY_CAST(JSON_EXTRACT_SCALAR(brand_visits_json, '$.${b.replace(/'/g, "''").replace(/"/g, '\\"')}') AS DOUBLE) > 0, 1, 0)`
    )
    .join(' + ');

  const cols = 'ad_id, brand_visits_json';
  const featureUnion = unionFeatureTables(featureTables, cols);

  return `
    CREATE TABLE ${outTable}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='${outS3Prefix}')
    AS
    SELECT DISTINCT pf.ad_id
    FROM (
      SELECT ad_id, brand_visits_json,
             ${matchExprs} AS match_count
      FROM (${featureUnion})
    ) pf
    LEFT JOIN ${excludeFromTable} ex ON pf.ad_id = ex.ad_id
    WHERE ex.ad_id IS NULL
      AND pf.match_count >= ${minMatches}
  `;
}

/** Build the S3 prefix used by all three modes. */
export function lookalikeS3Prefix(table: string): string {
  return `s3://${BUCKET}/athena-temp/${table}/`;
}
