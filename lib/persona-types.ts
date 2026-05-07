/**
 * Personas analysis — shared types.
 *
 * The full pipeline:
 *   1. Athena CTAS produces a per-device feature row (DeviceFeatures).
 *   2. Top-N feed k-means → centroides (Stage 1).
 *   3. All devices nearest-centroid assigned (Stage 2).
 *   4. Per-cluster aggregation over the full population (Stage 3) → PersonaCluster[].
 *   5. RFM grid, cohabitation matrix, journey patterns, insights are derived
 *      from the same in-memory population.
 *   6. Final report PersonaReport is cached and rendered in /personas/[runId].
 */

// ── Inputs / config ──────────────────────────────────────────────────

export interface PersonaRunConfig {
  /** Megajob IDs (whole megajob = UNION ALL of its sub-jobs). */
  megaJobIds: string[];
  /** Standalone job IDs (jobs NOT part of any megajob). Treated as a single
   *  "synthetic megajob" of one sub-job. */
  jobIds?: string[];
  /** Optional source-side filters (mirror reach-poll filters). */
  filters?: {
    hourFrom?: number;
    hourTo?: number;
    minDwell?: number;
    maxDwell?: number;
    minVisits?: number;
    gpsOnly?: boolean;
    maxCircleScore?: number;
    /** Days of week 1..7 (Mon..Sun, ISO 8601). Empty/all-7 = no filter. */
    daysOfWeek?: number[];
    /** Drop devices that look like employees (heavy recurrent dwell, work-hour heavy, no overnight). */
    discardEmployees?: boolean;
  };
}

// ── Per-device feature vector (one row of the CTAS output) ──────────

export interface DeviceFeatures {
  ad_id: string;
  total_visits: number;
  total_dwell_min: number;
  recency_days: number;
  avg_dwell_min: number;
  morning_share: number;
  midday_share: number;
  afternoon_share: number;
  evening_share: number;
  night_share: number;
  weekend_share: number;
  friday_evening_share: number;
  gyration_km: number;
  unique_h3_cells: number;
  home_zip: string;
  home_region: string;
  gps_share: number;
  avg_circle_score: number;
  /** brand → visits (e.g. "burger_king" → 12, "mcdonalds" → 4). Comes back as JSON or "k:v|k:v" string from Athena. */
  brand_visits: Record<string, number>;
  /** Herfindahl-Hirschman Index over brand_visits (0..1). 1 = full loyalty to one brand. */
  brand_loyalty_hhi: number;
  /** Top-5 nearby POI categories within ±2h of anchor visits (from mobility CTE). */
  nearby_categories_top5: string[];
  /** Whether the device passes a "high quality" gate (gps + tight circle). */
  tier_high_quality: boolean;
  /** NSE bracket from home_zip lookup; '' / undefined when not resolved. */
  nse_bracket?: string;
  /** Which megajob this device belongs to (for cross-dataset analysis). */
  source_megajob_id: string;
}

// ── Cluster output ───────────────────────────────────────────────────

export interface RadarAxis {
  /** Display label, e.g. "Frequency", "Weekend tilt". */
  label: string;
  /** 0..1 normalized for radar rendering (centroid value vs global p10/p90). */
  value: number;
}

export interface PersonaCluster {
  id: number;
  name: string;
  /** Optional emoji / icon hint. */
  icon?: string;
  /** Optional descriptive subtitle (auto-generated). */
  description: string;
  deviceCount: number;
  percentOfBase: number;
  /** Centroid in the normalized feature space (12-dim). */
  centroid: number[];
  /** 8-axis values for radar chart UI. */
  radarAxes: RadarAxis[];
  /** Top 5 zips (most frequent home_zip in cluster). */
  topZips: { zip: string; count: number }[];
  /** Top 5 nearby categories (mode across cluster members). */
  topNearbyCategories: { category: string; count: number }[];
  /** Brand mix: brand → total visits across cluster. */
  brandMix: Record<string, number>;
  /** NSE distribution: bracket → device count. */
  nseHistogram: Record<string, number>;
  /** Up to 5 example ad_ids (closest to centroid) for spot-check. */
  exampleAdIds: string[];
  /** Median values per feature, for narrative copy. */
  medians: {
    total_visits: number;
    avg_dwell_min: number;
    recency_days: number;
    weekend_share: number;
    gyration_km: number;
    brand_loyalty_hhi: number;
  };
}

// ── Auxiliary outputs ────────────────────────────────────────────────

/** RFM 9-cell. Cells are "R-tertile × FM-tertile" labels. */
export type RfmCellLabel =
  | 'Champions'
  | 'Loyal+'
  | "Can't Lose"
  | 'Promising'
  | 'Loyal'
  | 'Need Attention'
  | 'Hibernating'
  | 'At Risk'
  | 'Lost';

export interface RfmCell {
  label: RfmCellLabel;
  /** Recency tertile: 'high' = recent, 'low' = old. */
  rTertile: 'high' | 'mid' | 'low';
  /** Frequency+Monetary combined tertile. */
  fmTertile: 'high' | 'mid' | 'low';
  deviceCount: number;
  percentOfBase: number;
  medianRecencyDays: number;
  medianFrequency: number;
  medianMonetaryMin: number;
}

export interface RfmReport {
  totalDevices: number;
  cells: RfmCell[];
}

/** Brand cohabitation: pairwise Jaccard + directional shares. */
export interface CohabitationEntry {
  brandA: string;
  brandB: string;
  jaccard: number;
  shareAtoB: number; // |A∩B| / |A|
  shareBtoA: number; // |A∩B| / |B|
  intersectionDevices: number;
  brandADevices: number;
  brandBDevices: number;
}

export interface CohabitationReport {
  entries: CohabitationEntry[];
  brands: string[];
}

/** Frequent itemset from Apriori — typically 2-3 categories. */
export interface JourneyPattern {
  /** Categories visited together within ±60 min of an anchor visit. */
  itemset: string[];
  /** Fraction of device-day transactions matching this itemset. */
  support: number;
  /** Lift relative to independent occurrence. */
  lift: number;
  occurrences: number;
}

export interface JourneyReport {
  totalTransactions: number;
  patterns: JourneyPattern[];
}

/** A single quotable insight string + supporting metric. */
export interface PersonaInsight {
  id: string;
  title: string;
  /** Headline value (e.g. "47%", "1.4×", "12,345 MAIDs"). */
  value: string;
  /** Supporting one-liner. */
  detail: string;
  severity: 'positive' | 'neutral' | 'highlight' | 'warning';
}

// ── Top-level report ─────────────────────────────────────────────────

export interface PersonaReport {
  runId: string;
  generatedAt: string;
  config: PersonaRunConfig;
  /** Aggregate scorecard (donut data, distributions). */
  scorecard: {
    totalDevices: number;
    highQualityDevices: number;
    freqTiers: { tier: 'heavy' | 'regular' | 'light'; count: number; percent: number }[];
    dwellTiers: { tier: 'short' | 'medium' | 'long'; count: number; percent: number }[];
    hourBuckets: { bucket: 'morning' | 'midday' | 'afternoon' | 'evening' | 'night'; share: number }[];
    weekendShareMedian: number;
    weekendShareP90: number;
    gyrationKmP50: number;
    gyrationKmP90: number;
  };
  /** Auto-discovered clusters (k=5..8 picked by silhouette). */
  personas: PersonaCluster[];
  rfm: RfmReport;
  cohabitation?: CohabitationReport; // only when 2+ megajobs
  journey?: JourneyReport;
  /** Per-megajob persona × NSE crosstab. */
  nseCrosstab: {
    personaId: number;
    personaName: string;
    distribution: Record<string, number>;
  }[];
  insights: PersonaInsight[];
  /** Master MAIDs export tracking. */
  exports: {
    personaId: number;
    personaName: string;
    athenaTable: string;
    s3Prefix: string;
    maidCount: number;
    contributionId?: string;
  }[];
}

// ── State machine types (for /api/personas/poll) ──────────────────

export type PersonaPhase =
  | 'starting'
  | 'feature_ctas'
  | 'feature_polling'
  | 'enrichment_ctas'
  | 'enrichment_polling'
  | 'download_query'      // launch SELECT * async
  | 'download_polling'    // wait for SELECT to finish
  | 'download_read'       // stream CSV from S3 + parse
  | 'clustering'
  | 'aggregation'
  | 'master_maids_export'
  | 'export_polling'
  | 'done'
  | 'error';

/** Granular progress detail for the loader UI. */
export interface PersonaSubProgress {
  /** What's currently happening, e.g. "Athena scanning data", "Reading CSV chunk 3 of 12". */
  label: string;
  /** Optional 0-1 progress within the current phase. */
  ratio?: number;
  /** Optional human-readable details — bytes scanned, runtime, rows read, etc. */
  details?: string;
  /** Per-source breakdown (megajob id → status string) when relevant. */
  perSource?: Record<string, string>;
}

export interface PersonaState {
  phase: PersonaPhase;
  runId: string;
  config: PersonaRunConfig;
  /** Feature CTAS table per source. */
  featureCtas?: Record<string, { queryId: string; tableName: string }>;
  /** SELECT * download queries per source. */
  downloadQueries?: Record<string, { queryId: string; csvKey: string }>;
  enrichmentCtas?: { queryId: string; tableName: string };
  exportQueryIds?: Record<string, string>; // personaId → queryId
  exportTables?: Record<string, string>; // personaId → athena table name
  /** Granular progress info surfaced to UI loader. */
  subProgress?: PersonaSubProgress;
  /** Cached final report (when phase=done). */
  report?: PersonaReport;
  error?: string;
  /** ISO timestamp of last phase update. */
  updatedAt: string;
}
