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
    /** Drop devices that look like residents (heavy recurrent dwell + overnight share ≥ 0.5). */
    discardResidents?: boolean;
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
  /** Avg lat at the device's likely-home window (night-mode, fallback any-hour).
   *  Used as a reverse-geocode fallback for home_zip when geo_fields is sparse. */
  home_lat?: number | null;
  home_lng?: number | null;
  gps_share: number;
  avg_circle_score: number;
  /** brand → visits (e.g. "burger_king" → 12, "mcdonalds" → 4). Comes back as JSON or "k:v|k:v" string from Athena. */
  brand_visits: Record<string, number>;
  /** Herfindahl-Hirschman Index over brand_visits (0..1). 1 = full loyalty to one brand.
   *  CTAS emits 0 below the noise floor (5 brand-visit-days) to avoid the
   *  small-sample artefact where 1 visit = HHI=1.0. */
  brand_loyalty_hhi: number;
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
  /** Brand mix: brand → unique device count within this persona. */
  brandMix: Record<string, number>;
  /**
   * Peak-traffic time bucket — when this persona is most likely to be at
   * the POI set. Use this to schedule push / DOOH / ad delivery. The
   * bucket is whichever of morning/midday/afternoon/evening/night has
   * the highest mean share across cluster members. `share` is that mean
   * (0..1); `label` is a human-readable window like "2pm-6pm".
   */
  peakHour: {
    bucket: 'morning' | 'midday' | 'afternoon' | 'evening' | 'night';
    label: string;
    share: number;
  };
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

/**
 * Per-ZIP affinity for one source (megajob/job). Two indices are
 * computed so the user can pick the lens that fits their use case:
 *
 *   - affinityIndexPop (population-weighted, CPG style)
 *       = (visitor_share / pop_share) × 100, capped at 300
 *       100 = the ZIP delivers visitors proportional to its size.
 *       150 = over-indexes 1.5×.
 *       <50 = ZIP under-indexes (residents ignore the POI set).
 *       Requires a population lookup; falls back to volume-only when
 *       the ZIP has no population data (flagged via `noPopulation`).
 *
 *   - affinityIndexVolume (raw share, the original metric)
 *       = round(100 × visitors / max_visitors_in_source)
 *       Useful for media buying or coverage campaigns where absolute
 *       reach matters more than per-resident affinity.
 *
 * Both are computed always; the UI toggles between them.
 */
export interface ZipAffinityRow {
  zip: string;
  /** Distinct devices with home_zip = X who visited this source's POIs. */
  count: number;
  /** Population of the ZIP (from the country's NSE upload). 0 if unknown. */
  population: number;
  /** Weighted centroid (lat, lng) of devices from this zip — derived from
   *  home_lat/home_lng on the feature rows. Used for spatial decay in the
   *  smoothed headline. Empty when no valid coords were available. */
  centroidLat?: number;
  centroidLng?: number;
  /** Distance from the zip centroid to the source's POI centroid, in km.
   *  Inputs the Lift sub-index (devices that come from far have a lower
   *  expected share, so high visitor share at distance ranks higher). */
  distanceToPoiKm?: number;

  // ── Four sub-indices, each a percentile rank within the source (0..100) ──
  /** Volume: percentile_rank(count). High = many visitors come from here. */
  volumePct: number;
  /** Density: percentile_rank(count / population). High = oversampled vs
   *  the ZIP's demographic size. Requires NSE; otherwise NaN/undefined. */
  densityPct?: number;
  /** Loyalty: percentile_rank(mean_dwell × log(mean_visits)). High =
   *  devices from this zip have stronger engagement at the POIs. */
  loyaltyPct: number;
  /** Lift: percentile_rank(visitor_share / expected_share_at_distance).
   *  High = zip over-delivers visitors relative to what a simple
   *  distance-decay model predicts. */
  liftPct: number;

  /** Headline: geometric mean of the available sub-indices (0..100).
   *  Multiplicative composition forces real spread — a zip mediocre on
   *  any dimension is penalized hard. */
  scoreRaw: number;
  /** Same as scoreRaw but smoothed via Gaussian decay (σ=15 km default)
   *  over zip centroids — useful for continuous-surface heatmaps. */
  scoreSmoothed: number;

  // ── Legacy fields, kept for backwards compatibility with existing UI ──
  /** @deprecated Population-weighted index (0..100). Equals densityPct now. */
  affinityIndexPop: number;
  /** @deprecated Volume-only index (0..100). Equals volumePct now. */
  affinityIndexVolume: number;
  /** @deprecated True when no population was available. Equals (densityPct === undefined). */
  noPopulation: boolean;
}

export interface SourceZipAffinity {
  /** Source megajob/job id. */
  sourceId: string;
  /** Human-readable label for tab + CSV filename. */
  sourceLabel: string;
  /** Country (ISO2) for which the population lookup was applied. Empty when none. */
  country: string;
  /** Sorted desc by scoreRaw. */
  rows: ZipAffinityRow[];
  /** Sum of all visitor counts (for "X% of base" labels). */
  totalDevicesWithZip: number;
  /** True when population data was used to compute Density. */
  hasPopulation: boolean;
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
  /** Per-source ZIP affinity (one entry per megajob/job in the run). */
  zipAffinity?: SourceZipAffinity[];
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
  | 'visitor_pings_ctas'    // NEW Stage 1: materialize visitor_pings per source
  | 'visitor_pings_polling' // wait for Stage 1, then fan out Stage 2
  | 'feature_ctas'          // Stage 2: build feature vector from visitor_pings
  | 'feature_polling'
  | 'enrichment_ctas'
  | 'enrichment_polling'
  | 'download_query'      // launch SELECT * async
  | 'download_polling'    // wait for SELECT to finish
  | 'download_read'       // stream CSV from S3 + parse (now: no-op transition to geocode_cells)
  | 'geocode_cells'       // stream CSV (lite) → aggregate unique 0.1° cells → S3
  | 'geocode_lookup'      // load cellMap → batchReverseGeocode unique cells → cell→zip map to S3
  | 'clustering'          // stream CSV → features with zip lookup → k-means + report
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
  /** Stage 1: visitor_pings CTAS per source — pre-materialized table of
   *  all sub-job pings of devices that visited at least one POI, with an
   *  is_at_poi flag. Stage 2 reads from this instead of re-scanning the
   *  raw sub-jobs (which exhausted Athena resources for big runs). */
  visitorPingsCtas?: Record<string, { queryId: string; tableName: string }>;
  /** Stage 2: feature CTAS table per source. */
  featureCtas?: Record<string, { queryId: string; tableName: string }>;
  /** Per-source extra metadata captured during Stage 1, needed when
   *  Stage 2 launches (poiToBrand, dateRangeTo, country, sourceMegajobId,
   *  etc.). Persisted in S3 to survive multi-invoke polling. */
  sourceMeta?: Record<string, {
    sourceId: string;
    label: string;
    rangeTo: string;
    country: string;
    poiToBrand: Array<{ poiId: string; brand: string }>;
    /** Mean of all POI lat/lng for this source. Used as the reference
     *  point for the distance-decay Lift sub-index in zip affinity. */
    poiCentroid?: { lat: number; lng: number };
  }>;
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
