/**
 * Resident audience counter — METHODOLOGY.md §3.3.
 *
 * Computes the third tier of the audience methodology: how many MAIDs
 * have a stable home in country c AND were active in ≥ 50 % of the
 * weeks of the observation window. The output is paired with the
 * per-country κ_md (lib/country-params.ts) to yield Unique-User-grade
 * Resident counts that we can take to a client without overclaiming.
 *
 * Formally:
 *   Resident_MAIDs(c, W) = |{ m ∈ M | home(m).country = c
 *                                  AND home_confidence(m) ≥ τ_c
 *                                  AND n_weeks_active(m) ≥ ⌈|W| × 0.5⌉ }|
 *
 *   Resident_Users(c, W) = Resident_MAIDs(c, W) × κ_md(c)
 *
 * Inputs:
 *   homeTable     — the TC-WK-19-7 home-locations table for the dataset
 *                   (produced by lib/home-detector.ts). Carries
 *                   ad_id, home_country, home_confidence, n_nights.
 *   sourceTable   — the dataset's raw pings table. Used to count
 *                   active weeks per MAID.
 *   country       — ISO-3166-1 alpha-2 (filters home_country).
 *   weeksInWindow — total number of ISO weeks the dataset spans.
 *                   We require ad_id to appear in ⌈weeksInWindow × 0.5⌉
 *                   distinct weeks to count as "resident-grade active".
 *   tauC          — confidence floor (default 0.5, METHODOLOGY §3.3).
 *
 * The query produces a single row:
 *   resident_maids       BIGINT
 *   active_weeks_floor   INT     (= ⌈|W| × 0.5⌉)
 *   weeks_in_window      INT
 *
 * Reach (Unique Users) is computed Node-side by multiplying
 * resident_maids by κ_md(country) — see audience-estimator.ts.
 */

import { getCountryParams } from './country-params';

export const DEFAULT_HOME_CONFIDENCE_THRESHOLD = 0.5;
export const DEFAULT_MIN_NIGHTS_AT_HOME = 3;
export const RESIDENT_WEEK_ACTIVITY_FRACTION = 0.5;

export interface AudienceCounterInputs {
  homeTable: string;
  sourceTable: string;
  country: string;
  /** Distinct ISO weeks the dataset window spans. Use `weeksInRange` to
   *  derive from a [from, to] pair. */
  weeksInWindow: number;
  /** Per-MAID home_confidence floor. METHODOLOGY §3.3 default = 0.5. */
  tauC?: number;
  /** Minimum nights observed in the home bucket (defensive — the home
   *  detection CTAS already enforces n_nights >= MIN_NIGHTS_AT_HOME,
   *  but stating the floor here makes the SQL self-documenting). */
  minNights?: number;
}

/**
 * Build the Athena SQL for the Resident count. Single result row.
 */
export function buildAudienceCounterSQL(opts: AudienceCounterInputs): string {
  const tau = opts.tauC ?? DEFAULT_HOME_CONFIDENCE_THRESHOLD;
  const minNights = opts.minNights ?? DEFAULT_MIN_NIGHTS_AT_HOME;
  const weeksFloor = Math.ceil(opts.weeksInWindow * RESIDENT_WEEK_ACTIVITY_FRACTION);
  // Country code is uppercased; we trust the caller has validated it
  // against COUNTRY_PARAMS but quote-escape defensively anyway.
  const country = opts.country.toUpperCase().replace(/'/g, "''");
  return `
    WITH residents_by_home AS (
      SELECT ad_id, home_confidence
      FROM ${opts.homeTable}
      WHERE UPPER(home_country) = '${country}'
        AND home_confidence >= ${tau}
        AND n_nights >= ${minNights}
    ),
    weeks_active AS (
      SELECT t.ad_id, COUNT(DISTINCT DATE_TRUNC('week', t.utc_timestamp)) AS n_weeks
      FROM ${opts.sourceTable} t
      INNER JOIN residents_by_home h ON t.ad_id = h.ad_id
      WHERE t.ad_id IS NOT NULL AND TRIM(t.ad_id) != ''
      GROUP BY t.ad_id
    )
    SELECT
      COUNT(*) AS resident_maids,
      ${weeksFloor} AS active_weeks_floor,
      ${opts.weeksInWindow} AS weeks_in_window
    FROM weeks_active
    WHERE n_weeks >= ${weeksFloor}
  `;
}

/**
 * Parse the single-row Athena result + apply κ_md to derive the
 * Unique-User Resident count and the calibration ceiling check.
 */
export function parseAudienceCounterResult(
  rows: Record<string, any>[],
  country: string,
): {
  residentMaids: number;
  residentUsers: number;
  kappaMd: number;
  ceilingM: number;
  overCeiling: boolean;
  activeWeeksFloor: number;
  weeksInWindow: number;
} {
  const r = rows[0] || {};
  const residentMaids = parseInt(String(r.resident_maids ?? '0'), 10) || 0;
  const activeWeeksFloor = parseInt(String(r.active_weeks_floor ?? '0'), 10) || 0;
  const weeksInWindow = parseInt(String(r.weeks_in_window ?? '0'), 10) || 0;
  const params = getCountryParams(country);
  const residentUsers = Math.round(residentMaids * params.kappa_md);
  const ceilingMaids = params.maid_ceiling_M * 1_000_000;
  const overCeiling = ceilingMaids > 0 && residentMaids > ceilingMaids;
  return {
    residentMaids,
    residentUsers,
    kappaMd: params.kappa_md,
    ceilingM: params.maid_ceiling_M,
    overCeiling,
    activeWeeksFloor,
    weeksInWindow,
  };
}

/**
 * Count distinct ISO weeks between two ISO-formatted dates (inclusive).
 * Used by callers to derive `weeksInWindow` from a dataset's date range.
 */
export function weeksInRange(
  fromIso: string | null | undefined,
  toIso: string | null | undefined,
): number {
  if (!fromIso || !toIso) return 0;
  const f = new Date(fromIso).getTime();
  const t = new Date(toIso).getTime();
  if (!Number.isFinite(f) || !Number.isFinite(t) || t < f) return 0;
  const days = (t - f) / (1000 * 60 * 60 * 24);
  return Math.max(1, Math.ceil(days / 7));
}

export interface AudienceCounterReport {
  datasetName: string;
  country: string;
  analyzedAt: string;
  /** Window the count was computed over. */
  dateFrom: string | null;
  dateTo: string | null;
  weeksInWindow: number;
  activeWeeksFloor: number;
  homeConfidenceThreshold: number;
  /** Tier 1 — raw distinct MAID count over the window (not just
   *  residents). Cached from the basic dataset analysis. */
  uniqueMaids: number | null;
  /** Tier 2 — uniqueMaids × κ_md(country). */
  uniqueUsers: number | null;
  /** Tier 3 — METHODOLOGY §3.3 resident filter applied. */
  residentMaids: number;
  residentUsers: number;
  /** Per-country calibration ceiling (pop × sub_pen × dev_per_sub). */
  maidCeilingM: number;
  /** True when uniqueMaids > ceiling — flags bot inflation. */
  overCeiling: boolean;
  kappaMd: number;
}
