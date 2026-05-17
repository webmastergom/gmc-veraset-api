/**
 * Heuristic estimator for *real-person* audience behind a raw MAID count.
 *
 * Raw MAID counts in mobility data overstate reachable people by 5-10×
 * because MAIDs are not stable identifiers:
 *   - iOS post-ATT (≈80% opt-out): zeros dedupe, but session-randoms
 *     inflate counts massively (often the dominant noise source)
 *   - Android AAID resets (settings, factory reset, new phone)
 *   - Multi-device per real person (phone + tablet + work phone)
 *   - Bot/test traffic
 *
 * We model three multiplicative effects:
 *
 *   1. Churn during the observation window.
 *      AAID/IDFA effectively rotate on a ~12-month timescale (Android
 *      resets, new-phone purchases). Spans shorter than that get no
 *      churn discount: churnFactor = 1 + max(0, span - lifespan) / lifespan.
 *
 *   2. Cleanliness — flat 0.20 multiplier capturing the structural
 *      inflation present in spans of any length: session-randoms,
 *      multi-device per person, bots. Tuned empirically against Mexico's
 *      master (555M raw → ~109M plausible reach across ~85M smartphone
 *      adults) and individual datasets (~52M for a 30-day extract).
 *
 *   3. Time-decay since the dataset ended.
 *      Even a stable MAID becomes harder to reach as the underlying
 *      person rotates devices. decayFactor = 0.5 ^ (monthsSinceEnd / lifespan).
 *
 * Constants are still industry rule-of-thumb (not measured against
 * ground truth). With a 12-month lifespan and 0.20 cleanliness, the
 * function is monotonic-by-intuition: subset estimates rarely exceed
 * their superset, and global totals stay within a country's plausible
 * smartphone-adult population. Treat the output as a conservative
 * upper bound for activatable audience size, not a precise count.
 */

export const MAID_LIFESPAN_MONTHS = 12;
export const MAID_CLEANLINESS_FACTOR = 0.20;

export function monthsBetween(
  fromIso: string | null | undefined,
  toIso: string | null | undefined,
): number {
  if (!fromIso || !toIso || fromIso === 'unknown' || toIso === 'unknown') return 0;
  const f = new Date(fromIso).getTime();
  const t = new Date(toIso).getTime();
  if (!Number.isFinite(f) || !Number.isFinite(t) || t < f) return 0;
  return (t - f) / (1000 * 60 * 60 * 24 * 30.44);
}

export interface EstimateInput {
  totalMaids: number | null | undefined;
  /** ISO date — start of the data window. */
  dateFrom?: string | null;
  /** ISO date — end of the data window. */
  dateTo?: string | null;
  /** Override for "today" — used by tests; defaults to new Date(). */
  now?: Date;
}

/**
 * Returns a single number: estimated real people reachable today.
 * Null if the input MAID count is missing/zero.
 */
export function estimateRealAudience(opts: EstimateInput): number | null {
  if (!opts.totalMaids || opts.totalMaids <= 0) return null;

  const span = monthsBetween(opts.dateFrom, opts.dateTo);
  const churnFactor = 1 + Math.max(0, span - MAID_LIFESPAN_MONTHS) / MAID_LIFESPAN_MONTHS;
  const atDataTime = (opts.totalMaids / churnFactor) * MAID_CLEANLINESS_FACTOR;

  const now = opts.now || new Date();
  const monthsSinceEnd = opts.dateTo
    ? monthsBetween(opts.dateTo, now.toISOString())
    : 0;
  const decayFactor = Math.pow(0.5, monthsSinceEnd / MAID_LIFESPAN_MONTHS);

  return Math.round(atDataTime * decayFactor);
}

/**
 * Returns the same estimate plus the components, useful for breaking
 * down the math in tooltips ("you started with X MAIDs, churn shrinks
 * to Y, cleanliness to Z, decay to W").
 */
export function estimateRealAudienceBreakdown(opts: EstimateInput) {
  if (!opts.totalMaids || opts.totalMaids <= 0) {
    return null;
  }
  const span = monthsBetween(opts.dateFrom, opts.dateTo);
  const churnFactor = 1 + Math.max(0, span - MAID_LIFESPAN_MONTHS) / MAID_LIFESPAN_MONTHS;
  const now = opts.now || new Date();
  const monthsSinceEnd = opts.dateTo
    ? monthsBetween(opts.dateTo, now.toISOString())
    : 0;
  const decayFactor = Math.pow(0.5, monthsSinceEnd / MAID_LIFESPAN_MONTHS);
  const atDataTime = (opts.totalMaids / churnFactor) * MAID_CLEANLINESS_FACTOR;
  const now_est = Math.round(atDataTime * decayFactor);
  return {
    rawMaids: opts.totalMaids,
    spanMonths: span,
    monthsSinceEnd,
    churnFactor,
    cleanlinessFactor: MAID_CLEANLINESS_FACTOR,
    decayFactor,
    atDataTime: Math.round(atDataTime),
    today: now_est,
  };
}

export const ESTIMATE_TOOLTIP =
  'Heuristic estimate of real people behind the MAIDs. Accounts for: ' +
  '(1) MAID churn during the observation window (~12-month effective lifespan, ' +
  'mainly from AAID resets and new-phone purchases); ' +
  '(2) ~80% structural inflation (iOS session-randomization, multi-device per ' +
  'person, bots) — applied as a flat 0.20 multiplier; ' +
  '(3) exponential decay since the dataset ended (half-life ≈ 12 months). ' +
  'Rough heuristic — subset estimates may slightly exceed their superset due to ' +
  'per-source noise; treat the master / parent estimate as the conservative upper bound.';
