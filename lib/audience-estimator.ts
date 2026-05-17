/**
 * Heuristic estimator for *real-person* audience behind a raw MAID count.
 *
 * Raw MAID counts in mobility data overstate reachable people by 3-5×
 * because MAIDs are not stable identifiers:
 *   - iOS post-ATT (≈80% opt-out): IDFA → zeros or session-random strings
 *   - Android AAID resets (settings, factory reset, new phone)
 *   - App reinstalls / multi-account → new MAID for same person
 *   - Bot/test traffic
 *
 * We model three multiplicative effects:
 *
 *   1. Churn during the observation window.
 *      The longer the dataset spans, the more times each person rotates
 *      MAIDs within it. churnFactor = 1 + max(0, span - lifespan) / lifespan.
 *
 *   2. Cleanliness — flat 0.6 multiplier accounting for iOS-zeros,
 *      session-randoms, bots, short-lived test MAIDs.
 *
 *   3. Time-decay since the dataset ended.
 *      A 12-month-old MAID has likely rotated three times. We use
 *      exponential decay with the same lifespan as half-life.
 *      decayFactor = 0.5 ^ (monthsSinceEnd / lifespan).
 *
 * The constants are industry rule-of-thumb (not measured). Treat the
 * output as a conservative upper bound for "people you could actually
 * reach if you activated this audience today", not a precise count.
 */

export const MAID_LIFESPAN_MONTHS = 4;
export const MAID_CLEANLINESS_FACTOR = 0.6;

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
  '(1) MAID churn during the observation window (~4-month effective lifespan); ' +
  '(2) ~40% noise (iOS-zero strings, session-randoms, bots, short-lived MAIDs); ' +
  '(3) exponential decay since the dataset ended (half-life ≈ 4 months). ' +
  'Use as a conservative upper bound for activatable audience size — not a precise count.';
