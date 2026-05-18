/**
 * Audience estimator — converts raw MAID counts to a defensible Unique
 * Users figure using per-country multi-device factors.
 *
 * Methodology lives in METHODOLOGY.md §3.2 and the per-country
 * calibration in §3.5 / lib/country-params.ts. Summary of the math:
 *
 *   raw_MAIDs              = COUNT(DISTINCT ad_id) over the window
 *   churn_factor           = 1 + max(0, span_months - 12) / 12
 *                            (AAID/IDFA effective lifespan ≈ 12 months;
 *                            windows shorter than that get no discount)
 *   MAIDs_at_data_time     = raw_MAIDs / churn_factor
 *   Unique_Users(c)        = MAIDs_at_data_time × κ_md(c)
 *                            (κ_md per country from country-params.ts)
 *   decay_factor           = 0.5 ^ (months_since_end / 12)
 *                            (half-life of stable reachability ≈ 12 mo)
 *   Unique_Users_today(c)  = Unique_Users(c) × decay_factor
 *
 * Backward compatibility: `estimateRealAudience` keeps its previous
 * signature; new optional `country` field uses the per-country κ_md.
 * If no country is provided the global fallback (κ_md = 0.80) is used,
 * matching the prior behaviour with the documented constant instead of
 * a hand-tuned 0.20 cleanliness factor.
 *
 * The cleanliness factor and methodology shift is documented in
 * commit history; see METHODOLOGY.md §3.5 for sources.
 */

import { getCountryParams } from './country-params';

export const MAID_LIFESPAN_MONTHS = 12;

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
  /** ISO-3166-1 alpha-2. Drives the per-country κ_md (multi-device
   *  factor). When omitted or unknown, falls back to global average
   *  κ_md = 0.80 (METHODOLOGY §3.5). */
  country?: string | null;
  /** Override for "today" — used by tests; defaults to new Date(). */
  now?: Date;
}

/**
 * Returns a single number: estimated real people reachable today.
 * Null if the input MAID count is missing/zero.
 */
export function estimateRealAudience(opts: EstimateInput): number | null {
  if (!opts.totalMaids || opts.totalMaids <= 0) return null;
  const breakdown = estimateRealAudienceBreakdown(opts);
  return breakdown ? breakdown.today : null;
}

/**
 * Returns the estimate plus the components, useful for tooltips
 * ("started with X MAIDs, churn-adjusted to Y, × κ_md = Z users, × decay = W").
 */
export function estimateRealAudienceBreakdown(opts: EstimateInput) {
  if (!opts.totalMaids || opts.totalMaids <= 0) return null;

  const params = getCountryParams(opts.country);
  const span = monthsBetween(opts.dateFrom, opts.dateTo);
  const churnFactor = 1 + Math.max(0, span - MAID_LIFESPAN_MONTHS) / MAID_LIFESPAN_MONTHS;
  const now = opts.now || new Date();
  const monthsSinceEnd = opts.dateTo ? monthsBetween(opts.dateTo, now.toISOString()) : 0;
  const decayFactor = Math.pow(0.5, monthsSinceEnd / MAID_LIFESPAN_MONTHS);

  const maidsAtDataTime = opts.totalMaids / churnFactor;
  const uniqueUsersAtDataTime = maidsAtDataTime * params.kappa_md;
  const today = Math.round(uniqueUsersAtDataTime * decayFactor);

  // Sanity flag — if the input MAID count exceeds the country's MAID
  // ceiling (smartphone-adults × devices/subscriber), we have either
  // bot inflation or a calibration miss; surface it so the caller can
  // refuse to show the figure or attach a warning.
  const ceilingMaids = params.maid_ceiling_M * 1_000_000;
  const overCeiling = ceilingMaids > 0 && opts.totalMaids > ceilingMaids;

  return {
    rawMaids: opts.totalMaids,
    country: params.iso,
    kappaMd: params.kappa_md,
    devicesPerSubscriber: params.devices_per_subscriber,
    spanMonths: span,
    monthsSinceEnd,
    churnFactor,
    decayFactor,
    maidsAtDataTime: Math.round(maidsAtDataTime),
    uniqueUsersAtDataTime: Math.round(uniqueUsersAtDataTime),
    today,
    maidCeilingM: params.maid_ceiling_M,
    overCeiling,
  };
}

/**
 * Builds a human-readable tooltip with the per-country math worked out.
 * Generic callers can keep using `ESTIMATE_TOOLTIP` (a static string);
 * country-aware callers should prefer this for accurate sourcing.
 */
export function estimateTooltipFor(country: string | null | undefined): string {
  const p = getCountryParams(country);
  const isFallback = p.iso === 'XX';
  const lead = isFallback
    ? 'Estimated real people behind the MAID count (no country set — using global fallback κ_md = 0.80).'
    : `Estimated real people behind the MAID count for ${p.name} (${p.iso}).`;
  return (
    `${lead} ` +
    `Method (METHODOLOGY §3.2): churn-adjust for window > 12 months, ` +
    `multiply by κ_md = ${p.kappa_md.toFixed(2)} (= 1 / ${p.devices_per_subscriber.toFixed(2)} devices/subscriber), ` +
    `then decay 0.5^(months_since_end / 12). ` +
    (isFallback
      ? `Per-country κ_md available for: MX, ES, FR, DE, UK (see lib/country-params.ts).`
      : `MAID ceiling for ${p.iso} is ~${p.maid_ceiling_M}M; ` +
        `counts above the ceiling indicate bot inflation or methodology drift.`)
  );
}

/**
 * Static tooltip kept for callers that don't have country context.
 * Country-aware callers should use `estimateTooltipFor(country)`.
 */
export const ESTIMATE_TOOLTIP =
  'Estimated real people behind the MAID count. ' +
  'Applies (1) churn correction for windows > 12 months, ' +
  '(2) per-country multi-device factor κ_md (default 0.80 globally, ' +
  '0.69 UK / 0.70 DE / 0.74 FR / 0.79 ES / 0.87 MX — see METHODOLOGY §3.5), ' +
  '(3) exponential decay since the dataset ended (half-life 12 months). ' +
  'Counts above the country MAID ceiling (smartphone-adults × devices/subscriber) ' +
  'are flagged as likely bot inflation.';
