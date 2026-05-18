/**
 * Affinity Index v2 — percentile-rank composite over the per-zip
 * signals, re-ranked to integer 1..100 with uniform distribution.
 *
 * Why v2 exists. The v1 score (lib/mega-report-consolidation.ts) is a
 * weighted sum of visit / dwell / frequency normalised to max — that
 * compresses everything toward the centre because the head dominates
 * the maximum and the long tail vanishes. v2 expresses each signal as
 * its percentile rank inside the dataset, blends the four signals at
 * declared weights, then re-ranks the composite percentile to
 * guarantee a uniform 1..100 spread. The re-rank step is the
 * load-bearing part — without it the weighted composite still bunches
 * around 0.5.
 *
 * Algorithm (matches the spec the user proposed):
 *
 *   1. Log-compress the two heavy-tailed signals so percentile ranks
 *      are well-defined on the head end:
 *          log_visits  = log(1 + total_visits)
 *          log_devices = log(1 + unique_devices)
 *      avg_dwell_minutes and avg_frequency stay as-is. Nulls / NaN
 *      are coerced to 0 before ranking.
 *
 *   2. Percentile rank each of the four series across the input
 *      (equivalent to pandas `series.rank(pct=True)`, SQL
 *      `PERCENT_RANK() OVER (ORDER BY x)`). Ties get the average rank
 *      so log-zero entries cluster at the bottom rather than
 *      arbitrarily ordering.
 *
 *   3. Composite:
 *          c = w_devices * pr_devices  // breadth / reach
 *            + w_visits  * pr_visits   // volume
 *            + w_dwell   * pr_dwell    // engagement
 *            + w_freq    * pr_freq     // loyalty
 *      Default weights 0.30 / 0.25 / 0.25 / 0.20.
 *
 *   4. Re-rank the composite to percentile, scale to 1..100, clip.
 *
 * Caveats / contract:
 *   - The score is relative to the input set. It is NOT comparable
 *     across countries or datasets — a 90 in MX and a 90 in DE mean
 *     "top decile of THIS dataset", not "same level of affinity".
 *   - Filter sentinel rows (postal_code === 'UNKNOWN') before
 *     ranking via `filterPlaceholders: true` — otherwise their
 *     non-trivial deviceDays / visit count drag the percentile
 *     distribution.
 *   - Ties: 'average' (default) splits the rank evenly across the
 *     tied group. Switch to 'min' if you want a sharper top edge.
 *   - Empty / single-row input is handled — returns the row(s)
 *     unchanged with affinity_index_v2 = 50 (single row is by
 *     definition the median).
 */

export interface AffinityV2Row {
  postalCode: string;
  totalVisits?: number;
  uniqueDevices?: number;
  avgDwell?: number;       // minutes
  avgFrequency?: number;
  /** Set by computeAffinityIndexV2. */
  affinityIndexV2?: number;
}

export interface AffinityV2Weights {
  devices: number;
  visits: number;
  dwell: number;
  frequency: number;
}

export const DEFAULT_AFFINITY_V2_WEIGHTS: AffinityV2Weights = {
  devices: 0.30,
  visits: 0.25,
  dwell: 0.25,
  frequency: 0.20,
};

type TieMethod = 'average' | 'min';

/**
 * Returns percentile ranks (0..1) for the given numeric series. Ties
 * are handled per `method`:
 *   - 'average': each tied value gets the mean of the ranks it would
 *     otherwise span (pandas default).
 *   - 'min': all tied values get the minimum rank in the span.
 *
 * Implementation: sort indices by value, walk and bucket ties, emit
 * the rank per the chosen method. O(N log N) for the sort + O(N) for
 * the walk. The denominator is (N - 1) so the highest value maps to
 * 1.0 and the lowest to 0.0 — matching pandas `rank(pct=True)` when
 * method is 'average' and the series has > 1 entry.
 */
export function percentileRank(values: number[], method: TieMethod = 'average'): number[] {
  const n = values.length;
  if (n === 0) return [];
  if (n === 1) return [0.5];
  const indices = values.map((_, i) => i).sort((a, b) => values[a] - values[b]);
  const ranks = new Array<number>(n);
  let i = 0;
  while (i < n) {
    let j = i;
    while (j + 1 < n && values[indices[j + 1]] === values[indices[i]]) j++;
    // ranks are 1-indexed inside the bucket so the eventual pct-rank
    // hits [0, 1] correctly with the (n - 1) divisor.
    const tieMinRank = i;
    const tieMaxRank = j;
    const rank = method === 'min'
      ? tieMinRank
      : (tieMinRank + tieMaxRank) / 2;
    const pct = rank / (n - 1);
    for (let k = i; k <= j; k++) ranks[indices[k]] = pct;
    i = j + 1;
  }
  return ranks;
}

/**
 * Compute affinity_index_v2 (integer 1..100) for each input row.
 * Mutates `affinityIndexV2` on each row and returns the same array
 * for chaining. Rows where postalCode === 'UNKNOWN' (case-insensitive)
 * are skipped from the ranking when `filterPlaceholders` is true
 * (default), and assigned affinity_index_v2 = 0 in the output so the
 * UI can choose to render them as "no data".
 */
export function computeAffinityIndexV2<T extends AffinityV2Row>(
  rows: T[],
  opts?: {
    weights?: Partial<AffinityV2Weights>;
    filterPlaceholders?: boolean;
    tieMethod?: TieMethod;
  },
): T[] {
  const w: AffinityV2Weights = { ...DEFAULT_AFFINITY_V2_WEIGHTS, ...(opts?.weights || {}) };
  const tieMethod = opts?.tieMethod || 'average';
  const filterPlaceholders = opts?.filterPlaceholders ?? true;
  const sumW = w.devices + w.visits + w.dwell + w.frequency;
  if (sumW <= 0) throw new Error('computeAffinityIndexV2: weights must sum to > 0');

  // Partition rows: ranked vs. placeholder. We compute the rank on
  // the non-placeholder subset and write 0 into the placeholder rows.
  const rankIdx: number[] = [];
  const skipIdx: number[] = [];
  for (let i = 0; i < rows.length; i++) {
    const pc = String(rows[i].postalCode || '').trim().toUpperCase();
    if (filterPlaceholders && (pc === '' || pc === 'UNKNOWN')) {
      skipIdx.push(i);
    } else {
      rankIdx.push(i);
    }
  }

  if (rankIdx.length === 0) {
    for (let i = 0; i < rows.length; i++) rows[i].affinityIndexV2 = 0;
    return rows;
  }

  const safe = (v: unknown): number => {
    const n = typeof v === 'number' ? v : parseFloat(String(v));
    return Number.isFinite(n) && n >= 0 ? n : 0;
  };

  const logVisits  = rankIdx.map((i) => Math.log1p(safe(rows[i].totalVisits)));
  const logDevices = rankIdx.map((i) => Math.log1p(safe(rows[i].uniqueDevices)));
  const dwell      = rankIdx.map((i) => safe(rows[i].avgDwell));
  const frequency  = rankIdx.map((i) => safe(rows[i].avgFrequency));

  const prVisits  = percentileRank(logVisits,  tieMethod);
  const prDevices = percentileRank(logDevices, tieMethod);
  const prDwell   = percentileRank(dwell,      tieMethod);
  const prFreq    = percentileRank(frequency,  tieMethod);

  const composite = rankIdx.map((_, j) =>
    (w.devices   * prDevices[j] +
     w.visits    * prVisits[j] +
     w.dwell     * prDwell[j] +
     w.frequency * prFreq[j]) / sumW,
  );

  // Final re-rank — the spread guarantee. Without this the weighted
  // composite still bunches around 0.5.
  const reranked = percentileRank(composite, tieMethod);

  for (let j = 0; j < rankIdx.length; j++) {
    const idx = rankIdx[j];
    // Clip to 1..100 so the lowest item still appears on the colour
    // ramp (rather than indistinguishable from "no data").
    const v = Math.round(reranked[j] * 100);
    rows[idx].affinityIndexV2 = Math.min(100, Math.max(1, v));
  }
  for (const i of skipIdx) rows[i].affinityIndexV2 = 0;

  return rows;
}
