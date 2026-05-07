/**
 * RFM 9-cell grid:
 *   - Recency: tertile of (days since last visit, inverted so low days = "high recency")
 *   - Frequency: tertile of total_visits
 *   - Monetary-equivalent: tertile of total_dwell_min
 *
 * Combined into 9 cells (R-tertile × FM-combined-tertile) using the
 * standard e-commerce convention.
 */

import { type DeviceFeatures, type RfmCell, type RfmCellLabel, type RfmReport } from './persona-types';

function tertile<T>(items: T[], getter: (x: T) => number): { low: number; high: number } {
  const sorted = items.map(getter).sort((a, b) => a - b);
  const n = sorted.length;
  if (n === 0) return { low: 0, high: 0 };
  return { low: sorted[Math.floor(n / 3)], high: sorted[Math.floor((2 * n) / 3)] };
}

function median(xs: number[]): number {
  if (xs.length === 0) return 0;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** Map (R-tertile, F+M-tertile) → label. R 'high' = recent. */
const CELL_LABEL_MAP: Record<string, RfmCellLabel> = {
  'high-high': 'Champions',
  'high-mid': 'Promising',
  'high-low': 'Promising',
  'mid-high': 'Loyal+',
  'mid-mid': 'Loyal',
  'mid-low': 'Need Attention',
  'low-high': "Can't Lose",
  'low-mid': 'At Risk',
  'low-low': 'Lost',
};

/** Bucket helper: low/mid/high based on tertiles. */
function bucket(value: number, t: { low: number; high: number }, invert = false): 'high' | 'mid' | 'low' {
  if (invert) {
    // low value = "high" tertile (e.g. recency — fewer days = more recent).
    if (value <= t.low) return 'high';
    if (value <= t.high) return 'mid';
    return 'low';
  }
  if (value <= t.low) return 'low';
  if (value <= t.high) return 'mid';
  return 'high';
}

export function computeRfm(features: DeviceFeatures[]): RfmReport {
  if (features.length === 0) {
    return { totalDevices: 0, cells: [] };
  }

  const tR = tertile(features, (f) => f.recency_days);
  const tF = tertile(features, (f) => f.total_visits);
  const tM = tertile(features, (f) => f.total_dwell_min);

  // Pre-aggregate FM combined tertile by averaging the F and M tertile ranks.
  // We compute per-device tertile and combine them.
  const cellMap = new Map<string, RfmCell>();
  const cellAggBuckets = new Map<string, { recency: number[]; freq: number[]; monetary: number[] }>();
  for (const f of features) {
    const r = bucket(f.recency_days, tR, true);
    const fT = bucket(f.total_visits, tF);
    const mT = bucket(f.total_dwell_min, tM);
    // FM combined: average of frequency + monetary tertiles (mapped to 1/2/3).
    const rank = (b: 'low' | 'mid' | 'high') => (b === 'low' ? 1 : b === 'mid' ? 2 : 3);
    const fmAvg = (rank(fT) + rank(mT)) / 2;
    const fm: 'low' | 'mid' | 'high' = fmAvg < 1.7 ? 'low' : fmAvg < 2.7 ? 'mid' : 'high';
    const key = `${r}-${fm}`;
    if (!cellAggBuckets.has(key)) {
      cellAggBuckets.set(key, { recency: [], freq: [], monetary: [] });
    }
    const buckets = cellAggBuckets.get(key)!;
    buckets.recency.push(f.recency_days);
    buckets.freq.push(f.total_visits);
    buckets.monetary.push(f.total_dwell_min);
  }

  for (const [key, bucketData] of cellAggBuckets.entries()) {
    const [r, fm] = key.split('-') as ['high' | 'mid' | 'low', 'high' | 'mid' | 'low'];
    const label = CELL_LABEL_MAP[key] || 'Hibernating';
    cellMap.set(key, {
      label,
      rTertile: r,
      fmTertile: fm,
      deviceCount: bucketData.recency.length,
      percentOfBase: (bucketData.recency.length / features.length) * 100,
      medianRecencyDays: median(bucketData.recency),
      medianFrequency: median(bucketData.freq),
      medianMonetaryMin: median(bucketData.monetary),
    });
  }

  // Ensure all 9 cells exist for grid rendering, even if empty.
  const allCells: RfmCell[] = [];
  for (const r of ['high', 'mid', 'low'] as const) {
    for (const fm of ['low', 'mid', 'high'] as const) {
      const key = `${r}-${fm}`;
      const existing = cellMap.get(key);
      if (existing) {
        allCells.push(existing);
      } else {
        allCells.push({
          label: CELL_LABEL_MAP[key] || 'Hibernating',
          rTertile: r,
          fmTertile: fm,
          deviceCount: 0,
          percentOfBase: 0,
          medianRecencyDays: 0,
          medianFrequency: 0,
          medianMonetaryMin: 0,
        });
      }
    }
  }

  return {
    totalDevices: features.length,
    cells: allCells,
  };
}
