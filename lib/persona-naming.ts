/**
 * Persona auto-naming — converts a cluster's distinguishing-feature signature
 * into a readable persona name + description.
 *
 * Inputs:
 *   - centroidZScores: 12-dim vector of (cluster_mean - global_mean) / global_MAD
 *     for each feature, in the same column order used by the clusterer.
 *   - featureLabels: parallel array of human-readable feature names.
 *
 * Strategy:
 *   1. Pick top-3 features by |z|, with sign (positive = "high X", negative = "low X").
 *   2. Map each (feature, sign) pair to a short modifier word.
 *   3. Combine into a 2-3 word persona name using PERSONA_TEMPLATES.
 */

export interface NamingInput {
  centroidZ: number[];
  featureLabels: string[];
}

export interface NamingOutput {
  name: string;
  description: string;
  /** Top-3 (label, z-score, sign) for UI / logging. */
  topFeatures: { label: string; z: number }[];
}

/**
 * Map "feature name" + "high/low" → short adjective/phrase.
 * Used both for naming and for the description sentence.
 */
const FEATURE_PHRASES: Record<string, { high: string; low: string; tag: string }> = {
  log_total_visits: { high: 'Heavy', low: 'Casual', tag: 'frequency' },
  log_total_dwell_min: { high: 'Long-stay', low: 'Quick-stop', tag: 'dwell' },
  recency_days: { high: 'Lapsed', low: 'Recent', tag: 'recency' },
  log_avg_dwell_min: { high: 'Lingerer', low: 'Speedy', tag: 'avg dwell' },
  morning_share: { high: 'Morning', low: '', tag: 'morning visits' },
  midday_share: { high: 'Lunchtime', low: '', tag: 'lunchtime visits' },
  evening_share: { high: 'Evening', low: '', tag: 'evening visits' },
  night_share: { high: 'Late-Night', low: '', tag: 'night visits' },
  weekend_share: { high: 'Weekend', low: 'Weekday', tag: 'weekend tilt' },
  log_gyration_km: { high: 'Road-Tripper', low: 'Hyperlocal', tag: 'mobility radius' },
  brand_loyalty_hhi: { high: 'Brand-Loyal', low: 'Promiscuous', tag: 'brand loyalty' },
  log_unique_h3_cells: { high: 'Wide-Roaming', low: 'Neighborhood', tag: 'movement diversity' },
};

/**
 * Persona name templates: combine 1-3 modifiers into a memorable label.
 * Order in the array determines preference; first match wins.
 */
function composeName(mods: string[]): string {
  const filtered = mods.filter(Boolean);
  if (filtered.length === 0) return 'Casual Visitor';
  if (filtered.length === 1) return `${filtered[0]} Visitor`;
  if (filtered.length === 2) return `${filtered[0]} ${filtered[1]} Visitor`;
  return `${filtered[0]} ${filtered[1]} ${filtered[2]}`;
}

/**
 * Compose a one-sentence description that explains the cluster.
 */
function composeDescription(top: { label: string; z: number }[]): string {
  const parts: string[] = [];
  for (const t of top) {
    const ph = FEATURE_PHRASES[t.label];
    if (!ph) continue;
    if (t.z > 0 && ph.high) parts.push(`high ${ph.tag}`);
    else if (t.z < 0 && ph.low) parts.push(`low ${ph.tag}`);
  }
  if (parts.length === 0) return 'Average visitor profile.';
  return `Devices with ${parts.join(' + ')}.`;
}

export function autoName(input: NamingInput): NamingOutput {
  const { centroidZ, featureLabels } = input;
  const ranked = centroidZ
    .map((z, i) => ({ label: featureLabels[i], z }))
    .filter((e) => Number.isFinite(e.z))
    .sort((a, b) => Math.abs(b.z) - Math.abs(a.z))
    .slice(0, 3);

  const mods: string[] = [];
  for (const r of ranked) {
    const ph = FEATURE_PHRASES[r.label];
    if (!ph) continue;
    if (r.z > 0 && ph.high) mods.push(ph.high);
    else if (r.z < 0 && ph.low) mods.push(ph.low);
  }
  return {
    name: composeName(mods),
    description: composeDescription(ranked),
    topFeatures: ranked,
  };
}

/**
 * Disambiguate names: if 2 clusters end up with the same name, append a
 * numeric suffix in centroid order.
 */
export function disambiguateNames(names: string[]): string[] {
  const seen = new Map<string, number>();
  return names.map((n) => {
    const c = seen.get(n) || 0;
    seen.set(n, c + 1);
    return c === 0 ? n : `${n} ${c + 1}`;
  });
}
