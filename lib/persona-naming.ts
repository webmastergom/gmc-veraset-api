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
  /**
   * Raw cluster medians (the same numbers shown to the user). Used as a
   * SANITY CHECK so the auto-generated description never says "high dwell"
   * when median avg_dwell_min is 0. The centroid z-score and the median can
   * legitimately disagree (a skewed cluster with a few high-dwell members
   * pushes the mean up while the median stays near 0) — but the user reads
   * the median, so the description must respect it.
   */
  rawMedians?: {
    total_visits: number;
    avg_dwell_min: number;
    recency_days: number;
    weekend_share: number;
    gyration_km: number;
    brand_loyalty_hhi: number;
  };
  /**
   * Global p50/p75/p25 of each median feature, for the sanity check above.
   * "high X" needs cluster median ≥ global p75; "low X" needs ≤ p25. If
   * absent we fall back to z-only (legacy behaviour).
   */
  globalMedianBands?: {
    avg_dwell_min: { p25: number; p50: number; p75: number };
    total_visits: { p25: number; p50: number; p75: number };
    recency_days: { p25: number; p50: number; p75: number };
    gyration_km: { p25: number; p50: number; p75: number };
    weekend_share: { p25: number; p50: number; p75: number };
    brand_loyalty_hhi: { p25: number; p50: number; p75: number };
  };
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
 * Map z-score feature labels to the median key used for the sanity check.
 * Keys not in this map skip the median check (they're share-style features
 * where z direction is enough).
 */
const Z_TO_MEDIAN_KEY: Record<string, keyof NonNullable<NamingInput['rawMedians']>> = {
  log_total_visits: 'total_visits',
  log_total_dwell_min: 'avg_dwell_min', // we display avg, so check avg
  recency_days: 'recency_days',
  log_avg_dwell_min: 'avg_dwell_min',
  log_gyration_km: 'gyration_km',
  weekend_share: 'weekend_share',
  brand_loyalty_hhi: 'brand_loyalty_hhi',
};

/**
 * Sanity-check a "high X" / "low X" claim against the cluster's median
 * feature value vs the global p25/p75 bands. If the claim contradicts what
 * the user will see in the metrics card, drop it.
 *
 * Returns null when the claim is dropped, else 'high' | 'low'.
 */
function validateDirection(
  zLabel: string,
  z: number,
  medians?: NamingInput['rawMedians'],
  bands?: NamingInput['globalMedianBands']
): 'high' | 'low' | null {
  const direction = z > 0 ? 'high' : 'low';
  if (!medians || !bands) return direction; // legacy: trust z
  const medianKey = Z_TO_MEDIAN_KEY[zLabel];
  if (!medianKey) return direction; // share-style features pass through
  const value = medians[medianKey];
  const band = bands[medianKey as keyof typeof bands];
  if (!band) return direction;
  // For recency_days, "high z" still means "high recency_days" (older), which
  // we read as "Lapsed". The phrase mapping handles inversion. Just check the
  // band:
  if (direction === 'high') {
    return value >= band.p75 ? 'high' : null;
  } else {
    return value <= band.p25 ? 'low' : null;
  }
}

/**
 * Compose a one-sentence description that explains the cluster, using
 * sanity-checked directions (drops "high dwell" when median is 0, etc.).
 */
function composeDescription(
  top: { label: string; z: number; direction: 'high' | 'low' | null }[]
): string {
  const parts: string[] = [];
  for (const t of top) {
    const ph = FEATURE_PHRASES[t.label];
    if (!ph || !t.direction) continue;
    if (t.direction === 'high' && ph.high) parts.push(`high ${ph.tag}`);
    else if (t.direction === 'low' && ph.low) parts.push(`low ${ph.tag}`);
  }
  if (parts.length === 0) return 'Average visitor profile.';
  return `Devices with ${parts.join(' + ')}.`;
}

export function autoName(input: NamingInput): NamingOutput {
  const { centroidZ, featureLabels, rawMedians, globalMedianBands } = input;
  const ranked = centroidZ
    .map((z, i) => ({ label: featureLabels[i], z }))
    .filter((e) => Number.isFinite(e.z))
    .sort((a, b) => Math.abs(b.z) - Math.abs(a.z))
    .slice(0, 5); // peek at top-5 so we have replacements when sanity drops one

  const validated = ranked.map((r) => ({
    ...r,
    direction: validateDirection(r.label, r.z, rawMedians, globalMedianBands),
  }));

  const mods: string[] = [];
  for (const r of validated) {
    if (mods.length >= 3) break;
    const ph = FEATURE_PHRASES[r.label];
    if (!ph || !r.direction) continue;
    if (r.direction === 'high' && ph.high) mods.push(ph.high);
    else if (r.direction === 'low' && ph.low) mods.push(ph.low);
  }
  // Description uses only the validated top-3 (whichever passed the sanity check)
  const validForDesc = validated.filter((v) => v.direction !== null).slice(0, 3);
  return {
    name: composeName(mods),
    description: composeDescription(validForDesc),
    // Return the full top-5 so disambiguation can pull a 4th/5th feature
    // when two clusters collide on the top-3 name.
    topFeatures: ranked,
  };
}

/**
 * Disambiguate names. When two clusters end up with the same name, derive
 * a discriminator from the "next" feature (4th by |z|) of each colliding
 * cluster — gives readable distinctions like "Lapsed Road-Tripper Long-stay
 * (weekend)" / "(weekday)" instead of just appending a meaningless "2".
 *
 * `extras[i]` is the cluster's full top-5 feature list. Pass undefined for
 * legacy callers — they'll get the old numeric suffix.
 */
export function disambiguateNames(
  names: string[],
  extras?: { label: string; z: number }[][]
): string[] {
  const groups = new Map<string, number[]>();
  for (let i = 0; i < names.length; i++) {
    const arr = groups.get(names[i]) || [];
    arr.push(i);
    groups.set(names[i], arr);
  }
  const out = [...names];
  for (const [name, indices] of groups.entries()) {
    if (indices.length < 2) continue;
    if (!extras) {
      // Legacy: numeric suffix
      indices.forEach((idx, k) => {
        if (k > 0) out[idx] = `${name} ${k + 1}`;
      });
      continue;
    }
    // Find the feature where the colliding clusters disagree the most.
    // For each cluster, take features 4..N (after the top-3 already used)
    // and pick the highest-|z| one that the others don't share.
    indices.forEach((idx, k) => {
      if (k === 0) return; // first occurrence keeps the bare name
      const own = extras[idx] || [];
      // Skip features already in the name (top-3 mapped to FEATURE_PHRASES).
      // Pick the next mappable feature with a non-empty modifier.
      let extra = '';
      for (const f of own.slice(3)) {
        const ph = FEATURE_PHRASES[f.label];
        if (!ph) continue;
        const word = f.z > 0 ? ph.high : ph.low;
        if (!word) continue;
        extra = word;
        break;
      }
      out[idx] = extra ? `${name} (${extra})` : `${name} ${k + 1}`;
    });
  }
  return out;
}
