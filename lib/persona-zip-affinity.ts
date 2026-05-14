/**
 * ZIP affinity score per source — multi-dimensional composite.
 *
 * The previous single Affinity Index (volume-normalized to top zip) ran
 * uniformly high because the underlying distribution is heavy-tailed —
 * most zips clustered near the top score, no real discrimination.
 *
 * The composite replaces that with four percentile-ranked sub-indices,
 * each capturing a distinct angle of "affinity":
 *
 *   Volume   — percentile_rank(unique device count from zip).
 *              "How many visitors come from here?"
 *
 *   Density  — percentile_rank(devices / population). REQUIRES NSE upload
 *              for the country. "Oversampled vs the zip's demographic size?"
 *
 *   Loyalty  — percentile_rank(mean_dwell × log1p(mean_visits)).
 *              "How engaged are devices from this zip?"
 *
 *   Lift     — percentile_rank(visitor_share / expected_share_at_distance),
 *              where expected_share decays with distance from POI centroid.
 *              "Does this zip over-deliver relative to its distance?"
 *
 * Headline (scoreRaw) = geometric mean of available sub-indices.
 * Multiplicative composition forces real spread — a zip mediocre on any
 * dimension is penalized hard. Geometric mean stays in 0..100.
 *
 * Headline smoothed (scoreSmoothed) = Gaussian-weighted spatial average
 * of scoreRaw across nearby zips (default σ = 15 km, cutoff at 3σ).
 * Useful for continuous-surface heatmap visualizations. The four
 * sub-indices stay RAW — analysts inspecting a specific zip want its
 * own values, not contaminated by neighbors.
 */

import type {
  DeviceFeatures,
  SourceZipAffinity,
  ZipAffinityRow,
} from './persona-types';

/** Skip ZIPs below this population — small enough that a few visitors give noisy >>100 indices. */
const MIN_POPULATION_FOR_DENSITY = 200;

/** Gaussian decay bandwidth (km). 12 km is roughly "next 1-2 postal-code
 *  neighborhoods" — enough to make the heat radiate visibly to immediate
 *  neighbors without bleeding across an entire city. */
const SMOOTH_SIGMA_KM = 12;

/** Cutoff in σ-units. exp(-3²/2) ≈ 0.011 — beyond this contribution is noise. */
const SMOOTH_CUTOFF_SIGMA = 3;

export interface PopulationLookup {
  /**
   * Map from sourceId → (postalCode → population). Built by the caller
   * from per-country NSE uploads. Missing entries imply Density is
   * skipped for that source.
   */
  bySource: Map<string, Map<string, number>>;
}

export interface PoiCentroidLookup {
  /** Map from sourceId → POI centroid (mean lat/lng of POIs). */
  bySource: Map<string, { lat: number; lng: number }>;
}

/** Per-zip raw aggregates derived from DeviceFeatures. */
interface ZipAgg {
  count: number;            // distinct device count
  sumDwell: number;         // sum of total_dwell_min
  sumVisits: number;        // sum of total_visits
  sumLat: number;           // sum of home_lat (for centroid)
  sumLng: number;           // sum of home_lng
  coordCount: number;       // number of devices with valid lat/lng
}

/** Convert a sorted array of numbers into a percentile-rank lookup.
 *  For value v, returns 100 * (rank_of_v / N), with ties getting the
 *  average rank. Result is 0..100 with guaranteed spread by construction. */
function buildPercentileLookup(values: number[]): (v: number) => number {
  if (values.length === 0) return () => 0;
  // Sort ascending. For ties, fractional ranks are computed per group.
  const sorted = [...values].sort((a, b) => a - b);
  // For lookup, do a binary search to find first index >= v, then count
  // the run of equal values to compute an average rank within the tie group.
  return (v: number): number => {
    let lo = 0, hi = sorted.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (sorted[mid] < v) lo = mid + 1;
      else hi = mid;
    }
    // lo = first index with sorted[lo] >= v
    let hiTie = lo;
    while (hiTie < sorted.length && sorted[hiTie] === v) hiTie++;
    if (hiTie === lo) {
      // v not present; lo is the strict rank of values < v
      return (lo / sorted.length) * 100;
    }
    // tied group [lo, hiTie); average rank
    const avgRank = (lo + hiTie - 1) / 2 + 0.5;
    return Math.max(0, Math.min(100, (avgRank / sorted.length) * 100));
  };
}

/** Haversine distance in km between two lat/lng points. */
function distanceKm(aLat: number, aLng: number, bLat: number, bLng: number): number {
  const toRad = (d: number) => (d * Math.PI) / 180;
  const R = 6371; // mean Earth radius km
  const dLat = toRad(bLat - aLat);
  const dLng = toRad(bLng - aLng);
  const lat1 = toRad(aLat), lat2 = toRad(bLat);
  const x = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(x)));
}

/** Geometric mean over a set of values in (0, 100]. Zero values are
 *  clamped to 0.5 to avoid annihilating the entire score when one
 *  dimension scores at the very bottom — a hard "0" multiplied in is
 *  too punitive on a continuous percentile. Returns 0..100. */
function geometricMean(values: number[]): number {
  if (values.length === 0) return 0;
  let logSum = 0;
  for (const v of values) {
    const clamped = Math.max(0.5, v);
    logSum += Math.log(clamped);
  }
  return Math.exp(logSum / values.length);
}

/**
 * Main entrypoint — compute per-source ZIP affinity with the 4-dim
 * composite. The first three arguments match the legacy signature; the
 * fourth (poiCentroids) is new and required to compute Lift. If a
 * source has no poiCentroid, Lift falls back to the volume score for
 * that source's rows.
 */
export function computeZipAffinityPerSource(
  features: DeviceFeatures[],
  sourceLabels: Record<string, string>,
  populationLookup?: PopulationLookup,
  sourceCountries?: Record<string, string>,
  poiCentroids?: PoiCentroidLookup,
): SourceZipAffinity[] {
  // ── Step 1: aggregate per (source, zip) ────────────────────────────
  const bySource = new Map<string, Map<string, ZipAgg>>();
  for (const f of features) {
    const zip = (f.home_zip || '').trim();
    if (!zip) continue;
    const src = f.source_megajob_id || '';
    if (!src) continue;
    let zipMap = bySource.get(src);
    if (!zipMap) {
      zipMap = new Map();
      bySource.set(src, zipMap);
    }
    let agg = zipMap.get(zip);
    if (!agg) {
      agg = { count: 0, sumDwell: 0, sumVisits: 0, sumLat: 0, sumLng: 0, coordCount: 0 };
      zipMap.set(zip, agg);
    }
    agg.count++;
    agg.sumDwell += f.total_dwell_min || 0;
    agg.sumVisits += f.total_visits || 0;
    if (f.home_lat != null && f.home_lng != null
        && Number.isFinite(f.home_lat) && Number.isFinite(f.home_lng)) {
      agg.sumLat += f.home_lat;
      agg.sumLng += f.home_lng;
      agg.coordCount++;
    }
  }

  const out: SourceZipAffinity[] = [];
  for (const [sourceId, zipMap] of bySource.entries()) {
    if (zipMap.size === 0) continue;
    const popMap = populationLookup?.bySource.get(sourceId);
    const poiCentroid = poiCentroids?.bySource.get(sourceId);
    let totalDevices = 0;
    const zipEntries: Array<{ zip: string; agg: ZipAgg; centroidLat?: number; centroidLng?: number; distanceKm?: number }> = [];
    for (const [zip, agg] of zipMap.entries()) {
      totalDevices += agg.count;
      const centroidLat = agg.coordCount > 0 ? agg.sumLat / agg.coordCount : undefined;
      const centroidLng = agg.coordCount > 0 ? agg.sumLng / agg.coordCount : undefined;
      const distance = (poiCentroid && centroidLat != null && centroidLng != null)
        ? distanceKm(centroidLat, centroidLng, poiCentroid.lat, poiCentroid.lng)
        : undefined;
      zipEntries.push({ zip, agg, centroidLat, centroidLng, distanceKm: distance });
    }

    // ── Step 2: compute raw sub-index values for each zip ─────────────
    const volumeValues: number[] = [];
    const densityValues: number[] = [];
    const densityZipIdx: number[] = []; // indices of zips with population
    const loyaltyValues: number[] = [];
    const liftValues: number[] = [];
    const liftZipIdx: number[] = [];    // indices of zips with distance info

    // Distance-decay scale for Lift's expected_share: half-life of ~30 km.
    // expected_share(d) = exp(-d / scale). At d=0 (POI itself), share=1;
    // at d=30 km, share≈0.37; at d=60 km, share≈0.14.
    const LIFT_DECAY_SCALE_KM = 30;

    zipEntries.forEach((e, i) => {
      volumeValues.push(e.agg.count);
      const meanDwell = e.agg.count > 0 ? e.agg.sumDwell / e.agg.count : 0;
      const meanVisits = e.agg.count > 0 ? e.agg.sumVisits / e.agg.count : 0;
      loyaltyValues.push(meanDwell * Math.log1p(meanVisits));
      const pop = popMap?.get(e.zip) || 0;
      if (pop >= MIN_POPULATION_FOR_DENSITY) {
        densityValues.push(e.agg.count / pop);
        densityZipIdx.push(i);
      }
      if (e.distanceKm != null && totalDevices > 0) {
        const visitorShare = e.agg.count / totalDevices;
        const expectedShare = Math.exp(-e.distanceKm / LIFT_DECAY_SCALE_KM);
        // Lift ratio (capped at large value to prevent log explosions
        // at extreme distances). Use the ratio directly — percentile
        // rank handles distribution.
        const lift = expectedShare > 0 ? visitorShare / expectedShare : 0;
        liftValues.push(lift);
        liftZipIdx.push(i);
      }
    });

    const volumeRank = buildPercentileLookup(volumeValues);
    const densityRank = densityValues.length > 0 ? buildPercentileLookup(densityValues) : null;
    const loyaltyRank = buildPercentileLookup(loyaltyValues);
    const liftRank = liftValues.length > 0 ? buildPercentileLookup(liftValues) : null;

    const hasPopulation = !!densityRank;

    // Build a map from zip → density value (for ranking) so we can look
    // up the per-zip raw value when computing densityPct.
    const densityByIdx = new Map<number, number>();
    densityZipIdx.forEach((idx, k) => densityByIdx.set(idx, densityValues[k]));
    const liftByIdx = new Map<number, number>();
    liftZipIdx.forEach((idx, k) => liftByIdx.set(idx, liftValues[k]));

    // ── Step 3: build rows with sub-indices + raw composite ───────────
    const rows: ZipAffinityRow[] = zipEntries.map((e, i) => {
      const meanDwell = e.agg.count > 0 ? e.agg.sumDwell / e.agg.count : 0;
      const meanVisits = e.agg.count > 0 ? e.agg.sumVisits / e.agg.count : 0;
      const volumePct = Math.round(volumeRank(e.agg.count));
      const loyaltyPct = Math.round(loyaltyRank(meanDwell * Math.log1p(meanVisits)));
      const densityPct = densityByIdx.has(i) && densityRank
        ? Math.round(densityRank(densityByIdx.get(i)!))
        : undefined;
      const liftPct = liftByIdx.has(i) && liftRank
        ? Math.round(liftRank(liftByIdx.get(i)!))
        : volumePct; // fallback when distance unavailable — use volume rank

      const subScores = [volumePct, loyaltyPct, liftPct];
      if (densityPct !== undefined) subScores.push(densityPct);
      const scoreRaw = Math.round(geometricMean(subScores));

      const pop = popMap?.get(e.zip) || 0;
      return {
        zip: e.zip,
        count: e.agg.count,
        population: pop,
        centroidLat: e.centroidLat,
        centroidLng: e.centroidLng,
        distanceToPoiKm: e.distanceKm,
        volumePct,
        densityPct,
        loyaltyPct,
        liftPct,
        scoreRaw,
        scoreSmoothed: scoreRaw, // placeholder — filled in by smoothScores below
        // Legacy fields, populated for UI backwards compat.
        affinityIndexPop: densityPct ?? volumePct,
        affinityIndexVolume: volumePct,
        noPopulation: densityPct === undefined,
      };
    });

    // ── Step 4: Gaussian-smoothed headline over zip centroids ─────────
    smoothScores(rows);

    rows.sort((a, b) => b.scoreRaw - a.scoreRaw);

    out.push({
      sourceId,
      sourceLabel: sourceLabels[sourceId] || sourceId.slice(0, 8),
      country: sourceCountries?.[sourceId] || '',
      rows,
      totalDevicesWithZip: totalDevices,
      hasPopulation,
    });
  }

  out.sort((a, b) => b.totalDevicesWithZip - a.totalDevicesWithZip);
  return out;
}

/**
 * Spatial heat field — Gaussian-decayed device-count contributions from
 * every measured zip, summed at each zip's centroid, then log-normalized
 * to 0..100.
 *
 * The previous implementation used a weighted AVERAGE which pulled every
 * zip toward the local mean — clusters of high-scoring zips ended up
 * with near-identical scoreSmoothed values (the user reported seeing
 * the whole top-band compressed to 86..97 with no visible decay). We
 * want the opposite: a zip near MANY high-count zips should be hottest,
 * and the heat should fall off with distance.
 *
 * For each centroid X:
 *
 *   heat(X) = Σ_Y count(Y) × exp(-d(X,Y)² / (2σ²))
 *
 * X's own contribution is included (Y=X, d=0, weight=1) so an isolated
 * high-count zip still has meaningful heat. Then log-normalize globally
 * so the long tail (which is heavy in catchment distributions) stays
 * visible:
 *
 *   scoreSmoothed = round(100 × log(heat+1) / log(maxHeat+1)), clamped ≥1.
 *
 * O(N²) worst case but the cutoff makes it effectively O(N·K).
 */
function smoothScores(rows: ZipAffinityRow[]): void {
  const cutoffKm = SMOOTH_SIGMA_KM * SMOOTH_CUTOFF_SIGMA;
  const twoSigmaSq = 2 * SMOOTH_SIGMA_KM * SMOOTH_SIGMA_KM;
  const pts: Array<{ idx: number; lat: number; lng: number; intensity: number }> = [];
  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    if (r.centroidLat != null && r.centroidLng != null
        && Number.isFinite(r.centroidLat) && Number.isFinite(r.centroidLng)) {
      // Use raw unique-device count as the heat-source intensity. A zip
      // with 50× the visitors of another emits 50× as much "warmth" to
      // its neighborhood. (Falling back to scoreRaw here would flatten
      // hotspots because the percentile rank collapses the order-of-
      // magnitude differences between top-tier and mid-tier zips.)
      pts.push({ idx: i, lat: r.centroidLat, lng: r.centroidLng, intensity: r.count });
    }
  }
  if (pts.length === 0) return;

  const heats = new Map<number, number>();
  let maxHeat = 0;
  for (const p of pts) {
    let heat = 0;
    for (const q of pts) {
      const d = distanceKm(p.lat, p.lng, q.lat, q.lng);
      if (d > cutoffKm) continue;
      heat += q.intensity * Math.exp(-(d * d) / twoSigmaSq);
    }
    heats.set(p.idx, heat);
    if (heat > maxHeat) maxHeat = heat;
  }

  if (maxHeat <= 0) return;
  const logMax = Math.log(maxHeat + 1);
  for (const [idx, heat] of heats.entries()) {
    rows[idx].scoreSmoothed = heat > 0
      ? Math.max(1, Math.round(100 * Math.log(heat + 1) / logMax))
      : 0;
  }
}
