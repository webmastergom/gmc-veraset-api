/**
 * Persona clustering pipeline — two-stage:
 *   Stage 1: top-50k devices → robust z-score → k-means sweep k∈{5..8}
 *            × 3 inits → silhouette pick → centroides + auto-names.
 *   Stage 2: ALL devices → nearest-centroid assignment using the same
 *            (median, MAD) scaling.
 *   Stage 3: per-cluster aggregation over the full population.
 *
 * No data is dropped — every device receives a persona. This is critical
 * for Master MAIDs export coverage.
 */

import { kmeans } from 'ml-kmeans';
import { type DeviceFeatures, type PersonaCluster, type RadarAxis } from './persona-types';
import { autoName, disambiguateNames } from './persona-naming';

// ── Feature engineering ──────────────────────────────────────────────

/** Column order for the 12-dim feature matrix. Must stay in sync with FEATURE_LABELS. */
const FEATURE_LABELS = [
  'log_total_visits',
  'log_total_dwell_min',
  'recency_days',
  'log_avg_dwell_min',
  'morning_share',
  'midday_share',
  'evening_share',
  'night_share',
  'weekend_share',
  'log_gyration_km',
  'brand_loyalty_hhi',
  'log_unique_h3_cells',
];
export const PERSONA_FEATURE_LABELS = FEATURE_LABELS;

function safeLog(x: number): number {
  return Math.log(Math.max(x, 0) + 1);
}

function toFeatureVector(d: DeviceFeatures): number[] {
  return [
    safeLog(d.total_visits),
    safeLog(d.total_dwell_min),
    d.recency_days,
    safeLog(d.avg_dwell_min),
    d.morning_share,
    d.midday_share,
    d.evening_share,
    d.night_share,
    d.weekend_share,
    safeLog(d.gyration_km),
    d.brand_loyalty_hhi,
    safeLog(d.unique_h3_cells),
  ];
}

function median(xs: number[]): number {
  if (xs.length === 0) return 0;
  const s = [...xs].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

/** Median Absolute Deviation. Returns 1 if MAD = 0 to avoid division-by-zero. */
function mad(xs: number[], med: number): number {
  if (xs.length === 0) return 1;
  const dev = xs.map((x) => Math.abs(x - med));
  const m = median(dev);
  return m > 1e-9 ? m : 1;
}

/** Robust z-score normalization: (x - median) / MAD, computed per column. */
function fitNormalize(matrix: number[][]): { medians: number[]; mads: number[]; normalized: number[][] } {
  const cols = matrix[0]?.length || 0;
  const medians: number[] = [];
  const mads: number[] = [];
  for (let c = 0; c < cols; c++) {
    const col = matrix.map((row) => row[c]);
    const m = median(col);
    medians.push(m);
    mads.push(mad(col, m));
  }
  const normalized = matrix.map((row) => row.map((v, i) => (v - medians[i]) / mads[i]));
  return { medians, mads, normalized };
}

function applyNormalize(row: number[], medians: number[], mads: number[]): number[] {
  return row.map((v, i) => (v - medians[i]) / mads[i]);
}

// ── k-means utilities ────────────────────────────────────────────────

function squaredDistance(a: number[], b: number[]): number {
  let s = 0;
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - b[i];
    s += d * d;
  }
  return s;
}

function nearestCluster(centroids: number[][], point: number[]): number {
  let best = 0;
  let bestD = Infinity;
  for (let i = 0; i < centroids.length; i++) {
    const d = squaredDistance(centroids[i], point);
    if (d < bestD) {
      bestD = d;
      best = i;
    }
  }
  return best;
}

/**
 * Sample-based silhouette score. Full silhouette is O(N²); we sample
 * `sampleSize` rows for the cluster-to-cluster distance computation.
 * Range: [-1, 1]; higher is better cluster separation.
 */
function silhouetteSample(
  data: number[][],
  assignments: number[],
  k: number,
  sampleSize = 2000
): number {
  if (data.length === 0 || k < 2) return 0;
  const idxs = data.length <= sampleSize
    ? data.map((_, i) => i)
    : Array.from({ length: sampleSize }, () => Math.floor(Math.random() * data.length));

  // Build per-cluster index list once.
  const byCluster: number[][] = Array.from({ length: k }, () => []);
  for (let i = 0; i < data.length; i++) byCluster[assignments[i]].push(i);

  let sum = 0;
  let count = 0;
  for (const i of idxs) {
    const ci = assignments[i];
    if (byCluster[ci].length <= 1) continue;
    // a(i): mean distance to own cluster
    let a = 0;
    let an = 0;
    for (const j of byCluster[ci]) {
      if (j !== i) {
        a += Math.sqrt(squaredDistance(data[i], data[j]));
        an++;
      }
    }
    a = an > 0 ? a / an : 0;
    // b(i): min mean distance to nearest other cluster
    let b = Infinity;
    for (let other = 0; other < k; other++) {
      if (other === ci) continue;
      const otherIdx = byCluster[other];
      if (otherIdx.length === 0) continue;
      let mean = 0;
      // sample up to 100 others to keep cost bounded
      const sampleOther = otherIdx.length > 100 ? otherIdx.slice(0, 100) : otherIdx;
      for (const j of sampleOther) {
        mean += Math.sqrt(squaredDistance(data[i], data[j]));
      }
      mean /= sampleOther.length;
      if (mean < b) b = mean;
    }
    if (b === Infinity) continue;
    const s = (b - a) / Math.max(a, b);
    if (Number.isFinite(s)) {
      sum += s;
      count++;
    }
  }
  return count > 0 ? sum / count : 0;
}

// ── Pipeline ─────────────────────────────────────────────────────────

export interface ClusteringResult {
  personas: PersonaCluster[];
  assignments: Map<string, number>; // ad_id → clusterId
}

const TOP_N_FOR_CLUSTERING = 50_000;
const K_VALUES = [5, 6, 7, 8];
const INITS_PER_K = 3;

/** Returns RAW (unnormalized) centroid for naming. */
function computeRawCentroid(features: DeviceFeatures[], indices: number[]): {
  centroidNorm: number[];
  rawMedians: { total_visits: number; avg_dwell_min: number; recency_days: number; weekend_share: number; gyration_km: number; brand_loyalty_hhi: number };
} {
  const cols = FEATURE_LABELS.length;
  const sums = new Array(cols).fill(0);
  const totalVisits: number[] = [];
  const avgDwell: number[] = [];
  const recency: number[] = [];
  const weekendShare: number[] = [];
  const gyration: number[] = [];
  const loyalty: number[] = [];
  for (const i of indices) {
    const v = toFeatureVector(features[i]);
    for (let c = 0; c < cols; c++) sums[c] += v[c];
    totalVisits.push(features[i].total_visits);
    avgDwell.push(features[i].avg_dwell_min);
    recency.push(features[i].recency_days);
    weekendShare.push(features[i].weekend_share);
    gyration.push(features[i].gyration_km);
    loyalty.push(features[i].brand_loyalty_hhi);
  }
  const n = indices.length;
  const centroid = sums.map((s) => (n > 0 ? s / n : 0));
  return {
    centroidNorm: centroid,
    rawMedians: {
      total_visits: median(totalVisits),
      avg_dwell_min: median(avgDwell),
      recency_days: median(recency),
      weekend_share: median(weekendShare),
      gyration_km: median(gyration),
      brand_loyalty_hhi: median(loyalty),
    },
  };
}

/** Build radar axes (8 dimensions, all 0..1) from the cluster's raw medians + global P10/P90 scaling. */
function buildRadarAxes(raw: ClusteringResult['personas'][number]['medians'], globalP: { p10: any; p90: any }): RadarAxis[] {
  const norm = (v: number, lo: number, hi: number) => {
    if (hi <= lo) return 0;
    const x = (v - lo) / (hi - lo);
    return Math.max(0, Math.min(1, x));
  };
  return [
    { label: 'Frequency', value: norm(raw.total_visits, globalP.p10.total_visits, globalP.p90.total_visits) },
    { label: 'Dwell', value: norm(raw.avg_dwell_min, globalP.p10.avg_dwell_min, globalP.p90.avg_dwell_min) },
    { label: 'Recency', value: 1 - norm(raw.recency_days, globalP.p10.recency_days, globalP.p90.recency_days) },
    { label: 'Weekend', value: norm(raw.weekend_share, 0, 1) },
    { label: 'Mobility', value: norm(raw.gyration_km, globalP.p10.gyration_km, globalP.p90.gyration_km) },
    { label: 'Loyalty', value: norm(raw.brand_loyalty_hhi, 0, 1) },
    { label: 'Evening', value: 0 }, // filled later from cluster's evening_share
    { label: 'Lunch', value: 0 }, // filled later from cluster's midday_share
  ];
}

function percentile(xs: number[], p: number): number {
  if (xs.length === 0) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(s.length * p))];
}

export function runClusteringPipeline(features: DeviceFeatures[]): ClusteringResult {
  if (features.length === 0) {
    return { personas: [], assignments: new Map() };
  }

  // Stage 1: select top-N most active for centroid estimation.
  const topN = features.length > TOP_N_FOR_CLUSTERING
    ? [...features].sort((a, b) => b.total_visits - a.total_visits).slice(0, TOP_N_FOR_CLUSTERING)
    : features;

  const topMatrix = topN.map(toFeatureVector);
  const { medians, mads, normalized } = fitNormalize(topMatrix);

  // Sweep k=5..8 × 3 inits, pick best silhouette.
  let bestK = K_VALUES[0];
  let bestCentroids: number[][] = [];
  let bestSil = -Infinity;

  for (const k of K_VALUES) {
    for (let init = 0; init < INITS_PER_K; init++) {
      const seed = (k * 31 + init) & 0xffff;
      const result = kmeans(normalized, k, {
        seed,
        initialization: init === 0 ? 'kmeans++' : 'random',
        maxIterations: 50,
      });
      const sil = silhouetteSample(normalized, result.clusters, k, 1500);
      if (sil > bestSil) {
        bestSil = sil;
        bestK = k;
        bestCentroids = result.centroids;
      }
    }
  }

  console.log(`[PERSONA-CLUSTER] Best k=${bestK} (silhouette=${bestSil.toFixed(3)})`);

  // Stage 2: nearest-centroid assignment for ALL features.
  const assignments = new Map<string, number>();
  const allCounts: number[] = new Array(bestK).fill(0);
  const perCluster: number[][] = Array.from({ length: bestK }, () => []);
  for (let i = 0; i < features.length; i++) {
    const v = toFeatureVector(features[i]);
    const norm = applyNormalize(v, medians, mads);
    const c = nearestCluster(bestCentroids, norm);
    assignments.set(features[i].ad_id, c);
    allCounts[c]++;
    perCluster[c].push(i);
  }

  // Compute global P10/P90 for radar axes scaling.
  const totalVisitsAll = features.map((f) => f.total_visits);
  const dwellAll = features.map((f) => f.avg_dwell_min);
  const recAll = features.map((f) => f.recency_days);
  const gyrAll = features.map((f) => f.gyration_km);
  const globalP = {
    p10: {
      total_visits: percentile(totalVisitsAll, 0.1),
      avg_dwell_min: percentile(dwellAll, 0.1),
      recency_days: percentile(recAll, 0.1),
      gyration_km: percentile(gyrAll, 0.1),
    },
    p90: {
      total_visits: percentile(totalVisitsAll, 0.9),
      avg_dwell_min: percentile(dwellAll, 0.9),
      recency_days: percentile(recAll, 0.9),
      gyration_km: percentile(gyrAll, 0.9),
    },
  };

  // Stage 3: per-cluster aggregation over the FULL population.
  const totalDevices = features.length;
  const personasRaw: { id: number; namingInfo: ReturnType<typeof autoName>; cluster: PersonaCluster }[] = [];

  // Compute global means in normalized space for naming.
  const globalNormMean = new Array(FEATURE_LABELS.length).fill(0);
  for (const r of normalized) for (let c = 0; c < r.length; c++) globalNormMean[c] += r[c];
  for (let c = 0; c < globalNormMean.length; c++) globalNormMean[c] /= normalized.length;

  for (let cid = 0; cid < bestK; cid++) {
    const indices = perCluster[cid];
    if (indices.length === 0) {
      personasRaw.push({
        id: cid,
        namingInfo: { name: `Empty ${cid}`, description: '', topFeatures: [] },
        cluster: {
          id: cid,
          name: `Empty ${cid}`,
          description: '',
          deviceCount: 0,
          percentOfBase: 0,
          centroid: bestCentroids[cid],
          radarAxes: [],
          topZips: [],
          topNearbyCategories: [],
          brandMix: {},
          nseHistogram: {},
          exampleAdIds: [],
          medians: {
            total_visits: 0, avg_dwell_min: 0, recency_days: 0,
            weekend_share: 0, gyration_km: 0, brand_loyalty_hhi: 0,
          },
        },
      });
      continue;
    }

    const { centroidNorm, rawMedians } = computeRawCentroid(features, indices);

    // z-score vs global mean for naming (centroid - global / spread).
    const z = centroidNorm.map((v, i) => (v - globalNormMean[i]));
    const naming = autoName({ centroidZ: z, featureLabels: FEATURE_LABELS });

    // Aggregate top zips, categories, brand mix, NSE histogram, exampleAdIds.
    const zipCounts = new Map<string, number>();
    const catCounts = new Map<string, number>();
    const brandTotals: Record<string, number> = {};
    const nseHistogram: Record<string, number> = {};
    let eveningSum = 0, middaySum = 0;

    for (const i of indices) {
      const f = features[i];
      if (f.home_zip) zipCounts.set(f.home_zip, (zipCounts.get(f.home_zip) || 0) + 1);
      for (const c of f.nearby_categories_top5) {
        catCounts.set(c, (catCounts.get(c) || 0) + 1);
      }
      for (const [brand, n] of Object.entries(f.brand_visits)) {
        brandTotals[brand] = (brandTotals[brand] || 0) + n;
      }
      const bracket = f.nse_bracket || 'unknown';
      nseHistogram[bracket] = (nseHistogram[bracket] || 0) + 1;
      eveningSum += f.evening_share;
      middaySum += f.midday_share;
    }
    const exampleAdIds: string[] = [];
    // Sort cluster members by distance to centroid; take top 5 closest.
    const distances = indices.map((i) => {
      const norm = applyNormalize(toFeatureVector(features[i]), medians, mads);
      return { idx: i, dist: squaredDistance(norm, bestCentroids[cid]) };
    }).sort((a, b) => a.dist - b.dist).slice(0, 5);
    for (const { idx } of distances) exampleAdIds.push(features[idx].ad_id);

    const topZips = Array.from(zipCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([zip, count]) => ({ zip, count }));
    const topNearby = Array.from(catCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([category, count]) => ({ category, count }));

    // Build radar with evening/lunch shares filled in.
    const radar = buildRadarAxes(rawMedians, globalP);
    radar[6].value = indices.length > 0 ? eveningSum / indices.length : 0;
    radar[7].value = indices.length > 0 ? middaySum / indices.length : 0;

    personasRaw.push({
      id: cid,
      namingInfo: naming,
      cluster: {
        id: cid,
        name: naming.name,
        description: naming.description,
        deviceCount: indices.length,
        percentOfBase: totalDevices > 0 ? (indices.length / totalDevices) * 100 : 0,
        centroid: bestCentroids[cid],
        radarAxes: radar,
        topZips,
        topNearbyCategories: topNearby,
        brandMix: brandTotals,
        nseHistogram,
        exampleAdIds,
        medians: rawMedians,
      },
    });
  }

  // Disambiguate names if collisions.
  const dis = disambiguateNames(personasRaw.map((p) => p.cluster.name));
  const personas: PersonaCluster[] = personasRaw.map((p, i) => ({ ...p.cluster, name: dis[i] }));
  return { personas, assignments };
}
