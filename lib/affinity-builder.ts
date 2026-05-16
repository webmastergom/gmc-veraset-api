/**
 * Affinity report builder — shared between dataset poll and megajob
 * category-affinity export. Computes a Gaussian heat-field affinity
 * score (0..100) per zip code from raw per-coordinate aggregates.
 *
 * Input rows must have:
 *   origin_lat, origin_lng        — coordinates of the visitor's home
 *   native_zip, native_city       — optional FULL-schema bypass
 *   unique_devices, total_visit_days, avg_dwell_minutes, avg_frequency
 *
 * Step 1: aggregate per zip code (FULL-schema bypass for natively
 *         resolved zips; otherwise reverse-geocode via coordToZip).
 * Step 2: build a Gaussian heat field over zip centroids — every zip's
 *         heat = Σ intensity × exp(-d²/2σ²), where intensity grows with
 *         visit volume and engagement.
 * Step 3: ALSO score neighboring polygons from the country GeoJSON
 *         (no own visit data — heat radiates from sources).
 * Step 4: log-normalize globally to 0..100 so the heavy tail stays
 *         visible.
 *
 * σ = 12 km gives a few-zip radius — enough to highlight regional
 * clusters without washing out a single city.
 */

import fs from 'fs';
import path from 'path';
import { CATEGORY_GROUPS, CATEGORY_LABELS } from './laboratory-types';

/**
 * Human-readable label for a category-affinity export. Used both at
 * write time (stored in the report JSON) and at list time (re-derived
 * so old reports pick up the new naming convention without rewriting
 * every S3 object).
 */
export function buildCategoryAffinityLabel(
  groupKey: string | undefined | null,
  categories: string[] | undefined | null,
  matchMode?: 'OR' | 'AND' | null,
): string {
  const prettify = (s: string) =>
    s.replace(/[_-]+/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  const n = categories?.length ?? 0;
  // Mode suffix is only informative for multi-cat AND — OR is the default
  // for everything and 1-cat selections collapse to the same audience either way.
  const modeSuffix = matchMode === 'AND' && n >= 2 ? ' [AND]' : '';
  if (n === 1 && categories) {
    const cat = categories[0];
    const nice = CATEGORY_LABELS[cat] || prettify(cat);
    return `MAIDs by Category: ${nice}`;
  }
  if (groupKey && groupKey !== 'custom' && CATEGORY_GROUPS[groupKey]) {
    const group = CATEGORY_GROUPS[groupKey];
    if (n > 0 && n < group.categories.length) {
      return `MAIDs by Category: ${group.label} (${n} subcategories)${modeSuffix}`;
    }
    return `MAIDs by Category: ${group.label}${modeSuffix}`;
  }
  return `MAIDs by Category: Custom (${n} categories)${modeSuffix}`;
}

export interface AffinityByZip {
  zipCode: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  uniqueDevices: number;
  totalVisitDays: number;
  avgDwellMinutes: number;
  avgFrequency: number;
  affinityIndex: number;  // 0-100
}

export interface AffinityReport {
  analyzedAt: string;
  subject: string;
  byZipCode: AffinityByZip[];
}

/** Baseline population reference (e.g. the job's main affinity report).
 *  When provided, scoring switches from raw heat to *lift over baseline*
 *  — i.e. where this category is over- or under-indexed relative to
 *  overall device density. Without it, every category-affinity map
 *  collapses to the same "where most people live" pattern. */
export interface BaselineZip {
  zipCode: string;
  lat: number;
  lng: number;
  uniqueDevices: number;
}

/** Haversine distance in km between two points. */
function haversineKm(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Legacy main-affinity σ — used when there's no baseline (the job's own
// affinity report). One σ for both because the legacy "heat" signal IS
// the only signal — no ratio.
const SIGMA_KM = 12;
const CUTOFF_KM = SIGMA_KM * 3;
const twoSigmaSq = 2 * SIGMA_KM * SIGMA_KM;

// ── Lift-mode sigmas (asymmetric) ───────────────────────────────────
// σ_cat = 3km → category heat concentrates at neighborhood scale.
// σ_base = 8km → population baseline smoothes to metro/district scale.
// The ratio heat_cat/heat_base then reads as "this neighborhood vs
// the surrounding metro" — which is the question we actually want
// answered. With a single σ=12km, the city collapses into a uniform
// blob and every category map ends up correlating r≈0.92 with each
// other (we measured this on Spain-Canarias automotive).
const SIGMA_CAT_KM = 3;
const SIGMA_BASE_KM = 8;
const CUTOFF_CAT_KM = SIGMA_CAT_KM * 3;
const CUTOFF_BASE_KM = SIGMA_BASE_KM * 3;
const twoSigmaCatSq = 2 * SIGMA_CAT_KM * SIGMA_CAT_KM;
const twoSigmaBaseSq = 2 * SIGMA_BASE_KM * SIGMA_BASE_KM;

// Min-baseline gate: zips whose smoothed baseline heat is under
// `BASE_GATE_FRAC × maxBase` are dropped (adjacents) or scored 0
// (measured zips kept in CSV so device counts still surface). 5% kills
// the rural sample-size noise without touching real metro signal.
const BASE_GATE_FRAC = 0.05;

export async function computeAffinityReport(
  subject: string,
  rows: any[],
  coordToZip: Map<string, { zipCode: string; city: string; country: string }>,
  country?: string,
  baseline?: BaselineZip[],
): Promise<AffinityReport> {
  // ── Step 1: Aggregate raw data by postal code ──────────────────
  const zipMap = new Map<string, {
    zipCode: string; city: string; country: string;
    lat: number; lng: number;
    uniqueDevices: number; totalVisitDays: number;
    dwellSum: number; dwellCount: number;
    freqSum: number; freqCount: number;
  }>();

  const fallbackCountry = country || 'UNKNOWN';
  for (const row of rows) {
    const lat = parseFloat(row.origin_lat) || 0;
    const lng = parseFloat(row.origin_lng) || 0;
    const key = `${lat},${lng}`;
    const nativeZip = String(row.native_zip || '').trim();
    const nativeCity = String(row.native_city || '').trim();
    const geo = nativeZip
      ? { zipCode: nativeZip, city: nativeCity || 'UNKNOWN', country: fallbackCountry }
      : (coordToZip.get(key) || { zipCode: 'UNKNOWN', city: 'UNKNOWN', country: 'UNKNOWN' });
    const zk = geo.zipCode;

    const devices = parseInt(row.unique_devices, 10) || 0;
    const visitDays = parseInt(row.total_visit_days, 10) || 0;
    const avgDwell = parseFloat(row.avg_dwell_minutes) || 0;
    const avgFreq = parseFloat(row.avg_frequency) || 1;

    const existing = zipMap.get(zk);
    if (existing) {
      existing.uniqueDevices += devices;
      existing.totalVisitDays += visitDays;
      existing.dwellSum += avgDwell * devices;
      existing.dwellCount += devices;
      existing.freqSum += avgFreq * devices;
      existing.freqCount += devices;
    } else {
      zipMap.set(zk, {
        zipCode: zk, city: geo.city, country: geo.country,
        lat, lng,
        uniqueDevices: devices, totalVisitDays: visitDays,
        dwellSum: avgDwell * devices, dwellCount: devices,
        freqSum: avgFreq * devices, freqCount: devices,
      });
    }
  }

  // ── Step 2: Heat sources from measured zips ─────────────────────
  const zips = Array.from(zipMap.values()).filter(z => z.zipCode !== 'UNKNOWN');
  type HotSource = typeof zips[0] & {
    avgDwell: number; avgFreq: number; intensity: number;
  };
  const hotSources: HotSource[] = zips.map(z => {
    const avgDwell = z.dwellCount > 0 ? z.dwellSum / z.dwellCount : 0;
    const avgFreq = z.freqCount > 0 ? z.freqSum / z.freqCount : 1;
    const engagementBoost = 1 + Math.log1p(avgDwell / 30) + Math.log1p(avgFreq / 2);
    const intensity = Math.max(1, z.totalVisitDays) * engagementBoost;
    return { ...z, avgDwell, avgFreq, intensity };
  });

  const heatAt = (lat: number, lng: number): number => {
    let heat = 0;
    for (const q of hotSources) {
      const d = haversineKm(lat, lng, q.lat, q.lng);
      if (d > CUTOFF_KM) continue;
      heat += q.intensity * Math.exp(-(d * d) / twoSigmaSq);
    }
    return heat;
  };

  const hotWithHeat = hotSources.map(z => ({ ...z, heat: heatAt(z.lat, z.lng) }));

  // ── Lift mode: score = over-indexing vs a baseline population ────
  //
  // Without this, every category map collapses to the same population-
  // density pattern (top zips are wherever most devices live, period).
  // With it, score reflects where the category is concentrated *relative
  // to* baseline. A zip with 3× the category share for its population
  // size scores high even if it isn't the most populous.
  //
  // Math:
  //   heat_cat(p)  = Σ_q intensity_q × g(dist(p, q))   over cat sources
  //   heat_base(p) = Σ_b devices_b × g(dist(p, b))     over baseline
  //   lift(p) = heat_cat(p) / (heat_base(p) + ε)
  //   score   = clamp(50 + 20·log2(lift / medianLift), 0, 100)
  //
  // Anchoring on the median lift means the "typical" zip scores ~50,
  // a 2× over-indexed zip scores ~70, and a 4× over-indexed zip caps at
  // 100. Under-indexed zips score below 50 → readable contrast.
  if (baseline && baseline.length > 0) {
    const baseSources = baseline
      .filter(b => Number.isFinite(b.lat) && Number.isFinite(b.lng) && b.uniqueDevices > 0)
      .map(b => ({ lat: b.lat, lng: b.lng, intensity: b.uniqueDevices }));

    // Asymmetric heat fields — see SIGMA_CAT_KM / SIGMA_BASE_KM comment.
    // catHeatAt shadows the outer `heatAt` within lift-mode scope so the
    // legacy mode (no baseline) keeps the wider σ_KM = 12.
    const catHeatAt = (lat: number, lng: number): number => {
      let heat = 0;
      for (const q of hotSources) {
        const d = haversineKm(lat, lng, q.lat, q.lng);
        if (d > CUTOFF_CAT_KM) continue;
        heat += q.intensity * Math.exp(-(d * d) / twoSigmaCatSq);
      }
      return heat;
    };
    const baseHeatAt = (lat: number, lng: number): number => {
      let h = 0;
      for (const q of baseSources) {
        const d = haversineKm(lat, lng, q.lat, q.lng);
        if (d > CUTOFF_BASE_KM) continue;
        h += q.intensity * Math.exp(-(d * d) / twoSigmaBaseSq);
      }
      return h;
    };

    const liftedHot = hotSources.map(z => {
      const heatCat = catHeatAt(z.lat, z.lng);
      const heatBase = baseHeatAt(z.lat, z.lng);
      return { ...z, heatCat, heatBase };
    });

    // Regularize the baseline so tiny-base zips don't blow up the ratio.
    // ε = 1% of max baseline heat: small enough to preserve real signal,
    // large enough to clip outliers from sparse rural zips.
    const maxBase = Math.max(1, ...liftedHot.map(z => z.heatBase));
    const eps = maxBase * 0.01;
    // Sample-size gate — see BASE_GATE_FRAC. Zips with baseline heat
    // below this are too sparse to estimate lift reliably.
    const baseGate = maxBase * BASE_GATE_FRAC;

    const liftFor = (heatCat: number, heatBase: number): number =>
      heatCat <= 0 ? 0 : heatCat / (heatBase + eps);

    // Median lift across measured zips that ALSO pass the baseline gate
    // (so the anchor isn't dragged by sparse rural noise).
    const measuredLifts = liftedHot
      .filter(z => z.heatBase >= baseGate)
      .map(z => liftFor(z.heatCat, z.heatBase))
      .filter(l => l > 0)
      .sort((a, b) => a - b);
    const medianLift = measuredLifts.length > 0
      ? measuredLifts[Math.floor(measuredLifts.length / 2)]
      : 1;

    const scoreFor = (lift: number): number => {
      if (lift <= 0 || !Number.isFinite(lift) || medianLift <= 0) return 0;
      const s = 50 + 20 * Math.log2(lift / medianLift);
      return Math.max(0, Math.min(100, Math.round(s)));
    };

    let gatedCount = 0;
    const hotCps: AffinityByZip[] = liftedHot.map(z => {
      // Keep the row (device counts are real data the user paid for)
      // but score 0 so the map doesn't paint sparse-baseline noise.
      const gated = z.heatBase < baseGate;
      if (gated) gatedCount++;
      return {
        zipCode: z.zipCode, city: z.city, country: z.country,
        lat: z.lat, lng: z.lng,
        uniqueDevices: z.uniqueDevices, totalVisitDays: z.totalVisitDays,
        avgDwellMinutes: Math.round(z.avgDwell * 10) / 10,
        avgFrequency: Math.round(z.avgFreq * 100) / 100,
        affinityIndex: gated ? 0 : scoreFor(liftFor(z.heatCat, z.heatBase)),
      };
    });

    // Adjacent polygons — only paint zips that have real category heat
    // AND pass the baseline gate AND aren't dramatically under-indexed.
    // With σ_cat=3km the cat heat dies off in ~9km so adjacents are
    // naturally limited to the immediate surroundings of real hotspots.
    type AdjacentRaw = {
      zipCode: string; city: string; country: string;
      lat: number; lng: number; lift: number;
    };
    const adjacentRaw: AdjacentRaw[] = [];
    if (country && hotSources.length > 0) {
      try {
        const geojsonPath = path.join(process.cwd(), 'data', 'geojson', `${country}.geojson`);
        if (fs.existsSync(geojsonPath)) {
          const geojson = JSON.parse(fs.readFileSync(geojsonPath, 'utf-8'));
          const hotSet = new Set(hotSources.map(h => h.zipCode));
          for (const feature of geojson.features) {
            const cp = feature.properties?.postal_code || feature.properties?.postcode || '';
            if (!cp || hotSet.has(cp)) continue;

            let cLat = 0, cLng = 0;
            if (feature.properties?.latitude && feature.properties?.longitude) {
              cLat = parseFloat(feature.properties.latitude);
              cLng = parseFloat(feature.properties.longitude);
            } else if (feature.bbox) {
              cLat = (feature.bbox[1] + feature.bbox[3]) / 2;
              cLng = (feature.bbox[0] + feature.bbox[2]) / 2;
            } else {
              const coords = feature.geometry?.coordinates;
              if (!coords) continue;
              const flat = feature.geometry.type === 'MultiPolygon' ? coords[0][0] : coords[0];
              if (!flat?.length) continue;
              const sumLat = flat.reduce((s: number, c: number[]) => s + c[1], 0);
              const sumLng = flat.reduce((s: number, c: number[]) => s + c[0], 0);
              cLat = sumLat / flat.length;
              cLng = sumLng / flat.length;
            }
            if (!cLat || !cLng) continue;

            const heatCat = catHeatAt(cLat, cLng);
            if (heatCat <= 0) continue;
            const heatBase = baseHeatAt(cLat, cLng);
            if (heatBase < baseGate) continue;          // sample-size gate
            const lift = liftFor(heatCat, heatBase);
            // Drop adjacents under half the median lift — they'd just
            // clutter the map at 20-30 score and dilute the signal.
            if (lift < medianLift * 0.5) continue;

            const city = feature.properties?.city || feature.properties?.estado || '';
            adjacentRaw.push({ zipCode: cp, city, country, lat: cLat, lng: cLng, lift });
          }
        }
      } catch (e: any) {
        console.warn(`[AFFINITY-LIFT] Adjacent compute failed: ${e.message}`);
      }
    }

    const adjacentCps: AffinityByZip[] = adjacentRaw.map(a => ({
      zipCode: a.zipCode, city: a.city, country: a.country,
      lat: a.lat, lng: a.lng,
      uniqueDevices: 0, totalVisitDays: 0,
      avgDwellMinutes: 0, avgFrequency: 0,
      affinityIndex: scoreFor(a.lift),
    }));

    console.log(`[AFFINITY-LIFT] ${subject}: ${hotCps.length} hot (${gatedCount} gated→0) + ${adjacentCps.length} adjacent (σ_cat=${SIGMA_CAT_KM}km, σ_base=${SIGMA_BASE_KM}km, medianLift=${medianLift.toExponential(2)})`);

    const allCps = [...hotCps, ...adjacentCps];
    allCps.sort((a, b) => b.affinityIndex - a.affinityIndex);

    return {
      analyzedAt: new Date().toISOString(),
      subject,
      byZipCode: allCps,
    };
  }

  // ── Step 3: Adjacent polygons from country GeoJSON ─────────────
  type AdjacentRaw = {
    zipCode: string; city: string; country: string;
    lat: number; lng: number; heat: number;
  };
  const adjacentRaw: AdjacentRaw[] = [];
  if (country && hotSources.length > 0) {
    try {
      const geojsonPath = path.join(process.cwd(), 'data', 'geojson', `${country}.geojson`);
      if (fs.existsSync(geojsonPath)) {
        const geojson = JSON.parse(fs.readFileSync(geojsonPath, 'utf-8'));
        const hotSet = new Set(hotSources.map(h => h.zipCode));
        for (const feature of geojson.features) {
          const cp = feature.properties?.postal_code || feature.properties?.postcode || '';
          if (!cp || hotSet.has(cp)) continue;

          let cLat = 0, cLng = 0;
          if (feature.properties?.latitude && feature.properties?.longitude) {
            cLat = parseFloat(feature.properties.latitude);
            cLng = parseFloat(feature.properties.longitude);
          } else if (feature.bbox) {
            cLat = (feature.bbox[1] + feature.bbox[3]) / 2;
            cLng = (feature.bbox[0] + feature.bbox[2]) / 2;
          } else {
            const coords = feature.geometry?.coordinates;
            if (!coords) continue;
            const flat = feature.geometry.type === 'MultiPolygon' ? coords[0][0] : coords[0];
            if (!flat?.length) continue;
            const sumLat = flat.reduce((s: number, c: number[]) => s + c[1], 0);
            const sumLng = flat.reduce((s: number, c: number[]) => s + c[0], 0);
            cLat = sumLat / flat.length;
            cLng = sumLng / flat.length;
          }
          if (!cLat || !cLng) continue;

          const heat = heatAt(cLat, cLng);
          if (heat <= 0) continue;
          const city = feature.properties?.city || feature.properties?.estado || '';
          adjacentRaw.push({ zipCode: cp, city, country, lat: cLat, lng: cLng, heat });
        }
      }
    } catch (e: any) {
      console.warn(`[AFFINITY] Adjacent compute failed: ${e.message}`);
    }
  }

  // ── Step 4: Global log normalize to 0..100 ───────────────────────
  const maxHeat = Math.max(
    1,
    ...hotWithHeat.map(z => z.heat),
    ...adjacentRaw.map(a => a.heat),
  );
  const logMax = Math.log(maxHeat + 1);
  const scoreFor = (heat: number): number =>
    heat > 0 ? Math.max(1, Math.round(100 * Math.log(heat + 1) / logMax)) : 0;

  const hotCps: AffinityByZip[] = hotWithHeat.map(z => ({
    zipCode: z.zipCode, city: z.city, country: z.country,
    lat: z.lat, lng: z.lng,
    uniqueDevices: z.uniqueDevices, totalVisitDays: z.totalVisitDays,
    avgDwellMinutes: Math.round(z.avgDwell * 10) / 10,
    avgFrequency: Math.round(z.avgFreq * 100) / 100,
    affinityIndex: scoreFor(z.heat),
  }));

  const adjacentCps: AffinityByZip[] = adjacentRaw.map(a => ({
    zipCode: a.zipCode, city: a.city, country: a.country,
    lat: a.lat, lng: a.lng,
    uniqueDevices: 0, totalVisitDays: 0,
    avgDwellMinutes: 0, avgFrequency: 0,
    affinityIndex: scoreFor(a.heat),
  }));

  console.log(`[AFFINITY] ${subject}: ${hotCps.length} hot + ${adjacentCps.length} adjacent (maxHeat=${maxHeat.toFixed(0)})`);

  const allCps = [...hotCps, ...adjacentCps];
  allCps.sort((a, b) => b.affinityIndex - a.affinityIndex);

  return {
    analyzedAt: new Date().toISOString(),
    subject,
    byZipCode: allCps,
  };
}

/** Serialize an AffinityReport as the canonical 8-column CSV used by
 *  the dataset & megajob affinity downloads. Sorted desc by affinity. */
export function affinityReportToCsv(report: AffinityReport): string {
  const header = 'zip_code,city,country,affinity_index,avg_dwell_min,avg_frequency,unique_devices,total_visit_days';
  const esc = (s: string) => /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  const rows = report.byZipCode.map(r =>
    [esc(r.zipCode), esc(r.city || ''), esc(r.country || ''), r.affinityIndex,
     r.avgDwellMinutes, r.avgFrequency, r.uniqueDevices, r.totalVisitDays].join(',')
  );
  return [header, ...rows].join('\n');
}
