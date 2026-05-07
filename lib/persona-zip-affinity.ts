/**
 * ZIP affinity score per source — two complementary lenses:
 *
 *   - Population-weighted (CPG-style index vs baseline). Visitors per
 *     resident, normalized to the source's expected baseline. Best for
 *     understanding which ZIPs DISPROPORTIONATELY engage with the POI
 *     set, regardless of size. Reads as: 100 = proportional to size,
 *     150 = over-indexes 1.5×, <50 = under-indexes.
 *
 *   - Volume-only (the original 0..100 share-of-top metric). Best for
 *     coverage / media-buying when raw audience size is what matters.
 *
 * Both are computed; the UI toggles between them. Computed in Node
 * from the home_zip column of the persona feature vector — no extra
 * Athena query.
 */

import type {
  DeviceFeatures,
  SourceZipAffinity,
  ZipAffinityRow,
} from './persona-types';

/** Skip ZIPs below this population — small enough that a few visitors give noisy >>100 indices. */
const MIN_POPULATION_FOR_POP_INDEX = 200;

/** Cap the population-weighted index. Beyond 300 the differences stop being meaningful (saturated affinity). */
const POP_INDEX_CAP = 300;

export interface PopulationLookup {
  /**
   * Map from sourceId → (postalCode → population). Built by the caller
   * from per-country NSE uploads. Missing entries fall back to volume-only.
   */
  bySource: Map<string, Map<string, number>>;
}

/**
 * Group features by (source_megajob_id, home_zip), count uniques,
 * and produce a ZIP affinity table per source. Both
 * volume-only and population-weighted indices are computed.
 *
 * Devices without a resolved home_zip are dropped.
 */
export function computeZipAffinityPerSource(
  features: DeviceFeatures[],
  sourceLabels: Record<string, string>,
  populationLookup?: PopulationLookup,
  sourceCountries?: Record<string, string>
): SourceZipAffinity[] {
  // source → (zip → unique adIds)
  const bySource = new Map<string, Map<string, Set<string>>>();
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
    let ids = zipMap.get(zip);
    if (!ids) {
      ids = new Set();
      zipMap.set(zip, ids);
    }
    ids.add(f.ad_id);
  }

  const out: SourceZipAffinity[] = [];
  for (const [sourceId, zipMap] of bySource.entries()) {
    let totalDevices = 0;
    const counts: { zip: string; count: number }[] = [];
    for (const [zip, ids] of zipMap.entries()) {
      counts.push({ zip, count: ids.size });
      totalDevices += ids.size;
    }
    if (counts.length === 0) continue;

    // Volume-only: normalize to top ZIP. Same formula as before.
    counts.sort((a, b) => b.count - a.count);
    const topCount = counts[0].count || 1;

    // Population lookup for this source (may be missing if no NSE uploaded
    // for the country). Compute pop_share denominator from pops we DO have
    // — using the pop-uploaded ZIPs as the universe of comparison.
    const popMap = populationLookup?.bySource.get(sourceId);
    let totalPopWithVisitors = 0;
    if (popMap) {
      for (const c of counts) {
        const pop = popMap.get(c.zip) || 0;
        if (pop >= MIN_POPULATION_FOR_POP_INDEX) totalPopWithVisitors += pop;
      }
    }

    let hasPopulation = false;
    const rows: ZipAffinityRow[] = counts.map((c) => {
      const pop = popMap?.get(c.zip) || 0;
      const volumeIndex = Math.max(0, Math.min(100, Math.round((c.count / topCount) * 100)));
      let popIndex = volumeIndex; // fallback when no population
      let noPop = true;
      if (pop >= MIN_POPULATION_FOR_POP_INDEX && totalPopWithVisitors > 0 && totalDevices > 0) {
        // CPG-style: (visitor_share) / (pop_share) × 100
        const visitorShare = c.count / totalDevices;
        const popShare = pop / totalPopWithVisitors;
        const raw = popShare > 0 ? (visitorShare / popShare) * 100 : 0;
        popIndex = Math.max(0, Math.min(POP_INDEX_CAP, Math.round(raw)));
        noPop = false;
        hasPopulation = true;
      }
      return {
        zip: c.zip,
        count: c.count,
        population: pop,
        affinityIndexPop: popIndex,
        affinityIndexVolume: volumeIndex,
        noPopulation: noPop,
      };
    });

    // Sort desc by population-weighted index (the new default).
    rows.sort((a, b) => b.affinityIndexPop - a.affinityIndexPop);

    out.push({
      sourceId,
      sourceLabel: sourceLabels[sourceId] || sourceId.slice(0, 8),
      country: sourceCountries?.[sourceId] || '',
      rows,
      totalDevicesWithZip: totalDevices,
      hasPopulation,
    });
  }

  // Stable order: largest source first.
  out.sort((a, b) => b.totalDevicesWithZip - a.totalDevicesWithZip);
  return out;
}
