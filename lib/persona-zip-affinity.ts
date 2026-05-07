/**
 * ZIP affinity score per source — the relative attractiveness of each
 * residential ZIP toward a POI set.
 *
 * Computed from the persona feature vector (already in memory after the
 * clustering phase) — no extra Athena query needed. Each device has a
 * `home_zip` (mode of geo_fields['zipcode'] during night hours, with
 * reverse-geocoded fallback) and a `source_megajob_id`. We group by
 * (source, zip), count distinct devices, then normalize 0..100 per
 * source against its own top ZIP.
 *
 * Why per-source normalization (not global): each megajob has its own
 * geographic footprint and base size. A ZIP that contributes 1000
 * visitors to BK might mean little for Auto Dealerships of 50 POIs
 * total. Normalizing inside the source produces an "affinity" reading
 * that's comparable across ZIPs within the same study.
 */

import type {
  DeviceFeatures,
  SourceZipAffinity,
  ZipAffinityRow,
} from './persona-types';

/**
 * Group features by (source_megajob_id, home_zip), count uniques,
 * and produce a ZIP affinity table per source.
 *
 * Devices without a resolved home_zip are dropped — they can't be
 * attributed to any geography. The resulting per-source list is sorted
 * by affinityIndex desc.
 */
export function computeZipAffinityPerSource(
  features: DeviceFeatures[],
  sourceLabels: Record<string, string>
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
    counts.sort((a, b) => b.count - a.count);
    const top = counts[0].count || 1;
    const rows: ZipAffinityRow[] = counts.map((c) => ({
      zip: c.zip,
      count: c.count,
      affinityIndex: Math.max(0, Math.min(100, Math.round((c.count / top) * 100))),
    }));
    out.push({
      sourceId,
      sourceLabel: sourceLabels[sourceId] || sourceId.slice(0, 8),
      rows,
      totalDevicesWithZip: totalDevices,
    });
  }

  // Stable order: largest source first.
  out.sort((a, b) => b.totalDevicesWithZip - a.totalDevicesWithZip);
  return out;
}
