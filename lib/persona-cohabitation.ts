/**
 * Brand cohabitation matrix — for cross-dataset analysis.
 *
 * For every pair of brands present in the feature vectors, compute:
 *   - Jaccard = |A ∩ B| / |A ∪ B|
 *   - share_A_to_B = |A ∩ B| / |A|
 *   - share_B_to_A = |A ∩ B| / |B|
 *
 * "Visitor of brand X" = device with brand_visits[X] > 0.
 *
 * Useful insights:
 *   - "47% of BK visitors also went to McDonald's" → share_BK_to_MCD
 *   - Brand-pair Jaccard reveals tightest cross-shopping pairs.
 *   - Allows ranking BK's "fiercest competitor" vs "least overlapping".
 */

import {
  type DeviceFeatures,
  type CohabitationEntry,
  type CohabitationReport,
} from './persona-types';

export function computeCohabitation(
  features: DeviceFeatures[],
  _megaJobIds: string[]
): CohabitationReport {
  // Build per-brand visitor sets.
  const brandToVisitors = new Map<string, Set<string>>();
  for (const f of features) {
    for (const [brand, count] of Object.entries(f.brand_visits)) {
      if (count <= 0) continue;
      if (!brandToVisitors.has(brand)) brandToVisitors.set(brand, new Set());
      brandToVisitors.get(brand)!.add(f.ad_id);
    }
  }

  // Filter brands with very few visitors (noise).
  const MIN_BRAND_VISITORS = 50;
  const brands = Array.from(brandToVisitors.entries())
    .filter(([_, set]) => set.size >= MIN_BRAND_VISITORS)
    .sort((a, b) => b[1].size - a[1].size)
    .map(([brand]) => brand);

  const entries: CohabitationEntry[] = [];
  for (let i = 0; i < brands.length; i++) {
    for (let j = i + 1; j < brands.length; j++) {
      const a = brands[i];
      const b = brands[j];
      const setA = brandToVisitors.get(a)!;
      const setB = brandToVisitors.get(b)!;
      // Intersect via smaller set
      const [smaller, larger] = setA.size <= setB.size ? [setA, setB] : [setB, setA];
      let intersect = 0;
      for (const id of smaller) if (larger.has(id)) intersect++;
      const union = setA.size + setB.size - intersect;
      const jaccard = union > 0 ? intersect / union : 0;
      entries.push({
        brandA: a,
        brandB: b,
        jaccard,
        shareAtoB: setA.size > 0 ? intersect / setA.size : 0,
        shareBtoA: setB.size > 0 ? intersect / setB.size : 0,
        intersectionDevices: intersect,
        brandADevices: setA.size,
        brandBDevices: setB.size,
      });
    }
  }
  entries.sort((a, b) => b.jaccard - a.jaccard);
  return { entries, brands };
}
