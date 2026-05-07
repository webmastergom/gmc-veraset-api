/**
 * Persona insights generator — produces 8-10 quotable one-liners from
 * the feature vector + clusters + RFM + cohabitation.
 *
 * Keep each insight: pure function over inputs, NaN-safe, copy is short.
 */

import {
  type DeviceFeatures,
  type PersonaCluster,
  type RfmReport,
  type CohabitationReport,
  type PersonaInsight,
  type PersonaRunConfig,
} from './persona-types';

interface InsightInput {
  features: DeviceFeatures[];
  personas: PersonaCluster[];
  rfm: RfmReport;
  cohabitation?: CohabitationReport;
  config: PersonaRunConfig;
}

function pct(v: number, total: number): string {
  if (!total) return '0%';
  return `${((v / total) * 100).toFixed(1)}%`;
}

function fmt(n: number): string {
  if (!Number.isFinite(n)) return '0';
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return n.toFixed(0);
}

export function generateInsights(input: InsightInput): PersonaInsight[] {
  const { features, personas, rfm, cohabitation, config } = input;
  const out: PersonaInsight[] = [];
  const total = features.length;
  if (total === 0) return out;

  // 1. Top persona by size
  const topPersona = [...personas].sort((a, b) => b.deviceCount - a.deviceCount)[0];
  if (topPersona) {
    out.push({
      id: 'top_persona',
      title: 'Largest persona',
      value: `${topPersona.name} (${pct(topPersona.deviceCount, total)})`,
      detail: topPersona.description || `${fmt(topPersona.deviceCount)} devices fall in this cluster.`,
      severity: 'highlight',
    });
  }

  // 2. Champions cell impact
  const champions = rfm.cells.find((c) => c.label === 'Champions');
  if (champions && champions.deviceCount > 0) {
    out.push({
      id: 'rfm_champions',
      title: 'Champions concentration',
      value: pct(champions.deviceCount, total),
      detail: `${fmt(champions.deviceCount)} devices form the top RFM cell — recent + frequent + high-dwell. Median ${champions.medianFrequency} visits, ${champions.medianMonetaryMin.toFixed(0)} min total dwell.`,
      severity: 'positive',
    });
  }

  // 3. At-risk + Lost
  const lostLike = rfm.cells.filter((c) => c.label === 'At Risk' || c.label === 'Lost' || c.label === 'Hibernating');
  const lostCount = lostLike.reduce((s, c) => s + c.deviceCount, 0);
  if (lostCount > 0) {
    out.push({
      id: 'rfm_at_risk',
      title: 'Lapsing audience',
      value: pct(lostCount, total),
      detail: `${fmt(lostCount)} devices haven't visited recently — re-engagement priority for retention campaigns.`,
      severity: 'warning',
    });
  }

  // 4. Top brand cohabitation pair
  if (cohabitation && cohabitation.entries.length > 0) {
    const top = cohabitation.entries[0];
    out.push({
      id: 'top_cohab',
      title: 'Tightest brand overlap',
      value: `${top.brandA.replace(/_/g, ' ')} ↔ ${top.brandB.replace(/_/g, ' ')} (Jaccard ${top.jaccard.toFixed(2)})`,
      detail: `${pct(top.intersectionDevices, top.brandADevices)} of ${top.brandA.replace(/_/g, ' ')} visitors also visit ${top.brandB.replace(/_/g, ' ')}.`,
      severity: 'highlight',
    });
  }

  // 5. Brand-loyal devices share
  const loyal = features.filter((f) => f.brand_loyalty_hhi >= 0.7).length;
  if (loyal > 0) {
    out.push({
      id: 'loyalty',
      title: 'Brand-loyal segment',
      value: pct(loyal, total),
      detail: `${fmt(loyal)} devices show high single-brand loyalty (HHI ≥ 0.7) — prime targets for brand-promotion uplift.`,
      severity: 'positive',
    });
  }

  // 6. Weekend warriors
  const weekendHeavy = features.filter((f) => f.weekend_share >= 0.6 && f.total_visits >= 3).length;
  if (weekendHeavy > 0) {
    out.push({
      id: 'weekend_warriors',
      title: 'Weekend warriors',
      value: pct(weekendHeavy, total),
      detail: `${fmt(weekendHeavy)} regulars whose visits are >60% on weekends — match for Friday-Sunday combos.`,
      severity: 'neutral',
    });
  }

  // 7. Late-night cluster
  const lateNight = features.filter((f) => f.night_share >= 0.4).length;
  if (lateNight > 0) {
    out.push({
      id: 'late_night',
      title: 'Late-night audience',
      value: pct(lateNight, total),
      detail: `${fmt(lateNight)} devices visit after 10pm or before 5am — drive-thru + delivery sweet spot.`,
      severity: 'neutral',
    });
  }

  // 8. Hyperlocal
  const hyperlocal = features.filter((f) => f.gyration_km > 0 && f.gyration_km < 2).length;
  if (hyperlocal > 0) {
    out.push({
      id: 'hyperlocal',
      title: 'Hyperlocal segment',
      value: pct(hyperlocal, total),
      detail: `${fmt(hyperlocal)} devices barely leave a 2-km radius — hyperlocal OOH and store-radius campaigns.`,
      severity: 'neutral',
    });
  }

  // 9. Long-distance / road-trippers
  const roadTrip = features.filter((f) => f.gyration_km > 50).length;
  if (roadTrip > 0) {
    out.push({
      id: 'road_trippers',
      title: 'Road-trippers',
      value: pct(roadTrip, total),
      detail: `${fmt(roadTrip)} devices show >50 km gyration — likely highway / travel commuters.`,
      severity: 'neutral',
    });
  }

  // 10. High-quality tier coverage
  const highQ = features.filter((f) => f.tier_high_quality).length;
  if (highQ > 0) {
    out.push({
      id: 'high_quality',
      title: 'High-quality tier',
      value: pct(highQ, total),
      detail: `${fmt(highQ)} devices pass the GPS-only + tight-circle quality gate — highest-confidence audience.`,
      severity: 'positive',
    });
  }

  // 11. Cross-dataset exclusivity (only when 2+ megajobs)
  if (config.megaJobIds.length >= 2 && cohabitation && cohabitation.entries.length > 0) {
    // Heuristic: a device whose brand_loyalty_hhi is very high AND has only 1 brand → exclusive.
    const exclusive = features.filter((f) => {
      const brands = Object.keys(f.brand_visits || {});
      return brands.length === 1 && f.brand_loyalty_hhi > 0.95;
    }).length;
    if (exclusive > 0) {
      out.push({
        id: 'exclusive',
        title: 'Brand-exclusive devices',
        value: pct(exclusive, total),
        detail: `${fmt(exclusive)} devices visit a single brand exclusively — pure loyalist segment for retention spend.`,
        severity: 'positive',
      });
    }
  }

  return out;
}
