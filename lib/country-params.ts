/**
 * Per-country mobility calibration parameters.
 *
 * Single source of truth for the constants the methodology layer needs
 * to translate raw MAID counts into Unique Users and into a defensible
 * Resident-audience ceiling. Documented in METHODOLOGY.md §3.5.
 *
 * Three numbers per country, all derived from public sources cited
 * inline. Add a new country by appending a row + its citations; the
 * downstream consumers (audience-estimator, audience-counter) read from
 * here and require no other code change.
 *
 *   subscriber_penetration       Unique mobile subscribers ÷ total
 *                                population. GSMA Intelligence /
 *                                DataReportal. A "subscriber" is a
 *                                person with ≥1 SIM, NOT a SIM count
 *                                (which is `mobile connections`).
 *
 *   devices_per_subscriber       Multi-device factor κ⁻¹. How many
 *                                MAID-emitting devices the average
 *                                subscriber owns (smartphone + tablet
 *                                + dual-phone for work). The MAID is
 *                                per-device (IDFA on iOS, AAID on
 *                                Android), not per-SIM, so a multi-SIM
 *                                user with one phone still counts as
 *                                one device here.
 *
 *   kappa_md                     1 / devices_per_subscriber. Used to
 *                                convert Unique MAIDs → Unique Users
 *                                (METHODOLOGY §3.2). The METHODOLOGY
 *                                doc previously used a single global
 *                                κ_md = 0.80; the per-country numbers
 *                                here replace that.
 *
 * Derived `maid_ceiling_M` is the upper-bound MAID universe in the
 * country (pop × sub_pen × dev_per_sub, in millions). If our raw
 * COUNT(DISTINCT ad_id) for a country exceeds this ceiling, we have
 * a bot / IDFA-churn / methodology bug — surface as a sanity flag
 * before showing audience figures to a client.
 */

export interface CountryParams {
  iso: string;                    // ISO-3166-1 alpha-2
  name: string;
  population_M: number;           // millions (most recent reliable estimate)
  subscriber_penetration: number; // unique mobile subscribers ÷ population, in [0,1]
  devices_per_subscriber: number; // multi-device factor κ⁻¹
  kappa_md: number;               // = 1 / devices_per_subscriber
  maid_ceiling_M: number;         // = population_M × subscriber_penetration × devices_per_subscriber
  sources: string[];              // public URLs / citations
  calibration_debt: string[];     // what's extrapolated vs measured — be honest
}

/**
 * Per-country table. Keep entries sorted by iso for diffability.
 *
 * Adding a country:
 *   1. Pick the most recent DataReportal "Digital YYYY: <Country>" report.
 *   2. Pull `mobile_connections` (the headline) and `population`.
 *   3. Find `subscriber_penetration` — GSMA Mobile Economy report for
 *      the region. Direct country numbers when available (e.g. MX in
 *      Mobile Economy LatAm 2025 p.6). Otherwise use the regional
 *      aggregate and note it under calibration_debt.
 *   4. Estimate `devices_per_subscriber` from tablet penetration
 *      (Comscore EU5 / Statista) + dual-phone / work-phone share for
 *      the market (typical developed-market business mobile rate ≈
 *      25-30%, developing ≈ 5-10%).
 *   5. Compute kappa_md and maid_ceiling_M.
 *   6. Cite everything in `sources`. List extrapolations in
 *      `calibration_debt`.
 */
export const COUNTRY_PARAMS: Record<string, CountryParams> = {
  MX: {
    iso: 'MX',
    name: 'Mexico',
    population_M: 131,
    subscriber_penetration: 0.73,
    devices_per_subscriber: 1.15, // 1.00 phone + 0.10 tablet + 0.05 dual-phone
    kappa_md: 0.87,
    maid_ceiling_M: 110,
    sources: [
      'GSMA Mobile Economy Latin America 2025, p.6 (Mexico panel) — subscriber penetration 73% direct: https://www.gsma.com/solutions-and-impact/connectivity-for-good/mobile-economy/wp-content/uploads/2025/05/GSMA_Latam_ME2025_R_Web.pdf',
      'DataReportal Digital 2025 Mexico (127M connections, 131M pop, 96.5% conn/cap): https://datareportal.com/reports/digital-2025-mexico',
      'Tablet share extrapolated from Comscore EU5 with Latin America income adjustment.',
    ],
    calibration_debt: [
      'tablet share extrapolated, not directly measured for MX',
      'dual-phone / work-phone share is regional-typical, not market-specific',
    ],
  },
  ES: {
    iso: 'ES',
    name: 'Spain',
    population_M: 47.9,
    subscriber_penetration: 0.85,
    devices_per_subscriber: 1.27, // 1.00 phone + 0.17 tablet + 0.10 dual-phone
    kappa_md: 0.79,
    maid_ceiling_M: 52,
    sources: [
      'DataReportal Digital 2025 Spain (56.1M connections, 47.9M pop, 117% conn/cap): https://datareportal.com/reports/digital-2025-spain',
      'Comscore EU5: 16.9% tablet penetration among Spanish smartphone owners: https://www.comscore.com/Insights/Infographics/15-5-percent-of-European-Smartphone-Owners-Have-a-Tablet',
      'GSMA Mobile Economy Europe 2025: regional subscriber penetration estimate ~85% (89% forecast 2030): https://www.gsma.com/solutions-and-impact/connectivity-for-good/mobile-economy/europe/',
    ],
    calibration_debt: [
      'subscriber penetration is European aggregate (~85%), not Spain-specific',
    ],
  },
  FR: {
    iso: 'FR',
    name: 'France',
    population_M: 66.6,
    subscriber_penetration: 0.85,
    devices_per_subscriber: 1.35, // 1.00 phone + 0.15 tablet + 0.20 dual-phone (TGV / business travel)
    kappa_md: 0.74,
    maid_ceiling_M: 76,
    sources: [
      'DataReportal Digital 2025 France (74.5M connections, 66.6M pop, 112% conn/cap): https://datareportal.com/reports/digital-2025-france',
      'Statista France smartphone penetration forecast 97% by 2029 (stable from 2024): https://www.statista.com/topics/5707/smartphone-users-in-france/',
      'GSMA Mobile Economy Europe 2025: regional subscriber penetration estimate ~85%.',
    ],
    calibration_debt: [
      'tablet share interpolated between DE (12.8%) and ES (16.9%) — Comscore EU5 does not split FR out',
      'dual-phone share estimated from developed-market business-mobile typical, not measured for FR',
    ],
  },
  DE: {
    iso: 'DE',
    name: 'Germany',
    population_M: 83.9,
    subscriber_penetration: 0.85,
    devices_per_subscriber: 1.43, // 1.00 phone + 0.13 tablet + 0.30 dual-phone (high biz-mobile)
    kappa_md: 0.70,
    maid_ceiling_M: 102,
    sources: [
      'DataReportal Digital 2026 Germany (110M connections, 83.9M pop, 131% conn/cap): https://datareportal.com/reports/digital-2026-germany',
      'Comscore EU5: 12.8% tablet penetration among German smartphone owners.',
      'GSMA Mobile Economy Europe 2025: regional subscriber penetration estimate ~85%.',
    ],
    calibration_debt: [
      'subscriber penetration is European aggregate (~85%)',
      'dual-phone 30% estimate reflects high DE business-mobile prevalence — not measured directly',
    ],
  },
  UK: {
    iso: 'UK',
    name: 'United Kingdom',
    population_M: 69.4,
    subscriber_penetration: 0.85,
    devices_per_subscriber: 1.45, // 1.00 phone + 0.18 tablet + 0.27 dual-phone
    kappa_md: 0.69,
    maid_ceiling_M: 86,
    sources: [
      'DataReportal Digital 2025 UK (88.4M connections, 69.4M pop, 127% conn/cap): https://datareportal.com/reports/digital-2025-united-kingdom',
      'Statista UK smartphone penetration 94% of mobile users: https://www.statista.com/topics/4540/mobile-communications-in-the-united-kingdom-uk/',
      'Comscore EU5: 17.7% tablet penetration — highest in EU5.',
      'GSMA Mobile Economy Europe 2025: regional subscriber penetration estimate ~85%.',
    ],
    calibration_debt: [
      'subscriber penetration is European aggregate (~85%)',
      'Ofcom Mobile Matters 2025 (https://www.ofcom.org.uk/phones-and-broadband/mobile-phones/mobile-matters) likely has UK-direct numbers — pending fetch (PDF currently 403 from CLI)',
    ],
  },
};

/**
 * Global fallback for countries not yet calibrated. Uses the
 * METHODOLOGY.md §3.5 global-average defaults: 80% subscriber
 * penetration, 1.25 devices per subscriber (Bankmycell / GSMA
 * global ratio).
 */
export const GLOBAL_FALLBACK_PARAMS: CountryParams = {
  iso: 'XX',
  name: 'Global fallback',
  population_M: 0,
  subscriber_penetration: 0.80,
  devices_per_subscriber: 1.25,
  kappa_md: 0.80,
  maid_ceiling_M: 0,
  sources: [
    'Bankmycell 2026 — 1.25 mobile connections per smartphone-owning adult globally: https://www.bankmycell.com/blog/how-many-phones-are-in-the-world',
    'METHODOLOGY.md §3.5 — derivation of κ_md = 1/1.25 = 0.80 as global baseline.',
  ],
  calibration_debt: [
    'used only when the country is unknown or not in COUNTRY_PARAMS',
  ],
};

/**
 * Look up country params with graceful fallback.
 *
 * Accepts ISO-3166-1 alpha-2 codes ("MX", "es", "UK") or null /
 * undefined. Returns the fallback for unknown / null inputs so callers
 * don't need to branch.
 */
export function getCountryParams(iso: string | null | undefined): CountryParams {
  if (!iso) return GLOBAL_FALLBACK_PARAMS;
  const key = iso.trim().toUpperCase();
  return COUNTRY_PARAMS[key] || GLOBAL_FALLBACK_PARAMS;
}

/**
 * Convenience: list of supported ISO codes. Useful for UI dropdowns
 * and tests that want to assert "all calibrated countries have X".
 */
export function listCalibratedCountries(): string[] {
  return Object.keys(COUNTRY_PARAMS).sort();
}
