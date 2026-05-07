/**
 * Persona brand resolution — POI name → canonical brand slug.
 *
 * Used by the persona feature CTAS to compute brand_visits / brand_loyalty.
 * Comparison is case-insensitive and ignores accents.
 *
 * Add brands here as needed; no other code change is required because the
 * SQL builder reads the table at query-build time.
 */

export interface BrandRule {
  brand: string;
  /** Exact match (case-insensitive, after normalization). Use for short, ambiguous names. */
  exact?: string[];
  /** Substring match (case-insensitive, after normalization). Most common. */
  contains?: string[];
}

/**
 * Canonical brand slugs are short, lowercase, snake-case.
 * Keep the list focused on Spain fast-food + key chains visible in our data.
 */
export const BRAND_RULES: BrandRule[] = [
  { brand: 'burger_king', contains: ['burger king', 'burger-king'] },
  { brand: 'mcdonalds', contains: ["mcdonald", 'mcdonalds', 'mc donald'] },
  { brand: 'kfc', contains: ['kfc', 'kentucky fried chicken'] },
  { brand: 'telepizza', contains: ['telepizza'] },
  { brand: 'dominos', contains: ['domino'] },
  { brand: 'pizza_hut', contains: ['pizza hut', 'pizzahut'] },
  { brand: 'goiko', contains: ['goiko'] },
  { brand: 'five_guys', contains: ['five guys'] },
  { brand: 'subway', contains: ['subway'] },
  { brand: 'taco_bell', contains: ['taco bell', 'tacobell'] },
  { brand: 'starbucks', contains: ['starbucks'] },
  { brand: 'tim_hortons', contains: ['tim horton'] },
  { brand: 'foster_hollywood', contains: ['foster', 'fosters hollywood'] },
  { brand: 'tgb', exact: ['tgb', 'the good burger'] },
  { brand: 'vips', contains: ['vips'] },
  { brand: '100_montaditos', contains: ['100 montaditos', 'cien montaditos'] },
  { brand: 'tagliatella', contains: ['tagliatella'] },
];

/** Strip accents + lowercase for robust matching. */
function normalize(s: string): string {
  return s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
}

export function resolveBrand(poiName: string | null | undefined): string {
  if (!poiName) return 'other';
  const n = normalize(poiName);
  for (const rule of BRAND_RULES) {
    if (rule.exact?.some((e) => n === normalize(e))) return rule.brand;
    if (rule.contains?.some((c) => n.includes(normalize(c)))) return rule.brand;
  }
  return 'other';
}

/**
 * Build a SQL CASE expression that maps a `poi_name` column to a brand slug.
 * Used inside the feature CTAS so brand resolution happens server-side
 * (avoids shipping POI names through the feature CSV download).
 *
 * NOTE: SQL pattern matching is much more limited than JS — we use LOWER()
 * + LIKE patterns. We can't do diacritic stripping in pure SQL, so for
 * rare accent cases we add explicit unaccented variants below.
 */
export function buildBrandCaseSql(poiNameCol: string): string {
  const branches: string[] = [];
  for (const rule of BRAND_RULES) {
    const conditions: string[] = [];
    if (rule.exact) {
      for (const e of rule.exact) {
        conditions.push(`LOWER(${poiNameCol}) = '${e.replace(/'/g, "''").toLowerCase()}'`);
      }
    }
    if (rule.contains) {
      for (const c of rule.contains) {
        conditions.push(`LOWER(${poiNameCol}) LIKE '%${c.replace(/'/g, "''").toLowerCase()}%'`);
      }
    }
    if (conditions.length > 0) {
      branches.push(`WHEN ${conditions.join(' OR ')} THEN '${rule.brand}'`);
    }
  }
  return `CASE\n  ${branches.join('\n  ')}\n  ELSE 'other'\nEND`;
}

/** All canonical brand slugs (for matrix axes etc.). */
export function allBrands(): string[] {
  return BRAND_RULES.map((r) => r.brand).concat(['other']);
}
