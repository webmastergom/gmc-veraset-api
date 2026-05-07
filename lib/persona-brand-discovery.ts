/**
 * Auto-discover brand names from a collection of POI names.
 *
 * Useful when there's no hand-crafted lookup table for the dataset (e.g.
 * "Auto Dealerships Madrid" with KIA / Ford / Volkswagen / Toyota / …
 * or "Competencia Six Guadalajara" with Six / AT&T / Telcel / …). The
 * pre-built BRAND_RULES list covers fast-food chains and won't help here.
 *
 * Strategy:
 *   1. Tokenize each POI name (1-grams + 2-grams).
 *   2. Strip diacritics + lowercase + filter out stopwords (cities, articles,
 *      generic POI words like "centro", "plaza", "store", "sucursal").
 *   3. Count token frequency across the POI collection.
 *   4. Keep tokens that occur in ≥ MIN_OCCURRENCES POIs as "brand candidates".
 *   5. For each POI, assign the *most specific* candidate it contains
 *      (lowest count among the candidates that match) — that's the
 *      strongest signal of a brand vs a city/generic word.
 *
 * Properly tuned, this produces clean results for typical 200-2000-POI
 * studies. For specialty cases the user can still override per-POI by
 * setting `properties.brand` (or `chain`) in the GeoJSON.
 */

// ── Stopwords ────────────────────────────────────────────────────────

const STOPWORDS = new Set([
  // Articles / prepositions (ES + EN)
  'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas', 'de', 'del',
  'al', 'en', 'y', 'o', 'u', 'a', 'an', 'the', 'and', 'or', 'of',
  'in', 'on', 'at', 'to', 'for', 'by', 'with', 'sin',
  // Address words
  'centro', 'plaza', 'avenida', 'av', 'calle', 'c/', 'colonia', 'col',
  'norte', 'sur', 'este', 'oeste', 'central', 'esquina', 'no',
  'street', 'street.', 'st', 'road', 'rd', 'blvd', 'boulevard',
  'mall', 'shopping', 'center', 'centre', 'park', 'parque',
  'piso', 'local', 'edif', 'edificio', 'plaza', 'galeria', 'galería',
  'station', 'estación', 'estacion', 'paseo', 'rambla', 'ronda',
  // Generic POI / business words
  'store', 'tienda', 'sucursal', 'restaurant', 'restaurante',
  'cafe', 'cafetería', 'cafeteria', 'shop', 'outlet', 'office',
  'oficina', 'branch', 'agencia', 'agency', 'point', 'punto',
  'concesionario', 'dealership', 'dealer', 'showroom', 'service',
  'autorizado', 'official', 'oficial', 'main', 'principal', 'nuevo',
  'nueva', 'expo', 'fair', 'feria',
  // Direction / size adjectives
  'big', 'small', 'mini', 'mega', 'super', 'plus', 'pro', 'vip',
  'premium', 'gold', 'silver', 'platinum',
  // Brand-decorative noise
  'company', 'co', 'ltd', 'sa', 'sl', 'srl', 'inc', 'corp', 'group',
  'grupo', 'holding',
]);

const MIN_TOKEN_LENGTH = 2;
const MIN_OCCURRENCES_FACTOR = 0.005; // ≥ 0.5% of POIs OR…
const MIN_OCCURRENCES_FLOOR = 3;       // …at least 3, whichever is higher
const MAX_CANDIDATES = 200;            // cap candidate brands so the lookup is tight

// ── Public types ─────────────────────────────────────────────────────

export interface PoiNameLookup {
  poiId: string;
  name: string;
  /** Optional explicit override from the source GeoJSON. */
  brandOverride?: string;
}

export interface BrandDiscoveryResult {
  /** poi_id → brand slug (or 'other' if nothing matched). */
  poiToBrand: Map<string, string>;
  /** Candidate list (sorted by frequency desc). For UI / debugging. */
  candidates: Array<{ brand: string; count: number; exemplar: string }>;
  /** How many POIs ended up as 'other' (no candidate matched). */
  unmatched: number;
  /** Source of resolution per POI (for debugging). */
  source: { override: number; discovered: number; other: number };
}

// ── Implementation ───────────────────────────────────────────────────

function normalize(s: string): string {
  return s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().trim();
}

/**
 * Split a POI name into 1-gram tokens. We don't bother with 2-grams here —
 * for the typical "Brand X City Y Address Z" pattern, the brand is one
 * word and 1-grams are sufficient. 2-grams would help with multi-word
 * brands like "Five Guys" but those are rare in POI catalogues and the
 * BRAND_RULES fallback covers them anyway.
 */
function tokenize(name: string): string[] {
  return normalize(name)
    .split(/[\s\-,/.()&·|]+/)
    .filter((t) => t.length >= MIN_TOKEN_LENGTH && !/^\d+$/.test(t) && !STOPWORDS.has(t));
}

/**
 * Slugify a token for use as a brand identifier. We don't do anything fancy
 * — the token is already lowercase ASCII after normalize().
 */
function slug(token: string): string {
  return token.replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
}

export function discoverBrands(pois: PoiNameLookup[]): BrandDiscoveryResult {
  const poiTokens = new Map<string, Set<string>>();
  for (const p of pois) {
    poiTokens.set(p.poiId, new Set(tokenize(p.name || '')));
  }

  // Frequency
  const tokenFreq = new Map<string, { count: number; exemplar: string }>();
  for (const p of pois) {
    const tokens = poiTokens.get(p.poiId)!;
    for (const tok of tokens) {
      const ex = tokenFreq.get(tok);
      if (ex) {
        ex.count++;
        // Keep the shortest example name — tends to be the cleanest brand-only POI
        if (p.name && p.name.length < ex.exemplar.length) ex.exemplar = p.name;
      } else {
        tokenFreq.set(tok, { count: 1, exemplar: p.name || tok });
      }
    }
  }

  // Brand candidates threshold
  const minOcc = Math.max(MIN_OCCURRENCES_FLOOR, Math.ceil(pois.length * MIN_OCCURRENCES_FACTOR));
  const candidates = Array.from(tokenFreq.entries())
    .filter(([_, v]) => v.count >= minOcc)
    .map(([tok, v]) => ({ brand: slug(tok), token: tok, count: v.count, exemplar: v.exemplar }))
    .sort((a, b) => b.count - a.count)
    .slice(0, MAX_CANDIDATES);

  const candidateByToken = new Map(candidates.map((c) => [c.token, c]));

  // Assign each POI to most-specific candidate it contains.
  // "Most specific" = lowest count (still ≥ minOcc) — likely the brand
  // rather than a city or filler word.
  const poiToBrand = new Map<string, string>();
  let overrideCount = 0;
  let discoveredCount = 0;
  let otherCount = 0;
  for (const p of pois) {
    if (p.brandOverride && p.brandOverride.trim()) {
      poiToBrand.set(p.poiId, slug(normalize(p.brandOverride)) || 'other');
      overrideCount++;
      continue;
    }
    const tokens = poiTokens.get(p.poiId)!;
    let bestBrand = 'other';
    let bestSpecificity = Infinity;
    for (const tok of tokens) {
      const cand = candidateByToken.get(tok);
      if (!cand) continue;
      if (cand.count < bestSpecificity) {
        bestSpecificity = cand.count;
        bestBrand = cand.brand;
      }
    }
    poiToBrand.set(p.poiId, bestBrand);
    if (bestBrand === 'other') otherCount++;
    else discoveredCount++;
  }

  return {
    poiToBrand,
    candidates: candidates.map((c) => ({ brand: c.brand, count: c.count, exemplar: c.exemplar })),
    unmatched: otherCount,
    source: { override: overrideCount, discovered: discoveredCount, other: otherCount },
  };
}
