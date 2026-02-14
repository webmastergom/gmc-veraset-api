/**
 * Affinity Index Laboratory types.
 *
 * The laboratory computes affinity indices (0–100) by postal code based on
 * visits to different POI categories.
 *
 * The affinity index combines three normalized signals:
 *
 *   1. Visit Concentration (40%):
 *      How much a postal code over-indexes on a category vs national average.
 *      Raw ratio capped at 5x, then scaled 0–100.
 *
 *   2. Frequency Score (35%):
 *      Average visits per unique device — measures repeat behavior / loyalty.
 *      log2(avgFreq) normalized to 0–100 with cap at 16 visits/device.
 *
 *   3. Time-of-Day Relevance (25%):
 *      Optional boost if visits align with configured time windows
 *      (e.g. gym visits 6–9am score higher than gym visits 2am).
 *      If no time windows configured, evenly distributed.
 *
 *   Final Affinity = 0.40 * concentration + 0.35 * frequency + 0.25 * temporal
 *   Rounded to integer 0–100.
 *
 *   0  = no affinity (no visits or far below average)
 *   50 = average affinity
 *   100 = maximum affinity (strong concentration + high frequency + relevant timing)
 */

// ── POI categories (27 from Overture/GMC) ──────────────────────────────
export const POI_CATEGORIES = [
  'restaurant', 'hotel', 'clothing_store', 'bar', 'supermarket',
  'gym', 'cafe', 'pharmacy', 'gas_station', 'fast_food_restaurant',
  'doctor', 'school', 'park', 'train_station', 'hospital',
  'parking', 'convenience_store', 'library', 'cinema', 'shopping_center',
  'amusement_park', 'department_store', 'airport', 'bus_station',
  'bowling_alley', 'arcade', 'metro_station',
] as const;

export type PoiCategory = typeof POI_CATEGORIES[number];

// Human-readable category labels
export const CATEGORY_LABELS: Record<PoiCategory, string> = {
  restaurant: 'Restaurants',
  hotel: 'Hotels',
  clothing_store: 'Clothing Stores',
  bar: 'Bars & Pubs',
  supermarket: 'Supermarkets',
  gym: 'Gyms & Fitness',
  cafe: 'Cafés',
  pharmacy: 'Pharmacies',
  gas_station: 'Gas Stations',
  fast_food_restaurant: 'Fast Food',
  doctor: 'Doctors',
  school: 'Schools',
  park: 'Parks',
  train_station: 'Train Stations',
  hospital: 'Hospitals',
  parking: 'Parking',
  convenience_store: 'Convenience Stores',
  library: 'Libraries',
  cinema: 'Cinemas',
  shopping_center: 'Shopping Centers',
  amusement_park: 'Amusement Parks',
  department_store: 'Department Stores',
  airport: 'Airports',
  bus_station: 'Bus Stations',
  bowling_alley: 'Bowling Alleys',
  arcade: 'Arcades',
  metro_station: 'Metro Stations',
};

// ── Category groups for UX ─────────────────────────────────────────────
export interface CategoryGroup {
  label: string;
  icon: string;
  categories: PoiCategory[];
  color: string; // tailwind color class
}

export const CATEGORY_GROUPS: Record<string, CategoryGroup> = {
  dining: {
    label: 'Dining & Nightlife',
    icon: 'UtensilsCrossed',
    color: 'text-orange-400',
    categories: ['restaurant', 'bar', 'cafe', 'fast_food_restaurant'],
  },
  retail: {
    label: 'Retail & Shopping',
    icon: 'ShoppingBag',
    color: 'text-pink-400',
    categories: ['clothing_store', 'supermarket', 'convenience_store', 'department_store', 'shopping_center'],
  },
  health: {
    label: 'Health & Wellness',
    icon: 'Heart',
    color: 'text-red-400',
    categories: ['pharmacy', 'doctor', 'hospital', 'gym'],
  },
  transport: {
    label: 'Transport',
    icon: 'Train',
    color: 'text-blue-400',
    categories: ['gas_station', 'train_station', 'airport', 'bus_station', 'metro_station', 'parking'],
  },
  leisure: {
    label: 'Leisure & Culture',
    icon: 'Palette',
    color: 'text-purple-400',
    categories: ['park', 'cinema', 'amusement_park', 'bowling_alley', 'arcade', 'library'],
  },
  education: {
    label: 'Education',
    icon: 'GraduationCap',
    color: 'text-emerald-400',
    categories: ['school'],
  },
  accommodation: {
    label: 'Accommodation',
    icon: 'Hotel',
    color: 'text-amber-400',
    categories: ['hotel'],
  },
};

// Reverse lookup: category → group key
export function getCategoryGroup(cat: PoiCategory): string {
  for (const [key, g] of Object.entries(CATEGORY_GROUPS)) {
    if (g.categories.includes(cat)) return key;
  }
  return 'other';
}

// ── Supported countries ────────────────────────────────────────────────
export interface LabCountry {
  code: string;        // ISO 2-letter (FR, DE, ES)
  name: string;
  flag: string;        // emoji
  datasetName: string; // Athena dataset name — must match S3 prefix
  poiParquetKey: string;  // S3 key for POI parquet
  totalPois: number;
  cities: string[];    // Top cities
}

export const LAB_COUNTRIES: LabCountry[] = [
  {
    code: 'FR',
    name: 'France',
    flag: '🇫🇷',
    datasetName: 'france_gmc_25k',
    poiParquetKey: 'pois_gmc/france_pois_gmc.parquet',
    totalPois: 645989,
    cities: ['Paris', 'Marseille', 'Lyon', 'Toulouse', 'Nice', 'Nantes', 'Strasbourg', 'Montpellier', 'Bordeaux', 'Lille'],
  },
  {
    code: 'DE',
    name: 'Germany',
    flag: '🇩🇪',
    datasetName: 'germany_gmc_25k',
    poiParquetKey: 'pois_gmc/germany_pois_gmc.parquet',
    totalPois: 465054,
    cities: ['Berlin', 'Hamburg', 'Munich', 'Cologne', 'Frankfurt', 'Stuttgart', 'Düsseldorf', 'Dortmund', 'Essen', 'Leipzig'],
  },
  {
    code: 'ES',
    name: 'Spain',
    flag: '🇪🇸',
    datasetName: 'spain_gmc_25k',
    poiParquetKey: 'pois_gmc/spain_pois_gmc.parquet',
    totalPois: 346514,
    cities: ['Madrid', 'Barcelona', 'Valencia', 'Seville', 'Zaragoza', 'Málaga', 'Murcia', 'Palma', 'Las Palmas', 'Bilbao'],
  },
];

// ── Filters ────────────────────────────────────────────────────────────
export interface LabFilters {
  country: string;            // ISO code
  categories: PoiCategory[];  // selected categories (empty = all 27)
  dateFrom?: string;
  dateTo?: string;
  cities?: string[];          // filter to specific cities
  minVisits?: number;         // min visits per postal code (noise filter, default 5)
  timeWindows?: TimeWindow[];
}

export interface TimeWindow {
  id: string;
  label: string;         // e.g. "Morning commute"
  hourFrom: number;      // 0-23 UTC
  hourTo: number;        // 0-23 UTC
  weight: number;        // 0-1 weight for temporal score
}

// ── Affinity weights ───────────────────────────────────────────────────
export const AFFINITY_WEIGHTS = {
  concentration: 0.40,
  frequency: 0.35,
  temporal: 0.25,
} as const;

export const CONCENTRATION_CAP = 5;  // ratio capped at 5x national avg
export const FREQUENCY_CAP = 16;     // cap at 16 visits/device
export const MIN_VISITS_DEFAULT = 5; // min visits to compute affinity

// ── Results ────────────────────────────────────────────────────────────
export interface AffinityRecord {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  category: PoiCategory;
  visits: number;            // device-days visiting this category from this zipcode
  uniqueDevices: number;     // unique devices from this zipcode
  frequency: number;         // visits / uniqueDevices
  totalVisits: number;       // total device-days from this zipcode (all categories)
  concentrationScore: number;  // 0-100
  frequencyScore: number;     // 0-100
  temporalScore: number;      // 0-100
  affinityIndex: number;      // 0-100 composite
}

export interface ZipcodeProfile {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  totalVisits: number;
  uniqueDevices: number;
  affinities: Partial<Record<PoiCategory, number>>;  // category → affinity 0-100
  topCategory: PoiCategory;
  topAffinity: number;
  dominantGroup: string;  // category group with highest average affinity
}

export interface LabAnalysisResult {
  country: string;
  countryName: string;
  dataset: string;
  analyzedAt: string;
  filters: LabFilters;
  records: AffinityRecord[];
  profiles: ZipcodeProfile[];
  stats: LabStats;
}

export interface LabStats {
  totalDeviceDays: number;
  totalUniqueDevices: number;
  totalPostalCodes: number;
  categoriesAnalyzed: number;
  avgAffinityIndex: number;
  categoryBreakdown: CategoryStat[];
  topHotspots: AffinityHotspot[];
}

export interface CategoryStat {
  category: PoiCategory;
  label: string;
  group: string;
  visits: number;
  percentOfTotal: number;
  postalCodesWithVisits: number;
  avgAffinity: number;
  maxAffinity: number;
  maxAffinityZipcode: string;
  maxAffinityCity: string;
}

export interface AffinityHotspot {
  zipcode: string;
  city: string;
  category: PoiCategory;
  categoryLabel: string;
  affinityIndex: number;
  visits: number;
  uniqueDevices: number;
}

// ── Progress callback ──────────────────────────────────────────────────
export type LabProgressCallback = (progress: {
  step: 'initializing' | 'loading_pois' | 'querying_visits' | 'geocoding' | 'computing_affinity' | 'aggregating' | 'completed' | 'error';
  percent: number;
  message: string;
  detail?: string;
}) => void;

// ── CSV export headers ─────────────────────────────────────────────────
export const LAB_CSV_HEADERS = [
  'postal_code', 'city', 'province', 'region', 'category',
  'visits', 'unique_devices', 'frequency',
  'concentration_score', 'frequency_score', 'temporal_score',
  'affinity_index',
] as const;
