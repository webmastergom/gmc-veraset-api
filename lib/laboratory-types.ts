/**
 * Affinity Index Laboratory types.
 *
 * The laboratory is a workspace for building "recipes" — combinations of
 * POI categories × time windows × dwell time thresholds — that produce:
 *
 *   1. Segments of ad_ids matching the criteria
 *   2. Affinity indices (0–100) by postal code of origin
 *
 * Data flow:
 *   Movement dataset (from synced job)
 *     → pings near real POIs (from pois_gmc parquets, spatial join)
 *       → filter by category + time + dwell
 *         → segment of ad_ids + origin geocode → affinity by zipcode
 *
 * Affinity index 0–100 combines:
 *   - Concentration (40%): zipcode over-index vs national average
 *   - Frequency (35%): avg visits per device (repeat behavior)
 *   - Dwell depth (25%): avg dwell time vs category median
 */

// ── POI categories (27 real categories from pois_gmc parquets) ─────────
export const POI_CATEGORIES = [
  'restaurant', 'hotel', 'clothing_store', 'bar', 'supermarket',
  'gym', 'cafe', 'pharmacy', 'gas_station', 'fast_food_restaurant',
  'doctor', 'school', 'park', 'train_station', 'hospital',
  'parking', 'convenience_store', 'library', 'cinema', 'shopping_center',
  'amusement_park', 'department_store', 'airport', 'bus_station',
  'bowling_alley', 'arcade', 'metro_station',
] as const;

export type PoiCategory = typeof POI_CATEGORIES[number];

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
  color: string;
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

export function getCategoryGroup(cat: PoiCategory): string {
  for (const [key, g] of Object.entries(CATEGORY_GROUPS)) {
    if (g.categories.includes(cat)) return key;
  }
  return 'other';
}

// ── Recipe: a single category criterion ────────────────────────────────
export interface RecipeStep {
  id: string;
  categories: PoiCategory[];         // one or more categories (OR within step)
  timeWindow?: {
    hourFrom: number;                 // 0-23
    hourTo: number;                   // 0-23
  };
  minDwellMinutes?: number;           // minimum dwell time in minutes
  maxDwellMinutes?: number;           // maximum dwell time
  minFrequency?: number;              // minimum visits in date range
}

// ── Recipe: the full experiment ────────────────────────────────────────
export interface Recipe {
  id: string;
  name: string;
  steps: RecipeStep[];
  logic: 'AND' | 'OR';               // how steps combine
  ordered: boolean;                   // if AND, must visits happen in step order?
}

// ── Lab configuration (wizard output) ──────────────────────────────────
export interface LabConfig {
  datasetId: string;                  // S3 folder name (e.g. "job-fa899c22")
  datasetName: string;                // human-readable name
  jobId: string;                      // Veraset job ID for poiMapping lookup
  country: string;                    // ISO 2-letter code for geocoding + POI parquet
  dateFrom?: string;
  dateTo?: string;
  recipe: Recipe;
  minVisitsPerZipcode: number;        // noise filter (default 5)
  spatialJoinRadiusMeters: number;    // max distance ping↔real POI (default 200m)
}

// ── Analysis results ───────────────────────────────────────────────────

/** A single device that matched the recipe */
export interface SegmentDevice {
  adId: string;
  matchedSteps: number;               // how many recipe steps matched
  totalVisits: number;                 // total POI visits
  avgDwellMinutes: number;             // average dwell time across visits
  categories: PoiCategory[];           // categories visited
}

/** Affinity record: one postal code × one category */
export interface AffinityRecord {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  category: PoiCategory;
  visits: number;
  uniqueDevices: number;
  avgDwellMinutes: number;
  frequency: number;                   // visits / uniqueDevices
  totalVisitsFromZipcode: number;
  concentrationScore: number;          // 0-100
  frequencyScore: number;              // 0-100
  dwellScore: number;                  // 0-100
  affinityIndex: number;               // 0-100 composite
}

/** Aggregated profile for a postal code */
export interface ZipcodeProfile {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  totalVisits: number;
  uniqueDevices: number;
  avgDwellMinutes: number;
  affinities: Partial<Record<PoiCategory, number>>;
  topCategory: PoiCategory;
  topAffinity: number;
  dominantGroup: string;
}

/** Full result of a laboratory analysis */
export interface LabAnalysisResult {
  config: LabConfig;
  analyzedAt: string;
  // Segment
  segment: {
    totalDevices: number;
    devices: SegmentDevice[];           // top N for display (full list exported via CSV)
  };
  // Affinity
  records: AffinityRecord[];
  profiles: ZipcodeProfile[];
  stats: LabStats;
}

export interface LabStats {
  totalPingsAnalyzed: number;
  totalDevicesInDataset: number;
  segmentSize: number;                  // devices matching recipe
  segmentPercent: number;               // % of total
  totalPostalCodes: number;
  categoriesAnalyzed: number;
  avgAffinityIndex: number;
  avgDwellMinutes: number;
  categoryBreakdown: CategoryStat[];
  topHotspots: AffinityHotspot[];
}

export interface CategoryStat {
  category: PoiCategory;
  label: string;
  group: string;
  visits: number;
  uniqueDevices: number;
  avgDwellMinutes: number;
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
  avgDwellMinutes: number;
}

// ── Affinity weights ───────────────────────────────────────────────────
export const AFFINITY_WEIGHTS = {
  concentration: 0.40,
  frequency: 0.35,
  dwell: 0.25,
} as const;

export const CONCENTRATION_CAP = 5;
export const FREQUENCY_CAP = 16;
export const DWELL_CAP_MINUTES = 120;   // 2h cap for dwell score normalization
export const MIN_VISITS_DEFAULT = 5;
export const SPATIAL_JOIN_RADIUS_DEFAULT = 200; // meters

// ── Progress ───────────────────────────────────────────────────────────
export type LabProgressCallback = (progress: {
  step: 'initializing' | 'loading_pois' | 'spatial_join' | 'querying_visits' | 'computing_dwell' | 'geocoding' | 'computing_affinity' | 'building_segments' | 'completed' | 'error';
  percent: number;
  message: string;
  detail?: string;
}) => void;
