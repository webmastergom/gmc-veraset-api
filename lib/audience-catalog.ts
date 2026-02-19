/**
 * Audience Agent — Catalog of predefined audience definitions.
 *
 * Each audience maps industry-relevant segments to POI categories
 * with optional filters (time windows, dwell thresholds, frequency).
 *
 * The agent runs these definitions against mobility datasets to produce
 * ready-to-distribute segments of ad_ids with affinity indices.
 */

import type { PoiCategory, LabConfig, RecipeStep, Recipe } from './laboratory-types';
import { SPATIAL_JOIN_RADIUS_DEFAULT, MIN_VISITS_DEFAULT } from './laboratory-types';

// ── Types ────────────────────────────────────────────────────────────────

export interface AudienceDefinition {
  id: string;
  name: string;
  description: string;
  icon: string;                        // Lucide icon name
  group: AudienceGroup;
  color: string;                       // Tailwind text color class
  categories: PoiCategory[];
  timeWindow?: { hourFrom: number; hourTo: number };
  minDwellMinutes?: number;
  maxDwellMinutes?: number;
  minFrequency?: number;
}

export type AudienceGroup =
  | 'entertainment'
  | 'fitness'
  | 'education'
  | 'food'
  | 'transport'
  | 'automotive'
  | 'retail'
  | 'wellness'
  | 'families'
  | 'tech'
  | 'culture'
  | 'corporate';

export const AUDIENCE_GROUP_LABELS: Record<AudienceGroup, string> = {
  entertainment: 'Entertainment',
  fitness: 'Health & Fitness',
  education: 'Education',
  food: 'Food & Beverage',
  transport: 'Transport',
  automotive: 'Automotive',
  retail: 'Retail & Luxury',
  wellness: 'Wellness',
  families: 'Families & Pets',
  tech: 'Tech & Gaming',
  culture: 'Culture & Tourism',
  corporate: 'Corporate',
};

export interface AudienceRunResult {
  audienceId: string;
  datasetId: string;
  country: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  startedAt?: string;
  completedAt?: string;
  error?: string;
  // Stats summary
  segmentSize?: number;
  segmentPercent?: number;
  totalDevicesInDataset?: number;
  totalPostalCodes?: number;
  avgAffinityIndex?: number;
  avgDwellMinutes?: number;
  topHotspots?: Array<{
    zipcode: string;
    city: string;
    category: string;
    affinityIndex: number;
    visits: number;
  }>;
  // S3 paths
  s3ResultPath?: string;
  s3SegmentCsvPath?: string;
}

export interface AudienceBatchState {
  id: string;
  datasetId: string;
  datasetName: string;
  country: string;
  startedAt: string;
  completedAt?: string;
  audiences: Record<string, AudienceRunResult>;
}

// ── Audience Catalog ─────────────────────────────────────────────────────

export const AUDIENCE_CATALOG: AudienceDefinition[] = [
  // ── Entertainment ──────────────────────────────────────────────────────
  {
    id: 'moviegoers',
    name: 'Moviegoers',
    description: 'Cinema, drive-in, outdoor movies, film festival visitors',
    icon: 'Film',
    group: 'entertainment',
    color: 'text-rose-400',
    categories: ['cinema', 'drive_in_theater', 'outdoor_movies', 'film_festival'],
  },
  {
    id: 'sports_event_attendees',
    name: 'Sports Event Attendees',
    description: 'Stadium, arena, and sports venue visitors',
    icon: 'Trophy',
    group: 'entertainment',
    color: 'text-lime-400',
    categories: ['stadium_arena', 'sports_and_recreation_venue'],
  },
  {
    id: 'nightlife',
    name: 'Nightlife',
    description: 'Bars, clubs, pubs, karaoke, comedy clubs, casinos',
    icon: 'Wine',
    group: 'entertainment',
    color: 'text-purple-400',
    categories: ['bar', 'pub', 'dance_club', 'comedy_club', 'karaoke', 'casino'],
    timeWindow: { hourFrom: 18, hourTo: 6 },
  },
  {
    id: 'live_music_fans',
    name: 'Live Music Fans',
    description: 'Music venues and theatre attendees',
    icon: 'Music',
    group: 'entertainment',
    color: 'text-violet-400',
    categories: ['music_venue', 'theatre'],
  },

  // ── Health & Fitness ───────────────────────────────────────────────────
  {
    id: 'gym_visitors',
    name: 'Gym & Fitness',
    description: 'Gym, yoga, pilates, martial arts, climbing visitors',
    icon: 'Dumbbell',
    group: 'fitness',
    color: 'text-lime-400',
    categories: ['gym', 'yoga_studio', 'pilates_studio', 'martial_arts_club', 'rock_climbing_gym'],
    minDwellMinutes: 15,
  },
  {
    id: 'golfers',
    name: 'Golfers',
    description: 'Golf course visitors',
    icon: 'Flag',
    group: 'fitness',
    color: 'text-green-400',
    categories: ['golf_course'],
    minDwellMinutes: 30,
  },
  {
    id: 'swimmers',
    name: 'Swimmers',
    description: 'Swimming pool visitors',
    icon: 'Waves',
    group: 'fitness',
    color: 'text-cyan-400',
    categories: ['swimming_pool'],
    minDwellMinutes: 15,
  },

  // ── Education ──────────────────────────────────────────────────────────
  {
    id: 'students',
    name: 'Students',
    description: 'School, university, and college visitors',
    icon: 'GraduationCap',
    group: 'education',
    color: 'text-cyan-400',
    categories: ['school', 'university', 'college'],
    minDwellMinutes: 30,
  },

  // ── Food & Beverage ────────────────────────────────────────────────────
  {
    id: 'fast_food_visitors',
    name: 'Fast Food Visitors',
    description: 'Fast food restaurant visitors',
    icon: 'Utensils',
    group: 'food',
    color: 'text-orange-400',
    categories: ['fast_food_restaurant'],
  },
  {
    id: 'fine_diners',
    name: 'Fine Diners',
    description: 'Fine dining, wine bars, cocktail bars',
    icon: 'Wine',
    group: 'food',
    color: 'text-amber-400',
    categories: ['fine_dining', 'wine_bar', 'cocktail_bar', 'champagne_bar'],
    minDwellMinutes: 45,
  },
  {
    id: 'coffee_lovers',
    name: 'Coffee Lovers',
    description: 'Coffee shop and cafe regulars',
    icon: 'Coffee',
    group: 'food',
    color: 'text-amber-600',
    categories: ['coffee_shop', 'cafe'],
    minFrequency: 3,
  },

  // ── Transport ──────────────────────────────────────────────────────────
  {
    id: 'metro_users',
    name: 'Metro / Subway Users',
    description: 'Metro and subway station visitors',
    icon: 'Train',
    group: 'transport',
    color: 'text-blue-400',
    categories: ['subway_station', 'metro_station'],
  },
  {
    id: 'bus_users',
    name: 'Bus Users',
    description: 'Bus station visitors',
    icon: 'Bus',
    group: 'transport',
    color: 'text-blue-300',
    categories: ['bus_station'],
  },
  {
    id: 'train_commuters',
    name: 'Train Commuters',
    description: 'Train station visitors',
    icon: 'TrainFront',
    group: 'transport',
    color: 'text-indigo-400',
    categories: ['train_station'],
  },
  {
    id: 'air_travelers',
    name: 'Air Travelers',
    description: 'Airport visitors',
    icon: 'Plane',
    group: 'transport',
    color: 'text-sky-400',
    categories: ['airport'],
    minDwellMinutes: 30,
  },

  // ── Automotive ─────────────────────────────────────────────────────────
  {
    id: 'car_shoppers',
    name: 'Car Shoppers',
    description: 'Car dealer and used car dealer visitors',
    icon: 'Car',
    group: 'automotive',
    color: 'text-slate-400',
    categories: ['car_dealer', 'used_car_dealer'],
  },
  {
    id: 'ev_adopters',
    name: 'EV Adopters',
    description: 'EV charging station users',
    icon: 'Zap',
    group: 'automotive',
    color: 'text-emerald-400',
    categories: ['ev_charging_station'],
  },

  // ── Retail & Luxury ────────────────────────────────────────────────────
  {
    id: 'luxury_shoppers',
    name: 'Luxury Shoppers',
    description: 'Jewelry, watches, designer clothing, antiques',
    icon: 'Crown',
    group: 'retail',
    color: 'text-yellow-400',
    categories: ['jewelry_store', 'watch_store', 'designer_clothing', 'antique_store'],
  },
  {
    id: 'home_improvers',
    name: 'Home Improvers',
    description: 'Home improvement, furniture, garden center visitors',
    icon: 'Home',
    group: 'retail',
    color: 'text-teal-400',
    categories: ['home_improvement_store', 'furniture_store', 'garden_center'],
  },
  {
    id: 'supermarket_shoppers',
    name: 'Supermarket Shoppers',
    description: 'Supermarket and convenience store visitors',
    icon: 'ShoppingCart',
    group: 'retail',
    color: 'text-green-400',
    categories: ['supermarket', 'convenience_store'],
  },

  // ── Wellness ───────────────────────────────────────────────────────────
  {
    id: 'spa_wellness',
    name: 'Spa & Wellness',
    description: 'Spa, massage, medical spa visitors',
    icon: 'Sparkles',
    group: 'wellness',
    color: 'text-fuchsia-400',
    categories: ['spa', 'massage', 'medical_spa', 'day_spa', 'health_spa'],
  },

  // ── Families & Pets ────────────────────────────────────────────────────
  {
    id: 'pet_owners',
    name: 'Pet Owners',
    description: 'Pet store, grooming, boarding, dog park visitors',
    icon: 'PawPrint',
    group: 'families',
    color: 'text-orange-300',
    categories: ['pet_store', 'pet_grooming', 'pet_boarding', 'dog_park'],
  },
  {
    id: 'family_entertainment',
    name: 'Family Entertainment',
    description: 'Zoos, aquariums, amusement parks, botanical gardens',
    icon: 'Tent',
    group: 'families',
    color: 'text-green-400',
    categories: ['zoo', 'aquarium', 'amusement_park', 'botanical_garden'],
  },

  // ── Tech & Gaming ─────────────────────────────────────────────────────
  {
    id: 'gamers',
    name: 'Gamers',
    description: 'Arcades, esports, VR centers, internet cafes',
    icon: 'Gamepad2',
    group: 'tech',
    color: 'text-violet-400',
    categories: ['arcade', 'esports_league', 'esports_team', 'virtual_reality_center', 'internet_cafe', 'video_game_store'],
  },

  // ── Culture & Tourism ──────────────────────────────────────────────────
  {
    id: 'museum_goers',
    name: 'Museum & Culture',
    description: 'Museums, castles, landmarks, monuments',
    icon: 'Landmark',
    group: 'culture',
    color: 'text-amber-400',
    categories: ['museum', 'landmark_and_historical_building', 'castle', 'monument'],
  },

  // ── Corporate ──────────────────────────────────────────────────────────
  {
    id: 'coworkers',
    name: 'Coworking Users',
    description: 'Coworking space visitors',
    icon: 'Briefcase',
    group: 'corporate',
    color: 'text-neutral-400',
    categories: ['coworking_space'],
    minDwellMinutes: 60,
  },
];

// ── Conversion helper ────────────────────────────────────────────────────

/**
 * Convert an AudienceDefinition into a LabConfig ready for analyzeLaboratory().
 */
export function audienceToLabConfig(
  audience: AudienceDefinition,
  dataset: { id: string; name: string; jobId: string },
  country: string,
  dateFrom?: string,
  dateTo?: string,
): LabConfig {
  const step: RecipeStep = {
    id: `audience_${audience.id}`,
    categories: audience.categories,
    timeWindow: audience.timeWindow,
    minDwellMinutes: audience.minDwellMinutes,
    maxDwellMinutes: audience.maxDwellMinutes,
    minFrequency: audience.minFrequency,
  };

  const recipe: Recipe = {
    id: `audience_recipe_${audience.id}`,
    name: audience.name,
    steps: [step],
    logic: 'OR',
    ordered: false,
  };

  return {
    datasetId: dataset.id,
    datasetName: dataset.name,
    jobId: dataset.jobId,
    country,
    dateFrom,
    dateTo,
    recipe,
    minVisitsPerZipcode: MIN_VISITS_DEFAULT,
    spatialJoinRadiusMeters: SPATIAL_JOIN_RADIUS_DEFAULT,
  };
}

/**
 * Collect all unique POI categories across a set of audience definitions.
 */
export function collectAllCategories(audiences: AudienceDefinition[]): PoiCategory[] {
  const all = new Set<PoiCategory>();
  for (const a of audiences) {
    for (const c of a.categories) all.add(c);
  }
  return Array.from(all);
}
