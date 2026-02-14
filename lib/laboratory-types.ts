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

// ── POI categories from pois_gmc parquets ──────────────────────────────
// Organized by signal tier:
//   T1 = High affinity signal (core GMC verticals)
//   T2 = Lifestyle / contextual signal
//   T3 = Mobility & urban context
//   T4 = Special-vertical signal
export const POI_CATEGORIES = [
  // ── TIER 1: High affinity signal ──────────────────────────────────
  // Retail / Commerce
  'clothing_store', 'department_store', 'discount_store', 'shopping_mall',
  'outlet_store', 'thrift_store', 'gift_shop', 'convenience_store',
  'general_merchandise_store',
  // Food & Beverage
  'restaurant', 'fast_food_restaurant', 'fine_dining', 'cafe', 'pizza',
  'sushi', 'bar', 'pub', 'brewery', 'winery', 'coffee_shop', 'bakery',
  'juice_bar', 'food_truck',
  // Automotive
  'car_dealer', 'used_car_dealer', 'motorcycle_dealer', 'gas_station',
  'ev_charging_station', 'car_wash', 'auto_body_shop', 'tire_dealer_and_repair',
  // Beauty & Personal Care
  'beauty_salon', 'hair_salon', 'nail_salon', 'barber', 'spa', 'massage',
  'tattoo_and_piercing', 'skin_care', 'tanning_salon',
  // Healthcare
  'hospital', 'clinic', 'doctor', 'dentist', 'pharmacy', 'urgent_care',
  'mental_health_service', 'physical_therapy', 'optometrist', 'chiropractor',
  'veterinarian',
  // Financial Services
  'bank', 'atm', 'credit_union', 'financial_advisor', 'insurance_agency',
  'tax_advisor', 'investment_company', 'accounting_firm',
  // Sports & Fitness
  'gym', 'yoga_studio', 'pilates_studio', 'swimming_pool', 'tennis_court',
  'golf_course', 'sports_and_recreation_venue', 'martial_arts_club',
  'rock_climbing_gym',
  // Entertainment
  'cinema', 'comedy_club', 'music_venue', 'casino', 'arcade', 'dance_club',
  'karaoke', 'stadium_arena', 'theatre', 'escape_rooms',
  // Accommodation
  'hotel', 'hostel', 'motel', 'resort', 'bed_and_breakfast', 'campground',
  'rv_park', 'holiday_rental_home',
  // Education
  'school', 'university', 'college', 'preschool', 'tutoring_service',
  'driving_school', 'language_school', 'music_school', 'art_school',

  // ── TIER 2: Lifestyle / contextual signal ─────────────────────────
  // Luxury
  'jewelry_store', 'watch_store', 'designer_clothing', 'fur_store',
  'antique_store', 'wine_bar', 'cocktail_bar', 'champagne_bar',
  'medical_spa', 'day_spa', 'health_spa',
  // Home & Living
  'interior_designer', 'furniture_store', 'home_improvement_store',
  'garden_center', 'mattress_store', 'kitchen_supply_store',
  'lighting_store', 'carpet_store', 'real_estate_agent', 'moving_company',
  'self_storage', 'locksmith',
  // Electronics & Telco
  'electronics_store', 'mobile_phone_store', 'computer_store',
  'camera_store', 'video_game_store', 'telecommunications_company',
  'internet_service_provider',
  // Pet Care
  'pet_store', 'pet_grooming', 'pet_boarding', 'dog_park', 'pet_adoption',
  // Pharmaceutical
  'drugstore', 'pharmaceutical_company', 'biotechnology_company',

  // ── TIER 3: Mobility & urban context ──────────────────────────────
  // Transport
  'bus_station', 'train_station', 'airport', 'taxi_stand', 'ferry_terminal',
  'subway_station', 'parking', 'bike_rental', 'car_rental',
  'ride_hailing_service', 'metro_station',
  // Logistics & Delivery
  'freight_and_cargo_service', 'warehouse', 'distribution_service',
  'motor_freight_trucking', 'courier_service', 'postal_service',
  // Government
  'government_office', 'city_hall', 'courthouse', 'embassy', 'consulate',
  'fire_station', 'police_station', 'post_office', 'library',
  'community_center',
  // Energy & Utilities
  'energy_equipment_and_solution', 'pipeline_transportation',
  'electric_utility_provider', 'water_utility_company', 'gas_company',

  // ── TIER 4: Special-vertical signal ───────────────────────────────
  // Gaming
  'esports_league', 'esports_team', 'virtual_reality_center', 'internet_cafe',
  // Moviegoers
  'drive_in_theater', 'outdoor_movies', 'film_festival',
  // Corporate / C-Level
  'coworking_space', 'information_technology_company', 'management_consultant',
  'legal_services', 'executive_search', 'business_consultant',
  'venture_capital', 'private_equity',
  // Attractions & Activities
  'museum', 'aquarium', 'zoo', 'botanical_garden',
  'national_park', 'beach', 'landmark_and_historical_building', 'castle',
  'monument', 'hot_springs', 'ski_area',
  // Legacy (kept for backward compat, grouped elsewhere)
  'supermarket', 'park', 'amusement_park', 'bowling_alley',
] as const;

export type PoiCategory = typeof POI_CATEGORIES[number];

export const CATEGORY_LABELS: Record<PoiCategory, string> = {
  // ── T1: Retail / Commerce ───────────────────────────────────────────
  clothing_store: 'Clothing Stores',
  department_store: 'Department Stores',
  discount_store: 'Discount Stores',
  shopping_mall: 'Shopping Malls',
  outlet_store: 'Outlet Stores',
  thrift_store: 'Thrift Stores',
  gift_shop: 'Gift Shops',
  convenience_store: 'Convenience Stores',
  general_merchandise_store: 'General Merchandise',
  // ── T1: Food & Beverage ─────────────────────────────────────────────
  restaurant: 'Restaurants',
  fast_food_restaurant: 'Fast Food',
  fine_dining: 'Fine Dining',
  cafe: 'Cafés',
  pizza: 'Pizza',
  sushi: 'Sushi',
  bar: 'Bars',
  pub: 'Pubs',
  brewery: 'Breweries',
  winery: 'Wineries',
  coffee_shop: 'Coffee Shops',
  bakery: 'Bakeries',
  juice_bar: 'Juice Bars',
  food_truck: 'Food Trucks',
  // ── T1: Automotive ──────────────────────────────────────────────────
  car_dealer: 'Car Dealers',
  used_car_dealer: 'Used Car Dealers',
  motorcycle_dealer: 'Motorcycle Dealers',
  gas_station: 'Gas Stations',
  ev_charging_station: 'EV Charging',
  car_wash: 'Car Wash',
  auto_body_shop: 'Auto Body Shops',
  tire_dealer_and_repair: 'Tire & Repair',
  // ── T1: Beauty & Personal Care ──────────────────────────────────────
  beauty_salon: 'Beauty Salons',
  hair_salon: 'Hair Salons',
  nail_salon: 'Nail Salons',
  barber: 'Barbers',
  spa: 'Spas',
  massage: 'Massage',
  tattoo_and_piercing: 'Tattoo & Piercing',
  skin_care: 'Skin Care',
  tanning_salon: 'Tanning Salons',
  // ── T1: Healthcare ──────────────────────────────────────────────────
  hospital: 'Hospitals',
  clinic: 'Clinics',
  doctor: 'Doctors',
  dentist: 'Dentists',
  pharmacy: 'Pharmacies',
  urgent_care: 'Urgent Care',
  mental_health_service: 'Mental Health',
  physical_therapy: 'Physical Therapy',
  optometrist: 'Optometrists',
  chiropractor: 'Chiropractors',
  veterinarian: 'Veterinarians',
  // ── T1: Financial Services ──────────────────────────────────────────
  bank: 'Banks',
  atm: 'ATMs',
  credit_union: 'Credit Unions',
  financial_advisor: 'Financial Advisors',
  insurance_agency: 'Insurance Agencies',
  tax_advisor: 'Tax Advisors',
  investment_company: 'Investment Companies',
  accounting_firm: 'Accounting Firms',
  // ── T1: Sports & Fitness ────────────────────────────────────────────
  gym: 'Gyms & Fitness',
  yoga_studio: 'Yoga Studios',
  pilates_studio: 'Pilates Studios',
  swimming_pool: 'Swimming Pools',
  tennis_court: 'Tennis Courts',
  golf_course: 'Golf Courses',
  sports_and_recreation_venue: 'Sports Venues',
  martial_arts_club: 'Martial Arts',
  rock_climbing_gym: 'Rock Climbing',
  // ── T1: Entertainment ───────────────────────────────────────────────
  cinema: 'Cinemas',
  comedy_club: 'Comedy Clubs',
  music_venue: 'Music Venues',
  casino: 'Casinos',
  arcade: 'Arcades',
  dance_club: 'Dance Clubs',
  karaoke: 'Karaoke',
  stadium_arena: 'Stadiums & Arenas',
  theatre: 'Theatres',
  escape_rooms: 'Escape Rooms',
  // ── T1: Accommodation ───────────────────────────────────────────────
  hotel: 'Hotels',
  hostel: 'Hostels',
  motel: 'Motels',
  resort: 'Resorts',
  bed_and_breakfast: 'Bed & Breakfast',
  campground: 'Campgrounds',
  rv_park: 'RV Parks',
  holiday_rental_home: 'Holiday Rentals',
  // ── T1: Education ───────────────────────────────────────────────────
  school: 'Schools',
  university: 'Universities',
  college: 'Colleges',
  preschool: 'Preschools',
  tutoring_service: 'Tutoring',
  driving_school: 'Driving Schools',
  language_school: 'Language Schools',
  music_school: 'Music Schools',
  art_school: 'Art Schools',

  // ── T2: Luxury ──────────────────────────────────────────────────────
  jewelry_store: 'Jewelry Stores',
  watch_store: 'Watch Stores',
  designer_clothing: 'Designer Clothing',
  fur_store: 'Fur Stores',
  antique_store: 'Antique Stores',
  wine_bar: 'Wine Bars',
  cocktail_bar: 'Cocktail Bars',
  champagne_bar: 'Champagne Bars',
  medical_spa: 'Medical Spas',
  day_spa: 'Day Spas',
  health_spa: 'Health Spas',
  // ── T2: Home & Living ───────────────────────────────────────────────
  interior_designer: 'Interior Design',
  furniture_store: 'Furniture Stores',
  home_improvement_store: 'Home Improvement',
  garden_center: 'Garden Centers',
  mattress_store: 'Mattress Stores',
  kitchen_supply_store: 'Kitchen Supply',
  lighting_store: 'Lighting Stores',
  carpet_store: 'Carpet Stores',
  real_estate_agent: 'Real Estate Agents',
  moving_company: 'Moving Companies',
  self_storage: 'Self Storage',
  locksmith: 'Locksmiths',
  // ── T2: Electronics & Telco ─────────────────────────────────────────
  electronics_store: 'Electronics Stores',
  mobile_phone_store: 'Mobile Phone Stores',
  computer_store: 'Computer Stores',
  camera_store: 'Camera Stores',
  video_game_store: 'Video Game Stores',
  telecommunications_company: 'Telecom Companies',
  internet_service_provider: 'Internet Providers',
  // ── T2: Pet Care ────────────────────────────────────────────────────
  pet_store: 'Pet Stores',
  pet_grooming: 'Pet Grooming',
  pet_boarding: 'Pet Boarding',
  dog_park: 'Dog Parks',
  pet_adoption: 'Pet Adoption',
  // ── T2: Pharmaceutical ──────────────────────────────────────────────
  drugstore: 'Drugstores',
  pharmaceutical_company: 'Pharma Companies',
  biotechnology_company: 'Biotech Companies',

  // ── T3: Transport ───────────────────────────────────────────────────
  bus_station: 'Bus Stations',
  train_station: 'Train Stations',
  airport: 'Airports',
  taxi_stand: 'Taxi Stands',
  ferry_terminal: 'Ferry Terminals',
  subway_station: 'Subway Stations',
  parking: 'Parking',
  bike_rental: 'Bike Rentals',
  car_rental: 'Car Rentals',
  ride_hailing_service: 'Ride Hailing',
  metro_station: 'Metro Stations',
  // ── T3: Logistics & Delivery ────────────────────────────────────────
  freight_and_cargo_service: 'Freight & Cargo',
  warehouse: 'Warehouses',
  distribution_service: 'Distribution',
  motor_freight_trucking: 'Freight Trucking',
  courier_service: 'Courier Services',
  postal_service: 'Postal Services',
  // ── T3: Government ──────────────────────────────────────────────────
  government_office: 'Government Offices',
  city_hall: 'City Halls',
  courthouse: 'Courthouses',
  embassy: 'Embassies',
  consulate: 'Consulates',
  fire_station: 'Fire Stations',
  police_station: 'Police Stations',
  post_office: 'Post Offices',
  library: 'Libraries',
  community_center: 'Community Centers',
  // ── T3: Energy & Utilities ──────────────────────────────────────────
  energy_equipment_and_solution: 'Energy Equipment',
  pipeline_transportation: 'Pipelines',
  electric_utility_provider: 'Electric Utilities',
  water_utility_company: 'Water Utilities',
  gas_company: 'Gas Companies',

  // ── T4: Gaming ──────────────────────────────────────────────────────
  esports_league: 'Esports Leagues',
  esports_team: 'Esports Teams',
  virtual_reality_center: 'VR Centers',
  internet_cafe: 'Internet Cafés',
  // ── T4: Moviegoers ──────────────────────────────────────────────────
  drive_in_theater: 'Drive-in Theaters',
  outdoor_movies: 'Outdoor Movies',
  film_festival: 'Film Festivals',
  // ── T4: Corporate / C-Level ─────────────────────────────────────────
  coworking_space: 'Coworking Spaces',
  information_technology_company: 'IT Companies',
  management_consultant: 'Management Consulting',
  legal_services: 'Legal Services',
  executive_search: 'Executive Search',
  business_consultant: 'Business Consulting',
  venture_capital: 'Venture Capital',
  private_equity: 'Private Equity',
  // ── T4: Attractions & Activities ────────────────────────────────────
  museum: 'Museums',
  aquarium: 'Aquariums',
  zoo: 'Zoos',
  botanical_garden: 'Botanical Gardens',
  national_park: 'National Parks',
  beach: 'Beaches',
  landmark_and_historical_building: 'Landmarks & Historical',
  castle: 'Castles',
  monument: 'Monuments',
  hot_springs: 'Hot Springs',
  ski_area: 'Ski Areas',
  // ── Legacy (grouped elsewhere) ──────────────────────────────────────
  supermarket: 'Supermarkets',
  park: 'Parks',
  amusement_park: 'Amusement Parks',
  bowling_alley: 'Bowling Alleys',
};

// ── Category groups for UX ─────────────────────────────────────────────
export interface CategoryGroup {
  label: string;
  icon: string;
  categories: PoiCategory[];
  color: string;
}

export const CATEGORY_GROUPS: Record<string, CategoryGroup> = {
  // ── TIER 1 ──────────────────────────────────────────────────────────
  retail: {
    label: 'Retail & Shopping',
    icon: 'ShoppingBag',
    color: 'text-pink-400',
    categories: [
      'clothing_store', 'department_store', 'discount_store', 'shopping_mall',
      'outlet_store', 'thrift_store', 'gift_shop', 'convenience_store',
      'general_merchandise_store', 'supermarket',
    ],
  },
  food_and_beverage: {
    label: 'Food & Beverage',
    icon: 'UtensilsCrossed',
    color: 'text-orange-400',
    categories: [
      'restaurant', 'fast_food_restaurant', 'fine_dining', 'cafe', 'pizza',
      'sushi', 'bar', 'pub', 'brewery', 'winery', 'coffee_shop', 'bakery',
      'juice_bar', 'food_truck',
    ],
  },
  automotive: {
    label: 'Automotive',
    icon: 'Car',
    color: 'text-slate-400',
    categories: [
      'car_dealer', 'used_car_dealer', 'motorcycle_dealer', 'gas_station',
      'ev_charging_station', 'car_wash', 'auto_body_shop', 'tire_dealer_and_repair',
    ],
  },
  beauty: {
    label: 'Beauty & Personal Care',
    icon: 'Sparkles',
    color: 'text-fuchsia-400',
    categories: [
      'beauty_salon', 'hair_salon', 'nail_salon', 'barber', 'spa', 'massage',
      'tattoo_and_piercing', 'skin_care', 'tanning_salon',
    ],
  },
  healthcare: {
    label: 'Healthcare',
    icon: 'Heart',
    color: 'text-red-400',
    categories: [
      'hospital', 'clinic', 'doctor', 'dentist', 'pharmacy', 'urgent_care',
      'mental_health_service', 'physical_therapy', 'optometrist', 'chiropractor',
      'veterinarian',
    ],
  },
  finance: {
    label: 'Financial Services',
    icon: 'Landmark',
    color: 'text-emerald-400',
    categories: [
      'bank', 'atm', 'credit_union', 'financial_advisor', 'insurance_agency',
      'tax_advisor', 'investment_company', 'accounting_firm',
    ],
  },
  sports: {
    label: 'Sports & Fitness',
    icon: 'Dumbbell',
    color: 'text-lime-400',
    categories: [
      'gym', 'yoga_studio', 'pilates_studio', 'swimming_pool', 'tennis_court',
      'golf_course', 'sports_and_recreation_venue', 'martial_arts_club',
      'rock_climbing_gym',
    ],
  },
  entertainment: {
    label: 'Entertainment',
    icon: 'Clapperboard',
    color: 'text-purple-400',
    categories: [
      'cinema', 'comedy_club', 'music_venue', 'casino', 'arcade', 'dance_club',
      'karaoke', 'stadium_arena', 'theatre', 'escape_rooms', 'bowling_alley',
      'amusement_park',
    ],
  },
  accommodation: {
    label: 'Accommodation',
    icon: 'Hotel',
    color: 'text-amber-400',
    categories: [
      'hotel', 'hostel', 'motel', 'resort', 'bed_and_breakfast', 'campground',
      'rv_park', 'holiday_rental_home',
    ],
  },
  education: {
    label: 'Education',
    icon: 'GraduationCap',
    color: 'text-cyan-400',
    categories: [
      'school', 'university', 'college', 'preschool', 'tutoring_service',
      'driving_school', 'language_school', 'music_school', 'art_school',
    ],
  },
  // ── TIER 2 ──────────────────────────────────────────────────────────
  luxury: {
    label: 'Luxury',
    icon: 'Crown',
    color: 'text-yellow-400',
    categories: [
      'jewelry_store', 'watch_store', 'designer_clothing', 'fur_store',
      'antique_store', 'wine_bar', 'cocktail_bar', 'champagne_bar',
      'medical_spa', 'day_spa', 'health_spa',
    ],
  },
  home: {
    label: 'Home & Living',
    icon: 'Home',
    color: 'text-teal-400',
    categories: [
      'interior_designer', 'furniture_store', 'home_improvement_store',
      'garden_center', 'mattress_store', 'kitchen_supply_store',
      'lighting_store', 'carpet_store', 'real_estate_agent', 'moving_company',
      'self_storage', 'locksmith',
    ],
  },
  electronics: {
    label: 'Electronics & Telco',
    icon: 'Smartphone',
    color: 'text-indigo-400',
    categories: [
      'electronics_store', 'mobile_phone_store', 'computer_store',
      'camera_store', 'video_game_store', 'telecommunications_company',
      'internet_service_provider',
    ],
  },
  pets: {
    label: 'Pet Care',
    icon: 'PawPrint',
    color: 'text-orange-300',
    categories: [
      'pet_store', 'pet_grooming', 'pet_boarding', 'dog_park', 'pet_adoption',
      'veterinarian',
    ],
  },
  pharma: {
    label: 'Pharmaceutical',
    icon: 'Pill',
    color: 'text-sky-400',
    categories: ['drugstore', 'pharmaceutical_company', 'biotechnology_company'],
  },
  // ── TIER 3 ──────────────────────────────────────────────────────────
  transport: {
    label: 'Transport',
    icon: 'Train',
    color: 'text-blue-400',
    categories: [
      'bus_station', 'train_station', 'airport', 'taxi_stand', 'ferry_terminal',
      'subway_station', 'parking', 'bike_rental', 'car_rental',
      'ride_hailing_service', 'metro_station',
    ],
  },
  logistics: {
    label: 'Logistics & Delivery',
    icon: 'Truck',
    color: 'text-stone-400',
    categories: [
      'freight_and_cargo_service', 'warehouse', 'distribution_service',
      'motor_freight_trucking', 'courier_service', 'postal_service',
    ],
  },
  government: {
    label: 'Government & Public',
    icon: 'Building2',
    color: 'text-zinc-400',
    categories: [
      'government_office', 'city_hall', 'courthouse', 'embassy', 'consulate',
      'fire_station', 'police_station', 'post_office', 'library',
      'community_center',
    ],
  },
  energy: {
    label: 'Energy & Utilities',
    icon: 'Zap',
    color: 'text-amber-300',
    categories: [
      'energy_equipment_and_solution', 'pipeline_transportation',
      'electric_utility_provider', 'water_utility_company', 'gas_company',
    ],
  },
  // ── TIER 4 ──────────────────────────────────────────────────────────
  gaming: {
    label: 'Gaming',
    icon: 'Gamepad2',
    color: 'text-violet-400',
    categories: [
      'arcade', 'esports_league', 'esports_team', 'virtual_reality_center',
      'internet_cafe', 'video_game_store',
    ],
  },
  moviegoers: {
    label: 'Moviegoers',
    icon: 'Film',
    color: 'text-rose-400',
    categories: ['cinema', 'drive_in_theater', 'outdoor_movies', 'film_festival'],
  },
  corporate: {
    label: 'Corporate / C-Level',
    icon: 'Briefcase',
    color: 'text-neutral-400',
    categories: [
      'coworking_space', 'information_technology_company', 'management_consultant',
      'legal_services', 'executive_search', 'business_consultant',
      'venture_capital', 'private_equity',
    ],
  },
  attractions: {
    label: 'Attractions & Activities',
    icon: 'MapPin',
    color: 'text-green-400',
    categories: [
      'museum', 'aquarium', 'zoo', 'botanical_garden', 'national_park',
      'beach', 'landmark_and_historical_building', 'castle', 'monument',
      'hot_springs', 'ski_area', 'park',
    ],
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
