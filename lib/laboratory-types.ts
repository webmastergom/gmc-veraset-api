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

// ── POI categories from Overture Maps parquets ──────────────────────────
// Names match EXACTLY the `category` column in Overture parquets.
// Organized by signal tier:
//   T1 = High affinity signal (core GMC verticals)
//   T2 = Lifestyle / contextual signal
//   T3 = Mobility & urban context
//   T4 = Special-vertical signal
// POI_CATEGORIES is derived from CATEGORY_GROUPS to avoid maintaining two lists
// Flatten all categories from all groups, deduplicate, and freeze as const
function _buildPoiCategories() {
  // Defined inline to avoid circular reference — CATEGORY_GROUPS is below
  return [] as string[];
}

// We'll populate this after CATEGORY_GROUPS is defined (see bottom of file)
// For now, keep the expanded list for type safety
export const POI_CATEGORIES = [
  // Retail
  'clothing_store', 'department_store', 'discount_store', 'shopping_center',
  'outlet_store', 'thrift_store', 'gift_shop', 'convenience_store',
  'supermarket', 'grocery_store', 'shopping', 'retail',
  'shoe_store', 'womens_clothing_store', 'liquor_store', 'flowers_and_gifts_shop',
  'butcher_shop',
  // Food & Beverage
  'restaurant', 'fast_food_restaurant', 'cafe', 'pizza_restaurant', 'pizza',
  'sushi_restaurant', 'sushi', 'bar', 'pub', 'brewery', 'winery', 'coffee_shop',
  'bakery', 'smoothie_juice_bar', 'food_truck', 'ice_cream_shop',
  'mexican_restaurant', 'italian_restaurant', 'seafood_restaurant',
  'burger_restaurant', 'chinese_restaurant', 'sandwich_shop',
  'american_restaurant', 'french_restaurant', 'barbecue_restaurant',
  'taco_restaurant', 'bar_and_grill_restaurant', 'chicken_restaurant',
  // Automotive
  'car_dealer', 'used_car_dealer', 'motorcycle_dealer', 'gas_station',
  'ev_charging_station', 'car_wash', 'auto_body_shop', 'tire_shop',
  'automotive_repair', 'automotive_parts_and_accessories', 'tire_dealer_and_repair',
  // Beauty
  'beauty_salon', 'hair_salon', 'nail_salon', 'barber', 'spas', 'spa', 'massage',
  'massage_therapy', 'beauty_and_spa', 'cosmetic_and_beauty_supplies',
  'tattoo_and_piercing', 'skin_care', 'tanning_salon',
  // Healthcare
  'hospital', 'medical_center', 'walk_in_clinic', 'clinic', 'doctor', 'dentist',
  'pharmacy', 'urgent_care_clinic', 'psychologist', 'psychiatrist',
  'counseling_and_mental_health', 'mental_health_service', 'physical_therapy',
  'optometrist', 'eyewear_and_optician', 'health_and_medical',
  'chiropractor', 'veterinarian', 'retirement_home',
  // Finance
  'banks', 'bank', 'bank_credit_union', 'atms', 'atm', 'credit_union',
  'financial_advising', 'financial_advisor', 'financial_service',
  'insurance_agency', 'tax_services', 'investing', 'accountant', 'accounting_firm',
  'money_transfer_services',
  // Sports
  'gym', 'yoga_studio', 'pilates_studio', 'swimming_pool', 'tennis_court',
  'golf_course', 'stadium_arena', 'martial_arts_club', 'rock_climbing_gym',
  'sports_club_and_league', 'sports_and_recreation_venue', 'dance_school',
  // Entertainment
  'cinema', 'comedy_club', 'music_venue', 'casino', 'arcade', 'dance_club',
  'karaoke', 'theatre', 'escape_rooms', 'bowling_alley', 'amusement_park',
  // Accommodation
  'hotel', 'hostel', 'motel', 'resort', 'bed_and_breakfast', 'campground',
  'rv_park', 'holiday_rental_home',
  // Education
  'school', 'college_university', 'college', 'campus_building', 'education',
  'elementary_school', 'high_school', 'middle_school', 'private_school', 'public_school',
  'preschool', 'day_care_preschool', 'child_care_and_day_care',
  'tutoring_center', 'tutoring_service', 'specialty_school', 'vocational_and_technical_school',
  'adult_education', 'religious_school',
  'driving_school', 'language_school', 'music_school', 'art_school',
  // Luxury
  'jewelry_store', 'watch_store', 'designer_clothing', 'fur_clothing',
  'antique_store', 'wine_bar', 'cocktail_bar', 'champagne_bar',
  'medical_spa', 'day_spa', 'health_spa',
  // Home
  'interior_design', 'interior_designer', 'furniture_store', 'home_improvement_store',
  'hardware_store', 'building_supply_store', 'nursery_and_gardening', 'garden_center',
  'mattress_store', 'kitchen_and_bath', 'home_goods_store',
  'lighting_store', 'carpet_store', 'real_estate_agent', 'real_estate', 'movers',
  'storage_facility', 'self_storage_facility', 'self_storage',
  'key_and_locksmith', 'locksmith',
  'contractor', 'electrician', 'plumbing', 'hvac_services', 'construction_services',
  // Electronics
  'electronics', 'electronics_store', 'mobile_phone_store', 'computer_store',
  'photography_store_and_services', 'video_game_store', 'telecommunications',
  'telecommunications_company', 'internet_service_provider',
  'it_service_and_computer_repair',
  // Pets
  'pet_store', 'pet_groomer', 'pet_grooming', 'pet_boarding', 'dog_park', 'pet_adoption',
  // Pharma
  'drugstore', 'pharmaceutical_companies', 'biotechnology_company',
  // Transport
  'bus_station', 'train_station', 'airport', 'taxi_service', 'taxi_rank',
  'ferry_service', 'metro_station', 'parking', 'bike_rentals',
  'car_rental_agency', 'car_rental', 'ride_sharing', 'travel_services',
  // Logistics
  'freight_and_cargo_service', 'warehouses', 'distribution_services',
  'motor_freight_trucking', 'courier_and_delivery_services', 'courier_service', 'post_office',
  // Government
  'federal_government_offices', 'local_and_state_government_offices',
  'government_office', 'town_hall', 'city_hall', 'courthouse', 'embassy',
  'fire_department', 'fire_station', 'police_department', 'police_station',
  'library', 'community_center',
  'community_services_non_profits', 'public_and_government_association',
  // Energy
  'energy_equipment_and_solution', 'pipeline_transportation',
  'electric_utility_provider', 'water_supplier', 'natural_gas_supplier',
  // Gaming
  'esports_league', 'esports_team', 'virtual_reality_center', 'internet_cafe',
  // Moviegoers
  'drive_in_theater', 'outdoor_movies', 'film_festival', 'film_festivals_and_organizations',
  // Corporate
  'coworking_space', 'information_technology_company', 'corporate_office',
  'software_development', 'professional_services',
  'legal_services', 'executive_search_consultants', 'business_consulting',
  'private_equity_firm', 'advertising_agency',
  // Attractions
  'museum', 'art_gallery', 'aquarium', 'zoo', 'botanical_garden',
  'national_park', 'beach', 'landmark_and_historical_building', 'castle',
  'monument', 'hot_springs', 'ski_area', 'park',
] as const;

export type PoiCategory = typeof POI_CATEGORIES[number];

export const CATEGORY_LABELS: Record<string, string> = {
  // ── T1: Retail / Commerce ───────────────────────────────────────────
  clothing_store: 'Clothing Stores',
  department_store: 'Department Stores',
  discount_store: 'Discount Stores',
  shopping_center: 'Shopping Centers',
  outlet_store: 'Outlet Stores',
  thrift_store: 'Thrift Stores',
  gift_shop: 'Gift Shops',
  convenience_store: 'Convenience Stores',
  supermarket: 'Supermarkets',
  // ── T1: Food & Beverage ─────────────────────────────────────────────
  restaurant: 'Restaurants',
  fast_food_restaurant: 'Fast Food',
  cafe: 'Cafés',
  pizza_restaurant: 'Pizza',
  sushi_restaurant: 'Sushi',
  bar: 'Bars',
  pub: 'Pubs',
  brewery: 'Breweries',
  winery: 'Wineries',
  coffee_shop: 'Coffee Shops',
  bakery: 'Bakeries',
  smoothie_juice_bar: 'Juice Bars',
  food_truck: 'Food Trucks',
  // ── T1: Automotive ──────────────────────────────────────────────────
  car_dealer: 'Car Dealers',
  used_car_dealer: 'Used Car Dealers',
  motorcycle_dealer: 'Motorcycle Dealers',
  gas_station: 'Gas Stations',
  ev_charging_station: 'EV Charging',
  car_wash: 'Car Wash',
  auto_body_shop: 'Auto Body Shops',
  tire_shop: 'Tire & Repair',
  // ── T1: Beauty & Personal Care ──────────────────────────────────────
  beauty_salon: 'Beauty Salons',
  hair_salon: 'Hair Salons',
  nail_salon: 'Nail Salons',
  barber: 'Barbers',
  spas: 'Spas',
  massage: 'Massage',
  tattoo_and_piercing: 'Tattoo & Piercing',
  skin_care: 'Skin Care',
  tanning_salon: 'Tanning Salons',
  // ── T1: Healthcare ──────────────────────────────────────────────────
  hospital: 'Hospitals',
  medical_center: 'Medical Centers',
  walk_in_clinic: 'Walk-in Clinics',
  doctor: 'Doctors',
  dentist: 'Dentists',
  pharmacy: 'Pharmacies',
  urgent_care_clinic: 'Urgent Care',
  psychologist: 'Psychologists',
  psychiatrist: 'Psychiatrists',
  counseling_and_mental_health: 'Mental Health Counseling',
  physical_therapy: 'Physical Therapy',
  optometrist: 'Optometrists',
  chiropractor: 'Chiropractors',
  veterinarian: 'Veterinarians',
  // ── T1: Financial Services ──────────────────────────────────────────
  banks: 'Banks',
  atms: 'ATMs',
  credit_union: 'Credit Unions',
  financial_advising: 'Financial Advisors',
  insurance_agency: 'Insurance Agencies',
  tax_services: 'Tax Advisors',
  investing: 'Investment Companies',
  accountant: 'Accounting Firms',
  // ── T1: Sports & Fitness ────────────────────────────────────────────
  gym: 'Gyms & Fitness',
  yoga_studio: 'Yoga Studios',
  pilates_studio: 'Pilates Studios',
  swimming_pool: 'Swimming Pools',
  tennis_court: 'Tennis Courts',
  golf_course: 'Golf Courses',
  stadium_arena: 'Stadiums & Arenas',
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
  theatre: 'Theatres',
  escape_rooms: 'Escape Rooms',
  bowling_alley: 'Bowling Alleys',
  amusement_park: 'Amusement Parks',
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
  college_university: 'Universities & Colleges',
  preschool: 'Preschools',
  tutoring_center: 'Tutoring',
  driving_school: 'Driving Schools',
  language_school: 'Language Schools',
  music_school: 'Music Schools',
  art_school: 'Art Schools',

  // ── T2: Luxury ──────────────────────────────────────────────────────
  jewelry_store: 'Jewelry Stores',
  watch_store: 'Watch Stores',
  designer_clothing: 'Designer Clothing',
  fur_clothing: 'Fur Stores',
  antique_store: 'Antique Stores',
  wine_bar: 'Wine Bars',
  cocktail_bar: 'Cocktail Bars',
  champagne_bar: 'Champagne Bars',
  medical_spa: 'Medical Spas',
  day_spa: 'Day Spas',
  health_spa: 'Health Spas',
  // ── T2: Home & Living ───────────────────────────────────────────────
  interior_design: 'Interior Design',
  furniture_store: 'Furniture Stores',
  home_improvement_store: 'Home Improvement',
  nursery_and_gardening: 'Garden Centers',
  mattress_store: 'Mattress Stores',
  kitchen_and_bath: 'Kitchen Supply',
  lighting_store: 'Lighting Stores',
  carpet_store: 'Carpet Stores',
  real_estate_agent: 'Real Estate Agents',
  movers: 'Moving Companies',
  storage_facility: 'Self Storage',
  key_and_locksmith: 'Locksmiths',
  // ── T2: Electronics & Telco ─────────────────────────────────────────
  electronics: 'Electronics Stores',
  mobile_phone_store: 'Mobile Phone Stores',
  computer_store: 'Computer Stores',
  photography_store_and_services: 'Camera Stores',
  video_game_store: 'Video Game Stores',
  telecommunications: 'Telecom Companies',
  internet_service_provider: 'Internet Providers',
  // ── T2: Pet Care ────────────────────────────────────────────────────
  pet_store: 'Pet Stores',
  pet_groomer: 'Pet Grooming',
  pet_boarding: 'Pet Boarding',
  dog_park: 'Dog Parks',
  pet_adoption: 'Pet Adoption',
  // ── T2: Pharmaceutical ──────────────────────────────────────────────
  drugstore: 'Drugstores',
  pharmaceutical_companies: 'Pharma Companies',
  biotechnology_company: 'Biotech Companies',

  // ── T3: Transport ───────────────────────────────────────────────────
  bus_station: 'Bus Stations',
  train_station: 'Train Stations',
  airport: 'Airports',
  taxi_service: 'Taxi Services',
  taxi_rank: 'Taxi Stands',
  ferry_service: 'Ferry Terminals',
  metro_station: 'Metro Stations',
  parking: 'Parking',
  bike_rentals: 'Bike Rentals',
  car_rental_agency: 'Car Rentals',
  ride_sharing: 'Ride Hailing',
  // ── T3: Logistics & Delivery ────────────────────────────────────────
  freight_and_cargo_service: 'Freight & Cargo',
  warehouses: 'Warehouses',
  distribution_services: 'Distribution',
  motor_freight_trucking: 'Freight Trucking',
  courier_and_delivery_services: 'Courier Services',
  post_office: 'Post Offices',
  // ── T3: Government ──────────────────────────────────────────────────
  federal_government_offices: 'Federal Government',
  local_and_state_government_offices: 'Local Government',
  town_hall: 'City Halls',
  courthouse: 'Courthouses',
  embassy: 'Embassies',
  fire_department: 'Fire Stations',
  police_department: 'Police Stations',
  library: 'Libraries',
  community_center: 'Community Centers',
  // ── T3: Energy & Utilities ──────────────────────────────────────────
  energy_equipment_and_solution: 'Energy Equipment',
  pipeline_transportation: 'Pipelines',
  electric_utility_provider: 'Electric Utilities',
  water_supplier: 'Water Utilities',
  natural_gas_supplier: 'Gas Companies',

  // ── T4: Gaming ──────────────────────────────────────────────────────
  esports_league: 'Esports Leagues',
  esports_team: 'Esports Teams',
  virtual_reality_center: 'VR Centers',
  internet_cafe: 'Internet Cafés',
  // ── T4: Moviegoers ──────────────────────────────────────────────────
  drive_in_theater: 'Drive-in Theaters',
  outdoor_movies: 'Outdoor Movies',
  film_festival: 'Film Festivals',
  film_festivals_and_organizations: 'Film Festivals',
  // ── T4: Corporate / C-Level ─────────────────────────────────────────
  coworking_space: 'Coworking Spaces',
  information_technology_company: 'IT Companies',
  legal_services: 'Legal Services',
  executive_search_consultants: 'Executive Search',
  business_consulting: 'Business Consulting',
  private_equity_firm: 'Private Equity',
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
  park: 'Parks',
  art_gallery: 'Art Galleries',
  // ── New Overture variant labels ──────────────────────────────────
  grocery_store: 'Grocery Stores',
  shopping: 'Shopping',
  retail: 'Retail',
  shoe_store: 'Shoe Stores',
  womens_clothing_store: "Women's Clothing",
  liquor_store: 'Liquor Stores',
  flowers_and_gifts_shop: 'Flowers & Gifts',
  butcher_shop: 'Butcher Shops',
  pizza: 'Pizza',
  sushi: 'Sushi',
  ice_cream_shop: 'Ice Cream',
  mexican_restaurant: 'Mexican Restaurants',
  italian_restaurant: 'Italian Restaurants',
  seafood_restaurant: 'Seafood Restaurants',
  burger_restaurant: 'Burger Restaurants',
  chinese_restaurant: 'Chinese Restaurants',
  sandwich_shop: 'Sandwich Shops',
  american_restaurant: 'American Restaurants',
  french_restaurant: 'French Restaurants',
  barbecue_restaurant: 'BBQ Restaurants',
  taco_restaurant: 'Taco Restaurants',
  bar_and_grill_restaurant: 'Bar & Grill',
  chicken_restaurant: 'Chicken Restaurants',
  automotive_repair: 'Auto Repair',
  automotive_parts_and_accessories: 'Auto Parts',
  tire_dealer_and_repair: 'Tire Dealers',
  spa: 'Spas',
  massage_therapy: 'Massage Therapy',
  beauty_and_spa: 'Beauty & Spa',
  cosmetic_and_beauty_supplies: 'Cosmetic Supplies',
  clinic: 'Clinics',
  mental_health_service: 'Mental Health',
  eyewear_and_optician: 'Eyewear & Opticians',
  health_and_medical: 'Health & Medical',
  retirement_home: 'Retirement Homes',
  bank: 'Banks',
  bank_credit_union: 'Banks & Credit Unions',
  atm: 'ATMs',
  financial_advisor: 'Financial Advisors',
  financial_service: 'Financial Services',
  accounting_firm: 'Accounting Firms',
  money_transfer_services: 'Money Transfer',
  sports_club_and_league: 'Sports Clubs',
  sports_and_recreation_venue: 'Sports Venues',
  dance_school: 'Dance Schools',
  college: 'Colleges',
  campus_building: 'Campus Buildings',
  education: 'Education',
  elementary_school: 'Elementary Schools',
  high_school: 'High Schools',
  middle_school: 'Middle Schools',
  private_school: 'Private Schools',
  public_school: 'Public Schools',
  day_care_preschool: 'Day Care / Preschool',
  child_care_and_day_care: 'Child Care',
  tutoring_service: 'Tutoring Services',
  specialty_school: 'Specialty Schools',
  vocational_and_technical_school: 'Vocational Schools',
  adult_education: 'Adult Education',
  religious_school: 'Religious Schools',
  interior_designer: 'Interior Designers',
  hardware_store: 'Hardware Stores',
  building_supply_store: 'Building Supplies',
  garden_center: 'Garden Centers',
  home_goods_store: 'Home Goods',
  real_estate: 'Real Estate',
  self_storage_facility: 'Self Storage',
  self_storage: 'Self Storage',
  locksmith: 'Locksmiths',
  contractor: 'Contractors',
  electrician: 'Electricians',
  plumbing: 'Plumbing',
  hvac_services: 'HVAC Services',
  construction_services: 'Construction',
  electronics_store: 'Electronics Stores',
  telecommunications_company: 'Telecom Companies',
  it_service_and_computer_repair: 'IT & Computer Repair',
  pet_grooming: 'Pet Grooming',
  car_rental: 'Car Rental',
  travel_services: 'Travel Services',
  courier_service: 'Courier Services',
  government_office: 'Government Offices',
  city_hall: 'City Hall',
  fire_station: 'Fire Stations',
  police_station: 'Police Stations',
  community_services_non_profits: 'Community & Non-Profits',
  public_and_government_association: 'Public Associations',
  corporate_office: 'Corporate Offices',
  software_development: 'Software Development',
  professional_services: 'Professional Services',
  advertising_agency: 'Advertising Agencies',
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
      'clothing_store', 'department_store', 'discount_store', 'shopping_center',
      'outlet_store', 'thrift_store', 'gift_shop', 'convenience_store',
      'supermarket', 'grocery_store', 'shopping', 'retail',
      'shoe_store', 'womens_clothing_store', 'liquor_store', 'flowers_and_gifts_shop',
      'butcher_shop',
    ],
  },
  food_and_beverage: {
    label: 'Food & Beverage',
    icon: 'UtensilsCrossed',
    color: 'text-orange-400',
    categories: [
      'restaurant', 'fast_food_restaurant', 'cafe', 'pizza_restaurant', 'pizza',
      'sushi_restaurant', 'sushi', 'bar', 'pub', 'brewery', 'winery', 'coffee_shop',
      'bakery', 'smoothie_juice_bar', 'food_truck', 'ice_cream_shop',
      'mexican_restaurant', 'italian_restaurant', 'seafood_restaurant',
      'burger_restaurant', 'chinese_restaurant', 'sandwich_shop',
      'american_restaurant', 'french_restaurant', 'barbecue_restaurant',
      'taco_restaurant', 'bar_and_grill_restaurant', 'chicken_restaurant',
    ],
  },
  automotive: {
    label: 'Automotive',
    icon: 'Car',
    color: 'text-slate-400',
    categories: [
      'car_dealer', 'used_car_dealer', 'motorcycle_dealer', 'gas_station',
      'ev_charging_station', 'car_wash', 'auto_body_shop', 'tire_shop',
      'automotive_repair', 'automotive_parts_and_accessories', 'tire_dealer_and_repair',
    ],
  },
  beauty: {
    label: 'Beauty & Personal Care',
    icon: 'Sparkles',
    color: 'text-fuchsia-400',
    categories: [
      'beauty_salon', 'hair_salon', 'nail_salon', 'barber', 'spas', 'spa', 'massage',
      'massage_therapy', 'beauty_and_spa', 'cosmetic_and_beauty_supplies',
      'tattoo_and_piercing', 'skin_care', 'tanning_salon',
    ],
  },
  healthcare: {
    label: 'Healthcare',
    icon: 'Heart',
    color: 'text-red-400',
    categories: [
      'hospital', 'medical_center', 'walk_in_clinic', 'clinic', 'doctor', 'dentist',
      'pharmacy', 'urgent_care_clinic', 'psychologist', 'psychiatrist',
      'counseling_and_mental_health', 'mental_health_service', 'physical_therapy',
      'optometrist', 'eyewear_and_optician', 'health_and_medical',
      'chiropractor', 'veterinarian', 'retirement_home',
    ],
  },
  finance: {
    label: 'Financial Services',
    icon: 'Landmark',
    color: 'text-emerald-400',
    categories: [
      'banks', 'bank', 'bank_credit_union', 'atms', 'atm', 'credit_union',
      'financial_advising', 'financial_advisor', 'financial_service',
      'insurance_agency', 'tax_services', 'investing', 'accountant', 'accounting_firm',
      'money_transfer_services',
    ],
  },
  sports: {
    label: 'Sports & Fitness',
    icon: 'Dumbbell',
    color: 'text-lime-400',
    categories: [
      'gym', 'yoga_studio', 'pilates_studio', 'swimming_pool', 'tennis_court',
      'golf_course', 'stadium_arena', 'martial_arts_club', 'rock_climbing_gym',
      'sports_club_and_league', 'sports_and_recreation_venue', 'dance_school',
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
      'school', 'college_university', 'college', 'campus_building', 'education',
      'elementary_school', 'high_school', 'middle_school', 'private_school', 'public_school',
      'preschool', 'day_care_preschool', 'child_care_and_day_care',
      'tutoring_center', 'tutoring_service', 'specialty_school', 'vocational_and_technical_school',
      'adult_education', 'religious_school',
      'driving_school', 'language_school', 'music_school', 'art_school',
    ],
  },
  // ── TIER 2 ──────────────────────────────────────────────────────────
  luxury: {
    label: 'Luxury',
    icon: 'Crown',
    color: 'text-yellow-400',
    categories: [
      'jewelry_store', 'watch_store', 'designer_clothing', 'fur_clothing',
      'antique_store', 'wine_bar', 'cocktail_bar', 'champagne_bar',
      'medical_spa', 'day_spa', 'health_spa',
    ],
  },
  home: {
    label: 'Home & Living',
    icon: 'Home',
    color: 'text-teal-400',
    categories: [
      'interior_design', 'interior_designer', 'furniture_store', 'home_improvement_store',
      'hardware_store', 'building_supply_store', 'nursery_and_gardening', 'garden_center',
      'mattress_store', 'kitchen_and_bath', 'home_goods_store',
      'lighting_store', 'carpet_store', 'real_estate_agent', 'real_estate', 'movers',
      'storage_facility', 'self_storage_facility', 'self_storage',
      'key_and_locksmith', 'locksmith',
      'contractor', 'electrician', 'plumbing', 'hvac_services', 'construction_services',
    ],
  },
  electronics: {
    label: 'Electronics & Telco',
    icon: 'Smartphone',
    color: 'text-indigo-400',
    categories: [
      'electronics', 'electronics_store', 'mobile_phone_store', 'computer_store',
      'photography_store_and_services', 'video_game_store', 'telecommunications',
      'telecommunications_company', 'internet_service_provider',
      'it_service_and_computer_repair',
    ],
  },
  pets: {
    label: 'Pet Care',
    icon: 'PawPrint',
    color: 'text-orange-300',
    categories: [
      'pet_store', 'pet_groomer', 'pet_grooming', 'pet_boarding', 'dog_park',
      'pet_adoption', 'veterinarian',
    ],
  },
  pharma: {
    label: 'Pharmaceutical',
    icon: 'Pill',
    color: 'text-sky-400',
    categories: ['drugstore', 'pharmaceutical_companies', 'biotechnology_company'],
  },
  // ── TIER 3 ──────────────────────────────────────────────────────────
  transport: {
    label: 'Transport',
    icon: 'Train',
    color: 'text-blue-400',
    categories: [
      'bus_station', 'train_station', 'airport', 'taxi_service', 'taxi_rank',
      'ferry_service', 'metro_station', 'parking', 'bike_rentals',
      'car_rental_agency', 'car_rental', 'ride_sharing', 'travel_services',
    ],
  },
  logistics: {
    label: 'Logistics & Delivery',
    icon: 'Truck',
    color: 'text-stone-400',
    categories: [
      'freight_and_cargo_service', 'warehouses', 'distribution_services',
      'motor_freight_trucking', 'courier_and_delivery_services', 'courier_service',
      'post_office',
    ],
  },
  government: {
    label: 'Government & Public',
    icon: 'Building2',
    color: 'text-zinc-400',
    categories: [
      'federal_government_offices', 'local_and_state_government_offices',
      'government_office', 'town_hall', 'city_hall', 'courthouse', 'embassy',
      'fire_department', 'fire_station', 'police_department', 'police_station',
      'post_office', 'library', 'community_center',
      'community_services_non_profits', 'public_and_government_association',
    ],
  },
  energy: {
    label: 'Energy & Utilities',
    icon: 'Zap',
    color: 'text-amber-300',
    categories: [
      'energy_equipment_and_solution', 'pipeline_transportation',
      'electric_utility_provider', 'water_supplier', 'natural_gas_supplier',
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
      'coworking_space', 'information_technology_company', 'corporate_office',
      'software_development', 'professional_services',
      'legal_services', 'executive_search_consultants', 'business_consulting',
      'private_equity_firm', 'advertising_agency',
    ],
  },
  attractions: {
    label: 'Attractions & Activities',
    icon: 'MapPin',
    color: 'text-green-400',
    categories: [
      'museum', 'art_gallery', 'aquarium', 'zoo', 'botanical_garden', 'national_park',
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
    devices: SegmentDevice[];           // top 1000 for UI display
    allSegmentDevices?: SegmentDevice[]; // full list for batch export (audience agent)
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
