/**
 * Origin-Destination analysis types.
 * For each device that visited a POI on a given day, we identify:
 * - Origin: first ping of the day (where they came from)
 * - Destination: last ping of the day (where they went after)
 *
 * This replaces the nighttime-based residential catchment with real observed movement.
 */

export interface ODFilters {
  dateFrom?: string;
  dateTo?: string;
  poiIds?: string[];
}

export interface ODZipcode {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  devices: number;
  percentOfTotal: number;
  source?: 'geojson' | 'nominatim' | 'mixed';
}

export interface ODTemporalPattern {
  hour: number;        // 0-23 UTC
  deviceDays: number;  // count of device-day visits with first ping at this hour
  percentOfTotal: number;
}

export interface ODCoverage {
  totalDevicesVisitedPois: number;
  totalDeviceDays: number;
  devicesWithOrigin: number;
  devicesWithDestination: number;
  originZipcodes: number;
  destinationZipcodes: number;
  coverageRatePercent: number; // % of device-days with valid origin+destination
  geocodingComplete: boolean;
}

export interface ODMethodology {
  approach: 'first_last_ping_per_day';
  description: string;
  accuracyThresholdMeters: number;
  coordinatePrecision: number; // decimal places for grouping (4 ≈ 11m)
}

export interface ODAnalysisResult {
  dataset: string;
  analyzedAt: string;
  filters: ODFilters;
  methodology: ODMethodology;
  coverage: ODCoverage;
  summary: {
    totalDeviceDays: number;
    topOriginZipcode: string | null;
    topOriginCity: string | null;
    topDestinationZipcode: string | null;
    topDestinationCity: string | null;
  };
  origins: ODZipcode[];
  destinations: ODZipcode[];
  temporalPatterns: ODTemporalPattern[];
}
