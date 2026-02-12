/**
 * Catchment (residential origin) analysis types.
 * Shared types for dataset-analyzer-residential, reverse-geocode, and API responses.
 */

export const MAX_NOMINATIM_CALLS = 200;

export interface ResidentialFilters {
  dateFrom?: string;
  dateTo?: string;
  poiIds?: string[];
  minNightPings?: number; // default 3
  minDistinctNights?: number; // default 2 — filters tourists (single-night stays)
}

export interface ResidentialZipcode {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  devices: number;
  percentOfClassified: number; // % over classified devices (spanish + foreign)
  percentOfTotal: number; // % over ALL visitors — scientifically correct metric
  /** @deprecated Use percentOfTotal. Kept for backward compatibility. */
  percentage?: number; // = percentOfTotal
  /** Origin of geocoding: GeoJSON (Spain), Nominatim, or mixed */
  source?: 'geojson' | 'nominatim' | 'mixed';
}

/** Classification of a device's home location after reverse geocoding */
export type HomeClassification =
  | { type: 'spanish'; zipcode: string; city: string; province: string; region: string; devices: number }
  | { type: 'foreign'; devices: number }
  | { type: 'unmatched_domestic'; devices: number };

export interface CatchmentClassification {
  zipcodes: ResidentialZipcode[];
  foreignDevices: number;
  unmatchedDomestic: number;
  noHomeLocation: number;
  classificationRate: number; // % of total devices that could be classified
}

export interface CatchmentMethodology {
  nightWindowUtc: string;
  nightWindowLocal: string;
  minNightPings: number;
  minDistinctNights: number;
  accuracyThresholdMeters: number;
  homeEstimationMethod: string;
  privacyMinDevices: number;
  etagSampleNote?: string;
}

export interface CatchmentCoverage {
  totalDevicesVisitedPois: number;
  devicesWithHomeEstimate: number;
  devicesMatchedToSpanishZipcode: number; // GeoJSON + Nominatim with country=ES
  devicesForeignOrigin: number;
  devicesUnmatchedDomestic: number;
  devicesNominatimTruncated: number; // not geocoded due to Nominatim API limit
  devicesInsufficientNightData: number;
  classificationRatePercent: number;
  geocodingComplete: boolean; // false if nominatim truncation occurred
}

export interface ResidentialAnalysisResult {
  dataset: string;
  analyzedAt: string;
  filters: ResidentialFilters;
  methodology: CatchmentMethodology;
  coverage: CatchmentCoverage;
  summary: {
    totalDevicesInDataset: number;
    devicesWithHomeLocation: number;
    devicesMatchedToZipcode: number;
    totalZipcodes: number;
    topZipcode: string | null;
    topCity: string | null;
  };
  zipcodes: ResidentialZipcode[];
}
