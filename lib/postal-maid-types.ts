/**
 * Postal Code → MAID lookup types.
 *
 * Given a set of postal codes + country + dataset, find all MAIDs
 * whose catchment (first ping of day = residential origin) falls
 * within those postal codes.
 */

export interface PostalMaidFilters {
  postalCodes: string[];       // postal codes to look up
  country: string;             // ISO 2-letter code (for GeoJSON reverse geocoding)
  dateFrom?: string;           // YYYY-MM-DD
  dateTo?: string;             // YYYY-MM-DD
}

export interface PostalMaidDevice {
  adId: string;
  deviceDays: number;          // how many days this device's origin was in the postal codes
  postalCodes: string[];       // which of the requested postal codes matched
}

export interface PostalMaidResult {
  dataset: string;
  analyzedAt: string;
  filters: PostalMaidFilters;
  methodology: {
    approach: 'first_ping_per_day_reverse_geocoded';
    description: string;
    accuracyThresholdMeters: number;
    coordinatePrecision: number;
  };
  coverage: {
    totalDevicesInDataset: number;
    totalDeviceDays: number;
    devicesMatchedToPostalCodes: number;
    matchedDeviceDays: number;
    postalCodesRequested: number;
    postalCodesWithDevices: number;
  };
  summary: {
    totalMaids: number;
    topPostalCode: string | null;
    topPostalCodeDevices: number;
  };
  /** Matched MAIDs — all devices whose first-ping origin fell in the requested postal codes */
  devices: PostalMaidDevice[];
  /**
   * When the SSE payload would exceed safe size, full result is stored in S3 config.
   * Client should GET /api/zip-code-signals/spill?key=… (after merge, devices holds a preview only).
   */
  devicesSpillKey?: string;
  /** Total MAIDs when devicesSpillKey is set (devices array may be shorter) */
  devicesSpillTotal?: number;
  /** Breakdown by postal code: how many devices per requested postal code */
  postalCodeBreakdown: Array<{
    postalCode: string;
    devices: number;
    deviceDays: number;
  }>;
}
