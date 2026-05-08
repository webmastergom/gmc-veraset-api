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
  // ── FULL-schema extras ──────────────────────────────────────────
  /** Most-common region/state for this device's origins (FULL only). */
  region?: string;
  /** Most-common city for this device's origins (FULL only). */
  city?: string;
  /**
   * Quality tier derived from per-day GPS share + circle score:
   *   high   = avg GPS share > 0.7 AND avg circle score < 1
   *   medium = avg GPS share > 0.4
   *   low    = otherwise
   */
  qualityTier?: 'high' | 'medium' | 'low';
  /** Did at least one origin day include an overnight ping (22h-6h)? */
  overnightPresence?: boolean;
}

/**
 * FULL-schema only — per-ZIP enrichment derived from `geo_fields` on each
 * ping. Computed alongside the device list in a single Athena pass.
 */
export interface ZipSignature {
  postalCode: string;
  /** Most common region/state (e.g. "Ciudad de México", "Jalisco"). */
  region: string | null;
  /** Top 3 cities within this ZIP, ranked by device count. */
  topCities: { city: string; devices: number }[];
  // ── Counts ────────────────────────────────────────────────────
  devices: number;
  deviceDays: number;
  // ── Time signature ────────────────────────────────────────────
  /** Per-device-day origin hour bucketed: morning(5-10) / midday(11-13) / afternoon(14-17) / evening(18-21) / night(22-4). */
  hourBuckets: {
    morning: number;
    midday: number;
    afternoon: number;
    evening: number;
    night: number;
  };
  /** Bucket with the highest share — when these residents typically originate from this ZIP. */
  peakHourBucket: 'morning' | 'midday' | 'afternoon' | 'evening' | 'night';
  /** Share of device-days originating on Sat/Sun (0..1). */
  weekendShare: number;
  /** Share of device-days that include an overnight ping in 22h-6h (0..1). */
  overnightShare: number;
  // ── Quality ───────────────────────────────────────────────────
  qualityTier: 'high' | 'mixed' | 'low';
  gpsShare: number;       // avg across this zip's pings
  avgCircleScore: number; // avg across this zip's pings (lower = better)
  // ── Persistence (a "stickiness" signature) ────────────────────
  /** Devices grouped by how many days they were observed originating from this ZIP. */
  persistence: {
    onceOnly: number;     // exactly 1 day
    casual: number;       // 2-7 days
    regular: number;      // 8-30 days
    resident: number;     // 30+ days
  };
  // ── Geo ───────────────────────────────────────────────────────
  /** Geographic centroid of all origin points in this ZIP. */
  centroid: { lat: number; lng: number };
  /** Top 5 H3 res-10 cells (~70m hex) within this ZIP — sub-zip hotspots for activation. */
  topH3Cells: Array<{ h3: string; lat: number; lng: number; devices: number; pings: number }>;
}

export interface RegionSummary {
  region: string;
  devices: number;
  zips: number;
  /** Share of total matched devices in this region (0..1). */
  shareOfTotal: number;
}

export interface FullSchemaEnrichment {
  /** When the FULL schema sniff confirmed geo_fields presence. */
  detectedAt: string;
  /** Per-ZIP enriched signatures (only for matched ZIPs). */
  zipSignatures: ZipSignature[];
  /** Region rollup, sorted by devices desc. */
  regionSummary: RegionSummary[];
  /** Global quality histogram across all matched devices. */
  qualityHistogram: { high: number; medium: number; low: number };
}

export interface PostalMaidResult {
  dataset: string;
  analyzedAt: string;
  filters: PostalMaidFilters;
  methodology: {
    approach: 'first_ping_per_day_reverse_geocoded' | 'first_ping_per_day_geo_fields';
    description: string;
    accuracyThresholdMeters: number;
    coordinatePrecision: number;
    /** FULL-schema fast path uses geo_fields directly — no Node-side reverse geocoding. */
    fastPath?: boolean;
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
  /** Present when dataset has FULL schema (geo_fields populated). */
  fullSchema?: FullSchemaEnrichment;
}
