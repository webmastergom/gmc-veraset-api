/**
 * Country → default dataset mapping for external postal-maid API.
 *
 * The team populates this manually as datasets become available per country.
 * The dataset name must match the S3 prefix that getTableName() converts
 * to an Athena table.
 */

export interface CountryDatasetEntry {
  /** S3/Athena dataset name (same string passed to analyzePostalMaid) */
  dataset: string;
  /** Human-readable country label for logs/error messages */
  label: string;
}

/**
 * Map of ISO 3166-1 alpha-2 country code → dataset entry.
 * Add entries here as datasets are onboarded per country.
 */
export const COUNTRY_DATASETS: Record<string, CountryDatasetEntry> = {
  // ES: { dataset: 'Spain-Cities-Pois-March-2026', label: 'Spain' },
  // FR: { dataset: 'France-90pct-March-2026', label: 'France' },
  // MX: { dataset: 'Mexico-Cities-Pois-March-2026', label: 'Mexico' },
};

/** Look up the dataset for a country code. Returns undefined if not configured. */
export function getDatasetForCountry(country: string): CountryDatasetEntry | undefined {
  return COUNTRY_DATASETS[country.toUpperCase()];
}

/** All currently configured country codes (sorted). */
export function getConfiguredCountries(): string[] {
  return Object.keys(COUNTRY_DATASETS).sort();
}
