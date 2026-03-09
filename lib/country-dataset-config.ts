/**
 * Country → default dataset mapping for external postal-maid API.
 *
 * Reads from S3 config (managed via Settings UI) with a local fallback.
 * The team manages this via /settings in the UI.
 */

import { getConfig } from './s3-config';

export interface CountryDatasetEntry {
  /** S3/Athena dataset name (same string passed to analyzePostalMaid) */
  dataset: string;
  /** Human-readable country label for logs/error messages */
  label: string;
}

interface CountryDatasetConfig {
  entries: Record<string, CountryDatasetEntry>;
  updatedAt: string;
}

const CONFIG_KEY = 'country-dataset-config';

/** Look up the dataset for a country code. Returns undefined if not configured. */
export async function getDatasetForCountry(country: string): Promise<CountryDatasetEntry | undefined> {
  const config = await getConfig<CountryDatasetConfig>(CONFIG_KEY);
  return config?.entries?.[country.toUpperCase()];
}

/** All currently configured country codes (sorted). */
export async function getConfiguredCountries(): Promise<string[]> {
  const config = await getConfig<CountryDatasetConfig>(CONFIG_KEY);
  return config?.entries ? Object.keys(config.entries).sort() : [];
}
