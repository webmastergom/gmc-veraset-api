/**
 * Country (ISO2) → primary IANA timezone for converting Veraset's UTC
 * timestamps to local time when computing hour-of-day buckets, day-of-week
 * filters, and night/work shifts.
 *
 * Veraset stores all pings in UTC. Without this conversion, local
 * "afternoon" visits in MX (UTC-6) end up in the night bucket (UTC 22+)
 * and dealership traffic looks like overnight activity.
 *
 * For multi-tz countries we pick a "primary" zone — close enough for
 * coarse hour buckets at the country level. (US uses NYC, BR uses São
 * Paulo, etc.) For per-region precision we'd need state-level mapping.
 */

const COUNTRY_TZ: Record<string, string> = {
  // North & Central America
  MX: 'America/Mexico_City',
  US: 'America/New_York',
  CA: 'America/Toronto',
  GT: 'America/Guatemala',
  HN: 'America/Tegucigalpa',
  SV: 'America/El_Salvador',
  NI: 'America/Managua',
  CR: 'America/Costa_Rica',
  PA: 'America/Panama',
  // Caribbean
  CU: 'America/Havana',
  DO: 'America/Santo_Domingo',
  PR: 'America/Puerto_Rico',
  // South America
  AR: 'America/Argentina/Buenos_Aires',
  BO: 'America/La_Paz',
  BR: 'America/Sao_Paulo',
  CL: 'America/Santiago',
  CO: 'America/Bogota',
  EC: 'America/Guayaquil',
  PE: 'America/Lima',
  PY: 'America/Asuncion',
  UY: 'America/Montevideo',
  VE: 'America/Caracas',
  // Europe
  ES: 'Europe/Madrid',
  PT: 'Europe/Lisbon',
  FR: 'Europe/Paris',
  GB: 'Europe/London',
  DE: 'Europe/Berlin',
  IT: 'Europe/Rome',
};

export const DEFAULT_TZ = 'UTC';

/** Resolve country → IANA timezone. Returns 'UTC' when unknown. */
export function tzForCountry(country: string | null | undefined): string {
  if (!country) return DEFAULT_TZ;
  return COUNTRY_TZ[country.toUpperCase()] || DEFAULT_TZ;
}

/**
 * Build a SQL fragment that converts a UTC timestamp column to local time.
 * When tz is empty/UTC, returns the column as-is (no overhead). Trino's
 * `at_timezone()` handles DST automatically.
 *
 * Example:
 *   localTimestamp('utc_timestamp', 'America/Mexico_City')
 *     → "at_timezone(utc_timestamp, 'America/Mexico_City')"
 */
export function localTimestamp(tsCol: string, tz: string | null | undefined): string {
  if (!tz || tz === 'UTC') return tsCol;
  return `at_timezone(${tsCol}, '${tz}')`;
}
