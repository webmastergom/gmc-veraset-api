/**
 * Infers country ISO code from a job name string.
 * Used as fallback when the country field is not explicitly set on a job.
 */

const COUNTRY_PATTERNS: Array<{ pattern: RegExp; code: string }> = [
  // Spain
  { pattern: /\bspain\b/i, code: 'ES' },
  { pattern: /\bmadrid\b/i, code: 'ES' },
  { pattern: /\bbarcelona\b/i, code: 'ES' },
  // France
  { pattern: /\bfrance\b/i, code: 'FR' },
  { pattern: /\bparis\b/i, code: 'FR' },
  { pattern: /\bmartyrs\b/i, code: 'FR' },
  { pattern: /\bchappe\b/i, code: 'FR' },
  // United Kingdom
  { pattern: /\buk\b/i, code: 'UK' },
  { pattern: /\bunited kingdom\b/i, code: 'UK' },
  { pattern: /\blondon\b/i, code: 'UK' },
  { pattern: /\blionel\b/i, code: 'UK' },
  // Mexico
  { pattern: /\bmexico\b/i, code: 'MX' },
  { pattern: /\bméxico\b/i, code: 'MX' },
  // Panama
  { pattern: /\bpanama\b/i, code: 'PA' },
  { pattern: /\bpanamá\b/i, code: 'PA' },
  // Central America
  { pattern: /\bcosta rica\b/i, code: 'CR' },
  { pattern: /\bel salvador\b/i, code: 'SV' },
  { pattern: /\bguatemala\b/i, code: 'GT' },
  { pattern: /\bhonduras\b/i, code: 'HN' },
  { pattern: /\bnicaragua\b/i, code: 'NI' },
  // Caribbean
  { pattern: /\bdominican\b/i, code: 'DO' },
  // Europe
  { pattern: /\bgermany\b/i, code: 'DE' },
  { pattern: /\bitaly\b/i, code: 'IT' },
  { pattern: /\bnetherlands\b/i, code: 'NL' },
  { pattern: /\bportugal\b/i, code: 'PT' },
  { pattern: /\bsweden\b/i, code: 'SE' },
  { pattern: /\bireland\b/i, code: 'IE' },
  { pattern: /\bbelgium\b/i, code: 'BE' },
  // South America
  { pattern: /\bargentina\b/i, code: 'AR' },
  { pattern: /\bchile\b/i, code: 'CL' },
  { pattern: /\bcolombia\b/i, code: 'CO' },
  { pattern: /\becuador\b/i, code: 'EC' },
];

export function inferCountryFromName(name: string): string {
  for (const { pattern, code } of COUNTRY_PATTERNS) {
    if (pattern.test(name)) return code;
  }
  return '';
}
