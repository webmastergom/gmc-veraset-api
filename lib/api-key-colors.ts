/**
 * Deterministic color mapping for API key badges.
 * Each API key name always gets the same color.
 */

// Maximally distinct colors on dark backgrounds — no two should look alike
const COLOR_PALETTE = [
  { bg: 'bg-violet-500/10', text: 'text-violet-400', border: 'border-violet-500/20', bgSolid: 'bg-violet-500/20' },
  { bg: 'bg-pink-500/10', text: 'text-pink-400', border: 'border-pink-500/20', bgSolid: 'bg-pink-500/20' },
  { bg: 'bg-cyan-500/10', text: 'text-cyan-400', border: 'border-cyan-500/20', bgSolid: 'bg-cyan-500/20' },
  { bg: 'bg-orange-500/10', text: 'text-orange-400', border: 'border-orange-500/20', bgSolid: 'bg-orange-500/20' },
  { bg: 'bg-emerald-500/10', text: 'text-emerald-400', border: 'border-emerald-500/20', bgSolid: 'bg-emerald-500/20' },
  { bg: 'bg-red-500/10', text: 'text-red-400', border: 'border-red-500/20', bgSolid: 'bg-red-500/20' },
  { bg: 'bg-amber-500/10', text: 'text-amber-400', border: 'border-amber-500/20', bgSolid: 'bg-amber-500/20' },
  { bg: 'bg-sky-500/10', text: 'text-sky-400', border: 'border-sky-500/20', bgSolid: 'bg-sky-500/20' },
] as const;

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash |= 0; // Convert to 32bit integer
  }
  return Math.abs(hash);
}

export function getApiKeyColor(apiKeyName?: string) {
  if (!apiKeyName) {
    // Default blue for unknown external jobs
    return { bg: 'bg-blue-500/10', text: 'text-blue-400', border: 'border-blue-500/20', bgSolid: 'bg-blue-500/20' };
  }
  const index = hashString(apiKeyName) % COLOR_PALETTE.length;
  return COLOR_PALETTE[index];
}
