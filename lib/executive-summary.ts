/**
 * Auto-generated English executive summary for a job or megajob analysis.
 * Synthesizes catchment + temporal + hourly + visits + mobility reports
 * into a short, copy-paste-friendly text block for commercial proposals.
 *
 * Each summary returns:
 *   - `headline`: a single sentence with the scale + addressable core.
 *   - `bullets`: 4-6 short statements for slides / decks.
 *   - `paragraph`: a 3-4 sentence narrative version.
 *
 * Numbers are pre-formatted (commas, percentages, ranges); the caller
 * just renders the strings.
 *
 * Always in English regardless of the project's UI language.
 */

export interface ExecutiveSummary {
  generatedAt: string;
  headline: string;
  bullets: string[];
  paragraph: string;
}

export interface ExecutiveSummaryInputs {
  /** Human-readable label for the analysis subject (job name, megajob name). */
  subjectLabel: string;
  /** Optional: total POIs analyzed (visits report). */
  totalPois?: number;
  /** Date range covered. */
  dateRange?: { from: string; to: string };
  /** Country ISO2 code (for context lines). */
  country?: string;

  // ── Numeric inputs (any subset; absent ones are skipped in copy) ──
  totalUniqueDevices?: number;
  totalDeviceDays?: number;
  totalPings?: number;
  totalVisitDays?: number;

  /** Catchment: number of zip codes scored. */
  totalZips?: number;
  /** Catchment: top zip code + its share. */
  topZip?: { zip: string; city?: string; share: number };
  /** Catchment: capture-ring summary. */
  captureRings?: {
    p70: { zips: number; share: number };
    p80: { zips: number; share: number };
    p90: { zips: number; share: number };
  };

  /** Temporal/hourly: peak hour [0..23] in local time. */
  peakHour?: { hour: number; share: number };
  /** Temporal: peak day of week (1=Mon..7=Sun) + share of weekly traffic. */
  peakDow?: { dow: number; share: number };
  /** Share of pings on Saturday+Sunday. */
  weekendShare?: number;

  /** Engagement: average dwell minutes across all visits. */
  avgDwellMinutes?: number;
  /** Engagement: median repeat-visit frequency (visits per unique device). */
  medianFrequency?: number;
  /** Loyalty: % of devices with 3+ visits. */
  repeatVisitorShare?: number;

  /** Mobility: top POI category visited within ±2h of the focal POIs. */
  topMobilityCategory?: { category: string; share: number };

  /** Persona run (if available) — number of personas + dominant one. */
  personaSummary?: {
    totalPersonas: number;
    dominantPersona: { name: string; share: number };
  };
}

const DOW_NAMES = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];

const fmtNum = (n: number): string => n.toLocaleString('en-US');

const fmtPct = (n: number): string =>
  n >= 10 ? `${Math.round(n)}%` : `${n.toFixed(1)}%`;

const fmtRange = (from?: string, to?: string): string => {
  if (!from && !to) return '';
  if (from === to || !to) return from || '';
  return `${from} to ${to}`;
};

const fmtHour = (h: number): string => {
  const ampm = h < 12 ? 'AM' : 'PM';
  const h12 = h % 12 === 0 ? 12 : h % 12;
  return `${h12}${ampm} (local time)`;
};

/**
 * Build the executive summary. Skips any line whose inputs are absent
 * so a partial report still produces clean copy.
 */
export function buildExecutiveSummary(i: ExecutiveSummaryInputs): ExecutiveSummary {
  const bullets: string[] = [];

  // ── Scale ─────────────────────────────────────────────────────────
  const scaleParts: string[] = [];
  if (i.totalUniqueDevices != null) {
    scaleParts.push(`${fmtNum(i.totalUniqueDevices)} unique devices`);
  }
  if (i.totalDeviceDays != null) {
    scaleParts.push(`${fmtNum(i.totalDeviceDays)} device-days`);
  } else if (i.totalVisitDays != null) {
    scaleParts.push(`${fmtNum(i.totalVisitDays)} visit-days`);
  }
  if (scaleParts.length > 0) {
    let scale = `**Scale**: ${scaleParts.join(' across ')}`;
    if (i.totalPois && i.totalPois > 1) scale += ` to ${fmtNum(i.totalPois)} POIs`;
    if (i.dateRange?.from || i.dateRange?.to) {
      const range = fmtRange(i.dateRange.from, i.dateRange.to);
      if (range) scale += ` (${range})`;
    }
    bullets.push(`${scale}.`);
  }

  // ── Trade area / addressable core ─────────────────────────────────
  if (i.captureRings && i.totalZips) {
    const p70 = i.captureRings.p70;
    const pctOfTotal = i.totalZips > 0 ? Math.round((p70.zips / i.totalZips) * 100) : 0;
    bullets.push(
      `**Addressable core**: the top ${fmtNum(p70.zips)} zip codes ` +
      `(${pctOfTotal}% of ${fmtNum(i.totalZips)} measured) hold ${fmtPct(p70.share)} of the catchment; ` +
      `the top ${fmtNum(i.captureRings.p90.zips)} hold ${fmtPct(i.captureRings.p90.share)}.`
    );
  } else if (i.topZip) {
    bullets.push(
      `**Top zip**: ${i.topZip.zip}${i.topZip.city ? ` (${i.topZip.city})` : ''} delivers ${fmtPct(i.topZip.share)} of the catchment.`
    );
  }

  // ── Timing ────────────────────────────────────────────────────────
  const timingParts: string[] = [];
  if (i.peakHour) {
    timingParts.push(`peaks at ${fmtHour(i.peakHour.hour)} (${fmtPct(i.peakHour.share)} of daily traffic)`);
  }
  if (i.peakDow) {
    timingParts.push(`busiest day is ${DOW_NAMES[i.peakDow.dow] || `day ${i.peakDow.dow}`} (${fmtPct(i.peakDow.share)} of weekly traffic)`);
  }
  if (i.weekendShare != null) {
    timingParts.push(`${fmtPct(i.weekendShare)} of visits fall on weekends`);
  }
  if (timingParts.length > 0) {
    bullets.push(`**Timing**: ${timingParts.join('; ')}.`);
  }

  // ── Engagement ───────────────────────────────────────────────────
  const engagementParts: string[] = [];
  if (i.avgDwellMinutes != null) {
    const dwellRounded = Math.round(i.avgDwellMinutes);
    engagementParts.push(`average dwell ${dwellRounded} min`);
  }
  if (i.medianFrequency != null) {
    engagementParts.push(`median repeat frequency of ${i.medianFrequency.toFixed(1)} visits per device`);
  }
  if (i.repeatVisitorShare != null) {
    engagementParts.push(`${fmtPct(i.repeatVisitorShare)} of devices return 3+ times`);
  }
  if (engagementParts.length > 0) {
    bullets.push(`**Engagement**: ${engagementParts.join(', ')}.`);
  }

  // ── Mobility ─────────────────────────────────────────────────────
  if (i.topMobilityCategory) {
    bullets.push(
      `**Adjacent behavior**: the top destination category within ±2h of the POIs is ` +
      `*${i.topMobilityCategory.category}* (${fmtPct(i.topMobilityCategory.share)} of mobility traffic).`
    );
  }

  // ── Personas ─────────────────────────────────────────────────────
  if (i.personaSummary) {
    bullets.push(
      `**Personas**: ${i.personaSummary.totalPersonas} distinct audience segments identified; ` +
      `the dominant cluster "*${i.personaSummary.dominantPersona.name}*" represents ${fmtPct(i.personaSummary.dominantPersona.share)} of the base.`
    );
  }

  // ── Headline (one-sentence) ──────────────────────────────────────
  const headlineParts: string[] = [];
  if (i.totalUniqueDevices != null) headlineParts.push(`${fmtNum(i.totalUniqueDevices)} unique devices`);
  else if (i.totalDeviceDays != null) headlineParts.push(`${fmtNum(i.totalDeviceDays)} device-days`);
  if (i.captureRings) {
    headlineParts.push(`top ${fmtNum(i.captureRings.p80.zips)} zips capture ${fmtPct(i.captureRings.p80.share)}`);
  }
  const headline = headlineParts.length > 0
    ? `${i.subjectLabel}: ${headlineParts.join('; ')}.`
    : `${i.subjectLabel}: analysis ready.`;

  // ── Paragraph (3-4 sentence narrative) ───────────────────────────
  const paraSentences: string[] = [];
  if (scaleParts.length > 0) {
    let s = `${i.subjectLabel} reached ${scaleParts.join(' (')}`;
    if (scaleParts.length > 1) s += ')';
    if (i.dateRange?.from || i.dateRange?.to) {
      const r = fmtRange(i.dateRange.from, i.dateRange.to);
      if (r) s += ` between ${r}`;
    }
    s += '.';
    paraSentences.push(s);
  }
  if (i.captureRings) {
    paraSentences.push(
      `The addressable trade area is highly concentrated: the top ${fmtNum(i.captureRings.p70.zips)} zip codes ` +
      `cumulatively account for ${fmtPct(i.captureRings.p70.share)} of all device-days, and ${fmtNum(i.captureRings.p90.zips)} zips for ${fmtPct(i.captureRings.p90.share)}.`
    );
  }
  if (i.peakHour && i.weekendShare != null) {
    paraSentences.push(
      `Visit timing peaks at ${fmtHour(i.peakHour.hour)} with ${fmtPct(i.weekendShare)} of traffic falling on weekends.`
    );
  } else if (i.peakHour) {
    paraSentences.push(`Visit timing peaks at ${fmtHour(i.peakHour.hour)}.`);
  }
  if (i.avgDwellMinutes != null || i.repeatVisitorShare != null) {
    const eng: string[] = [];
    if (i.avgDwellMinutes != null) eng.push(`an average dwell of ${Math.round(i.avgDwellMinutes)} minutes`);
    if (i.repeatVisitorShare != null) eng.push(`${fmtPct(i.repeatVisitorShare)} of devices returning 3+ times`);
    paraSentences.push(`Engagement signals show ${eng.join(' and ')}.`);
  }
  const paragraph = paraSentences.join(' ');

  return {
    generatedAt: new Date().toISOString(),
    headline,
    bullets,
    paragraph,
  };
}
