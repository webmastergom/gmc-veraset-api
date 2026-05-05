/**
 * Mega-job report consolidation.
 * Merges analysis results from multiple sub-jobs into unified reports.
 */

import { getTableName, startQueryAsync, fetchQueryResults, startCTASAsync, tempTableName } from './athena';
import { type Job } from './jobs';
import {
  type ConsolidatedQueryHandle,
  type DwellFilter,
  type PoiCoord,
  type VisitorFilter,
  buildHourFilterClause,
  hasMinVisitsFilter,
  buildAtPoiPingsCTE,
  buildDwellFilterCTEs,
  hasDwellFilter,
} from './mega-consolidation-queries';
import { getConfig, putConfig } from './s3-config';
import { megaReportKey } from './mega-jobs';

// ── Types ─────────────────────────────────────────────────────────────

export interface ConsolidatedVisitByPoi {
  poiId: string;
  poiName: string;
  visits: number;
  devices: number; // exact unique via UNION ALL + COUNT(DISTINCT)
}

export interface ConsolidatedVisitsReport {
  megaJobId: string;
  analyzedAt: string;
  totalPois: number;
  visitsByPoi: ConsolidatedVisitByPoi[];
}

export interface ConsolidatedTemporalTrends {
  megaJobId: string;
  analyzedAt: string;
  /** Daily data across full date range (from all sub-jobs, sorted) */
  daily: Array<{ date: string; pings: number; devices: number }>;
  /** Weekly aggregation */
  weekly: Array<{ weekStart: string; pings: number; devices: number }>;
  /** Monthly aggregation */
  monthly: Array<{ month: string; pings: number; devices: number }>;
  /** Day-of-week averages (0=Sun, 6=Sat) */
  dayOfWeek: Array<{ day: number; dayName: string; avgPings: number; avgDevices: number }>;
}

export interface ODCluster {
  originLat: number;
  originLng: number;
  destLat: number;
  destLng: number;
  departureHour: number;
  poiArrivalHour: number;
  deviceDays: number;
}

export interface ConsolidatedODReport {
  megaJobId: string;
  analyzedAt: string;
  totalDeviceDays: number;
  /** Aggregated origin clusters with zip code (after geocoding) */
  origins: Array<{
    lat: number;
    lng: number;
    zipCode: string;
    city: string;
    country: string;
    deviceDays: number;
  }>;
  /** Aggregated destination clusters with zip code */
  destinations: Array<{
    lat: number;
    lng: number;
    zipCode: string;
    city: string;
    country: string;
    deviceDays: number;
  }>;
  /** Departure hour distribution (0-23) */
  departureByHour: Array<{ hour: number; deviceDays: number }>;
  /** POI arrival hour distribution (0-23) */
  arrivalByHour: Array<{ hour: number; deviceDays: number }>;
}

export interface ConsolidatedHourlyReport {
  megaJobId: string;
  analyzedAt: string;
  /** POI activity by hour of day */
  hourly: Array<{ hour: number; pings: number; devices: number }>;
}

export interface ConsolidatedCatchmentReport {
  megaJobId: string;
  analyzedAt: string;
  totalDeviceDays: number;
  /** Origins aggregated by zip code */
  byZipCode: Array<{
    zipCode: string;
    city: string;
    country: string;
    lat: number;
    lng: number;
    deviceDays: number;
    sharePercentage: number;
  }>;
  /** Departure hour distribution */
  departureByHour: Array<{ hour: number; deviceDays: number }>;
}

export interface ConsolidatedAffinityReport {
  megaJobId: string;
  analyzedAt: string;
  byZipCode: Array<{
    postalCode: string;
    city: string;
    country: string;
    affinityIndex: number;
    totalVisits: number;
    uniqueDevices: number;
    avgDwell: number;
    avgFrequency: number;
  }>;
}

export interface ConsolidatedMobilityReport {
  megaJobId: string;
  analyzedAt: string;
  /** Top POI categories visited ±2h of target POI */
  categories: Array<{
    category: string;
    deviceDays: number;
    hits: number;
  }>;
  /** Categories visited BEFORE arriving at target POI (up to 2h prior) */
  before: Array<{
    category: string;
    deviceDays: number;
    hits: number;
  }>;
  /** Categories visited AFTER leaving target POI (up to 2h after) */
  after: Array<{
    category: string;
    deviceDays: number;
    hits: number;
  }>;
}

// ── Visits by POI consolidation (Athena UNION ALL) ────────────────────

/**
 * Build and execute a UNION ALL query across all sub-job tables
 * for consolidated visits by POI with exact unique device counts.
 *
 * Returns the Athena queryId (caller must poll for completion).
 */
export async function startConsolidatedVisitsQuery(
  megaJobId: string,
  runId: string,
  subJobs: Job[],
  poiCoords?: PoiCoord[],
  dwell?: DwellFilter,
  poiTableRef?: string,
  visitorFilter?: VisitorFilter,
): Promise<ConsolidatedQueryHandle> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs');

  const useDwell = hasDwellFilter(dwell);
  const useMinVisits = hasMinVisitsFilter(visitorFilter);
  const hourClause = buildHourFilterClause(visitorFilter);
  const useHour = hourClause.length > 0;
  // The visits query historically uses an inline poi_ids approach (UNION ALL of
  // (poi_id, ad_id) pairs from each sub-job's raw table) which doesn't apply
  // any filter. That made visits disagree with the rest of the reports
  // whenever a dwell/hour/minVisits filter was active — the box "total devices"
  // could end up smaller than visits for a single POI. To keep the megajob
  // graphs internally consistent, when ANY filter is active we restrict the
  // poi_ids stream to (ad_id, date) pairs that pass the same filters that
  // the at_poi_pings-based queries use.
  const filtersActive = useDwell || useMinVisits || useHour;

  let sql: string;
  if (!filtersActive || !poiCoords?.length) {
    // No filters → keep the cheap original SQL (also the legacy fallback when
    // a megajob has no poi coordinates available for the spatial pre-filter).
    const unionParts = syncedJobs.map((job) => {
      const tableName = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT poi_id, ad_id FROM ${tableName} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != ''`;
    });
    sql = `
    SELECT
      poi_id,
      COUNT(*) as visits,
      COUNT(DISTINCT ad_id) as devices
    FROM (
      ${unionParts.join('\n      UNION ALL\n      ')}
    )
    GROUP BY poi_id
    ORDER BY visits DESC
    `;
  } else {
    // Filters active → JOIN against the same qualified-visitor-day set the
    // other reports use. We build:
    //   1. all_pings: union of raw pings (with hour filter pushed down)
    //   2. at_poi_pings: spatial filter to actual POI proximity
    //   3. dwell_filtered: at_poi (ad_id, date) pairs whose dwell matches
    //   4. qualified_visitor_days: dwell_filtered (or at_poi_pings if no
    //      dwell) further narrowed by minVisits (≥N distinct visit-days)
    // Then UNION ALL the (poi_id, ad_id, date) stream and INNER JOIN against
    // qualified_visitor_days. Result mirrors the visits semantics but only
    // for visits where the device-day passed every active filter.

    // Build pings union via inline composition (we can't reuse buildUnionAll
    // here because we need `date` in the row for the dwell + minVisits CTEs).
    const pingsUnion = syncedJobs.map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT ad_id, date, utc_timestamp,
                TRY_CAST(latitude AS DOUBLE) as lat,
                TRY_CAST(longitude AS DOUBLE) as lng
              FROM ${table}
              WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
                AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
                AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
                AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < 500)
                ${hourClause}`;
    }).join('\n      UNION ALL\n      ');

    const poiVisitsUnion = syncedJobs.map((job) => {
      const table = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
      return `SELECT poi_id, ad_id, date FROM ${table}
              CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
              WHERE poi_id IS NOT NULL AND poi_id != ''
                AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
                ${hourClause}`;
    }).join('\n      UNION ALL\n      ');

    const dwellCTEs = buildDwellFilterCTEs(dwell);
    // Source for the qualified visitor-day set after dwell filter (if dwell
    // active) or just at_poi_pings (if only hour/minVisits is active).
    const baseCTE = useDwell
      ? `SELECT DISTINCT ad_id, date FROM dwell_filtered`
      : `SELECT DISTINCT ad_id, date FROM at_poi_pings`;

    // minVisits applies on top: keep only ad_ids whose at-POI visit-day count ≥ N.
    const minVisits = visitorFilter?.minVisits ?? 1;
    const minVisitsCTE = useMinVisits ? `,
    qualified_visitors AS (
      SELECT ad_id FROM (${baseCTE}) base
      GROUP BY ad_id
      HAVING COUNT(DISTINCT date) >= ${minVisits}
    ),
    qualified_visitor_days AS (
      SELECT b.ad_id, b.date FROM (${baseCTE}) b
      INNER JOIN qualified_visitors qv ON b.ad_id = qv.ad_id
    )` : `,
    qualified_visitor_days AS (
      ${baseCTE}
    )`;

    sql = `
    WITH
    all_pings AS (
      ${pingsUnion}
    ),
    ${buildAtPoiPingsCTE('all_pings', poiCoords, poiTableRef)}${dwellCTEs}${minVisitsCTE},
    poi_visits AS (
      ${poiVisitsUnion}
    )
    SELECT
      pv.poi_id,
      COUNT(*) as visits,
      COUNT(DISTINCT pv.ad_id) as devices
    FROM poi_visits pv
    INNER JOIN qualified_visitor_days qvd
      ON pv.ad_id = qvd.ad_id AND pv.date = qvd.date
    GROUP BY pv.poi_id
    ORDER BY visits DESC
    `;
  }

  console.log(`[MEGA-CONSOLIDATION] Starting visits query (CTAS) across ${syncedJobs.length} tables (filters: dwell=${useDwell}, hour=${useHour}, minVisits=${useMinVisits})`);
  const ctasTable = tempTableName('mc_visits', `${megaJobId}_${runId}`);
  const queryId = await startCTASAsync(sql, ctasTable);
  return { queryId, ctasTable };
}

/**
 * Parse the visits query results and apply POI name mapping from all sub-jobs.
 * Uses fetchQueryResults which returns Record<string, any>[].
 */
export function parseConsolidatedVisits(
  rows: Record<string, any>[],
  subJobs: Job[],
  /** Optional external name map (e.g. from POI collection GeoJSON) */
  externalNames?: Map<string, string>
): ConsolidatedVisitByPoi[] {
  // Build unified POI mapping: verasetId → { originalId, name }
  // Since each sub-job has its own geo_radius_X → originalId mapping,
  // and the Athena query returns geo_radius_X IDs, we need all mappings.
  const poiNameMap = new Map<string, string>();

  // First, load external names (from POI collection GeoJSON)
  if (externalNames) {
    for (const [id, name] of externalNames) {
      poiNameMap.set(id, name);
    }
  }

  for (const job of subJobs) {
    if (job.poiMapping) {
      for (const [verasetId, originalId] of Object.entries(job.poiMapping)) {
        // Prefer external name, then job.poiNames, then originalId
        const name = externalNames?.get(originalId) || externalNames?.get(verasetId) || job.poiNames?.[verasetId] || originalId;
        poiNameMap.set(verasetId, name);
        poiNameMap.set(originalId, name);
      }
    }
  }

  const results: ConsolidatedVisitByPoi[] = [];
  for (const row of rows) {
    const poiId = String(row.poi_id || '').replace(/^"|"$/g, '').trim();
    if (!poiId) continue;

    results.push({
      poiId,
      poiName: poiNameMap.get(poiId) || poiId,
      visits: parseInt(row.visits, 10) || 0,
      devices: parseInt(row.devices, 10) || 0,
    });
  }

  return results;
}

// ── Temporal trends (in-memory from per-sub-job analysis) ─────────────

/**
 * Build temporal trends from sub-job daily data.
 * Each sub-job's dailyData covers its date chunk. Concatenating gives the full range.
 */
export function buildTemporalTrends(
  megaJobId: string,
  dailyDataByJob: Array<{ date: string; pings: number; devices: number }[]>
): ConsolidatedTemporalTrends {
  // Merge all daily entries, dedup by date (sum if overlapping — shouldn't happen with proper splits)
  const byDate = new Map<string, { pings: number; devices: number }>();

  for (const daily of dailyDataByJob) {
    for (const d of daily) {
      const existing = byDate.get(d.date);
      if (existing) {
        existing.pings += d.pings;
        existing.devices += d.devices;
      } else {
        byDate.set(d.date, { pings: d.pings, devices: d.devices });
      }
    }
  }

  // Sort by date
  const daily = Array.from(byDate.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, data]) => ({ date, ...data }));

  // Weekly aggregation (ISO week, starting Monday)
  const weeklyMap = new Map<string, { pings: number; devices: number }>();
  for (const d of daily) {
    const dt = new Date(d.date + 'T00:00:00Z');
    const dayOfWeek = dt.getUTCDay();
    const mondayOffset = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
    const monday = new Date(dt);
    monday.setUTCDate(monday.getUTCDate() - mondayOffset);
    const weekKey = monday.toISOString().split('T')[0];

    const existing = weeklyMap.get(weekKey);
    if (existing) {
      existing.pings += d.pings;
      existing.devices += d.devices;
    } else {
      weeklyMap.set(weekKey, { pings: d.pings, devices: d.devices });
    }
  }
  const weekly = Array.from(weeklyMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([weekStart, data]) => ({ weekStart, ...data }));

  // Monthly aggregation
  const monthlyMap = new Map<string, { pings: number; devices: number }>();
  for (const d of daily) {
    const month = d.date.substring(0, 7); // YYYY-MM
    const existing = monthlyMap.get(month);
    if (existing) {
      existing.pings += d.pings;
      existing.devices += d.devices;
    } else {
      monthlyMap.set(month, { pings: d.pings, devices: d.devices });
    }
  }
  const monthly = Array.from(monthlyMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([month, data]) => ({ month, ...data }));

  // Day-of-week averages
  const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const dowAccum = Array.from({ length: 7 }, () => ({ pings: 0, devices: 0, count: 0 }));
  for (const d of daily) {
    const dow = new Date(d.date + 'T00:00:00Z').getUTCDay();
    dowAccum[dow].pings += d.pings;
    dowAccum[dow].devices += d.devices;
    dowAccum[dow].count += 1;
  }
  const dayOfWeek = dowAccum.map((acc, i) => ({
    day: i,
    dayName: dayNames[i],
    avgPings: acc.count > 0 ? Math.round(acc.pings / acc.count) : 0,
    avgDevices: acc.count > 0 ? Math.round(acc.devices / acc.count) : 0,
  }));

  return {
    megaJobId,
    analyzedAt: new Date().toISOString(),
    daily,
    weekly,
    monthly,
    dayOfWeek,
  };
}

// ── OD result parser ─────────────────────────────────────────────────

/**
 * Parse OD query results into origin/destination clusters and hourly distributions.
 * Geocoding is done separately by the consolidation route.
 */
export function parseConsolidatedOD(
  rows: Record<string, any>[],
): {
  clusters: ODCluster[];
  totalDeviceDays: number;
} {
  let totalDeviceDays = 0;
  const clusters: ODCluster[] = [];

  for (const row of rows) {
    const deviceDays = parseInt(row.device_days, 10) || 0;
    totalDeviceDays += deviceDays;
    clusters.push({
      originLat: parseFloat(row.origin_lat) || 0,
      originLng: parseFloat(row.origin_lng) || 0,
      destLat: parseFloat(row.dest_lat) || 0,
      destLng: parseFloat(row.dest_lng) || 0,
      departureHour: parseInt(row.departure_hour, 10) || 0,
      poiArrivalHour: parseInt(row.poi_arrival_hour, 10) || 0,
      deviceDays,
    });
  }

  return { clusters, totalDeviceDays };
}

/**
 * Aggregate OD clusters into origin/destination zip code tables.
 * coordToZip is a map from "lat,lng" → { zipCode, city, country }.
 */
export function buildODReport(
  megaJobId: string,
  clusters: ODCluster[],
  coordToZip: Map<string, { zipCode: string; city: string; country: string }>,
): ConsolidatedODReport {
  const originByZip = new Map<string, { lat: number; lng: number; zip: string; city: string; country: string; deviceDays: number }>();
  const destByZip = new Map<string, { lat: number; lng: number; zip: string; city: string; country: string; deviceDays: number }>();
  const departureByHour = new Map<number, number>();
  const arrivalByHour = new Map<number, number>();
  let totalDeviceDays = 0;

  for (const c of clusters) {
    totalDeviceDays += c.deviceDays;

    // Origins
    const oKey = `${c.originLat},${c.originLng}`;
    const oGeo = coordToZip.get(oKey) || { zipCode: 'UNKNOWN', city: 'UNKNOWN', country: 'UNKNOWN' };
    const oZipKey = `${oGeo.zipCode}|${oGeo.city}|${oGeo.country}`;
    const existing = originByZip.get(oZipKey);
    if (existing) {
      existing.deviceDays += c.deviceDays;
    } else {
      originByZip.set(oZipKey, { lat: c.originLat, lng: c.originLng, zip: oGeo.zipCode, city: oGeo.city, country: oGeo.country, deviceDays: c.deviceDays });
    }

    // Destinations
    const dKey = `${c.destLat},${c.destLng}`;
    const dGeo = coordToZip.get(dKey) || { zipCode: 'UNKNOWN', city: 'UNKNOWN', country: 'UNKNOWN' };
    const dZipKey = `${dGeo.zipCode}|${dGeo.city}|${dGeo.country}`;
    const dExisting = destByZip.get(dZipKey);
    if (dExisting) {
      dExisting.deviceDays += c.deviceDays;
    } else {
      destByZip.set(dZipKey, { lat: c.destLat, lng: c.destLng, zip: dGeo.zipCode, city: dGeo.city, country: dGeo.country, deviceDays: c.deviceDays });
    }

    // Hourly
    departureByHour.set(c.departureHour, (departureByHour.get(c.departureHour) || 0) + c.deviceDays);
    if (c.poiArrivalHour >= 0) {
      arrivalByHour.set(c.poiArrivalHour, (arrivalByHour.get(c.poiArrivalHour) || 0) + c.deviceDays);
    }
  }

  const origins = Array.from(originByZip.values())
    .map((v) => ({ lat: v.lat, lng: v.lng, zipCode: v.zip, city: v.city, country: v.country, deviceDays: v.deviceDays }))
    .sort((a, b) => b.deviceDays - a.deviceDays);

  const destinations = Array.from(destByZip.values())
    .map((v) => ({ lat: v.lat, lng: v.lng, zipCode: v.zip, city: v.city, country: v.country, deviceDays: v.deviceDays }))
    .sort((a, b) => b.deviceDays - a.deviceDays);

  return {
    megaJobId,
    analyzedAt: new Date().toISOString(),
    totalDeviceDays,
    origins,
    destinations,
    departureByHour: Array.from({ length: 24 }, (_, h) => ({ hour: h, deviceDays: departureByHour.get(h) || 0 })),
    arrivalByHour: Array.from({ length: 24 }, (_, h) => ({ hour: h, deviceDays: arrivalByHour.get(h) || 0 })),
  };
}

// ── Hourly result parser ─────────────────────────────────────────────

export function parseConsolidatedHourly(
  megaJobId: string,
  rows: Record<string, any>[],
): ConsolidatedHourlyReport {
  const hourMap = new Map<number, { pings: number; devices: number }>();
  for (const row of rows) {
    const hour = parseInt(row.touch_hour, 10) || 0;
    hourMap.set(hour, {
      pings: parseInt(row.pings, 10) || 0,
      devices: parseInt(row.devices, 10) || 0,
    });
  }

  return {
    megaJobId,
    analyzedAt: new Date().toISOString(),
    hourly: Array.from({ length: 24 }, (_, h) => ({
      hour: h,
      pings: hourMap.get(h)?.pings || 0,
      devices: hourMap.get(h)?.devices || 0,
    })),
  };
}

// ── Catchment result parser ──────────────────────────────────────────

/**
 * Parse catchment origins and aggregate by zip code after geocoding.
 */
export function buildCatchmentReport(
  megaJobId: string,
  rows: Record<string, any>[],
  coordToZip: Map<string, { zipCode: string; city: string; country: string }>,
): ConsolidatedCatchmentReport {
  const byZip = new Map<string, { lat: number; lng: number; zip: string; city: string; country: string; deviceDays: number }>();
  const hourMap = new Map<number, number>();
  let totalDeviceDays = 0;

  for (const row of rows) {
    const lat = parseFloat(row.origin_lat) || 0;
    const lng = parseFloat(row.origin_lng) || 0;
    const deviceDays = parseInt(row.device_days, 10) || 0;
    const hour = parseInt(row.departure_hour, 10) || 0;
    totalDeviceDays += deviceDays;

    const key = `${lat},${lng}`;
    const geo = coordToZip.get(key) || { zipCode: 'UNKNOWN', city: 'UNKNOWN', country: 'UNKNOWN' };
    const zipKey = `${geo.zipCode}|${geo.city}|${geo.country}`;

    const existing = byZip.get(zipKey);
    if (existing) {
      existing.deviceDays += deviceDays;
    } else {
      byZip.set(zipKey, { lat, lng, zip: geo.zipCode, city: geo.city, country: geo.country, deviceDays });
    }

    hourMap.set(hour, (hourMap.get(hour) || 0) + deviceDays);
  }

  return {
    megaJobId,
    analyzedAt: new Date().toISOString(),
    totalDeviceDays,
    byZipCode: Array.from(byZip.values())
      .map((v) => ({
        zipCode: v.zip, city: v.city, country: v.country, lat: v.lat, lng: v.lng, deviceDays: v.deviceDays,
        sharePercentage: totalDeviceDays > 0 ? Math.round((v.deviceDays / totalDeviceDays) * 10000) / 100 : 0,
      }))
      .sort((a, b) => b.deviceDays - a.deviceDays),
    departureByHour: Array.from({ length: 24 }, (_, h) => ({ hour: h, deviceDays: hourMap.get(h) || 0 })),
  };
}

// ── Mobility result parser ───────────────────────────────────────────

export function parseConsolidatedMobility(
  megaJobId: string,
  rows: Record<string, any>[],
): ConsolidatedMobilityReport {
  const all: Array<{ timing: string; category: string; deviceDays: number; hits: number }> = rows.map((row) => ({
    timing: String(row.timing || 'after'),
    category: String(row.category || 'UNKNOWN'),
    deviceDays: parseInt(row.device_days, 10) || 0,
    hits: parseInt(row.hits, 10) || 0,
  }));

  const before = all.filter((r) => r.timing === 'before').slice(0, 25);
  const after = all.filter((r) => r.timing === 'after').slice(0, 25);

  // Combined (legacy compat) — merge before + after, sum device_days
  const merged = new Map<string, { deviceDays: number; hits: number }>();
  for (const r of all) {
    const existing = merged.get(r.category);
    if (existing) {
      existing.deviceDays += r.deviceDays;
      existing.hits += r.hits;
    } else {
      merged.set(r.category, { deviceDays: r.deviceDays, hits: r.hits });
    }
  }
  const categories = Array.from(merged.entries())
    .map(([category, v]) => ({ category, ...v }))
    .sort((a, b) => b.deviceDays - a.deviceDays)
    .slice(0, 50);

  return {
    megaJobId,
    analyzedAt: new Date().toISOString(),
    categories,
    before,
    after,
  };
}

// ── Save / load consolidated reports ──────────────────────────────────

export async function saveConsolidatedReport(
  megaJobId: string,
  reportType: string,
  data: any
): Promise<string> {
  const key = megaReportKey(megaJobId, reportType);
  await putConfig(key, data, { compact: true });
  return `config/${key}.json`;
}

export async function getConsolidatedReport<T>(
  megaJobId: string,
  reportType: string
): Promise<T | null> {
  return await getConfig<T>(megaReportKey(megaJobId, reportType));
}

// ── Affinity report builder ─────────────────────────────────────────

/**
 * Build affinity index report from Athena affinity query results.
 * Geocodes origins to postal codes, then scores each 0-100 based on:
 * - Visit count (40% weight)
 * - Average dwell time (30% weight, capped at 60 min)
 * - Average visit frequency (30% weight, capped at 10)
 */
export function buildAffinityReport(
  megaJobId: string,
  rows: Record<string, any>[],
  coordToZip: Map<string, { zipCode: string; city: string; country: string }>,
): ConsolidatedAffinityReport {
  // Aggregate by postal code
  const byZip = new Map<string, {
    city: string; country: string;
    totalVisits: number; uniqueDevices: number;
    dwellSum: number; freqSum: number; rowCount: number;
  }>();

  for (const row of rows) {
    const lat = parseFloat(row.origin_lat) || 0;
    const lng = parseFloat(row.origin_lng) || 0;
    const key = `${lat},${lng}`;
    const geo = coordToZip.get(key) || { zipCode: 'UNKNOWN', city: 'UNKNOWN', country: 'UNKNOWN' };
    const zipKey = `${geo.zipCode}|${geo.city}|${geo.country}`;

    const totalVisits = parseInt(row.total_visits, 10) || 0;
    const uniqueDevices = parseInt(row.unique_devices, 10) || 0;
    const avgDwell = parseFloat(row.avg_dwell) || 0;
    const avgFreq = parseFloat(row.avg_frequency) || 1;

    const existing = byZip.get(zipKey);
    if (existing) {
      existing.totalVisits += totalVisits;
      existing.uniqueDevices += uniqueDevices;
      existing.dwellSum += avgDwell * totalVisits;
      existing.freqSum += avgFreq * totalVisits;
      existing.rowCount += totalVisits;
    } else {
      byZip.set(zipKey, {
        city: geo.city, country: geo.country,
        totalVisits, uniqueDevices,
        dwellSum: avgDwell * totalVisits,
        freqSum: avgFreq * totalVisits,
        rowCount: totalVisits,
      });
    }
  }

  // Compute affinity scores
  const entries = Array.from(byZip.entries()).map(([key, v]) => {
    const [postalCode] = key.split('|');
    const avgDwell = v.rowCount > 0 ? v.dwellSum / v.rowCount : 0;
    const avgFrequency = v.rowCount > 0 ? v.freqSum / v.rowCount : 1;
    return { postalCode, city: v.city, country: v.country, totalVisits: v.totalVisits, uniqueDevices: v.uniqueDevices, avgDwell, avgFrequency };
  });

  // Normalize visit count to max
  const maxVisits = Math.max(...entries.map((e) => e.totalVisits), 1);

  const scored = entries.map((e) => {
    const visitScore = Math.min(e.totalVisits / maxVisits, 1) * 100;
    const dwellScore = Math.min(e.avgDwell / 60, 1) * 100;
    const freqScore = Math.min(Math.log2(Math.max(e.avgFrequency, 1)) / Math.log2(10), 1) * 100;
    const affinityIndex = Math.round(0.4 * visitScore + 0.3 * dwellScore + 0.3 * freqScore);

    return {
      postalCode: e.postalCode, city: e.city, country: e.country,
      affinityIndex,
      totalVisits: e.totalVisits,
      uniqueDevices: e.uniqueDevices,
      avgDwell: Math.round(e.avgDwell * 10) / 10,
      avgFrequency: Math.round(e.avgFrequency * 100) / 100,
    };
  }).sort((a, b) => b.affinityIndex - a.affinityIndex);

  return {
    megaJobId,
    analyzedAt: new Date().toISOString(),
    byZipCode: scored,
  };
}
