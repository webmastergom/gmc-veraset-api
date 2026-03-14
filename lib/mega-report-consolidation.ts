/**
 * Mega-job report consolidation.
 * Merges analysis results from multiple sub-jobs into unified reports.
 */

import { getTableName, startQueryAsync, fetchQueryResults } from './athena';
import { type Job } from './jobs';
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
  }>;
  /** Departure hour distribution */
  departureByHour: Array<{ hour: number; deviceDays: number }>;
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
}

// ── Visits by POI consolidation (Athena UNION ALL) ────────────────────

/**
 * Build and execute a UNION ALL query across all sub-job tables
 * for consolidated visits by POI with exact unique device counts.
 *
 * Returns the Athena queryId (caller must poll for completion).
 */
export async function startConsolidatedVisitsQuery(
  subJobs: Job[]
): Promise<string> {
  const syncedJobs = subJobs.filter((j) => j.s3DestPath && j.syncedAt);
  if (syncedJobs.length === 0) throw new Error('No synced sub-jobs');

  // Build UNION ALL of raw (poi_id, ad_id) pairs from all tables
  const unionParts = syncedJobs.map((job) => {
    const tableName = getTableName(job.s3DestPath!.replace(/\/$/, '').split('/').pop()!);
    return `SELECT poi_id, ad_id FROM ${tableName} CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL AND poi_id != '' AND ad_id IS NOT NULL AND TRIM(ad_id) != ''`;
  });

  const sql = `
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

  console.log(`[MEGA-CONSOLIDATION] Starting visits query across ${syncedJobs.length} tables`);
  return await startQueryAsync(sql);
}

/**
 * Parse the visits query results and apply POI name mapping from all sub-jobs.
 * Uses fetchQueryResults which returns Record<string, any>[].
 */
export function parseConsolidatedVisits(
  rows: Record<string, any>[],
  subJobs: Job[]
): ConsolidatedVisitByPoi[] {
  // Build unified POI mapping: verasetId → { originalId, name }
  // Since each sub-job has its own geo_radius_X → originalId mapping,
  // and the Athena query returns geo_radius_X IDs, we need all mappings.
  const poiNameMap = new Map<string, string>();

  for (const job of subJobs) {
    if (job.poiMapping) {
      for (const [verasetId, originalId] of Object.entries(job.poiMapping)) {
        const name = job.poiNames?.[verasetId] || originalId;
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
      .map((v) => ({ zipCode: v.zip, city: v.city, country: v.country, lat: v.lat, lng: v.lng, deviceDays: v.deviceDays }))
      .sort((a, b) => b.deviceDays - a.deviceDays),
    departureByHour: Array.from({ length: 24 }, (_, h) => ({ hour: h, deviceDays: hourMap.get(h) || 0 })),
  };
}

// ── Mobility result parser ───────────────────────────────────────────

export function parseConsolidatedMobility(
  megaJobId: string,
  rows: Record<string, any>[],
): ConsolidatedMobilityReport {
  const categories = rows.map((row) => ({
    category: String(row.category || 'UNKNOWN'),
    deviceDays: parseInt(row.device_days, 10) || 0,
    hits: parseInt(row.hits, 10) || 0,
  }));

  return {
    megaJobId,
    analyzedAt: new Date().toISOString(),
    categories,
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
