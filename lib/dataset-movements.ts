/**
 * Fetch movement trajectories for a sample of devices from a dataset.
 * Used for the movement map visualization.
 */

import { runQuery, createTableForDataset, tableExists, getTableName } from './athena';

const MAX_PINGS_PER_DEVICE = 2000; // Limit to avoid OOM / slow responses
const MAX_DEVICES = 50;

function sanitizeDate(d: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) {
    throw new Error(`Invalid date format: ${d}. Expected YYYY-MM-DD.`);
  }
  return d;
}

export interface MovementPoint {
  lat: number;
  lng: number;
  utc: string;
}

export interface DeviceMovement {
  adId: string;
  points: MovementPoint[];
}

export interface MovementsResult {
  devices: DeviceMovement[];
  dateRange: { from: string; to: string };
}

/**
 * Get movement trajectories for a random sample of devices that visited POIs.
 * Returns up to 50 devices with their pings ordered by time (max 2000 pings per device).
 */
export async function getDeviceMovements(
  datasetName: string,
  dateFrom: string,
  dateTo: string,
  sampleSize: number = MAX_DEVICES
): Promise<MovementsResult> {
  sanitizeDate(dateFrom);
  sanitizeDate(dateTo);
  const size = Math.min(Math.max(1, sampleSize), 50);

  const tableName = getTableName(datasetName);

  if (!(await tableExists(datasetName))) {
    await createTableForDataset(datasetName);
  }

  // Step 1: Get random sample of ad_ids that visited POIs
  const sampleSql = `
    SELECT ad_id
    FROM (
      SELECT ad_id
      FROM ${tableName}
      WHERE poi_ids[1] IS NOT NULL
        AND date >= '${dateFrom}'
        AND date <= '${dateTo}'
      GROUP BY ad_id
      LIMIT 500
    )
    ORDER BY RANDOM()
    LIMIT ${size}
  `;
  const sampleRes = await runQuery(sampleSql);
  const deviceIds = sampleRes.rows.map((r: any) => r.ad_id).filter(Boolean);

  if (deviceIds.length === 0) {
    return { devices: [], dateRange: { from: dateFrom, to: dateTo } };
  }

  // Sanitize ad_ids for SQL (UUID format: alphanumeric + hyphens)
  const safeIds = deviceIds.filter((id) => /^[a-fA-F0-9\-]{36}$/.test(id));
  if (safeIds.length === 0) {
    return { devices: [], dateRange: { from: dateFrom, to: dateTo } };
  }

  const idList = safeIds.map((id) => `'${id.replace(/'/g, "''")}'`).join(',');

  // Step 2: Get pings for those devices, ordered by time, limit per device
  const pingsSql = `
    SELECT ad_id, latitude, longitude, utc_timestamp
    FROM (
      SELECT
        ad_id,
        latitude,
        longitude,
        utc_timestamp,
        row_number() OVER (PARTITION BY ad_id ORDER BY utc_timestamp) as rn
      FROM ${tableName}
      WHERE ad_id IN (${idList})
        AND date >= '${dateFrom}'
        AND date <= '${dateTo}'
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
    )
    WHERE rn <= ${MAX_PINGS_PER_DEVICE}
    ORDER BY ad_id, utc_timestamp
  `;

  const pingsRes = await runQuery(pingsSql);

  const byDevice = new Map<string, MovementPoint[]>();
  for (const row of pingsRes.rows as any[]) {
    const adId = row.ad_id;
    const lat = parseFloat(row.latitude);
    const lng = parseFloat(row.longitude);
    if (isNaN(lat) || isNaN(lng)) continue;

    const utc = row.utc_timestamp ?? row.utc ?? '';
    if (!byDevice.has(adId)) byDevice.set(adId, []);
    byDevice.get(adId)!.push({ lat, lng, utc: String(utc) });
  }

  const devices: DeviceMovement[] = Array.from(byDevice.entries()).map(([adId, points]) => ({
    adId,
    points,
  }));

  console.log(`[MOVEMENTS] ${devices.length} devices, ${pingsRes.rows.length} total pings`);

  return {
    devices,
    dateRange: { from: dateFrom, to: dateTo },
  };
}
