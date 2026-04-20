/**
 * Multi-phase catchment (origin) analysis that works within 60s Vercel Hobby timeout.
 * Splits the analysis into: start queries → poll → geocode + aggregate.
 *
 * Follows the same pattern as dataset-analysis-multiphase.ts.
 */

import {
  createTableForDataset,
  tableExists,
  getTableName,
  startQueryAsync,
  checkQueryStatus,
} from './athena';
import { s3Client, BUCKET } from './s3-config';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { batchReverseGeocode, aggregateByZipcode, setCountryFilter } from './reverse-geocode';
import { getCountryForDataset } from './country-dataset-config';
import type { ODZipcode } from './od-types';

// ── State ────────────────────────────────────────────────────────────

export interface CatchmentState {
  status: 'starting' | 'queries_running' | 'processing' | 'completed' | 'error';
  datasetName: string;
  progress: { step: string; percent: number; message: string };
  /** Athena query IDs */
  queryIds?: { origins: string; total: string };
  /** Filters used for this run */
  filters?: { minPings?: number; poiIds?: string[] };
  /** Final result */
  result?: CatchmentResult;
  error?: string;
  startedAt: string;
  updatedAt: string;
}

export interface CatchmentResult {
  dataset: string;
  analyzedAt: string;
  totalDevicesVisitedPois: number;
  totalDeviceDays: number;
  origins: ODZipcode[];
  geocodingComplete: boolean;
  coverageRatePercent: number;
}

const STATE_PREFIX = 'config/catchment-state';

async function getState(datasetName: string): Promise<CatchmentState | null> {
  try {
    const res = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: `${STATE_PREFIX}/${datasetName}.json`,
    }));
    const body = await res.Body?.transformToString();
    return body ? JSON.parse(body) : null;
  } catch {
    return null;
  }
}

async function saveState(state: CatchmentState): Promise<void> {
  state.updatedAt = new Date().toISOString();
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${STATE_PREFIX}/${state.datasetName}.json`,
    Body: JSON.stringify(state),
    ContentType: 'application/json',
  }));
}

export async function resetCatchmentState(datasetName: string): Promise<void> {
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${STATE_PREFIX}/${datasetName}.json`,
      Body: JSON.stringify({ status: 'error', datasetName, progress: { step: 'reset', percent: 0, message: 'Reset' }, startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() }),
      ContentType: 'application/json',
    }));
  } catch { /* ignore */ }
}

const ACCURACY_THRESHOLD_METERS = 500;
const COORDINATE_PRECISION = 4; // ~11m resolution

// ── Main entry ───────────────────────────────────────────────────────

/**
 * Start or advance a multi-phase catchment analysis.
 * Each call completes within ~50s.
 */
export async function catchmentMultiPhase(
  datasetName: string,
  filters?: { minPings?: number; poiIds?: string[] },
): Promise<CatchmentState> {
  let state = await getState(datasetName);

  // Reset if previous run is done or stale (>10 min without completion)
  if (state) {
    const isTerminal = state.status === 'completed' || state.status === 'error';
    const ageMs = Date.now() - new Date(state.updatedAt || state.startedAt).getTime();
    const isStale = ageMs > 10 * 60 * 1000; // 10 minutes
    if (isTerminal || isStale) {
      if (isStale) console.log(`[CATCHMENT] ${datasetName}: resetting stale state (${Math.round(ageMs / 1000)}s old, status=${state.status})`);
      state = null;
    }
  }

  // ── Phase 1: Start Athena queries ──────────────────────────
  if (!state) {
    const tableName = getTableName(datasetName);

    if (!(await tableExists(datasetName))) {
      await createTableForDataset(datasetName);
    }

    // Build WHERE conditions
    const dateConditions: string[] = [];
    const dateWhere = dateConditions.length ? `AND ${dateConditions.join(' AND ')}` : '';

    let poiFilter = '';
    if (filters?.poiIds?.length) {
      const poiList = filters.poiIds.map(p => `'${p.replace(/'/g, "''")}'`).join(',');
      poiFilter = `AND poi_id IN (${poiList})`;
    }

    const minPings = filters?.minPings && filters.minPings > 1 ? filters.minPings : 0;
    const havingClause = minPings > 0 ? `HAVING COUNT(*) >= ${minPings}` : '';

    // Optimized origins query — uses MIN_BY, no window functions
    const originsQuery = `
      WITH
      poi_visitors AS (
        SELECT DISTINCT ad_id
        FROM ${tableName}
        CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
        WHERE poi_id IS NOT NULL AND poi_id != ''
          AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
          ${dateWhere} ${poiFilter}
      ),
      valid_pings AS (
        SELECT
          t.ad_id,
          t.date,
          t.utc_timestamp,
          TRY_CAST(t.latitude AS DOUBLE) as lat,
          TRY_CAST(t.longitude AS DOUBLE) as lng
        FROM ${tableName} t
        INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
        WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
          AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
          AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD_METERS})
          ${dateWhere}
      ),
      first_pings AS (
        SELECT
          ad_id,
          date,
          MIN_BY(lat, utc_timestamp) as origin_lat,
          MIN_BY(lng, utc_timestamp) as origin_lng
        FROM valid_pings
        GROUP BY ad_id, date
        ${havingClause}
      ),
      device_homes AS (
        SELECT ad_id,
          ROUND(origin_lat, ${COORDINATE_PRECISION}) as home_lat,
          ROUND(origin_lng, ${COORDINATE_PRECISION}) as home_lng,
          COUNT(DISTINCT date) as days_at_loc
        FROM first_pings
        GROUP BY ad_id,
          ROUND(origin_lat, ${COORDINATE_PRECISION}),
          ROUND(origin_lng, ${COORDINATE_PRECISION})
        HAVING COUNT(DISTINCT date) >= 3
      )
      SELECT
        home_lat as origin_lat,
        home_lng as origin_lng,
        COUNT(*) as device_days
      FROM device_homes
      GROUP BY home_lat, home_lng
      ORDER BY device_days DESC
      LIMIT 100000
    `;

    const totalQuery = `
      SELECT COUNT(DISTINCT ad_id) as total_devices
      FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        ${dateWhere} ${poiFilter}
    `;

    const [originsId, totalId] = await Promise.all([
      startQueryAsync(originsQuery),
      startQueryAsync(totalQuery),
    ]);

    state = {
      status: 'queries_running',
      datasetName,
      queryIds: { origins: originsId, total: totalId },
      filters,
      progress: { step: 'running_queries', percent: 15, message: 'Athena queries started...' },
      startedAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await saveState(state);
    console.log(`[CATCHMENT] ${datasetName}: started origins(${originsId}) + total(${totalId})`);
    return state;
  }

  // ── Phase 2: Poll Athena queries ──────────────────────────
  if (state.status === 'queries_running') {
    const ids = state.queryIds!;
    const [originsS, totalS] = await Promise.all([
      checkQueryStatus(ids.origins),
      checkQueryStatus(ids.total),
    ]);

    if (originsS.state === 'FAILED') {
      state.status = 'error';
      state.error = `Origins query failed: ${originsS.error}`;
      state.progress = { step: 'error', percent: 0, message: state.error || 'Unknown error' };
      await saveState(state);
      return state;
    }
    if (totalS.state === 'FAILED') {
      state.status = 'error';
      state.error = `Total query failed: ${totalS.error}`;
      state.progress = { step: 'error', percent: 0, message: state.error || 'Unknown error' };
      await saveState(state);
      return state;
    }

    const allDone = originsS.state === 'SUCCEEDED' && totalS.state === 'SUCCEEDED';

    if (!allDone) {
      const statusMsg = `Origins: ${originsS.state}, Total: ${totalS.state}`;
      state.progress = { step: 'running_queries', percent: 35, message: `Waiting for queries... (${statusMsg})` };
      await saveState(state);
      return state;
    }

    // All done — advance to processing in the NEXT poll (fresh 60s window)
    state.status = 'processing';
    state.progress = { step: 'geocoding', percent: 55, message: 'Queries completed. Starting geocoding...' };
    await saveState(state);
    return state;
  }

  // ── Phase 3: Download results, geocode, aggregate ─────────
  if (state.status === 'processing') {
    try {
      const ids = state.queryIds!;

      // Download results from Athena output CSVs
      const fetchCsv = async (queryId: string) => {
        const res = await s3Client.send(new GetObjectCommand({
          Bucket: BUCKET,
          Key: `athena-results/${queryId}.csv`,
        }));
        const text = await res.Body?.transformToString() || '';
        const lines = text.split('\n');
        const headers = lines[0]?.replace(/"/g, '').split(',').map(h => h.trim()) || [];
        const rows: Record<string, string>[] = [];
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].trim();
          if (!line) continue;
          const values = line.replace(/"/g, '').split(',');
          const row: Record<string, string> = {};
          headers.forEach((h, idx) => { row[h] = values[idx] || ''; });
          rows.push(row);
        }
        return rows;
      };

      const [originsRows, totalRows] = await Promise.all([
        fetchCsv(ids.origins),
        fetchCsv(ids.total),
      ]);

      const totalDevices = parseInt(String(totalRows[0]?.total_devices)) || 0;
      console.log(`[CATCHMENT] ${state.datasetName}: ${totalDevices} total devices, ${originsRows.length} origin clusters`);

      if (totalDevices === 0 || originsRows.length === 0) {
        state.status = 'completed';
        state.result = {
          dataset: state.datasetName,
          analyzedAt: new Date().toISOString(),
          totalDevicesVisitedPois: totalDevices,
          totalDeviceDays: 0,
          origins: [],
          geocodingComplete: true,
          coverageRatePercent: 0,
        };
        state.progress = { step: 'completed', percent: 100, message: 'No data found' };
        await saveState(state);
        return state;
      }

      // Parse origin points
      let totalDeviceDays = 0;
      const originPoints = originsRows.map(row => {
        const deviceDays = parseInt(String(row.device_days)) || 0;
        totalDeviceDays += deviceDays;
        return {
          lat: parseFloat(String(row.origin_lat)),
          lng: parseFloat(String(row.origin_lng)),
          deviceCount: deviceDays,
        };
      }).filter(p => !isNaN(p.lat) && !isNaN(p.lng));

      state.progress = { step: 'geocoding', percent: 65, message: `Geocoding ${originPoints.length.toLocaleString()} clusters...` };

      // Determine primary country — limit GeoJSON loading to avoid 60s timeout
      let primaryCountry = await getCountryForDataset(state.datasetName);
      if (!primaryCountry) {
        // Try to detect from job name by looking up the job
        try {
          const { getAllJobs } = await import('./jobs');
          const allJobs = await getAllJobs();
          const job = allJobs.find((j: any) => {
            if (!j.s3DestPath) return false;
            const path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
            const folder = path.split('/').filter(Boolean)[0] || path.replace(/\/$/, '');
            return folder === state!.datasetName;
          });
          if (job?.name) {
            primaryCountry = detectCountryFromJobName(job.name);
          }
        } catch { /* ignore */ }
      }
      // Fallback: detect country from median coordinates
      if (!primaryCountry && originPoints.length > 0) {
        primaryCountry = detectCountryFromCoordinates(originPoints);
        if (primaryCountry) {
          console.log(`[CATCHMENT] Country detected from coordinates: ${primaryCountry}`);
        }
      }
      if (primaryCountry) {
        setCountryFilter([primaryCountry]);
        console.log(`[CATCHMENT] Country filter set to: ${primaryCountry}`);
      }

      // Reverse geocode
      console.log(`[CATCHMENT] ${state.datasetName}: geocoding ${originPoints.length} origin clusters...`);
      const classified = await batchReverseGeocode(originPoints);

      // Remove country filter
      setCountryFilter(null);

      state.progress = { step: 'aggregating', percent: 90, message: 'Aggregating by zipcode...' };

      // Aggregate by zipcode
      const agg = aggregateByZipcode(classified, totalDeviceDays);

      const origins: ODZipcode[] = agg.zipcodes.map(z => ({
        zipcode: z.zipcode,
        city: z.city,
        province: z.province,
        region: z.region,
        devices: z.devices,
        percentOfTotal: z.percentOfTotal,
        source: z.source,
      }));

      const devicesWithOrigin = origins.reduce((s, z) => s + z.devices, 0);

      state.status = 'completed';
      state.result = {
        dataset: state.datasetName,
        analyzedAt: new Date().toISOString(),
        totalDevicesVisitedPois: totalDevices,
        totalDeviceDays,
        origins,
        geocodingComplete: agg.nominatimTruncated === 0,
        coverageRatePercent: totalDeviceDays > 0
          ? Math.round((devicesWithOrigin / totalDeviceDays) * 10000) / 100
          : 0,
      };
      state.progress = { step: 'completed', percent: 100, message: `${origins.length} zipcodes identified` };
      await saveState(state);
      console.log(`[CATCHMENT] ${state.datasetName}: done — ${origins.length} zipcodes, ${devicesWithOrigin} device-days matched`);
      return state;
    } catch (err: any) {
      // Save error to S3 so we don't loop forever on a failing Phase 3
      console.error(`[CATCHMENT] ${state.datasetName} Phase 3 error:`, err.message);
      state.status = 'error';
      state.error = err.message || 'Geocoding/aggregation failed';
      state.progress = { step: 'error', percent: 0, message: state.error || 'Unknown error' };
      await saveState(state).catch(() => {});
      throw err; // Re-throw so the route handler also returns 500
    }
  }

  // Shouldn't reach here
  return state!;
}

/** Detect country from median coordinates using rough bounding boxes. */
function detectCountryFromCoordinates(points: { lat: number; lng: number }[]): string | undefined {
  // Use weighted median (top points by device count are first due to ORDER BY)
  const sample = points.slice(0, Math.min(100, points.length));
  const medLat = sample.reduce((s, p) => s + p.lat, 0) / sample.length;
  const medLng = sample.reduce((s, p) => s + p.lng, 0) / sample.length;

  // Rough country bounding boxes (mainland only for precision)
  const boxes: [string, number, number, number, number][] = [
    // [code, minLat, maxLat, minLng, maxLng]
    ['FR', 42, 51, -5, 9],
    ['ES', 36, 44, -10, 4],
    ['DE', 47, 55, 5, 16],
    ['IT', 36, 47, 6, 19],
    ['PT', 37, 42, -10, -6],
    ['UK', 50, 59, -8, 2],
    ['NL', 51, 54, 3, 8],
    ['BE', 49, 52, 2, 7],
    ['MX', 14, 33, -118, -86],
    ['US', 24, 50, -125, -66],
    ['CO', -5, 14, -82, -66],
    ['AR', -55, -21, -74, -53],
    ['CL', -56, -17, -76, -66],
    ['PE', -19, 0, -82, -68],
    ['EC', -5, 2, -81, -75],
    ['CR', 8, 11, -86, -82],
    ['GT', 13, 18, -93, -88],
    ['BR', -34, 6, -74, -34],
    ['SE', 55, 70, 10, 25],
    ['IE', 51, 56, -11, -5],
  ];

  for (const [code, minLat, maxLat, minLng, maxLng] of boxes) {
    if (medLat >= minLat && medLat <= maxLat && medLng >= minLng && medLng <= maxLng) {
      return code;
    }
  }
  return undefined;
}

/** Detect country code from job name. */
function detectCountryFromJobName(name: string): string | undefined {
  const n = name.toLowerCase();
  const map: Record<string, string> = {
    germany: 'DE', deutschland: 'DE',
    france: 'FR', francia: 'FR',
    spain: 'ES', 'españa': 'ES', espana: 'ES',
    mexico: 'MX', 'méxico': 'MX',
    'united kingdom': 'UK', ' uk ': 'UK',
    'costa rica': 'CR',
    guatemala: 'GT',
    ecuador: 'EC',
    colombia: 'CO',
    brazil: 'BR', brasil: 'BR',
    argentina: 'AR',
    chile: 'CL',
    peru: 'PE', 'perú': 'PE',
    italy: 'IT', italia: 'IT',
    portugal: 'PT',
    netherlands: 'NL',
    belgium: 'BE',
    switzerland: 'CH', suiza: 'CH',
    austria: 'AT',
    poland: 'PL', polonia: 'PL',
  };
  for (const [keyword, code] of Object.entries(map)) {
    if (n.includes(keyword)) return code;
  }
  return undefined;
}
