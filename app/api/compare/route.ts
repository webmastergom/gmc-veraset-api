import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
  runQuery,
} from '@/lib/athena';
import { getConfig, putConfig, s3Client, BUCKET } from '@/lib/s3-config';
import { getAllJobs } from '@/lib/jobs';
import { setCountryFilter, batchReverseGeocode } from '@/lib/reverse-geocode';
// NOTE: @/lib/poi-storage is imported dynamically in the reading phase — it pulls in
// `fs` and triggers Next.js File Tracing on the POIs/ directory, which blows up the
// Lambda bundle for this route at cold-start. Keep it behind a lazy import.

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

interface DatasetSide {
  name: string;
  source: 'all' | 'category-export' | 'nse-export';
  minDwell?: number;
  maxDwell?: number;
  hourFrom?: number;
  hourTo?: number;
  minVisits?: number;
  exportFile?: string;
}

interface CompareState {
  phase:
    | 'home_polling'       // geocoding-prep: home-coord queries running
    | 'geocoding'          // reverse-geocode + ZIP filter in JS
    | 'main_polling'       // main 5 queries running (counts + POI overlap + export)
    | 'reading'            // fetch results
    | 'done'
    | 'error';
  stateId: string;
  datasetA: DatasetSide;
  datasetB: DatasetSide;
  zipCodes?: string[];           // normalized (trim/uppercase) — empty = no ZIP filter
  countryA?: string;
  countryB?: string;

  // Home-coord query ids (only set when zipCodes present)
  homeQueryIds?: { homeA?: string; homeB?: string };
  homeDone?: { homeA?: boolean; homeB?: boolean };

  // Kept ad_id temp-table names after geocoding (per side, only when ZIP filter applied)
  keptTables?: { keptA?: string; keptB?: string };
  keptCounts?: { keptA?: number; keptB?: number };

  // Main 5 queries
  queryIds: {
    countA?: string;
    countB?: string;
    countOverlap?: string;
    exportOverlap?: string;
    poiOverlap?: string;
  };
  done: {
    countA?: boolean;
    countB?: boolean;
    countOverlap?: boolean;
    exportOverlap?: boolean;
    poiOverlap?: boolean;
  };

  tempTables?: string[];      // CSV-export temp tables + kept ad_id temp tables (all cleaned up at end)
  error?: string;
  result?: CompareResult;
}

interface PoiMatchRow {
  side: 'A' | 'B';
  poiId: string;
  name?: string;
  lat?: number;
  lng?: number;
  overlapDevices: number;
}

interface CompareResult {
  totalA: number;
  totalB: number;
  overlap: number;
  overlapPctA: number;
  overlapPctB: number;
  downloadKey: string;
  pois: PoiMatchRow[];
  zipFilter?: {
    zipCodes: string[];
    countryA?: string;
    countryB?: string;
    keptA?: number;
    keptB?: number;
  };
}

const STATE_KEY = (id: string) => `compare-state/${id}`;
const HOME_COORD_PRECISION = 1; // 1-decimal ~= 11km — matches catchment-multiphase

/**
 * Build a SQL subquery that returns DISTINCT ad_id for a dataset side.
 * Supports dwell interval, hour-of-visit, and min-visits (distinct-date count).
 *
 * When `keptTable` is provided, the result is further narrowed to ad_ids
 * present in that table (ZIP-filter refiltering).
 */
function maidSubquery(side: DatasetSide, tableName: string, keptTable?: string): string {
  let base: string;

  if (side.source === 'all') {
    const minDwell = side.minDwell || 0;
    const maxDwell = side.maxDwell || 0;
    const hourFrom = side.hourFrom ?? 0;
    const hourTo = side.hourTo ?? 23;
    const minVisits = Math.max(1, side.minVisits || 1);
    const hasHourFilter = hourFrom > 0 || hourTo < 23;
    const hasDwellFilter = minDwell > 0 || maxDwell > 0;
    const hasVisitFilter = minVisits > 1;

    let hourWhere = '';
    if (hasHourFilter) {
      if (hourFrom <= hourTo) {
        hourWhere = `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
      } else {
        hourWhere = `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
      }
    }

    if (hasDwellFilter || hasHourFilter || hasVisitFilter) {
      const dwellHavings: string[] = [];
      if (minDwell > 0) dwellHavings.push(`dwell >= ${minDwell}`);
      if (maxDwell > 0) dwellHavings.push(`dwell <= ${maxDwell}`);
      const havingDwell = dwellHavings.length > 0 ? `WHERE ${dwellHavings.join(' AND ')}` : '';

      const perDay = `(
        SELECT ad_id, date FROM (
          SELECT ad_id, date,
            DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell
          FROM ${tableName}
          CROSS JOIN UNNEST(poi_ids) AS t2(poi_id)
          WHERE t2.poi_id IS NOT NULL AND t2.poi_id != ''
            AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
            ${hourWhere}
          GROUP BY ad_id, date, t2.poi_id
        ) dwell_per_day ${havingDwell}
      )`;

      if (hasVisitFilter) {
        base = `(
          SELECT ad_id FROM ${perDay}
          GROUP BY ad_id
          HAVING COUNT(DISTINCT date) >= ${minVisits}
        )`;
      } else {
        base = `(SELECT DISTINCT ad_id FROM ${perDay})`;
      }
    } else {
      base = `(SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '')`;
    }
  } else {
    // CSV export: temp external table
    base = `(SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '')`;
  }

  if (keptTable) {
    return `(SELECT b.ad_id FROM ${base} b INNER JOIN (SELECT DISTINCT ad_id FROM ${keptTable}) k ON b.ad_id = k.ad_id)`;
  }
  return base;
}

/**
 * Home-coord query (for ZIP filter only). Returns one row per device-location with
 * the count of distinct days where the first ping of the day was at that location.
 * Aggregates at HOME_COORD_PRECISION decimals and filters locations repeated 2+ days.
 */
function homeCoordSQL(side: DatasetSide, tableName: string): string {
  // Only meaningful for source='all' — CSV sides don't have pings to geocode here.
  const hourFrom = side.hourFrom ?? 0;
  const hourTo = side.hourTo ?? 23;
  let hourWhere = '';
  if (hourFrom > 0 || hourTo < 23) {
    if (hourFrom <= hourTo) {
      hourWhere = `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
    } else {
      hourWhere = `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
    }
  }

  // POI visitors + valid pings + first-ping-of-day (MIN_BY) → aggregate by rounded coord,
  // keep ad_ids where the coord is the first-ping home for 2+ distinct days.
  return `
    WITH poi_visitors AS (
      SELECT DISTINCT ad_id FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    ),
    valid_pings AS (
      SELECT t.ad_id, t.date, t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${tableName} t
      INNER JOIN poi_visitors v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < 100)
        ${hourWhere}
    ),
    first_pings AS (
      SELECT ad_id, date,
        MIN_BY(lat, utc_timestamp) as origin_lat,
        MIN_BY(lng, utc_timestamp) as origin_lng
      FROM valid_pings
      GROUP BY ad_id, date
    ),
    ad_home AS (
      SELECT ad_id,
        ROUND(origin_lat, ${HOME_COORD_PRECISION}) as home_lat,
        ROUND(origin_lng, ${HOME_COORD_PRECISION}) as home_lng,
        COUNT(DISTINCT date) as day_count
      FROM first_pings
      GROUP BY ad_id,
        ROUND(origin_lat, ${HOME_COORD_PRECISION}),
        ROUND(origin_lng, ${HOME_COORD_PRECISION})
      HAVING COUNT(DISTINCT date) >= 2
    )
    SELECT ad_id,
      home_lat,
      home_lng,
      day_count
    FROM ad_home
    LIMIT 5000000
  `;
}

async function createTempTableFromCsv(exportFile: string, _stateId: string, side: string): Promise<string> {
  const tempName = `compare_tmp_${side}_${Date.now()}`;
  const s3Key = `exports/${exportFile}`;
  const tempFolder = `compare-temp/${tempName}/`;

  const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
  await s3Client.send(new CopyObjectCommand({
    Bucket: BUCKET,
    CopySource: `${BUCKET}/${s3Key}`,
    Key: `${tempFolder}data.csv`,
  }));

  const createSql = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tempName} (
      ad_id STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    LOCATION 's3://${BUCKET}/${tempFolder}'
    TBLPROPERTIES ('skip.header.line.count'='1')
  `;
  await runQuery(createSql);
  console.log(`[COMPARE] Created temp table ${tempName} from ${exportFile}`);
  return tempName;
}

/**
 * Write a kept-ad_ids set to S3 as a one-column CSV and create an external Athena table over it.
 * Used to avoid giant IN-lists after ZIP geocoding.
 */
async function createKeptTable(adIds: string[], side: string): Promise<string> {
  const tempName = `compare_kept_${side}_${Date.now()}`;
  const tempFolder = `compare-temp/${tempName}/`;

  // Build CSV body. Skip empties.
  const unique = Array.from(new Set(adIds.filter(Boolean)));
  const body = 'ad_id\n' + unique.map(id => id.replace(/"/g, '""')).join('\n') + '\n';

  const { PutObjectCommand } = await import('@aws-sdk/client-s3');
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${tempFolder}data.csv`,
    Body: body,
    ContentType: 'text/csv',
  }));

  const createSql = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tempName} (
      ad_id STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    LOCATION 's3://${BUCKET}/${tempFolder}'
    TBLPROPERTIES ('skip.header.line.count'='1')
  `;
  await runQuery(createSql);
  console.log(`[COMPARE] Created kept table ${tempName} with ${unique.length} ad_ids`);
  return tempName;
}

async function cleanupTempTables(tables: string[]): Promise<void> {
  for (const t of tables) {
    try {
      await runQuery(`DROP TABLE IF EXISTS ${t}`);
      const { DeleteObjectCommand } = await import('@aws-sdk/client-s3');
      await s3Client.send(new DeleteObjectCommand({
        Bucket: BUCKET,
        Key: `compare-temp/${t}/data.csv`,
      })).catch(() => {});
    } catch (e: any) {
      console.warn(`[COMPARE] Cleanup ${t}:`, e.message);
    }
  }
}

/** Find job.country for a dataset — matches by s3DestPath folder. */
async function findDatasetCountry(datasetName: string): Promise<string | undefined> {
  const jobs = await getAllJobs();
  const job = jobs.find((j: any) => {
    if (!j.s3DestPath) return false;
    const s3Path = String(j.s3DestPath).replace('s3://', '').replace(`${BUCKET}/`, '').trim();
    const parts = s3Path.split('/').filter(Boolean);
    const jobFolderName = parts[0] || parts.pop() || s3Path.replace(/\/$/, '');
    return jobFolderName === datasetName;
  });
  return (job as any)?.country || undefined;
}

/**
 * Fire the main 5 queries (counts + POI overlap + export CSV).
 * When keptTables are provided, each subA/subB is additionally INNER JOINed to the kept list.
 */
async function fireMainQueries(
  datasetA: DatasetSide, datasetB: DatasetSide,
  tableA: string, tableB: string,
  keptA?: string, keptB?: string,
): Promise<{ countA: string; countB: string; countOverlap: string; exportOverlap: string; poiOverlap: string }> {
  const subA = maidSubquery(datasetA, tableA, keptA);
  const subB = maidSubquery(datasetB, tableB, keptB);

  // Only sides with source='all' have poi_ids — CSV-export temp tables are (ad_id) only.
  const overlapCte = `overlap_ids AS (SELECT a.ad_id FROM ${subA} a INNER JOIN ${subB} b ON a.ad_id = b.ad_id)`;
  const armA = datasetA.source === 'all'
    ? `SELECT 'A' AS side, t.ad_id, u.poi_id
       FROM ${tableA} t
       CROSS JOIN UNNEST(t.poi_ids) AS u(poi_id)
       WHERE u.poi_id IS NOT NULL AND u.poi_id != ''
         AND t.ad_id IN (SELECT ad_id FROM overlap_ids)`
    : null;
  const armB = datasetB.source === 'all'
    ? `SELECT 'B' AS side, t.ad_id, u.poi_id
       FROM ${tableB} t
       CROSS JOIN UNNEST(t.poi_ids) AS u(poi_id)
       WHERE u.poi_id IS NOT NULL AND u.poi_id != ''
         AND t.ad_id IN (SELECT ad_id FROM overlap_ids)`
    : null;
  const arms = [armA, armB].filter(Boolean) as string[];
  // Fallback: when both sides are CSV-export, there are no poi_ids — return empty result.
  const poiOverlapSQL = arms.length === 0
    ? `SELECT CAST(NULL AS VARCHAR) AS side, CAST(NULL AS VARCHAR) AS poi_id, CAST(0 AS BIGINT) AS overlap_devices WHERE 1=0`
    : `WITH ${overlapCte}
       SELECT side, poi_id, COUNT(DISTINCT ad_id) AS overlap_devices FROM (
         ${arms.join('\n       UNION ALL\n       ')}
       ) x
       GROUP BY side, poi_id
       ORDER BY overlap_devices DESC
       LIMIT 100000`;

  const [countA, countB, countOverlap, exportOverlap, poiOverlap] = await Promise.all([
    startQueryAsync(`SELECT COUNT(*) as total FROM ${subA} t`),
    startQueryAsync(`SELECT COUNT(*) as total FROM ${subB} t`),
    startQueryAsync(`SELECT COUNT(*) as total FROM ${subA} a INNER JOIN ${subB} b ON a.ad_id = b.ad_id`),
    startQueryAsync(`SELECT a.ad_id FROM ${subA} a INNER JOIN ${subB} b ON a.ad_id = b.ad_id`),
    // POI overlap — only for sides where we can meaningfully group by poi_id
    // For CSV-export sides we still run it (UNION ALL) but the CSV side has no poi_ids array, so it'll return nothing.
    // This is fine: the side's rows just won't appear.
    startQueryAsync(poiOverlapSQL),
  ]);

  return { countA, countB, countOverlap, exportOverlap, poiOverlap };
}

/**
 * POST /api/compare
 *
 * Multi-phase Athena-native comparison.
 *
 * Without zipCodes:
 *   starting → main_polling (5 queries) → reading → done
 *
 * With zipCodes:
 *   starting → home_polling (2 queries) → geocoding (JS) → main_polling (5 refilter queries) → reading → done
 */
export async function POST(request: NextRequest) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    let body: any;
    try { body = await request.json(); } catch { body = {}; }

    const isNewRequest = !!body.datasetA;
    const stateId = body.stateId || '';

    let state: CompareState | null = null;
    if (!isNewRequest && stateId) {
      state = await getConfig<CompareState>(STATE_KEY(stateId));

      if (state?.phase === 'done' && state.result) {
        return NextResponse.json({
          phase: 'done', stateId: state.stateId, result: state.result,
          progress: { step: 'done', percent: 100, message: 'Comparison complete' },
        });
      }
    }

    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: start ─────────────────────────────────────────────
    if (!state) {
      const datasetA: DatasetSide = body.datasetA;
      const datasetB: DatasetSide = body.datasetB;
      const rawZips: string[] = Array.isArray(body.zipCodes)
        ? body.zipCodes.map((z: any) => String(z || '').trim().toUpperCase()).filter(Boolean)
        : [];
      const zipCodes = Array.from(new Set(rawZips));
      const useZipFilter = zipCodes.length > 0;

      if (!datasetA?.name || !datasetB?.name) {
        return NextResponse.json({ error: 'Both datasetA and datasetB required' }, { status: 400 });
      }

      const newStateId = `${datasetA.name}-${datasetB.name}-${Date.now()}`;
      console.log(`[COMPARE] Starting: ${datasetA.name} (${datasetA.source}) vs ${datasetB.name} (${datasetB.source}) zipCodes=${zipCodes.length}`);

      // Resolve ZIP-filter prerequisites
      let countryA: string | undefined;
      let countryB: string | undefined;
      if (useZipFilter) {
        if (datasetA.source !== 'all' && datasetB.source !== 'all') {
          return NextResponse.json({ error: 'ZIP filter requires at least one side with source "all" (raw dataset).' }, { status: 400 });
        }
        if (datasetA.source === 'all') {
          countryA = await findDatasetCountry(datasetA.name);
          if (!countryA) return NextResponse.json({ error: `ZIP filter requires Country set on dataset "${datasetA.name}"'s job.` }, { status: 400 });
        }
        if (datasetB.source === 'all') {
          countryB = await findDatasetCountry(datasetB.name);
          if (!countryB) return NextResponse.json({ error: `ZIP filter requires Country set on dataset "${datasetB.name}"'s job.` }, { status: 400 });
        }
      }

      const tempTables: string[] = [];
      let tableA: string;
      let tableB: string;

      if (datasetA.source === 'all') {
        await ensureTableForDataset(datasetA.name);
        tableA = getTableName(datasetA.name);
      } else {
        tableA = await createTempTableFromCsv(datasetA.exportFile!, newStateId, 'a');
        tempTables.push(tableA);
      }

      if (datasetB.source === 'all') {
        await ensureTableForDataset(datasetB.name);
        tableB = getTableName(datasetB.name);
      } else {
        tableB = await createTempTableFromCsv(datasetB.exportFile!, newStateId, 'b');
        tempTables.push(tableB);
      }

      if (useZipFilter) {
        // Phase A: fire home-coord queries for side(s) with source='all'
        let homeA: string | undefined;
        let homeB: string | undefined;
        const toFire: Array<Promise<string>> = [];
        if (datasetA.source === 'all') toFire.push(startQueryAsync(homeCoordSQL(datasetA, tableA)).then(id => { homeA = id; return id; }));
        if (datasetB.source === 'all') toFire.push(startQueryAsync(homeCoordSQL(datasetB, tableB)).then(id => { homeB = id; return id; }));
        await Promise.all(toFire);

        console.log(`[COMPARE] Home-coord queries fired: homeA=${homeA} homeB=${homeB}`);
        state = {
          phase: 'home_polling',
          stateId: newStateId,
          datasetA, datasetB,
          zipCodes, countryA, countryB,
          homeQueryIds: { homeA, homeB },
          homeDone: {},
          queryIds: {},
          done: {},
          tempTables: tempTables.length > 0 ? tempTables : undefined,
        };
        await putConfig(STATE_KEY(newStateId), state, { compact: true });
        return NextResponse.json({
          phase: 'home_polling', stateId: newStateId,
          progress: { step: 'home_polling', percent: 5, message: 'Resolving home locations for ZIP filter...' },
        });
      }

      // Fast path (no ZIP filter): fire the 5 main queries now
      const ids = await fireMainQueries(datasetA, datasetB, tableA, tableB);
      console.log(`[COMPARE] 5 main queries started (no zip filter)`);

      state = {
        phase: 'main_polling', stateId: newStateId, datasetA, datasetB,
        zipCodes: [],
        queryIds: ids, done: {},
        tempTables: tempTables.length > 0 ? tempTables : undefined,
      };
      await putConfig(STATE_KEY(newStateId), state, { compact: true });
      return NextResponse.json({
        phase: 'main_polling', stateId: newStateId,
        progress: { step: 'queries_started', percent: 10, message: 'Running Athena queries...' },
      });
    }

    // ── Phase: home_polling ──────────────────────────────────────
    if (state.phase === 'home_polling') {
      let allDone = true;
      let anyFailed = false;
      let errorMsg = '';
      let scannedGB = 0;

      for (const key of ['homeA', 'homeB'] as const) {
        const qId = state.homeQueryIds?.[key];
        if (!qId || state.homeDone?.[key]) continue;
        try {
          const status = await checkQueryStatus(qId);
          if (status.state === 'RUNNING' || status.state === 'QUEUED') {
            allDone = false;
            scannedGB += (status.statistics?.dataScannedBytes || 0) / 1e9;
          } else if (status.state === 'FAILED' || status.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `${key} failed: ${status.error || 'unknown'}`;
          } else {
            state.homeDone = { ...state.homeDone, [key]: true };
          }
        } catch (err: any) {
          if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
            anyFailed = true; errorMsg = `${key} expired — please retry`;
          } else throw err;
        }
      }

      if (anyFailed) {
        if (state.tempTables?.length) await cleanupTempTables(state.tempTables).catch(() => {});
        state = { ...state, phase: 'error', error: errorMsg };
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        return NextResponse.json({ phase: 'error', stateId: state.stateId, error: errorMsg });
      }

      if (!allDone) {
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        return NextResponse.json({
          phase: 'home_polling', stateId: state.stateId,
          progress: { step: 'home_polling', percent: 15, message: `Resolving home locations (${scannedGB.toFixed(1)}GB scanned)` },
        });
      }

      // Advance to geocoding
      state = { ...state, phase: 'geocoding' };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
      return NextResponse.json({
        phase: 'geocoding', stateId: state.stateId,
        progress: { step: 'geocoding', percent: 30, message: 'Reverse-geocoding homes to ZIPs...' },
      });
    }

    // ── Phase: geocoding ─────────────────────────────────────────
    if (state.phase === 'geocoding') {
      const zipSet = new Set((state.zipCodes || []).map(z => z.toUpperCase()));

      // Ensure country filter covers any country we'll geocode
      const countries: string[] = [];
      if (state.countryA) countries.push(state.countryA);
      if (state.countryB) countries.push(state.countryB);
      setCountryFilter(countries.length ? countries : null);

      const keptTables: { keptA?: string; keptB?: string } = {};
      const keptCounts: { keptA?: number; keptB?: number } = {};
      const newTempTables: string[] = [];

      // Helper: process one side's home-coord query
      const processSide = async (sideLabel: 'A' | 'B', qId: string | undefined) => {
        if (!qId) return;
        const res = await fetchQueryResults(qId);
        // Rows: { ad_id, home_lat, home_lng, day_count }
        const rows = res.rows as Array<{ ad_id?: string; home_lat?: string; home_lng?: string; day_count?: string }>;

        // Aggregate unique (lat,lng) → geocode once
        const coordKey = (lat: number, lng: number) => `${lat.toFixed(HOME_COORD_PRECISION)}|${lng.toFixed(HOME_COORD_PRECISION)}`;
        const coordList: Array<{ lat: number; lng: number; deviceCount: number }> = [];
        const coordIndex = new Map<string, number>();
        const rowCoords: Array<{ adId: string; key: string }> = [];

        for (const r of rows) {
          const adId = (r.ad_id || '').trim();
          const lat = Number(r.home_lat);
          const lng = Number(r.home_lng);
          if (!adId || !Number.isFinite(lat) || !Number.isFinite(lng)) continue;
          const k = coordKey(lat, lng);
          if (!coordIndex.has(k)) {
            coordIndex.set(k, coordList.length);
            coordList.push({ lat, lng, deviceCount: 0 });
          }
          coordList[coordIndex.get(k)!].deviceCount += 1;
          rowCoords.push({ adId, key: k });
        }

        console.log(`[COMPARE] side=${sideLabel}: ${rows.length} device-home rows, ${coordList.length} unique coords`);

        const classified = await batchReverseGeocode(coordList);
        // Map key → postcode (or null)
        const keyToPostcode = new Map<string, string | null>();
        for (let i = 0; i < coordList.length; i++) {
          const cls = classified[i];
          const pc = cls && cls.type === 'geojson_local' && cls.postcode ? String(cls.postcode).toUpperCase() : null;
          keyToPostcode.set(coordKey(coordList[i].lat, coordList[i].lng), pc);
        }

        // Keep ad_ids whose postcode is in the requested set
        const keptIds = new Set<string>();
        for (const rc of rowCoords) {
          const pc = keyToPostcode.get(rc.key);
          if (pc && zipSet.has(pc)) keptIds.add(rc.adId);
        }

        console.log(`[COMPARE] side=${sideLabel}: ${keptIds.size} ad_ids match ZIP filter`);

        if (keptIds.size === 0) {
          // Materialize an empty table (so the INNER JOIN yields zero rows naturally)
          const t = await createKeptTable([], sideLabel.toLowerCase());
          newTempTables.push(t);
          if (sideLabel === 'A') { keptTables.keptA = t; keptCounts.keptA = 0; }
          else { keptTables.keptB = t; keptCounts.keptB = 0; }
          return;
        }

        const t = await createKeptTable(Array.from(keptIds), sideLabel.toLowerCase());
        newTempTables.push(t);
        if (sideLabel === 'A') { keptTables.keptA = t; keptCounts.keptA = keptIds.size; }
        else { keptTables.keptB = t; keptCounts.keptB = keptIds.size; }
      };

      try {
        await processSide('A', state.homeQueryIds?.homeA);
        await processSide('B', state.homeQueryIds?.homeB);
      } finally {
        setCountryFilter(null);
      }

      // Fire the 5 main queries with refilter joins
      const datasetA = state.datasetA;
      const datasetB = state.datasetB;
      const tableA = datasetA.source === 'all' ? getTableName(datasetA.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_a_'))!;
      const tableB = datasetB.source === 'all' ? getTableName(datasetB.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_b_'))!;

      const ids = await fireMainQueries(datasetA, datasetB, tableA, tableB, keptTables.keptA, keptTables.keptB);
      console.log(`[COMPARE] 5 refilter queries started after ZIP geocoding`);

      state = {
        ...state,
        phase: 'main_polling',
        queryIds: ids,
        done: {},
        keptTables,
        keptCounts,
        tempTables: [...(state.tempTables || []), ...newTempTables],
      };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
      return NextResponse.json({
        phase: 'main_polling', stateId: state.stateId,
        progress: { step: 'refilter', percent: 50, message: `Running Athena queries with ZIP-filtered sets (${keptCounts.keptA ?? '-'}/${keptCounts.keptB ?? '-'} devices)...` },
      });
    }

    // ── Phase: main_polling ──────────────────────────────────────
    if (state.phase === 'main_polling') {
      let allDone = true;
      let anyFailed = false;
      let errorMsg = '';
      let scannedGB = 0;

      for (const [key, qId] of Object.entries(state.queryIds)) {
        if (!qId || state.done[key as keyof typeof state.done]) continue;
        try {
          const status = await checkQueryStatus(qId);
          if (status.state === 'RUNNING' || status.state === 'QUEUED') {
            allDone = false;
            scannedGB += (status.statistics?.dataScannedBytes || 0) / 1e9;
          } else if (status.state === 'FAILED' || status.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `${key} failed: ${status.error || 'unknown'}`;
          } else {
            (state.done as any)[key] = true;
          }
        } catch (err: any) {
          if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
            anyFailed = true; errorMsg = `${key} expired — please retry`;
          } else throw err;
        }
      }

      if (anyFailed) {
        if (state.tempTables?.length) await cleanupTempTables(state.tempTables).catch(() => {});
        state = { ...state, phase: 'error', error: errorMsg };
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        return NextResponse.json({ phase: 'error', stateId: state.stateId, error: errorMsg });
      }

      if (!allDone) {
        await putConfig(STATE_KEY(state.stateId), state, { compact: true });
        const doneCount = Object.values(state.done).filter(Boolean).length;
        const totalQueries = Object.values(state.queryIds).filter(Boolean).length;
        const pct = 50 + Math.round((doneCount / Math.max(totalQueries, 1)) * 30);
        return NextResponse.json({
          phase: 'main_polling', stateId: state.stateId,
          progress: { step: 'polling', percent: pct, message: `Queries running (${doneCount}/${totalQueries} done, ${scannedGB.toFixed(1)}GB scanned)` },
        });
      }

      state = { ...state, phase: 'reading' };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
      return NextResponse.json({
        phase: 'reading', stateId: state.stateId,
        progress: { step: 'reading', percent: 85, message: 'Queries complete, reading results...' },
      });
    }

    // ── Phase: reading ───────────────────────────────────────────
    if (state.phase === 'reading') {
      const [countARes, countBRes, countOvlRes, poiOvlRes] = await Promise.all([
        fetchQueryResults(state.queryIds.countA!),
        fetchQueryResults(state.queryIds.countB!),
        fetchQueryResults(state.queryIds.countOverlap!),
        fetchQueryResults(state.queryIds.poiOverlap!),
      ]);

      const totalA = parseInt(countARes.rows[0]?.total, 10) || 0;
      const totalB = parseInt(countBRes.rows[0]?.total, 10) || 0;
      const overlapCount = parseInt(countOvlRes.rows[0]?.total, 10) || 0;

      // POI overlap: rows have { side: 'A'|'B', poi_id, overlap_devices }
      const poiRows = (poiOvlRes.rows || []) as Array<{ side?: string; poi_id?: string; overlap_devices?: string }>;

      // Fetch POI positions for both datasets (parallel) — dynamic import to keep
      // poi-storage (and its fs-traced POIs/ dir) out of this route's cold-start bundle.
      const { getPOIPositionsForDataset } = await import('@/lib/poi-storage');
      const [posA, posB] = await Promise.all([
        state.datasetA.source === 'all' ? getPOIPositionsForDataset(state.datasetA.name) : Promise.resolve([] as Awaited<ReturnType<typeof getPOIPositionsForDataset>>),
        state.datasetB.source === 'all' ? getPOIPositionsForDataset(state.datasetB.name) : Promise.resolve([] as Awaited<ReturnType<typeof getPOIPositionsForDataset>>),
      ]);
      const indexA = new Map(posA.map(p => [p.poiId, p]));
      const indexB = new Map(posB.map(p => [p.poiId, p]));

      const pois: PoiMatchRow[] = [];
      for (const r of poiRows) {
        const side = (r.side === 'B' ? 'B' : 'A') as 'A' | 'B';
        const poiId = String(r.poi_id || '');
        const overlapDevices = parseInt(r.overlap_devices || '0', 10) || 0;
        if (!poiId || overlapDevices <= 0) continue;
        const pos = side === 'A' ? indexA.get(poiId) : indexB.get(poiId);
        pois.push({
          side,
          poiId,
          name: pos?.name,
          lat: pos?.lat,
          lng: pos?.lng,
          overlapDevices,
        });
      }
      pois.sort((a, b) => b.overlapDevices - a.overlapDevices);

      const downloadKey = overlapCount > 0
        ? `/api/compare/download?queryId=${state.queryIds.exportOverlap}`
        : '';

      // Clean up temp tables
      if (state.tempTables?.length) await cleanupTempTables(state.tempTables).catch(() => {});

      const result: CompareResult = {
        totalA,
        totalB,
        overlap: overlapCount,
        overlapPctA: totalA > 0 ? Math.round((overlapCount / totalA) * 10000) / 100 : 0,
        overlapPctB: totalB > 0 ? Math.round((overlapCount / totalB) * 10000) / 100 : 0,
        downloadKey,
        pois,
        zipFilter: state.zipCodes && state.zipCodes.length > 0 ? {
          zipCodes: state.zipCodes,
          countryA: state.countryA,
          countryB: state.countryB,
          keptA: state.keptCounts?.keptA,
          keptB: state.keptCounts?.keptB,
        } : undefined,
      };

      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });

      console.log(`[COMPARE] Done: A=${totalA}, B=${totalB}, overlap=${overlapCount} (${result.overlapPctA}%/${result.overlapPctB}%), ${pois.length} POIs`);

      return NextResponse.json({
        phase: 'done', stateId: state.stateId, result,
        progress: { step: 'done', percent: 100, message: 'Comparison complete' },
      });
    }

    return NextResponse.json({ phase: 'error', stateId: stateId || '', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[COMPARE] Error:`, error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
