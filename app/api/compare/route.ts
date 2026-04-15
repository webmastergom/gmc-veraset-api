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
// NOTE: @/lib/jobs, @/lib/reverse-geocode, @/lib/poi-storage are imported dynamically
// inside the specific phases that need them. Keeping them out of the module-level
// import graph reduces the Lambda cold-start bundle. @/lib/poi-storage in particular
// pulls in `fs` and triggers Next.js File Tracing on the POIs/ directory.

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
    | 'home_polling'       // phase 1: unique home-coord queries running
    | 'geocoding'          // phase 2: JS geocodes small coord set, fires kept-devices queries
    | 'devices_polling'    // phase 3: kept-devices queries (Athena materializes ad_id lists)
    | 'main_polling'       // phase 4: main 5 queries running (counts + POI overlap + export)
    | 'reading'            // phase 5: fetch results, build POI list
    | 'done'
    | 'error';
  schemaVersion?: number;  // bump when changing phase semantics; old states rejected
  stateId: string;
  datasetA: DatasetSide;
  datasetB: DatasetSide;
  zipCodes?: string[];
  countryA?: string;
  countryB?: string;

  // Phase 1: home-coord-unique queries
  homeQueryIds?: { homeA?: string; homeB?: string };
  homeDone?: { homeA?: boolean; homeB?: boolean };

  // After geocoding: kept-coords tables (tiny) + kept-devices query IDs (Athena materializes ad_id list)
  keptCoordsTables?: { keptCoordsA?: string; keptCoordsB?: string };
  keptDevicesQueryIds?: { keptDevicesA?: string; keptDevicesB?: string };
  keptDevicesDone?: { keptDevicesA?: boolean; keptDevicesB?: boolean };

  // After devices_polling: kept ad_id tables (inputs to main queries)
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
const CURRENT_SCHEMA_VERSION = 2; // v2: coord-unique home query + Athena-side device materialization

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
 * Build the first_pings / ad_home CTEs once. Used by both the coord-unique
 * query (phase 1) and the kept-devices materialization query (phase 3).
 */
function homeCTEs(side: DatasetSide, tableName: string): string {
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
  return `
    poi_visitors AS (
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
        ROUND(origin_lng, ${HOME_COORD_PRECISION}) as home_lng
      FROM first_pings
      GROUP BY ad_id,
        ROUND(origin_lat, ${HOME_COORD_PRECISION}),
        ROUND(origin_lng, ${HOME_COORD_PRECISION})
      HAVING COUNT(DISTINCT date) >= 2
    )`;
}

/**
 * Phase 1 query: returns ONLY unique rounded home coords (not per-device).
 * This is what JS fetches & geocodes — size bounded by country geography (~50k coords).
 * CRITICAL: must NOT return per-device rows — that's what OOMed previously.
 */
function homeCoordsUniqueSQL(side: DatasetSide, tableName: string): string {
  return `
    WITH ${homeCTEs(side, tableName)}
    SELECT home_lat, home_lng, COUNT(DISTINCT ad_id) AS devices
    FROM ad_home
    GROUP BY home_lat, home_lng
    ORDER BY devices DESC
    LIMIT 500000
  `;
}

/**
 * Phase 3 query: uses the kept-coords table to materialize the ad_id list.
 * Output lands in Athena results S3; we wrap it with an external table later.
 */
function keptDevicesSQL(side: DatasetSide, tableName: string, keptCoordsTable: string): string {
  return `
    WITH ${homeCTEs(side, tableName)}
    SELECT DISTINCT h.ad_id
    FROM ad_home h
    INNER JOIN ${keptCoordsTable} k
      ON h.home_lat = k.home_lat AND h.home_lng = k.home_lng
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
 * Write kept (home_lat, home_lng) rows to S3 as CSV and create an external Athena
 * table. Tiny (< a few thousand rows even for a whole country).
 */
async function createKeptCoordsTable(coords: Array<{ lat: number; lng: number }>, side: string): Promise<string> {
  const tempName = `compare_kcrd_${side}_${Date.now()}`;
  const tempFolder = `compare-temp/${tempName}/`;

  // Deduplicate and stringify at HOME_COORD_PRECISION so Athena DOUBLE equality holds.
  const seen = new Set<string>();
  const lines: string[] = ['home_lat,home_lng'];
  for (const c of coords) {
    if (!Number.isFinite(c.lat) || !Number.isFinite(c.lng)) continue;
    const key = `${c.lat.toFixed(HOME_COORD_PRECISION)},${c.lng.toFixed(HOME_COORD_PRECISION)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    lines.push(key);
  }
  const body = lines.join('\n') + '\n';

  const { PutObjectCommand } = await import('@aws-sdk/client-s3');
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${tempFolder}data.csv`,
    Body: body,
    ContentType: 'text/csv',
  }));

  const createSql = `
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tempName} (
      home_lat DOUBLE,
      home_lng DOUBLE
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    LOCATION 's3://${BUCKET}/${tempFolder}'
    TBLPROPERTIES ('skip.header.line.count'='1')
  `;
  await runQuery(createSql);
  console.log(`[COMPARE] Created kept-coords table ${tempName} with ${seen.size} coords`);
  return tempName;
}

/**
 * Copy an Athena result CSV (the output of a `SELECT DISTINCT ad_id` query) into
 * its own S3 folder and create an external table over it. This is our "kept ad_ids"
 * set, used as keptTable in the main queries.
 */
async function createKeptAdIdsTableFromQuery(queryId: string, side: string): Promise<string> {
  const tempName = `compare_kadi_${side}_${Date.now()}`;
  const tempFolder = `compare-temp/${tempName}/`;

  // Athena writes query outputs to s3://<bucket>/athena-results/<queryId>.csv by default.
  const { CopyObjectCommand } = await import('@aws-sdk/client-s3');
  await s3Client.send(new CopyObjectCommand({
    Bucket: BUCKET,
    CopySource: `${BUCKET}/athena-results/${queryId}.csv`,
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
  console.log(`[COMPARE] Created kept-ad_ids table ${tempName} from query ${queryId}`);
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

/** Find job.country for a dataset — matches by s3DestPath folder. Lazy-loads jobs lib. */
async function findDatasetCountry(datasetName: string): Promise<string | undefined> {
  const { getAllJobs } = await import('@/lib/jobs');
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

      // Reject states from older deploys — their phase semantics differ and can OOM.
      if (state && (state.schemaVersion || 0) < CURRENT_SCHEMA_VERSION) {
        console.warn(`[COMPARE] Rejecting stale state ${stateId} (schema v${state.schemaVersion || 0} < v${CURRENT_SCHEMA_VERSION})`);
        return NextResponse.json({
          phase: 'error', stateId,
          error: 'This comparison was started before a code update — please retry from scratch.',
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
        // Phase 1: fire home-coords-UNIQUE queries (small result, safe to fetch in JS)
        let homeA: string | undefined;
        let homeB: string | undefined;
        const toFire: Array<Promise<string>> = [];
        if (datasetA.source === 'all') toFire.push(startQueryAsync(homeCoordsUniqueSQL(datasetA, tableA)).then(id => { homeA = id; return id; }));
        if (datasetB.source === 'all') toFire.push(startQueryAsync(homeCoordsUniqueSQL(datasetB, tableB)).then(id => { homeB = id; return id; }));
        await Promise.all(toFire);

        console.log(`[COMPARE] Home-coord queries fired: homeA=${homeA} homeB=${homeB}`);
        state = {
          schemaVersion: CURRENT_SCHEMA_VERSION,
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
        schemaVersion: CURRENT_SCHEMA_VERSION,
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
    // Fetch tiny coord set per side, geocode in JS, keep matching coords,
    // then fire an Athena query per side that materializes the ad_id list.
    if (state.phase === 'geocoding') {
      const zipSet = new Set((state.zipCodes || []).map(z => z.toUpperCase()));

      const { setCountryFilter, batchReverseGeocode } = await import('@/lib/reverse-geocode');
      const countries: string[] = [];
      if (state.countryA) countries.push(state.countryA);
      if (state.countryB) countries.push(state.countryB);
      setCountryFilter(countries.length ? countries : null);

      const keptCoordsTables: { keptCoordsA?: string; keptCoordsB?: string } = {};
      const keptDevicesQueryIds: { keptDevicesA?: string; keptDevicesB?: string } = {};
      const newTempTables: string[] = [];

      const datasetA = state.datasetA;
      const datasetB = state.datasetB;
      const tableA = datasetA.source === 'all' ? getTableName(datasetA.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_a_'))!;
      const tableB = datasetB.source === 'all' ? getTableName(datasetB.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_b_'))!;

      const processSide = async (sideLabel: 'A' | 'B', qId: string | undefined, side: DatasetSide, tableName: string) => {
        if (!qId) return;
        // Phase-1 results are SMALL: unique rounded coords with device counts.
        const res = await fetchQueryResults(qId);
        const rows = res.rows as Array<{ home_lat?: string; home_lng?: string; devices?: string }>;
        console.log(`[COMPARE] side=${sideLabel}: ${rows.length} unique home coords`);

        const coordList: Array<{ lat: number; lng: number; deviceCount: number }> = [];
        for (const r of rows) {
          const lat = Number(r.home_lat);
          const lng = Number(r.home_lng);
          const devices = Number(r.devices) || 0;
          if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
          coordList.push({ lat, lng, deviceCount: devices });
        }

        const classified = await batchReverseGeocode(coordList);
        // Keep only coords whose postcode is in the requested ZIP set
        const kept: Array<{ lat: number; lng: number }> = [];
        for (let i = 0; i < coordList.length; i++) {
          const cls = classified[i];
          const pc = cls && cls.type === 'geojson_local' && cls.postcode ? String(cls.postcode).toUpperCase() : null;
          if (pc && zipSet.has(pc)) kept.push({ lat: coordList[i].lat, lng: coordList[i].lng });
        }
        console.log(`[COMPARE] side=${sideLabel}: ${kept.length}/${coordList.length} coords match ZIP filter`);

        // Materialize kept coords as an external table (tiny CSV — Athena-safe)
        const keptCoordsTable = await createKeptCoordsTable(kept, sideLabel.toLowerCase());
        newTempTables.push(keptCoordsTable);

        // Fire the kept-devices Athena query (uses the kept_coords table).
        const keptDevicesId = await startQueryAsync(keptDevicesSQL(side, tableName, keptCoordsTable));
        console.log(`[COMPARE] side=${sideLabel}: kept-devices query ${keptDevicesId} started`);

        if (sideLabel === 'A') {
          keptCoordsTables.keptCoordsA = keptCoordsTable;
          keptDevicesQueryIds.keptDevicesA = keptDevicesId;
        } else {
          keptCoordsTables.keptCoordsB = keptCoordsTable;
          keptDevicesQueryIds.keptDevicesB = keptDevicesId;
        }
      };

      try {
        await processSide('A', state.homeQueryIds?.homeA, datasetA, tableA);
        await processSide('B', state.homeQueryIds?.homeB, datasetB, tableB);
      } finally {
        setCountryFilter(null);
      }

      state = {
        ...state,
        phase: 'devices_polling',
        keptCoordsTables,
        keptDevicesQueryIds,
        keptDevicesDone: {},
        tempTables: [...(state.tempTables || []), ...newTempTables],
      };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
      return NextResponse.json({
        phase: 'devices_polling', stateId: state.stateId,
        progress: { step: 'devices_polling', percent: 45, message: 'Resolving ZIP-matched devices in Athena...' },
      });
    }

    // ── Phase: devices_polling ───────────────────────────────────
    // Poll the 2 kept-devices Athena queries. When both done, copy their outputs
    // into compare-temp tables and fire the 5 main queries.
    if (state.phase === 'devices_polling') {
      let allDone = true;
      let anyFailed = false;
      let errorMsg = '';
      let scannedGB = 0;

      for (const key of ['keptDevicesA', 'keptDevicesB'] as const) {
        const qId = state.keptDevicesQueryIds?.[key];
        if (!qId || state.keptDevicesDone?.[key]) continue;
        try {
          const status = await checkQueryStatus(qId);
          if (status.state === 'RUNNING' || status.state === 'QUEUED') {
            allDone = false;
            scannedGB += (status.statistics?.dataScannedBytes || 0) / 1e9;
          } else if (status.state === 'FAILED' || status.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `${key} failed: ${status.error || 'unknown'}`;
          } else {
            state.keptDevicesDone = { ...state.keptDevicesDone, [key]: true };
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
          phase: 'devices_polling', stateId: state.stateId,
          progress: { step: 'devices_polling', percent: 55, message: `Resolving ZIP-matched devices (${scannedGB.toFixed(1)}GB scanned)` },
        });
      }

      // Materialize kept ad_ids and fire main queries
      const keptTables: { keptA?: string; keptB?: string } = {};
      const keptCounts: { keptA?: number; keptB?: number } = {};
      const newTempTables: string[] = [];

      for (const key of ['keptDevicesA', 'keptDevicesB'] as const) {
        const qId = state.keptDevicesQueryIds?.[key];
        if (!qId) continue;
        const sideLabel = key === 'keptDevicesA' ? 'a' : 'b';
        const keptTable = await createKeptAdIdsTableFromQuery(qId, sideLabel);
        newTempTables.push(keptTable);

        // Count kept ad_ids (small query)
        const countQ = await runQuery(`SELECT COUNT(*) as total FROM ${keptTable}`);
        const countRow = countQ.rows?.[0] as any;
        const count = parseInt(countRow?.total || countRow?.Total || '0', 10) || 0;
        if (key === 'keptDevicesA') { keptTables.keptA = keptTable; keptCounts.keptA = count; }
        else { keptTables.keptB = keptTable; keptCounts.keptB = count; }
      }

      const datasetA = state.datasetA;
      const datasetB = state.datasetB;
      const tableA = datasetA.source === 'all' ? getTableName(datasetA.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_a_'))!;
      const tableB = datasetB.source === 'all' ? getTableName(datasetB.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_b_'))!;

      const ids = await fireMainQueries(datasetA, datasetB, tableA, tableB, keptTables.keptA, keptTables.keptB);
      console.log(`[COMPARE] 5 main queries started with kept ad_ids tables`);

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
        progress: { step: 'main_polling', percent: 60, message: `Running Athena queries (ZIP-matched ${keptCounts.keptA ?? '-'}/${keptCounts.keptB ?? '-'})...` },
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
