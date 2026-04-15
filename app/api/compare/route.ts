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

// ─────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────

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

interface KeptCoord { lat: number; lng: number; }

interface CompareState {
  phase:
    | 'home_polling'    // Athena is resolving unique home coords per side
    | 'geocoding'       // JS fetches the (small) coord set, geocodes, filters by ZIPs
    | 'main_polling'    // 5 main Athena queries running (counts + POI overlap + export)
    | 'reading'         // fetch results, build POI list
    | 'done'
    | 'error';
  schemaVersion?: number;
  stateId: string;
  datasetA: DatasetSide;
  datasetB: DatasetSide;
  zipCodes?: string[];
  countryA?: string;
  countryB?: string;

  // Phase 1: home-coord-unique query ids (one per side, only for source='all')
  homeQueryIds?: { homeA?: string; homeB?: string };
  homeDone?: { homeA?: boolean; homeB?: boolean };

  // After geocoding: small list of kept coords per side (inlined into main queries as VALUES)
  keptCoords?: { A?: KeptCoord[]; B?: KeptCoord[] };
  keptCoordsCount?: { A?: number; B?: number };

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

  tempTables?: string[]; // CSV-export temp tables (cleaned up at end)
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
    keptCoordsA?: number;
    keptCoordsB?: number;
  };
}

const STATE_KEY = (id: string) => `compare-state/${id}`;
const HOME_COORD_PRECISION = 1; // 1-decimal ~= 11km
const CURRENT_SCHEMA_VERSION = 3; // v3: inline VALUES kept_coords (no intermediate Athena tables)

// ─────────────────────────────────────────────────────────────────────
// SQL builders
// ─────────────────────────────────────────────────────────────────────

/** Hour WHERE clause (supports cross-midnight ranges). */
function hourFilter(side: DatasetSide): string {
  const hourFrom = side.hourFrom ?? 0;
  const hourTo = side.hourTo ?? 23;
  if (hourFrom === 0 && hourTo === 23) return '';
  if (hourFrom <= hourTo) {
    return `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
  }
  return `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
}

/**
 * Shared home-computation CTEs for ZIP-filter flow. Returns ad_home CTE with
 * columns (ad_id, home_lat, home_lng), filtered to device-coord pairs that were
 * the first ping of the day on 2+ distinct days.
 */
function homeCTEs(side: DatasetSide, tableName: string, aliasSuffix = ''): string {
  const hWhere = hourFilter(side);
  const s = aliasSuffix;
  return `poi_visitors${s} AS (
      SELECT DISTINCT ad_id FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    ),
    valid_pings${s} AS (
      SELECT t.ad_id, t.date, t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${tableName} t
      INNER JOIN poi_visitors${s} v ON t.ad_id = v.ad_id
      WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < 100)
        ${hWhere}
    ),
    first_pings${s} AS (
      SELECT ad_id, date,
        MIN_BY(lat, utc_timestamp) as origin_lat,
        MIN_BY(lng, utc_timestamp) as origin_lng
      FROM valid_pings${s}
      GROUP BY ad_id, date
    ),
    ad_home${s} AS (
      SELECT ad_id,
        ROUND(origin_lat, ${HOME_COORD_PRECISION}) as home_lat,
        ROUND(origin_lng, ${HOME_COORD_PRECISION}) as home_lng
      FROM first_pings${s}
      GROUP BY ad_id,
        ROUND(origin_lat, ${HOME_COORD_PRECISION}),
        ROUND(origin_lng, ${HOME_COORD_PRECISION})
      HAVING COUNT(DISTINCT date) >= 2
    )`;
}

/** Phase 1: returns ONLY unique rounded home coords (tiny — bounded by country geography). */
function homeCoordsUniqueSQL(side: DatasetSide, tableName: string): string {
  // No ORDER BY (expensive, unnecessary — we geocode every row anyway).
  // LIMIT kept as a safety net; a whole country at 1-decimal precision has <50k cells.
  return `WITH ${homeCTEs(side, tableName)}
    SELECT home_lat, home_lng, COUNT(DISTINCT ad_id) AS devices
    FROM ad_home
    GROUP BY home_lat, home_lng
    LIMIT 200000`;
}

/** Build the VALUES clause for kept coords, or null if no coords/no filter. */
function keptCoordsValues(kept: KeptCoord[] | undefined | null): string | null {
  if (!kept || kept.length === 0) return null;
  const rows = kept.map(c => `(CAST(${c.lat.toFixed(HOME_COORD_PRECISION)} AS DOUBLE), CAST(${c.lng.toFixed(HOME_COORD_PRECISION)} AS DOUBLE))`);
  return `(VALUES ${rows.join(',\n')}) AS kept(home_lat, home_lng)`;
}

/**
 * Build a subquery returning DISTINCT ad_id for a dataset side. When `keptCoords`
 * is provided the subquery is further constrained to ad_ids whose home is in the
 * kept-coords set (re-computes home inline via CTEs in the same subquery).
 *
 * Note: Athena/Trino v3 supports WITH inside subqueries. We use it here to keep
 * each main query self-contained (no dependency on outer CTEs).
 */
function maidSubquery(side: DatasetSide, tableName: string, keptCoords?: KeptCoord[]): string {
  const minDwell = side.minDwell || 0;
  const maxDwell = side.maxDwell || 0;
  const hourFrom = side.hourFrom ?? 0;
  const hourTo = side.hourTo ?? 23;
  const minVisits = Math.max(1, side.minVisits || 1);
  const hasHourFilter = hourFrom > 0 || hourTo < 23;
  const hasDwellFilter = minDwell > 0 || maxDwell > 0;
  const hasVisitFilter = minVisits > 1;

  let base: string;

  if (side.source === 'all') {
    if (hasDwellFilter || hasHourFilter || hasVisitFilter) {
      const hWhere = hourFilter(side);
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
            ${hWhere}
          GROUP BY ad_id, date, t2.poi_id
        ) dwell_per_day ${havingDwell}
      )`;

      if (hasVisitFilter) {
        base = `(SELECT ad_id FROM ${perDay} GROUP BY ad_id HAVING COUNT(DISTINCT date) >= ${minVisits})`;
      } else {
        base = `(SELECT DISTINCT ad_id FROM ${perDay})`;
      }
    } else {
      base = `(SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '')`;
    }
  } else {
    // CSV export: ad_id-only temp table — no filters apply
    base = `(SELECT DISTINCT ad_id FROM ${tableName} WHERE ad_id IS NOT NULL AND TRIM(ad_id) != '')`;
  }

  const keptValues = keptCoordsValues(keptCoords);
  // CSV-export sides can't be ZIP-filtered (no ping coords) — skip the JOIN for those.
  if (!keptValues || side.source !== 'all') return base;

  // Inline the home computation + kept-coords VALUES JOIN.
  // Using a subquery WITH (supported in Athena engine v3 / Trino).
  return `(
    WITH ${homeCTEs(side, tableName)},
    kept AS (SELECT * FROM ${keptValues}),
    kept_devices AS (
      SELECT DISTINCT h.ad_id
      FROM ad_home h
      INNER JOIN kept k ON h.home_lat = k.home_lat AND h.home_lng = k.home_lng
    )
    SELECT b.ad_id FROM ${base} b
    INNER JOIN kept_devices kd ON b.ad_id = kd.ad_id
  )`;
}

// ─────────────────────────────────────────────────────────────────────
// CSV-export temp table (side A or B when source is an export)
// ─────────────────────────────────────────────────────────────────────

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

  await runQuery(`
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tempName} (
      ad_id STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
    LOCATION 's3://${BUCKET}/${tempFolder}'
    TBLPROPERTIES ('skip.header.line.count'='1')
  `);
  console.log(`[COMPARE] Created temp table ${tempName} from ${exportFile}`);
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

// ─────────────────────────────────────────────────────────────────────
// Main-queries builder
// ─────────────────────────────────────────────────────────────────────

/**
 * Fires 5 parallel Athena queries: count A, count B, count overlap, export overlap
 * (MAIDs CSV), and POI overlap. When keptCoordsA/B are provided, each subA/subB
 * is constrained to devices with home in the kept set.
 */
async function fireMainQueries(
  datasetA: DatasetSide, datasetB: DatasetSide,
  tableA: string, tableB: string,
  keptCoordsA?: KeptCoord[], keptCoordsB?: KeptCoord[],
): Promise<{ countA: string; countB: string; countOverlap: string; exportOverlap: string; poiOverlap: string }> {
  const subA = maidSubquery(datasetA, tableA, keptCoordsA);
  const subB = maidSubquery(datasetB, tableB, keptCoordsB);

  // POI overlap: union CROSS JOIN UNNEST from each side (only for source='all'),
  // filtered to ad_ids in subA ∩ subB, grouped by poi_id.
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
  const poiOverlapSQL = arms.length === 0
    ? `SELECT CAST(NULL AS VARCHAR) AS side, CAST(NULL AS VARCHAR) AS poi_id, CAST(0 AS BIGINT) AS overlap_devices WHERE 1=0`
    : `WITH overlap_ids AS (SELECT a.ad_id FROM ${subA} a INNER JOIN ${subB} b ON a.ad_id = b.ad_id)
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
    startQueryAsync(poiOverlapSQL),
  ]);

  return { countA, countB, countOverlap, exportOverlap, poiOverlap };
}

// ─────────────────────────────────────────────────────────────────────
// Route handler
// ─────────────────────────────────────────────────────────────────────

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
        // Phase 1: fire home-coords-UNIQUE queries per source='all' side.
        let homeA: string | undefined;
        let homeB: string | undefined;
        if (datasetA.source === 'all') homeA = await startQueryAsync(homeCoordsUniqueSQL(datasetA, tableA));
        if (datasetB.source === 'all') homeB = await startQueryAsync(homeCoordsUniqueSQL(datasetB, tableB));

        console.log(`[COMPARE] Home-unique queries fired: homeA=${homeA} homeB=${homeB}`);
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

      // Fast path (no ZIP filter): fire main queries directly.
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

      state = { ...state, phase: 'geocoding' };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
      return NextResponse.json({
        phase: 'geocoding', stateId: state.stateId,
        progress: { step: 'geocoding', percent: 30, message: 'Reverse-geocoding homes to ZIPs...' },
      });
    }

    // ── Phase: geocoding ─────────────────────────────────────────
    // Fetch tiny coord sets, geocode in JS, keep matching coords, advance to main_polling.
    if (state.phase === 'geocoding') {
      const zipSet = new Set((state.zipCodes || []).map(z => z.toUpperCase()));

      const { setCountryFilter, batchReverseGeocode } = await import('@/lib/reverse-geocode');
      const countries: string[] = [];
      if (state.countryA) countries.push(state.countryA);
      if (state.countryB) countries.push(state.countryB);
      setCountryFilter(countries.length ? countries : null);

      const keptCoords: { A?: KeptCoord[]; B?: KeptCoord[] } = {};
      const keptCoordsCount: { A?: number; B?: number } = {};

      const processSide = async (sideLabel: 'A' | 'B', qId: string | undefined) => {
        if (!qId) return;
        const res = await fetchQueryResults(qId);
        // SMALL result: unique rounded coords with device counts.
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
        const kept: KeptCoord[] = [];
        for (let i = 0; i < coordList.length; i++) {
          const cls = classified[i];
          const pc = cls && cls.type === 'geojson_local' && cls.postcode ? String(cls.postcode).toUpperCase() : null;
          if (pc && zipSet.has(pc)) kept.push({ lat: coordList[i].lat, lng: coordList[i].lng });
        }
        console.log(`[COMPARE] side=${sideLabel}: ${kept.length}/${coordList.length} coords match ZIP filter`);

        keptCoords[sideLabel] = kept;
        keptCoordsCount[sideLabel] = kept.length;
      };

      try {
        await processSide('A', state.homeQueryIds?.homeA);
        await processSide('B', state.homeQueryIds?.homeB);
      } finally {
        setCountryFilter(null);
      }

      // Fire the main 5 queries with kept_coords inlined as VALUES.
      const datasetA = state.datasetA;
      const datasetB = state.datasetB;
      const tableA = datasetA.source === 'all' ? getTableName(datasetA.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_a_'))!;
      const tableB = datasetB.source === 'all' ? getTableName(datasetB.name) : (state.tempTables || []).find(n => n.startsWith('compare_tmp_b_'))!;

      const ids = await fireMainQueries(datasetA, datasetB, tableA, tableB, keptCoords.A, keptCoords.B);
      console.log(`[COMPARE] 5 main queries started with inlined kept_coords`);

      state = {
        ...state,
        phase: 'main_polling',
        queryIds: ids,
        done: {},
        keptCoords,
        keptCoordsCount,
      };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
      return NextResponse.json({
        phase: 'main_polling', stateId: state.stateId,
        progress: { step: 'main_polling', percent: 55, message: `Running Athena queries (ZIP-matched coords ${keptCoordsCount.A ?? '-'}/${keptCoordsCount.B ?? '-'})...` },
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
        const pct = 55 + Math.round((doneCount / Math.max(totalQueries, 1)) * 30);
        return NextResponse.json({
          phase: 'main_polling', stateId: state.stateId,
          progress: { step: 'polling', percent: pct, message: `Queries running (${doneCount}/${totalQueries} done, ${scannedGB.toFixed(1)}GB scanned)` },
        });
      }

      state = { ...state, phase: 'reading' };
      await putConfig(STATE_KEY(state.stateId), state, { compact: true });
      return NextResponse.json({
        phase: 'reading', stateId: state.stateId,
        progress: { step: 'reading', percent: 88, message: 'Queries complete, reading results...' },
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
      const poiRows = (poiOvlRes.rows || []) as Array<{ side?: string; poi_id?: string; overlap_devices?: string }>;

      // Lazy-load poi-storage (heavy NFT trace — see top-of-file note)
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

      if (state.tempTables?.length) await cleanupTempTables(state.tempTables).catch(() => {});

      const result: CompareResult = {
        totalA, totalB, overlap: overlapCount,
        overlapPctA: totalA > 0 ? Math.round((overlapCount / totalA) * 10000) / 100 : 0,
        overlapPctB: totalB > 0 ? Math.round((overlapCount / totalB) * 10000) / 100 : 0,
        downloadKey,
        pois,
        zipFilter: state.zipCodes && state.zipCodes.length > 0 ? {
          zipCodes: state.zipCodes,
          countryA: state.countryA,
          countryB: state.countryB,
          keptCoordsA: state.keptCoordsCount?.A,
          keptCoordsB: state.keptCoordsCount?.B,
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
    console.error(`[COMPARE] Error:`, error?.message, error?.stack);
    return NextResponse.json({ error: error?.message || 'Unknown error' }, { status: 500 });
  }
}
