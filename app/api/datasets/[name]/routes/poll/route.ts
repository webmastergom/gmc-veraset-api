import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { toIsoCountry } from '@/lib/country-inference';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const STATE_KEY = (ds: string, filters: string) => `routes-state/${ds}-${filters}`;

const ACCURACY_THRESHOLD = 500;
const SPATIAL_RADIUS = 200;
const GRID_STEP = 0.01;
const BBOX_BUFFER = 0.02;

interface RoutesState {
  phase: 'starting' | 'polling' | 'reading' | 'done' | 'error';
  sankeyQueryId?: string;
  sampleQueryId?: string;
  sankeySeed?: number; // random seed for sample
  sankeyDone?: boolean;
  sampleDone?: boolean;
  filters: {
    minDwell: number;
    maxDwell: number;
    hourFrom: number;
    hourTo: number;
    country: string;
  };
  error?: string;
  result?: RoutesResult;
}

interface SankeyFlow {
  direction: 'before' | 'after';
  group_key: string;
  group_label: string;
  devices: number;
  pings: number;
}

interface SampleStop {
  ad_id: string;
  date: string;
  ts: string;
  direction: 'before' | 'during' | 'after';
  group_key: string;
  group_label: string;
  dwell_minutes: number;
}

interface RoutesResult {
  sankey: SankeyFlow[];
  sampleRoutes: SampleStop[];
  totalVisitors: number;
}

/**
 * Build SQL for the before/after target visits CTE.
 * Reused by both Sankey and Sample queries.
 */
function targetVisitsCTE(
  table: string,
  minDwell: number,
  maxDwell: number,
  hourFrom: number,
  hourTo: number,
): string {
  // Hour filter on pings
  let hourWhere = '';
  if (hourFrom > 0 || hourTo < 23) {
    if (hourFrom <= hourTo) {
      hourWhere = `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
    } else {
      hourWhere = `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
    }
  }

  // Dwell HAVING — must use full expression, not SELECT alias (Athena/Presto restriction)
  const dwellExpr = "DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp))";
  const dwellParts: string[] = [];
  if (minDwell > 0) dwellParts.push(`${dwellExpr} >= ${minDwell}`);
  if (maxDwell > 0) dwellParts.push(`${dwellExpr} <= ${maxDwell}`);
  const havingClause = dwellParts.length > 0 ? `HAVING ${dwellParts.join(' AND ')}` : '';

  return `
    target_visits AS (
      SELECT ad_id, date,
        MIN(utc_timestamp) as arrival,
        MAX(utc_timestamp) as departure,
        DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t2(poi_id)
      WHERE t2.poi_id IS NOT NULL AND t2.poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${hourWhere}
      GROUP BY ad_id, date, t2.poi_id
      ${havingClause}
    ),
    target_daily AS (
      SELECT ad_id, date, arrival as first_arrival, departure as last_departure, dwell
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ad_id, date ORDER BY dwell DESC, arrival) as rn
        FROM target_visits
      ) t_ranked WHERE rn = 1
    )`;
}

/**
 * Common CTEs for spatial join with lab_pois_gmc — POI grid + pings.
 * Returns the categorized pings with direction (before/during/after).
 */
function spatialJoinCTEs(
  table: string,
  country: string,
  hourFrom: number,
  hourTo: number,
  pinsCTE: string, // name of the CTE providing ad_id rows to join against
): string {
  const cc = toIsoCountry(country);

  // Hour filter on ALL pings (not just target visits)
  let hourWhere = '';
  if (hourFrom > 0 || hourTo < 23) {
    if (hourFrom <= hourTo) {
      hourWhere = `AND HOUR(p.utc_timestamp) >= ${hourFrom} AND HOUR(p.utc_timestamp) <= ${hourTo}`;
    } else {
      hourWhere = `AND (HOUR(p.utc_timestamp) >= ${hourFrom} OR HOUR(p.utc_timestamp) <= ${hourTo})`;
    }
  }

  return `
    gmc_pois AS (
      SELECT id as poi_id, category,
        CAST(latitude AS DOUBLE) as poi_lat,
        CAST(longitude AS DOUBLE) as poi_lng,
        CAST(FLOOR(CAST(latitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as base_lat_b,
        CAST(FLOOR(CAST(longitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as base_lng_b
      FROM lab_pois_gmc
      WHERE category IS NOT NULL
        AND country = '${cc}'
    ),
    poi_buckets AS (
      SELECT poi_id, category, poi_lat, poi_lng,
        base_lat_b + dlat as lat_b,
        base_lng_b + dlng as lng_b
      FROM gmc_pois
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
    ),
    all_pings AS (
      SELECT
        p.ad_id, p.date, p.utc_timestamp,
        TRY_CAST(p.latitude AS DOUBLE) as lat,
        TRY_CAST(p.longitude AS DOUBLE) as lng,
        CAST(FLOOR(TRY_CAST(p.latitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lat_b,
        CAST(FLOOR(TRY_CAST(p.longitude AS DOUBLE) / ${GRID_STEP}) AS BIGINT) as lng_b,
        CASE
          WHEN p.utc_timestamp < td.first_arrival THEN 'before'
          ELSE 'after'
        END as direction
      FROM ${table} p
      INNER JOIN ${pinsCTE} s ON p.ad_id = s.ad_id
      INNER JOIN target_daily td ON p.ad_id = td.ad_id AND p.date = td.date
      WHERE TRY_CAST(p.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(p.longitude AS DOUBLE) IS NOT NULL
        AND (p.horizontal_accuracy IS NULL OR TRY_CAST(p.horizontal_accuracy AS DOUBLE) < ${ACCURACY_THRESHOLD})
        AND p.ad_id IS NOT NULL AND TRIM(p.ad_id) != ''
        AND (p.utc_timestamp < td.first_arrival OR p.utc_timestamp > td.last_departure)
        ${hourWhere}
    ),
    matched AS (
      SELECT
        ap.ad_id, ap.date, ap.utc_timestamp, ap.direction,
        pb.category
      FROM all_pings ap
      INNER JOIN poi_buckets pb
        ON ap.lat_b = pb.lat_b AND ap.lng_b = pb.lng_b
      WHERE 111320 * SQRT(
          POW(ap.lat - pb.poi_lat, 2) +
          POW((ap.lng - pb.poi_lng) * COS(RADIANS((ap.lat + pb.poi_lat) / 2)), 2)
        ) <= ${SPATIAL_RADIUS}
    )`;
}

/**
 * Build the aggregated Sankey query.
 * Returns: direction, category_group, devices, pings
 * Groups individual POI categories into CATEGORY_GROUPS for cleaner Sankey.
 */
function buildSankeySQL(
  table: string,
  country: string,
  minDwell: number,
  maxDwell: number,
  hourFrom: number,
  hourTo: number,
): string {
  const tvCTE = targetVisitsCTE(table, minDwell, maxDwell, hourFrom, hourTo);
  // For Sankey we use ALL visitors (no sampling)
  // But we need a CTE that provides ad_id for the spatial join
  const sjCTEs = spatialJoinCTEs(table, country, hourFrom, hourTo, 'target_daily');

  return `
    WITH
    ${tvCTE},
    ${sjCTEs},
    categorized AS (
      SELECT
        ad_id, direction, date,
        category,
        DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0 as dwell_min
      FROM matched
      WHERE direction IN ('before', 'after')
      GROUP BY ad_id, direction, date, category
    )
    SELECT
      direction,
      category,
      COUNT(DISTINCT ad_id) as devices,
      COUNT(*) as visits
    FROM categorized
    GROUP BY direction, category
    ORDER BY direction, devices DESC
  `;
}

/**
 * Build the sample routes query.
 * Returns per-ping data for 1000 random devices, categorized.
 */
function buildSampleRoutesSQL(
  table: string,
  country: string,
  minDwell: number,
  maxDwell: number,
  hourFrom: number,
  hourTo: number,
): string {
  const tvCTE = targetVisitsCTE(table, minDwell, maxDwell, hourFrom, hourTo);

  return `
    WITH
    ${tvCTE},
    sampled AS (
      SELECT ad_id FROM (
        SELECT ad_id, ROW_NUMBER() OVER (ORDER BY XXHASH64(CAST(ad_id AS VARBINARY))) as rn
        FROM (SELECT DISTINCT ad_id FROM target_daily) t_uniq
      ) t_ranked WHERE rn <= 1000
    ),
    ${spatialJoinCTEs(table, country, hourFrom, hourTo, 'sampled')},
    categorized AS (
      -- Before/after stops: spatial join with lab_pois_gmc
      SELECT
        ad_id, date, direction, category,
        MIN(utc_timestamp) as first_ts,
        MAX(utc_timestamp) as last_ts,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_min
      FROM matched
      GROUP BY ad_id, date, direction, category

      UNION ALL

      -- Target visit: single entry per device-day (no spatial join explosion)
      SELECT
        td.ad_id, td.date, 'during' as direction, 'target' as category,
        td.first_arrival as first_ts,
        td.last_departure as last_ts,
        CAST(td.dwell AS DOUBLE) as dwell_min
      FROM target_daily td
      INNER JOIN sampled s ON td.ad_id = s.ad_id
    )
    SELECT
      ad_id, date,
      CAST(first_ts AS VARCHAR) as first_ts,
      direction,
      category,
      dwell_min
    FROM categorized
    ORDER BY ad_id, date, first_ts
  `;
}

/**
 * Build a simple count query for total visitors.
 */
function buildVisitorCountSQL(
  table: string,
  minDwell: number,
  maxDwell: number,
  hourFrom: number,
  hourTo: number,
): string {
  const tvCTE = targetVisitsCTE(table, minDwell, maxDwell, hourFrom, hourTo);
  return `
    WITH ${tvCTE}
    SELECT COUNT(DISTINCT ad_id) as total FROM target_daily
  `;
}

function filtersKey(f: RoutesState['filters']): string {
  return `d${f.minDwell}-${f.maxDwell}_h${f.hourFrom}-${f.hourTo}`;
}

/**
 * POST /api/datasets/[name]/routes/poll
 *
 * Multi-phase endpoint for device route analysis.
 * Phase 1 (starting): Launch 3 Athena queries (sankey + sample + count)
 * Phase 2 (polling): Poll all queries
 * Phase 3 (reading): Parse results
 * Phase 4 (done): Return cached result
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { name: datasetName } = await context.params;

  try {
    let body: any;
    try { body = await request.json(); } catch { body = {}; }
    const isNewRequest = !!body.country;

    const filters = {
      minDwell: parseInt(body.minDwell, 10) || 0,
      maxDwell: parseInt(body.maxDwell, 10) || 0,
      hourFrom: parseInt(body.hourFrom, 10) || 0,
      hourTo: body.hourTo != null ? parseInt(body.hourTo, 10) : 23,
      country: body.country || '',
    };

    const fKey = filtersKey(filters);
    // Try to load existing state (use filters from body if new, or from state)
    let stateKey = '';
    if (isNewRequest) {
      stateKey = STATE_KEY(datasetName, fKey);
    } else {
      // On polling, try to find the state key from the body
      stateKey = body.stateKey || '';
    }

    let state = stateKey ? await getConfig<RoutesState>(stateKey) : null;

    // Return cached result if done and not a new request
    if (state?.phase === 'done' && !isNewRequest && state.result) {
      return NextResponse.json({
        phase: 'done',
        stateKey,
        result: state.result,
        progress: { step: 'done', percent: 100, message: 'Route analysis complete' },
      });
    }

    // Reset on error or new request
    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: starting ──────────────────────────────────────────
    if (!state) {
      if (!filters.country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      stateKey = STATE_KEY(datasetName, fKey);
      console.log(`[ROUTES] Starting for ${datasetName}, ${JSON.stringify(filters)}`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      const sankeySql = buildSankeySQL(table, filters.country, filters.minDwell, filters.maxDwell, filters.hourFrom, filters.hourTo);
      const sampleSql = buildSampleRoutesSQL(table, filters.country, filters.minDwell, filters.maxDwell, filters.hourFrom, filters.hourTo);
      const countSql = buildVisitorCountSQL(table, filters.minDwell, filters.maxDwell, filters.hourFrom, filters.hourTo);

      const [sankeyQId, sampleQId, countQId] = await Promise.all([
        startQueryAsync(sankeySql).catch(e => { console.error('[ROUTES] Sankey query failed:', e.message); return undefined; }),
        startQueryAsync(sampleSql).catch(e => { console.error('[ROUTES] Sample query failed:', e.message); return undefined; }),
        startQueryAsync(countSql).catch(e => { console.error('[ROUTES] Count query failed:', e.message); return undefined; }),
      ]);

      console.log(`[ROUTES] 3 queries launched: sankey=${sankeyQId}, sample=${sampleQId}, count=${countQId}`);

      state = {
        phase: 'polling',
        sankeyQueryId: sankeyQId,
        sampleQueryId: sampleQId,
        sankeySeed: Date.now(),
        filters,
        sankeyDone: false,
        sampleDone: false,
      };
      // Store countQId in state too
      (state as any).countQueryId = countQId;
      await putConfig(stateKey, state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        stateKey,
        progress: { step: 'queries_started', percent: 10, message: 'Running route analysis queries...' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    if (state.phase === 'polling') {
      let allDone = true;
      let anyFailed = false;
      let errorMsg = '';

      const queries = [
        { name: 'sankey', id: state.sankeyQueryId, doneKey: 'sankeyDone' },
        { name: 'sample', id: state.sampleQueryId, doneKey: 'sampleDone' },
        { name: 'count', id: (state as any).countQueryId, doneKey: 'countDone' },
      ];

      for (const q of queries) {
        if (!q.id || (state as any)[q.doneKey]) continue;
        try {
          const status = await checkQueryStatus(q.id);
          if (status.state === 'RUNNING' || status.state === 'QUEUED') {
            allDone = false;
          } else if (status.state === 'FAILED' || status.state === 'CANCELLED') {
            anyFailed = true;
            errorMsg = `${q.name}: ${status.error || 'failed'}`;
          } else {
            (state as any)[q.doneKey] = true;
          }
        } catch (err: any) {
          if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
            anyFailed = true;
            errorMsg = `${q.name} expired — please retry`;
          } else throw err;
        }
      }

      if (anyFailed) {
        state = { ...state, phase: 'error', error: errorMsg };
        await putConfig(stateKey, state, { compact: true });
        return NextResponse.json({ phase: 'error', stateKey, error: errorMsg });
      }

      if (!allDone) {
        const doneCount = queries.filter(q => (state as any)[q.doneKey]).length;
        await putConfig(stateKey, state, { compact: true });
        return NextResponse.json({
          phase: 'polling',
          stateKey,
          progress: {
            step: 'polling',
            percent: 10 + Math.round((doneCount / 3) * 50),
            message: `Queries running (${doneCount}/3 done)...`,
          },
        });
      }

      // All done → reading
      state = { ...state, phase: 'reading' };
      await putConfig(stateKey, state, { compact: true });
      // Fall through
    }

    // ── Phase: reading ───────────────────────────────────────────
    if (state.phase === 'reading') {
      const { CATEGORY_GROUPS } = await import('@/lib/laboratory-types');

      // Helper: category → group key + label
      const catToGroup = (cat: string): { key: string; label: string } => {
        for (const [gk, g] of Object.entries(CATEGORY_GROUPS)) {
          if ((g.categories as readonly string[]).includes(cat)) {
            return { key: gk, label: g.label };
          }
        }
        return { key: 'other', label: 'Other' };
      };

      // Parse Sankey results
      let sankeyFlows: SankeyFlow[] = [];
      if (state.sankeyQueryId) {
        try {
          const res = await fetchQueryResults(state.sankeyQueryId);
          // Aggregate by group
          const grouped = new Map<string, SankeyFlow>();
          for (const row of res.rows) {
            const dir = row.direction as 'before' | 'after';
            const cat = row.category || 'unknown';
            const g = catToGroup(cat);
            const mapKey = `${dir}:${g.key}`;
            const existing = grouped.get(mapKey);
            if (existing) {
              existing.devices += parseInt(row.devices, 10) || 0;
              existing.pings += parseInt(row.visits, 10) || 0;
            } else {
              grouped.set(mapKey, {
                direction: dir,
                group_key: g.key,
                group_label: g.label,
                devices: parseInt(row.devices, 10) || 0,
                pings: parseInt(row.visits, 10) || 0,
              });
            }
          }
          sankeyFlows = Array.from(grouped.values())
            .sort((a, b) => b.devices - a.devices);
        } catch (err: any) {
          console.error('[ROUTES] Error reading sankey results:', err.message);
        }
      }

      // Parse sample routes
      let sampleStops: SampleStop[] = [];
      if (state.sampleQueryId) {
        try {
          const res = await fetchQueryResults(state.sampleQueryId);
          sampleStops = res.rows.map((row: any) => {
            const isTarget = row.category === 'target' || row.direction === 'during';
            const g = isTarget ? { key: 'target', label: 'Target POI' } : catToGroup(row.category || 'unknown');
            return {
              ad_id: row.ad_id,
              date: row.date,
              ts: row.first_ts,
              direction: row.direction as 'before' | 'during' | 'after',
              group_key: g.key,
              group_label: g.label,
              dwell_minutes: parseFloat(row.dwell_min) || 0,
            };
          });
        } catch (err: any) {
          console.error('[ROUTES] Error reading sample results:', err.message);
        }
      }

      // Parse total visitors count
      let totalVisitors = 0;
      if ((state as any).countQueryId) {
        try {
          const res = await fetchQueryResults((state as any).countQueryId);
          totalVisitors = parseInt(res.rows[0]?.total, 10) || 0;
        } catch (err: any) {
          console.error('[ROUTES] Error reading count:', err.message);
        }
      }

      const result: RoutesResult = {
        sankey: sankeyFlows,
        sampleRoutes: sampleStops,
        totalVisitors,
      };

      state = { ...state, phase: 'done', result };
      await putConfig(stateKey, state, { compact: true });

      console.log(`[ROUTES] Done: ${sankeyFlows.length} sankey flows, ${sampleStops.length} sample stops, ${totalVisitors} visitors`);

      return NextResponse.json({
        phase: 'done',
        stateKey,
        result,
        progress: { step: 'done', percent: 100, message: 'Route analysis complete' },
      });
    }

    return NextResponse.json({ phase: 'error', stateKey, error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[ROUTES] Error:`, error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
