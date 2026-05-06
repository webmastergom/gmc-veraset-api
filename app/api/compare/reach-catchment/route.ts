/**
 * Compare → Potential Reach → Catchment ZIPs export.
 *
 * Given a finished Reach analysis (referenced by `stateId` + `direction`),
 * this endpoint builds a CSV of `ad_id, home_lat, home_lng, home_zip,
 * home_city` for every device that qualified in that direction. Home is
 * resolved by:
 *   1. Per (ad_id, date) → first-of-day origin coord + native_zip from
 *      `geo_fields['zipcode']` when present.
 *   2. Per ad_id → rounded coord with the most distinct days-at-location
 *      (ROW_NUMBER pick-top-home).
 *   3. Coords without native_zip → batch reverse-geocoded server-side using
 *      the source dataset's `job.country`.
 *
 * Multi-phase polling: starting → polling → geocoding → done. The output
 * CSV lives at `compare/catchment-zips/{stateId}-{direction}.csv` and is
 * served via `/api/compare/download?key=...` (raw S3 path).
 */

import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig, BUCKET, s3Client } from '@/lib/s3-config';
import { PutObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

// ── Types ─────────────────────────────────────────────────────────────

interface PoiPosition {
  poiId: string;
  name?: string;
  lat: number;
  lng: number;
}

interface ReachConfig {
  maxDistanceMeters: number;
  minPings: number;
  minDwellMinutes: number;
}

interface SourceFilters {
  minDwell?: number;
  maxDwell?: number;
  hourFrom?: number;
  hourTo?: number;
  minVisits?: number;
}

interface ReachState {
  phase: string;
  stateId: string;
  datasetA: string;
  datasetB: string;
  config: ReachConfig;
  filtersA: SourceFilters;
  filtersB: SourceFilters;
  directions: ('aToB' | 'bToA')[];
}

interface CatchmentExportState {
  phase: 'querying' | 'polling' | 'geocoding' | 'done' | 'error';
  catchmentId: string;
  reachStateId: string;
  direction: 'aToB' | 'bToA';
  queryId?: string;
  downloadKey?: string;
  rowCount?: number;
  nativeZipCount?: number;
  geocodedCount?: number;
  error?: string;
}

const REACH_KEY = (id: string) => `compare-reach-state/${id}`;
const CATCHMENT_KEY = (id: string) => `compare-reach-catchment/${id}`;

const ACCURACY = 500;
const PREC = 4;
const GRID = 0.01;

// ── SQL helpers (mirror reach-poll's filters) ────────────────────────

function poisValues(pois: PoiPosition[]): string {
  return pois
    .map((p) => {
      const name = (p.name || p.poiId).replace(/'/g, "''");
      const id = p.poiId.replace(/'/g, "''");
      return `('${id}', '${name}', CAST(${p.lat} AS DOUBLE), CAST(${p.lng} AS DOUBLE))`;
    })
    .join(', ');
}

function buildSourceFilterSQL(f?: SourceFilters): { hourClause: string; dwellHaving: string; minVisits: number } {
  const hourFrom = f?.hourFrom ?? 0;
  const hourTo = f?.hourTo ?? 23;
  let hourClause = '';
  if (hourFrom > 0 || hourTo < 23) {
    hourClause = hourFrom <= hourTo
      ? `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`
      : `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
  }
  const minDwell = f?.minDwell ?? 0;
  const maxDwell = f?.maxDwell ?? 0;
  const dwellParts: string[] = [];
  if (minDwell > 0) dwellParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
  if (maxDwell > 0) dwellParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
  const dwellHaving = dwellParts.length > 0 ? `HAVING ${dwellParts.join(' AND ')}` : '';
  const minVisits = Math.max(1, f?.minVisits ?? 1);
  return { hourClause, dwellHaving, minVisits };
}

function sourceVisitorsCTE(sourceTable: string, f?: SourceFilters): string {
  const { hourClause, dwellHaving, minVisits } = buildSourceFilterSQL(f);
  const hasFilter = !!hourClause || dwellHaving.length > 0 || minVisits > 1;
  if (!hasFilter) {
    return `source_visitors AS (
      SELECT DISTINCT ad_id
      FROM ${sourceTable}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    )`;
  }
  return `qualifying_days AS (
      SELECT ad_id, date
      FROM ${sourceTable}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${hourClause}
      GROUP BY ad_id, date
      ${dwellHaving}
    ),
    source_visitors AS (
      SELECT ad_id
      FROM qualifying_days
      GROUP BY ad_id
      HAVING COUNT(DISTINCT date) >= ${minVisits}
    )`;
}

/**
 * Build the SQL that derives, for each qualified visitor, their `home`
 * (top-home rounded coord) plus the native_zip / native_city pulled from
 * `geo_fields` (FULL schema bypass; NULL on BASIC). Output columns:
 *   ad_id, home_lat, home_lng, native_zip, native_city, days_at_loc
 */
function buildCatchmentExportSQL(
  sourceTable: string,
  targetPois: PoiPosition[],
  cfg: ReachConfig,
  filters?: SourceFilters,
): string {
  return `
    WITH
    ${sourceVisitorsCTE(sourceTable, filters)},
    visitor_pings AS (
      SELECT
        p.ad_id, p.date, p.utc_timestamp,
        TRY_CAST(p.latitude AS DOUBLE) as lat,
        TRY_CAST(p.longitude AS DOUBLE) as lng,
        TRY(p.geo_fields['zipcode']) as native_zip,
        TRY(p.geo_fields['city']) as native_city,
        CAST(FLOOR(TRY_CAST(p.latitude AS DOUBLE) / ${GRID}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(TRY_CAST(p.longitude AS DOUBLE) / ${GRID}) AS BIGINT) as lng_bucket
      FROM ${sourceTable} p
      INNER JOIN source_visitors v ON p.ad_id = v.ad_id
      WHERE TRY_CAST(p.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(p.longitude AS DOUBLE) IS NOT NULL
        AND (p.horizontal_accuracy IS NULL OR TRY_CAST(p.horizontal_accuracy AS DOUBLE) < ${ACCURACY})
    ),
    target_pois AS (
      SELECT * FROM (VALUES ${poisValues(targetPois)}) AS t(poi_id, poi_name, poi_lat, poi_lng)
    ),
    target_poi_buckets AS (
      SELECT poi_id, poi_lat, poi_lng,
        CAST(FLOOR(poi_lat / ${GRID}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(poi_lng / ${GRID}) AS BIGINT) + dlng as lng_bucket
      FROM target_pois
      CROSS JOIN (VALUES (-1), (0), (1)) AS d1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS d2(dlng)
    ),
    matched AS (
      SELECT vp.ad_id, vp.utc_timestamp, bp.poi_id
      FROM visitor_pings vp
      INNER JOIN target_poi_buckets bp
        ON vp.lat_bucket = bp.lat_bucket
        AND vp.lng_bucket = bp.lng_bucket
      WHERE 111320 * SQRT(
          POW(vp.lat - bp.poi_lat, 2) +
          POW((vp.lng - bp.poi_lng) * COS(RADIANS((vp.lat + bp.poi_lat) / 2)), 2)
        ) <= ${cfg.maxDistanceMeters}
    ),
    visitor_b_poi AS (
      SELECT
        ad_id, poi_id,
        COUNT(*) as ping_count,
        DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0 as dwell_minutes
      FROM matched
      GROUP BY ad_id, poi_id
    ),
    qualified AS (
      SELECT DISTINCT ad_id FROM visitor_b_poi
      WHERE ping_count >= ${cfg.minPings} OR dwell_minutes >= ${cfg.minDwellMinutes}
    ),
    -- Home detection: per device-day, first ping; per device, top rounded coord.
    first_pings AS (
      SELECT vp.ad_id, vp.date,
        MIN_BY(vp.lat, vp.utc_timestamp) as origin_lat,
        MIN_BY(vp.lng, vp.utc_timestamp) as origin_lng,
        MIN_BY(vp.native_zip, vp.utc_timestamp) as origin_native_zip,
        MIN_BY(vp.native_city, vp.utc_timestamp) as origin_native_city
      FROM visitor_pings vp
      INNER JOIN qualified q ON vp.ad_id = q.ad_id
      GROUP BY vp.ad_id, vp.date
    ),
    device_homes_agg AS (
      SELECT ad_id,
        ROUND(origin_lat, ${PREC}) as home_lat,
        ROUND(origin_lng, ${PREC}) as home_lng,
        ARBITRARY(origin_native_zip) as native_zip,
        ARBITRARY(origin_native_city) as native_city,
        COUNT(DISTINCT date) as days_at_loc
      FROM first_pings
      GROUP BY ad_id, ROUND(origin_lat, ${PREC}), ROUND(origin_lng, ${PREC})
    ),
    device_homes AS (
      SELECT ad_id, home_lat, home_lng, native_zip, native_city, days_at_loc
      FROM (
        SELECT ad_id, home_lat, home_lng, native_zip, native_city, days_at_loc,
          ROW_NUMBER() OVER (
            PARTITION BY ad_id
            ORDER BY days_at_loc DESC, home_lat, home_lng
          ) as rn
        FROM device_homes_agg
      )
      WHERE rn = 1
    )
    SELECT ad_id, home_lat, home_lng, native_zip, native_city, days_at_loc
    FROM device_homes
  `;
}

// ── Helpers ──────────────────────────────────────────────────────────

async function loadPois(datasetName: string): Promise<PoiPosition[]> {
  const { getPOIPositionsForDataset } = await import('@/lib/poi-storage');
  const positions = await getPOIPositionsForDataset(datasetName);
  return positions
    .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lng))
    .map((p) => ({ poiId: p.poiId, name: p.name, lat: p.lat as number, lng: p.lng as number }));
}

async function findDatasetCountry(datasetName: string): Promise<string | undefined> {
  const { getJobByDatasetName } = await import('@/lib/jobs');
  const job = await getJobByDatasetName(datasetName);
  return (job as any)?.country || undefined;
}

// ── Route handler ────────────────────────────────────────────────────

export async function POST(request: NextRequest): Promise<NextResponse> {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    let body: any = {};
    try { body = await request.json(); } catch {}

    const reachStateId: string = body.stateId || '';
    const direction: 'aToB' | 'bToA' = body.direction === 'bToA' ? 'bToA' : 'aToB';
    const catchmentId: string = body.catchmentId || `${reachStateId}-${direction}`;
    const isNewRequest = !!body.stateId && !body.catchmentId;

    let state = await getConfig<CatchmentExportState>(CATCHMENT_KEY(catchmentId));

    // Reset on error or fresh request
    if (state?.phase === 'error' || (isNewRequest && state?.phase === 'done')) state = null;

    // ── Phase: starting (new request) ────────────────────────────────
    if (!state) {
      if (!reachStateId) {
        return NextResponse.json({ error: 'stateId required' }, { status: 400 });
      }
      const reachState = await getConfig<ReachState>(REACH_KEY(reachStateId));
      if (!reachState) {
        return NextResponse.json({ error: 'Reach state not found — please re-run the analysis first' }, { status: 404 });
      }
      if (!reachState.directions.includes(direction)) {
        return NextResponse.json({ error: `Direction ${direction} was not run in this reach analysis` }, { status: 400 });
      }

      const sourceDataset = direction === 'aToB' ? reachState.datasetA : reachState.datasetB;
      const targetDataset = direction === 'aToB' ? reachState.datasetB : reachState.datasetA;
      const sourceFilters = direction === 'aToB' ? reachState.filtersA : reachState.filtersB;

      await Promise.all([ensureTableForDataset(sourceDataset), ensureTableForDataset(targetDataset)]);
      const sourceTable = getTableName(sourceDataset);
      const targetPois = await loadPois(targetDataset);
      if (targetPois.length === 0) {
        return NextResponse.json({ error: `No POIs registered for ${targetDataset}` }, { status: 400 });
      }

      const sql = buildCatchmentExportSQL(sourceTable, targetPois, reachState.config, sourceFilters);
      const queryId = await startQueryAsync(sql);
      console.log(`[REACH-CATCHMENT] Started ${direction}: queryId=${queryId}, source=${sourceDataset}, target=${targetDataset}`);

      state = {
        phase: 'polling',
        catchmentId,
        reachStateId,
        direction,
        queryId,
      };
      await putConfig(CATCHMENT_KEY(catchmentId), state, { compact: true });
      return NextResponse.json({
        phase: 'polling',
        catchmentId,
        progress: { step: 'querying', percent: 10, message: 'Computing home locations for qualified visitors…' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────────
    if (state.phase === 'polling' && state.queryId) {
      const status = await checkQueryStatus(state.queryId);
      if (status.state === 'RUNNING' || status.state === 'QUEUED') {
        const scannedGB = status.statistics?.dataScannedBytes
          ? (status.statistics.dataScannedBytes / 1e9).toFixed(1)
          : '0';
        return NextResponse.json({
          phase: 'polling',
          catchmentId,
          progress: { step: 'polling', percent: 35, message: `Athena: ${scannedGB} GB scanned` },
        });
      }
      if (status.state === 'FAILED' || status.state === 'CANCELLED') {
        state = { ...state, phase: 'error', error: status.error || 'Query failed' };
        await putConfig(CATCHMENT_KEY(catchmentId), state, { compact: true });
        return NextResponse.json({ phase: 'error', catchmentId, error: state.error });
      }
      // SUCCEEDED → advance
      state = { ...state, phase: 'geocoding' };
      await putConfig(CATCHMENT_KEY(catchmentId), state, { compact: true });
      return NextResponse.json({
        phase: 'geocoding',
        catchmentId,
        progress: { step: 'geocoding', percent: 55, message: 'Reading homes & geocoding remaining coords…' },
      });
    }

    // ── Phase: geocoding (read result + reverse-geocode + write CSV) ──
    if (state.phase === 'geocoding' && state.queryId) {
      const reachState = await getConfig<ReachState>(REACH_KEY(state.reachStateId));
      if (!reachState) {
        state = { ...state, phase: 'error', error: 'Reach state expired' };
        await putConfig(CATCHMENT_KEY(catchmentId), state, { compact: true });
        return NextResponse.json({ phase: 'error', catchmentId, error: state.error });
      }
      const sourceDataset = state.direction === 'aToB' ? reachState.datasetA : reachState.datasetB;

      // Read all rows from the home query's Athena result CSV.
      const { fetchQueryResults } = await import('@/lib/athena');
      const homeResult = await fetchQueryResults(state.queryId);
      const rows = homeResult.rows || [];

      // Bucket coords for batch geocoding (only those without native_zip)
      type Row = {
        ad_id: string; home_lat: number; home_lng: number;
        native_zip: string; native_city: string; days_at_loc: number;
      };
      const parsed: Row[] = rows.map((r: any) => ({
        ad_id: String(r.ad_id || ''),
        home_lat: parseFloat(r.home_lat) || 0,
        home_lng: parseFloat(r.home_lng) || 0,
        native_zip: String(r.native_zip || '').trim(),
        native_city: String(r.native_city || '').trim(),
        days_at_loc: parseInt(r.days_at_loc, 10) || 0,
      })).filter((r) => r.ad_id && Number.isFinite(r.home_lat) && Number.isFinite(r.home_lng));

      let nativeZipCount = 0;
      const coordsToGeocode = new Map<string, { lat: number; lng: number }>();
      for (const r of parsed) {
        if (r.native_zip) { nativeZipCount++; continue; }
        const k = `${Math.round(r.home_lat * 10) / 10},${Math.round(r.home_lng * 10) / 10}`;
        if (!coordsToGeocode.has(k)) {
          coordsToGeocode.set(k, { lat: Math.round(r.home_lat * 10) / 10, lng: Math.round(r.home_lng * 10) / 10 });
        }
      }

      // Batch reverse-geocode the rest
      const country = await findDatasetCountry(sourceDataset);
      const roundedToZip = new Map<string, { zipCode: string; city: string; country: string }>();
      let geocodedCount = 0;
      if (coordsToGeocode.size > 0) {
        const { setCountryFilter, batchReverseGeocode } = await import('@/lib/reverse-geocode');
        if (country) setCountryFilter([country]);
        const points = Array.from(coordsToGeocode.entries()).map(([k, p]) => ({ key: k, lat: p.lat, lng: p.lng, deviceCount: 1 }));
        const geocoded = await batchReverseGeocode(points);
        for (let i = 0; i < points.length && i < geocoded.length; i++) {
          const g = geocoded[i];
          if (g.type === 'geojson_local' || g.type === 'nominatim_match') {
            roundedToZip.set(points[i].key, { zipCode: g.postcode, city: g.city, country: g.country });
            geocodedCount++;
          } else if (g.type === 'foreign') {
            roundedToZip.set(points[i].key, { zipCode: 'FOREIGN', city: 'FOREIGN', country: g.country });
          }
        }
        setCountryFilter(null);
      }

      // Build output CSV
      const lines: string[] = ['ad_id,home_lat,home_lng,home_zip,home_city,country,days_at_home'];
      const fallbackCountry = country || 'UNKNOWN';
      for (const r of parsed) {
        let zip = r.native_zip;
        let city = r.native_city || 'UNKNOWN';
        let cc = fallbackCountry;
        if (!zip) {
          const k = `${Math.round(r.home_lat * 10) / 10},${Math.round(r.home_lng * 10) / 10}`;
          const g = roundedToZip.get(k);
          if (g) {
            zip = g.zipCode;
            if (g.city && g.city !== 'FOREIGN') city = g.city;
            cc = g.country || cc;
          } else {
            zip = 'UNKNOWN';
          }
        } else {
          // strip optional country prefix (e.g. "ES-28001" → "28001")
          zip = zip.replace(/^[A-Z]{2}[-\s]/, '').trim();
        }
        const adIdQ = (r.ad_id.includes(',') || r.ad_id.includes('"')) ? `"${r.ad_id.replace(/"/g, '""')}"` : r.ad_id;
        const cityQ = (city.includes(',') || city.includes('"')) ? `"${city.replace(/"/g, '""')}"` : city;
        lines.push(`${adIdQ},${r.home_lat},${r.home_lng},${zip},${cityQ},${cc},${r.days_at_loc}`);
      }

      const downloadKey = `compare/catchment-zips/${state.catchmentId}.csv`;
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET, Key: downloadKey, Body: lines.join('\n'), ContentType: 'text/csv',
      }));
      console.log(`[REACH-CATCHMENT] ${direction}: ${parsed.length} devices (${nativeZipCount} native, ${geocodedCount} geocoded) → ${downloadKey}`);

      state = {
        ...state,
        phase: 'done',
        downloadKey,
        rowCount: parsed.length,
        nativeZipCount,
        geocodedCount,
      };
      await putConfig(CATCHMENT_KEY(catchmentId), state, { compact: true });
      return NextResponse.json({
        phase: 'done',
        catchmentId,
        downloadKey,
        rowCount: parsed.length,
        nativeZipCount,
        geocodedCount,
        progress: { step: 'done', percent: 100, message: `${parsed.length.toLocaleString()} devices ready` },
      });
    }

    if (state.phase === 'done') {
      return NextResponse.json({
        phase: 'done',
        catchmentId,
        downloadKey: state.downloadKey,
        rowCount: state.rowCount,
        nativeZipCount: state.nativeZipCount,
        geocodedCount: state.geocodedCount,
      });
    }

    return NextResponse.json({ phase: 'error', error: `Unexpected phase: ${state.phase}` });
  } catch (e: any) {
    console.error('[REACH-CATCHMENT] error:', e?.message, e?.stack);
    return NextResponse.json({ phase: 'error', error: e?.message || String(e) }, { status: 500 });
  }
}
