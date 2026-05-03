import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob, updateMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import {
  startConsolidatedVisitsQuery,
  parseConsolidatedVisits,
  buildTemporalTrends,
  saveConsolidatedReport,
  parseConsolidatedOD,
  buildODReport,
  parseConsolidatedHourly,
  buildCatchmentReport,
  parseConsolidatedMobility,
} from '@/lib/mega-report-consolidation';
import {
  startConsolidatedODQuery,
  startConsolidatedHourlyQuery,
  startConsolidatedCatchmentQuery,
  startConsolidatedMobilityQuery,
  startConsolidatedTemporalQuery,
  startConsolidatedTotalDevicesQuery,
  startConsolidatedMAIDsQuery,
  startConsolidatedAffinityQuery,
  extractPoiCoords,
  materializePoiCoordsTable,
  dropPoiCoordsTable,
  poiCoordsTablePrefix,
  MAX_INLINE_POIS,
  type PoiCoord,
  type DwellFilter,
  type ConsolidatedQueryHandle,
} from '@/lib/mega-consolidation-queries';
import {
  checkQueryStatus,
  ensureTableForDataset,
  createMegaDatasetView,
  megaJobNameToDatasetId,
  runQueryViaS3,
  startQueryAsync,
  dropTempTable,
  cleanupTempS3,
} from '@/lib/athena';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * Multi-query consolidation state, stored alongside the mega-job.
 * Tracks parallel Athena queries across phases.
 */
interface ConsolidationState {
  phase: 'starting' | 'launching' | 'polling' | 'parsing_visits' | 'parsing_geocode' | 'parsing_od' | 'cleanup' | 'done';
  /** Unique per-consolidation run identifier — prevents CTAS table name collisions on re-consolidate. */
  runId?: string;
  /** When poiCoords > MAX_INLINE_POIS, the spatial-join queries reference this external table instead of inlining VALUES. */
  poiTableRef?: string;
  /**
   * For each report type: the CTAS queryId (for polling) and the
   * resulting Parquet table name (for fetching results once it succeeds).
   *
   * Old runs may have stored just `string` (legacy queryId-only). The
   * polling/parsing phases handle both shapes for backward compatibility,
   * but the cleanup phase only drops tables it knows about.
   */
  queries?: {
    visits?: ConsolidatedQueryHandle;
    od?: ConsolidatedQueryHandle;
    hourly?: ConsolidatedQueryHandle;
    catchment?: ConsolidatedQueryHandle;
    mobility?: ConsolidatedQueryHandle;
    temporal?: ConsolidatedQueryHandle;
    totalDevices?: ConsolidatedQueryHandle;
    maids?: ConsolidatedQueryHandle;
    affinity?: ConsolidatedQueryHandle;
  };
  /** Track which queries have been parsed */
  parsed?: {
    visits?: boolean;
    hourly?: boolean;
    mobility?: boolean;
    temporal?: boolean;
  };
  /** Optional POI filter */
  poiIds?: string[];
  /** Optional dwell time filter */
  dwellFilter?: DwellFilter;
  /** Materialized MAIDs CSV key (set during parsing_od) — used as the export download URL. */
  maidsCsvKey?: string;
  error?: string;
}

const CONSOLIDATION_KEY = (id: string) => `mega-consolidation-state/${id}`;

/**
 * POST /api/mega-jobs/[id]/consolidate
 * Multi-phase consolidation with parallel Athena queries. Frontend polls until done.
 *
 * Phase 1 (starting): Start all Athena queries in parallel (visits, OD, hourly, catchment, mobility).
 * Phase 2 (polling): Poll all queries until all SUCCEEDED.
 * Phase 3 (parsing_visits): Parse visits + hourly + mobility (no geocoding needed).
 * Phase 4a (parsing_geocode): Fetch OD + catchment results, geocode coords, save geocode map to S3.
 * Phase 4b (parsing_od): Load geocode map from S3, build OD + catchment + affinity reports.
 * Phase 5 (done): All reports saved.
 */
export async function POST(
  _request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  try {
    const { id } = await context.params;
    const megaJob = await getMegaJob(id);

    if (!megaJob) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    // Parse optional POI filter and dwell filter from body
    let poiIds: string[] | undefined;
    let dwellFilter: DwellFilter | undefined;
    try {
      const body = await _request.json();
      if (body?.poiIds?.length) poiIds = body.poiIds;
      if (body?.dwellFilter) dwellFilter = body.dwellFilter;
    } catch { }

    // Load sub-jobs
    const subJobs = (
      await Promise.all(megaJob.subJobIds.map((jid) => getJob(jid)))
    ).filter((j): j is NonNullable<typeof j> => j !== null);

    const syncedJobs = subJobs.filter((j) => j.status === 'SUCCESS' && j.syncedAt);
    if (syncedJobs.length === 0) {
      return NextResponse.json({ error: 'No synced sub-jobs to consolidate' }, { status: 400 });
    }

    // Load or init consolidation state
    const { getConfig: getConf, putConfig: putConf } = await import('@/lib/s3-config');
    let state = await getConf<ConsolidationState>(CONSOLIDATION_KEY(id));

    // Reset if requested
    const url = new URL(_request.url);
    const resetRequested = url.searchParams.get('reset') === 'true';
    if (resetRequested) {
      state = null;
    }

    // If a previous consolidation already completed (state.phase === 'done' AND
    // megajob.status === 'completed'), short-circuit: don't re-start automatically.
    // Otherwise the frontend's status polling could repeatedly POST and clobber
    // status from 'completed' back to 'consolidating' on every request, then
    // crash mid-launch leaving things stuck.
    if (state?.phase === 'done' && megaJob.status === 'completed' && !resetRequested) {
      return NextResponse.json({
        phase: 'done',
        progress: { step: 'complete', percent: 100, message: 'Consolidation complete (use ?reset=true to re-run)' },
      });
    }

    if (!state || state.phase === 'done') {
      state = { phase: 'starting', poiIds, dwellFilter };
    }

    // Update mega-job status — only set to consolidating when starting fresh work.
    if (megaJob.status !== 'consolidating') {
      await updateMegaJob(id, { status: 'consolidating' });
    }

    // ── Phase 1a: Ensure Athena tables + create VIEW ────────────────────
    if (state.phase === 'starting') {
      try {
        const effectivePoiIds = state.poiIds || poiIds;

        // Ensure Athena tables exist for all sub-jobs (required before querying)
        console.log(`[MEGA] Ensuring Athena tables for ${syncedJobs.length} sub-jobs...`);
        await Promise.all(
          syncedJobs.map(async (job) => {
            const datasetName = job.s3DestPath?.replace(/\/$/, '').split('/').pop();
            if (datasetName) {
              console.log(`[MEGA] Ensuring table for ${datasetName}...`);
              await ensureTableForDataset(datasetName);
            }
          })
        );
        console.log(`[MEGA] All Athena tables ready`);

        // Create/refresh the mega-dataset VIEW (UNION ALL of sub-job tables)
        const subDatasetNames = syncedJobs
          .map(j => j.s3DestPath?.replace(/\/$/, '').split('/').pop())
          .filter((n): n is string => !!n);
        const megaDatasetId = megaJobNameToDatasetId(megaJob.name || id);
        if (megaDatasetId && subDatasetNames.length > 0) {
          try {
            await createMegaDatasetView(megaDatasetId, subDatasetNames);
            console.log(`[MEGA] Integrated dataset VIEW "${megaDatasetId}" created`);
          } catch (viewErr: any) {
            console.error(`[MEGA] Failed to create integrated VIEW:`, viewErr.message);
            // Non-fatal — consolidation continues without the integrated view
          }
        }

        // Tables + VIEW ready → advance to launching phase (separate HTTP request)
        state = {
          phase: 'launching',
          poiIds: effectivePoiIds,
          dwellFilter: state.dwellFilter || dwellFilter,
        };
        await putConf(CONSOLIDATION_KEY(id), state);

        return NextResponse.json({
          phase: 'launching',
          progress: { step: 'tables_ready', percent: 5, message: `Tables ready. Launching queries...` },
        });
      } catch (err: any) {
        state = { phase: 'starting', error: err.message };
        await putConf(CONSOLIDATION_KEY(id), state);
        return NextResponse.json({ error: err.message }, { status: 500 });
      }
    }

    // ── Phase 1b: Extract POI coords + launch all 9 Athena queries ─────
    if (state.phase === 'launching') {
      try {
        const effectivePoiIds = state.poiIds || poiIds;

        // Extract POI coordinates from sub-job metadata for spatial proximity queries
        const poiCoords = extractPoiCoords(syncedJobs);
        console.log(`[MEGA] Extracted ${poiCoords.length} POI coordinates from ${syncedJobs.length} sub-jobs`);

        // Get dwell filter from state (persisted across polls)
        const dwell = state.dwellFilter || dwellFilter;

        // Generate per-run id so CTAS tables never collide with leftover ones from previous runs
        const runId = (state as any).runId || Date.now().toString(36);

        // For megajobs with many POIs (e.g. 25k-grid), inlining VALUES would exceed
        // Athena's 256 KB SQL limit. Materialize them once as an external CSV-backed
        // table and reference it from each spatial join.
        let poiTableRef: string | undefined;
        if (poiCoords.length > MAX_INLINE_POIS) {
          try {
            poiTableRef = await materializePoiCoordsTable(id, runId, poiCoords);
            console.log(`[MEGA] Materialized ${poiCoords.length} POIs to external table ${poiTableRef}`);
          } catch (err: any) {
            console.error('[MEGA] Failed to materialize POI table:', err.message);
            // Continue — small queries (visits, no-dwell paths) still work without poiTableRef
          }
        }

        // Mobility query has a second spatial join against the global Overture POI catalog
        // (lab_pois_gmc) which doesn't scale to country-grid POI counts (>5000).
        // For dense grids the spatial join produces billions of intermediate rows and
        // can't finish within Athena's 30-min query limit. Skip it instead of letting it
        // time out — the other 8 reports give 90% of the analytics value.
        const MOBILITY_MAX_POIS = 5000;
        const skipMobility = poiCoords.length > MOBILITY_MAX_POIS;
        if (skipMobility) {
          console.log(`[MEGA] Skipping mobility query: ${poiCoords.length} POIs > ${MOBILITY_MAX_POIS} threshold (would exceed Athena 30-min limit)`);
        }

        // Start all queries in parallel as CTAS (no 30-min timeout, results in Parquet at s3://.../athena-temp/{ctasTable}/)
        const [visits, od, hourly, catchment, mobility, temporal, totalDevices, maids, affinity] = await Promise.all([
          startConsolidatedVisitsQuery(id, runId, syncedJobs).catch((e) => { console.error('[MEGA] visits query failed to start:', e.message); return undefined; }),
          startConsolidatedODQuery(id, runId, syncedJobs, effectivePoiIds, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] OD query failed to start:', e.message); return undefined; }),
          startConsolidatedHourlyQuery(id, runId, syncedJobs, effectivePoiIds, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] hourly query failed to start:', e.message); return undefined; }),
          startConsolidatedCatchmentQuery(id, runId, syncedJobs, effectivePoiIds, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] catchment query failed to start:', e.message); return undefined; }),
          skipMobility
            ? Promise.resolve(undefined)
            : startConsolidatedMobilityQuery(id, runId, syncedJobs, effectivePoiIds, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] mobility query failed to start:', e.message); return undefined; }),
          startConsolidatedTemporalQuery(id, runId, syncedJobs, effectivePoiIds, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] temporal query failed to start:', e.message); return undefined; }),
          startConsolidatedTotalDevicesQuery(id, runId, syncedJobs, effectivePoiIds, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] totalDevices query failed to start:', e.message); return undefined; }),
          startConsolidatedMAIDsQuery(id, runId, syncedJobs, effectivePoiIds, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] MAIDs query failed to start:', e.message); return undefined; }),
          poiCoords.length > 0 ? startConsolidatedAffinityQuery(id, runId, syncedJobs, poiCoords, dwell, poiTableRef).catch((e) => { console.error('[MEGA] affinity query failed to start:', e.message); return undefined; }) : Promise.resolve(undefined),
        ]);

        // If mobility was skipped, surface the reason so the UI can show an explanation
        if (skipMobility) {
          await updateMegaJob(id, {
            consolidationNotes: [`Mobility analysis skipped: POI count (${poiCoords.length.toLocaleString()}) exceeds ${MOBILITY_MAX_POIS.toLocaleString()} threshold. For dense grids the spatial join against the global POI catalog can't finish within Athena's query limits.`],
          } as any);
        }

        state = {
          phase: 'polling',
          runId,
          poiTableRef,
          poiIds: effectivePoiIds,
          dwellFilter: dwell,
          queries: {
            visits,
            od,
            hourly,
            catchment,
            mobility,
            temporal,
            totalDevices,
            maids,
            affinity,
          },
          parsed: {},
        };
        await putConf(CONSOLIDATION_KEY(id), state);

        const startedCount = [visits, od, hourly, catchment, mobility, temporal, totalDevices, maids, affinity].filter(Boolean).length;
        return NextResponse.json({
          phase: state.phase,
          progress: { step: 'queries_started', percent: 10, message: `Started ${startedCount}/9 Athena CTAS queries${poiTableRef ? ` (${poiCoords.length} POIs in external table)` : ''}...` },
        });
      } catch (err: any) {
        state = { phase: 'launching', error: err.message };
        await putConf(CONSOLIDATION_KEY(id), state);
        return NextResponse.json({ error: err.message }, { status: 500 });
      }
    }

    // ── Phase 2: Poll all queries ──────────────────────────────────────
    if (state.phase === 'polling' && state.queries) {
      const queries = state.queries;
      const statuses: Record<string, string> = {};
      let allDone = true;
      let anyFailed = false;

      // Check each CTAS query status
      const queryEntries = Object.entries(queries).filter(([, q]) => q);
      const statusResults = await Promise.all(
        queryEntries.map(async ([name, q]) => {
          const s = await checkQueryStatus((q as ConsolidatedQueryHandle).queryId);
          return { name, state: s.state, error: s.error };
        })
      );

      for (const { name, state: qState, error } of statusResults) {
        statuses[name] = qState;
        if (qState === 'RUNNING' || qState === 'QUEUED') {
          allDone = false;
        }
        if (qState === 'FAILED' || qState === 'CANCELLED') {
          anyFailed = true;
          console.error(`[MEGA] Query ${name} ${qState}: ${error || ''}`);
        }
      }

      if (!allDone) {
        const runningCount = Object.values(statuses).filter((s) => s === 'RUNNING' || s === 'QUEUED').length;
        const doneCount = Object.values(statuses).filter((s) => s === 'SUCCEEDED').length;
        return NextResponse.json({
          phase: 'polling',
          progress: {
            step: 'polling_queries',
            percent: 10 + Math.round((doneCount / queryEntries.length) * 30),
            message: `Queries: ${doneCount} done, ${runningCount} running...`,
          },
          statuses,
        });
      }

      // All done (some may have failed) → advance to parsing
      state = { ...state, phase: 'parsing_visits' };
      await putConf(CONSOLIDATION_KEY(id), state);
      // Fall through to parsing
    }

    // ── Phase 3: Parse visits + hourly + mobility + temporal (no geocoding) ──
    if (state.phase === 'parsing_visits' && state.queries) {
      const reportKeys: Record<string, string> = {};
      const errors: string[] = [];

      // Helper: parse one CTAS-materialized query result and save report.
      // Reads from the CTAS table via SELECT * (data is small Parquet — fast).
      const parseAndSave = async (
        name: string,
        handle: ConsolidatedQueryHandle | undefined,
        parser: (rows: any[]) => any,
        reportType: string
      ) => {
        if (!handle) return;
        try {
          console.log(`[MEGA] Phase 3: fetching ${name} from CTAS table ${handle.ctasTable}...`);
          const queryResult = await runQueryViaS3(`SELECT * FROM ${handle.ctasTable}`);
          console.log(`[MEGA] Phase 3: ${name} returned ${queryResult.rows?.length ?? 0} rows`);
          const report = parser(queryResult.rows);
          const key = await saveConsolidatedReport(id, reportType, report);
          console.log(`[MEGA] Phase 3: saved ${name} → ${key}`);
          reportKeys[name] = key;
        } catch (err: any) {
          const msg = `${name}: ${err.message}`;
          console.error(`[MEGA] Phase 3 ERROR ${msg}`);
          errors.push(msg);
        }
      };

      // Load POI names from collection GeoJSON
      let poiNameMap: Map<string, string> | undefined;
      const collectionId = megaJob.sourceScope?.poiCollectionIds?.[0];
      if (collectionId) {
        try {
          const { getPOICollection } = await import('@/lib/poi-storage');
          const geojson = await getPOICollection(collectionId);
          if (geojson?.features) {
            poiNameMap = new Map();
            for (const f of geojson.features) {
              const props = f.properties || {};
              const poiId = props.id || props.poi_id || '';
              // Try common name fields
              const name = props.name || props['nombre / name'] || props.nombre || props.Name || '';
              if (poiId && name) poiNameMap.set(poiId, name);
            }
            console.log(`[MEGA] Loaded ${poiNameMap.size} POI names from collection ${collectionId}`);
          }
        } catch (err: any) {
          console.warn(`[MEGA] Could not load POI names: ${err.message}`);
        }
      }

      // Parse visits
      await parseAndSave('visits', state.queries.visits, (rows) => {
        const visitsByPoi = parseConsolidatedVisits(rows, syncedJobs, poiNameMap);
        return { megaJobId: id, analyzedAt: new Date().toISOString(), totalPois: visitsByPoi.length, visitsByPoi };
      }, 'visits');

      // Parse hourly
      await parseAndSave('hourly', state.queries.hourly, (rows) => {
        return parseConsolidatedHourly(id, rows);
      }, 'hourly');

      // Parse mobility
      await parseAndSave('mobility', state.queries.mobility, (rows) => {
        return parseConsolidatedMobility(id, rows);
      }, 'mobility');

      // Parse total unique devices (global COUNT DISTINCT, not sum of daily)
      let totalUniqueDevices: number | undefined;
      if (state.queries.totalDevices) {
        try {
          const totalResult = await runQueryViaS3(`SELECT * FROM ${state.queries.totalDevices.ctasTable}`);
          totalUniqueDevices = parseInt(totalResult.rows[0]?.total_unique_devices, 10) || 0;
          console.log(`[MEGA] Total unique devices: ${totalUniqueDevices}`);
        } catch (err: any) {
          console.error('[MEGA] Error parsing totalDevices:', err.message);
          errors.push(`totalDevices: ${err.message}`);
        }
      }

      // Parse temporal
      if (state.queries.temporal) {
        await parseAndSave('temporal', state.queries.temporal, (rows) => {
          const dailyData = rows.map((row: Record<string, any>) => ({
            date: String(row.date || ''),
            pings: parseInt(row.pings, 10) || 0,
            devices: parseInt(row.devices, 10) || 0,
          })).sort((a: any, b: any) => a.date.localeCompare(b.date));
          const report = buildTemporalTrends(id, [dailyData]);
          if (totalUniqueDevices !== undefined) {
            (report as any).totalUniqueDevices = totalUniqueDevices;
          }
          return report;
        }, 'temporal');
      }
      // Temporal fallback from sub-job analyses
      if (!reportKeys.temporal) {
        try {
          const dailyDataByJob: Array<{ date: string; pings: number; devices: number }[]> = [];
          for (const job of syncedJobs) {
            const datasetName = job.s3DestPath?.replace(/\/$/, '').split('/').pop();
            if (!datasetName) continue;
            const analysis = await getConf<any>(`dataset-analysis/${datasetName}`);
            if (analysis?.dailyData) dailyDataByJob.push(analysis.dailyData);
          }
          if (dailyDataByJob.length > 0) {
            console.log(`[MEGA] Building temporal from ${dailyDataByJob.length} sub-job analyses (fallback)`);
            const temporal = buildTemporalTrends(id, dailyDataByJob);
            if (totalUniqueDevices !== undefined) {
              (temporal as any).totalUniqueDevices = totalUniqueDevices;
            }
            reportKeys.temporal = await saveConsolidatedReport(id, 'temporal', temporal);
          }
        } catch (err: any) {
          console.error('[MEGA] Error building temporal fallback:', err.message);
          errors.push(`temporal_fallback: ${err.message}`);
        }
      }

      console.log(`[MEGA] Phase 3 complete: saved=${Object.keys(reportKeys).join(',')}, errors=${errors.length}`);

      state = {
        ...state,
        phase: 'parsing_geocode',
        parsed: { visits: true, hourly: true, mobility: true, temporal: true },
        phase3ReportKeys: reportKeys,
        phase3Errors: errors,
      } as ConsolidationState & { phase3ReportKeys?: Record<string, string>; phase3Errors?: string[] };
      await putConf(CONSOLIDATION_KEY(id), state);

      return NextResponse.json({
        phase: 'parsing_geocode',
        progress: {
          step: 'parsing_geocode',
          percent: 60,
          message: errors.length > 0
            ? `Parsed ${Object.keys(reportKeys).length} reports (${errors.length} errors). Fetching results & geocoding...`
            : 'Fetching OD/catchment results & geocoding...',
        },
        ...(errors.length > 0 ? { errors } : {}),
        savedReports: Object.keys(reportKeys),
      });
    }

    // ── Phase 4a: Fetch OD + catchment results & geocode ────────────────
    if (state.phase === 'parsing_geocode' && state.queries) {
      try {
        // Collect all coordinates that need geocoding
        const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();

        // Fetch OD results from CTAS table
        if (state.queries.od) {
          try {
            const queryResult = await runQueryViaS3(`SELECT * FROM ${state.queries.od.ctasTable}`);
            const odClusters = parseConsolidatedOD(queryResult.rows);
            for (const c of odClusters.clusters) {
              const oKey = `${c.originLat},${c.originLng}`;
              const existing = coordsToGeocode.get(oKey);
              coordsToGeocode.set(oKey, { lat: c.originLat, lng: c.originLng, deviceCount: (existing?.deviceCount || 0) + c.deviceDays });
              const dKey = `${c.destLat},${c.destLng}`;
              const dExisting = coordsToGeocode.get(dKey);
              coordsToGeocode.set(dKey, { lat: c.destLat, lng: c.destLng, deviceCount: (dExisting?.deviceCount || 0) + c.deviceDays });
            }
          } catch (err: any) {
            console.error('[MEGA] Error fetching OD results:', err.message);
          }
        }

        // Fetch catchment results from CTAS table
        if (state.queries.catchment) {
          try {
            const queryResult = await runQueryViaS3(`SELECT * FROM ${state.queries.catchment.ctasTable}`);
            for (const row of queryResult.rows) {
              const lat = parseFloat(row.origin_lat) || 0;
              const lng = parseFloat(row.origin_lng) || 0;
              const dd = parseInt(row.device_days, 10) || 0;
              const key = `${lat},${lng}`;
              const existing = coordsToGeocode.get(key);
              coordsToGeocode.set(key, { lat, lng, deviceCount: (existing?.deviceCount || 0) + dd });
            }
          } catch (err: any) {
            console.error('[MEGA] Error fetching catchment results:', err.message);
          }
        }

        // Batch reverse geocode all coordinates
        const coordToZipEntries: Array<[string, { zipCode: string; city: string; country: string }]> = [];
        if (coordsToGeocode.size > 0) {
          // Detect country from first sub-job
          const firstJob = syncedJobs[0];
          const datasetName = firstJob?.s3DestPath?.replace(/\/$/, '').split('/').pop();
          let detectedCountry: string | undefined;
          if (datasetName) {
            const analysis = await getConf<any>(`dataset-analysis/${datasetName}`);
            detectedCountry = analysis?.country;
          }
          if (detectedCountry) {
            setCountryFilter([detectedCountry]);
          }

          // Use 1-decimal precision for geocoding (×10 performance)
          const uniqueRounded = new Map<string, { lat: number; lng: number; deviceCount: number }>();
          for (const p of coordsToGeocode.values()) {
            const key = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
            const ex = uniqueRounded.get(key);
            if (ex) ex.deviceCount += p.deviceCount;
            else uniqueRounded.set(key, { lat: Math.round(p.lat * 10) / 10, lng: Math.round(p.lng * 10) / 10, deviceCount: p.deviceCount });
          }

          console.log(`[MEGA] Geocoding ${uniqueRounded.size} unique rounded coords (from ${coordsToGeocode.size} original)`);
          const geocoded = await batchReverseGeocode(Array.from(uniqueRounded.values()));

          // Build rounded coord → zip lookup
          const roundedToZip = new Map<string, { zipCode: string; city: string; country: string }>();
          const roundedKeys = Array.from(uniqueRounded.keys());
          for (let i = 0; i < roundedKeys.length && i < geocoded.length; i++) {
            const g = geocoded[i];
            if (g.type === 'geojson_local' || g.type === 'nominatim_match') {
              roundedToZip.set(roundedKeys[i], { zipCode: g.postcode, city: g.city, country: g.country });
            } else if (g.type === 'foreign') {
              roundedToZip.set(roundedKeys[i], { zipCode: 'FOREIGN', city: 'FOREIGN', country: g.country });
            }
          }

          // Map original coords to geocoded results via rounded key
          for (const [key, p] of coordsToGeocode.entries()) {
            const roundedKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
            const zip = roundedToZip.get(roundedKey);
            if (zip) coordToZipEntries.push([key, zip]);
          }

          setCountryFilter(null);
        }

        console.log(`[MEGA] Geocoded ${coordToZipEntries.length} of ${coordsToGeocode.size} coords. Saving to S3...`);

        // Save geocode map to S3 for next phase
        await putConf(`mega-consolidation-geocode/${id}`, coordToZipEntries);

        state = {
          ...state,
          phase: 'parsing_od',
        };
        await putConf(CONSOLIDATION_KEY(id), state);

        return NextResponse.json({
          phase: 'parsing_od',
          progress: {
            step: 'parsing_reports',
            percent: 80,
            message: `Geocoded ${coordToZipEntries.length} coordinates. Building reports...`,
          },
        });
      } catch (err: any) {
        console.error('[MEGA] Error in geocoding phase:', err.message);
        state = { ...state, phase: 'parsing_geocode', error: err.message };
        await putConf(CONSOLIDATION_KEY(id), state);
        await updateMegaJob(id, { status: 'error', error: `Geocoding failed: ${err.message}` });
        return NextResponse.json({
          phase: 'error',
          progress: { step: 'error', percent: 0, message: `Geocoding failed: ${err.message}. Click Re-consolidate to retry.` },
        }, { status: 500 });
      }
    }

    // ── Phase 4b: Build OD + catchment + affinity reports ─────────────
    if (state.phase === 'parsing_od' && state.queries) {
      try {
        const reportKeys: Record<string, string> = {};

        // Load geocode map from S3
        const coordToZipEntries = await getConf<Array<[string, { zipCode: string; city: string; country: string }]>>(`mega-consolidation-geocode/${id}`) || [];
        const coordToZip = new Map(coordToZipEntries);
        console.log(`[MEGA] Loaded ${coordToZip.size} geocoded coords from S3`);

        // Re-fetch and build OD report (from CTAS table)
        if (state.queries.od) {
          try {
            const queryResult = await runQueryViaS3(`SELECT * FROM ${state.queries.od.ctasTable}`);
            const odClusters = parseConsolidatedOD(queryResult.rows);
            const odReport = buildODReport(id, odClusters.clusters, coordToZip);
            reportKeys.od = await saveConsolidatedReport(id, 'od', odReport);
          } catch (err: any) {
            console.error('[MEGA] Error building OD report:', err.message);
          }
        }

        // Re-fetch and build catchment report (from CTAS table)
        if (state.queries.catchment) {
          try {
            const queryResult = await runQueryViaS3(`SELECT * FROM ${state.queries.catchment.ctasTable}`);
            const catchmentReport = buildCatchmentReport(id, queryResult.rows, coordToZip);
            reportKeys.catchment = await saveConsolidatedReport(id, 'catchment', catchmentReport);
          } catch (err: any) {
            console.error('[MEGA] Error building catchment report:', err.message);
          }
        }

        // Build affinity report (from CTAS table)
        if (state.queries?.affinity) {
          try {
            const affinityResult = await runQueryViaS3(`SELECT * FROM ${state.queries.affinity.ctasTable}`);
            const { buildAffinityReport } = await import('@/lib/mega-report-consolidation');
            const affinityReport = buildAffinityReport(id, affinityResult.rows, coordToZip);
            reportKeys.affinity = await saveConsolidatedReport(id, 'affinity', affinityReport);
          } catch (err: any) {
            console.error('[MEGA] Error building affinity report:', err.message);
          }
        }

        // MAIDs: kick off a non-blocking SELECT to materialize the CTAS Parquet data as CSV
        // in athena-results/. Store the new queryId so the download endpoint can serve the CSV.
        // ALSO register the maids CTAS table as a 'plain' contribution to Master MAIDs for
        // the megajob's country, so it appears alongside dataset/category contributions.
        if (state.queries?.maids) {
          const maidsTable = state.queries.maids.ctasTable;
          try {
            const selectQueryId = await startQueryAsync(`SELECT ad_id FROM ${maidsTable}`);
            const csvKey = `athena-results/${selectQueryId}.csv`;
            reportKeys.maids = csvKey;
            state.maidsCsvKey = csvKey;
            console.log(`[MEGA] MAIDs CSV materialization started: queryId=${selectQueryId}, csv=${csvKey}`);

            // Register with Master MAIDs (skip if no country set on megajob)
            if (megaJob.country) {
              try {
                const { runQuery } = await import('@/lib/athena');
                const { registerAthenaContribution } = await import('@/lib/master-maids');
                const countResult = await runQuery(`SELECT COUNT(DISTINCT ad_id) as cnt FROM ${maidsTable}`);
                const maidCount = parseInt(String(countResult.rows[0]?.cnt)) || 0;

                // Aggregate date range across synced sub-jobs
                const fromDates: string[] = [];
                const toDates: string[] = [];
                for (const j of syncedJobs) {
                  const r: any = (j as any).actualDateRange || (j as any).dateRange;
                  if (r?.from) fromDates.push(r.from);
                  if (r?.to) toDates.push(r.to);
                }
                const dateRange = fromDates.length && toDates.length
                  ? { from: fromDates.sort()[0], to: toDates.sort().slice(-1)[0] }
                  : { from: 'unknown', to: 'unknown' };

                await registerAthenaContribution(
                  megaJob.country,
                  `mega-${id}`,        // distinct sourceDataset so it doesn't collide with dataset contributions
                  'plain',              // attribute type — same as dataset 'plain' MAIDs
                  '',                   // attribute value (empty for plain)
                  maidsTable,
                  `athena-temp/${maidsTable}/`,
                  maidCount,
                  dateRange,
                );
                console.log(`[MEGA] Registered Master MAIDs contribution: ${maidsTable} (${maidCount.toLocaleString()} MAIDs) for ${megaJob.country}`);
              } catch (err: any) {
                console.error('[MEGA] Failed to register Master MAIDs contribution:', err.message);
              }
            } else {
              console.log(`[MEGA] Skipping Master MAIDs registration: no country set on megajob ${id}`);
            }
          } catch (err: any) {
            console.error('[MEGA] Error starting MAIDs CSV materialization:', err.message);
          }
        }

        // Merge Phase 3 report keys with Phase 4 report keys
        const phase3Keys = (state as any).phase3ReportKeys || {};
        const allReportKeys = { ...phase3Keys, ...reportKeys };

        // Update mega-job with all report keys
        await updateMegaJob(id, {
          status: 'completed',
          consolidatedReports: allReportKeys,
        });

        // Move to cleanup phase (drops temp Athena tables + S3 Parquet files)
        state = { ...state, phase: 'cleanup' };
        await putConf(CONSOLIDATION_KEY(id), state);

        return NextResponse.json({
          phase: 'cleanup',
          progress: { step: 'cleanup', percent: 95, message: 'Reports saved. Cleaning up temp tables...' },
        });
      } catch (err: any) {
        console.error('[MEGA] Error in report building:', err.message);
        state = { ...state, phase: 'parsing_od', error: err.message };
        await putConf(CONSOLIDATION_KEY(id), state);
        await updateMegaJob(id, { status: 'error', error: `Consolidation failed: ${err.message}` });
        return NextResponse.json({
          phase: 'error',
          progress: { step: 'error', percent: 0, message: `Consolidation failed: ${err.message}. Click Re-consolidate to retry.` },
        }, { status: 500 });
      }
    }

    // ── Phase 5: Cleanup CTAS temp tables, POI temp table, and S3 artifacts ───
    // We DROP the Glue catalog entries (free) and delete the underlying S3
    // objects (costs $/GB/month if left). The MAIDs table is kept around
    // because the CSV materialization SELECT is reading from it — it'll be
    // garbage-collected by S3 lifecycle policy on athena-temp/ prefix.
    if (state.phase === 'cleanup' && state.queries) {
      const tablesToDrop: string[] = [];
      for (const [name, handle] of Object.entries(state.queries)) {
        if (!handle) continue;
        // Skip MAIDs — its Parquet may still be feeding the CSV SELECT
        if (name === 'maids') continue;
        tablesToDrop.push(handle.ctasTable);
      }

      console.log(`[MEGA] Cleanup: dropping ${tablesToDrop.length} CTAS tables${state.poiTableRef ? ' + POI temp table' : ''}...`);
      // Drop tables + clean S3 in parallel; failures are non-fatal (logged in helpers)
      const cleanupOps: Promise<unknown>[] = [
        ...tablesToDrop.map(t => dropTempTable(t)),
        ...tablesToDrop.map(t => cleanupTempS3(t)),
      ];
      if (state.poiTableRef && state.runId) {
        cleanupOps.push(dropPoiCoordsTable(state.poiTableRef, poiCoordsTablePrefix(id, state.runId)));
      }
      await Promise.all(cleanupOps);

      state = { phase: 'done' };
      await putConf(CONSOLIDATION_KEY(id), state);

      return NextResponse.json({
        phase: 'done',
        progress: { step: 'complete', percent: 100, message: 'Consolidation complete' },
      });
    }

    // Already done
    return NextResponse.json({
      phase: 'done',
      progress: { step: 'complete', percent: 100, message: 'Already consolidated' },
    });
  } catch (error: any) {
    console.error('[MEGA-CONSOLIDATE]', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
