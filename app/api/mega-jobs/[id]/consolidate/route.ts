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
  startConsolidatedMAIDsQuery,
  extractPoiCoords,
  type PoiCoord,
} from '@/lib/mega-consolidation-queries';
import { checkQueryStatus, fetchQueryResults } from '@/lib/athena';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * Multi-query consolidation state, stored alongside the mega-job.
 * Tracks parallel Athena queries across phases.
 */
interface ConsolidationState {
  phase: 'starting' | 'polling' | 'parsing_visits' | 'parsing_od' | 'done';
  /** Query IDs for each report type */
  queries?: {
    visits?: string;
    od?: string;
    hourly?: string;
    catchment?: string;
    mobility?: string;
    temporal?: string;
    maids?: string;
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
 * Phase 4 (parsing_od): Parse OD + catchment (requires geocoding → separate phase for timeout).
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

    // Parse optional POI filter from body
    let poiIds: string[] | undefined;
    try {
      const body = await _request.json();
      if (body?.poiIds?.length) poiIds = body.poiIds;
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
    if (url.searchParams.get('reset') === 'true') {
      state = null;
    }

    if (!state || state.phase === 'done') {
      state = { phase: 'starting', poiIds };
    }

    // Update mega-job status — always set to consolidating (even for re-consolidation)
    if (megaJob.status !== 'consolidating') {
      await updateMegaJob(id, { status: 'consolidating' });
    }

    // ── Phase 1: Start all queries in parallel ─────────────────────────
    if (state.phase === 'starting') {
      try {
        const effectivePoiIds = state.poiIds || poiIds;

        // Extract POI coordinates from sub-job metadata for spatial proximity queries
        const poiCoords = extractPoiCoords(syncedJobs);
        console.log(`[MEGA] Extracted ${poiCoords.length} POI coordinates from ${syncedJobs.length} sub-jobs`);

        // Start all 7 queries in parallel (pass poiCoords for spatial proximity)
        const [visitsQId, odQId, hourlyQId, catchmentQId, mobilityQId, temporalQId, maidsQId] = await Promise.all([
          startConsolidatedVisitsQuery(syncedJobs).catch((e) => { console.error('[MEGA] visits query failed to start:', e.message); return undefined; }),
          startConsolidatedODQuery(syncedJobs, effectivePoiIds, poiCoords).catch((e) => { console.error('[MEGA] OD query failed to start:', e.message); return undefined; }),
          startConsolidatedHourlyQuery(syncedJobs, effectivePoiIds, poiCoords).catch((e) => { console.error('[MEGA] hourly query failed to start:', e.message); return undefined; }),
          startConsolidatedCatchmentQuery(syncedJobs, effectivePoiIds, poiCoords).catch((e) => { console.error('[MEGA] catchment query failed to start:', e.message); return undefined; }),
          startConsolidatedMobilityQuery(syncedJobs, effectivePoiIds, poiCoords).catch((e) => { console.error('[MEGA] mobility query failed to start:', e.message); return undefined; }),
          startConsolidatedTemporalQuery(syncedJobs, effectivePoiIds).catch((e) => { console.error('[MEGA] temporal query failed to start:', e.message); return undefined; }),
          startConsolidatedMAIDsQuery(syncedJobs, effectivePoiIds).catch((e) => { console.error('[MEGA] MAIDs query failed to start:', e.message); return undefined; }),
        ]);

        state = {
          phase: 'polling',
          poiIds: effectivePoiIds,
          queries: {
            visits: visitsQId,
            od: odQId,
            hourly: hourlyQId,
            catchment: catchmentQId,
            mobility: mobilityQId,
            temporal: temporalQId,
            maids: maidsQId,
          },
          parsed: {},
        };
        await putConf(CONSOLIDATION_KEY(id), state);

        const startedCount = [visitsQId, odQId, hourlyQId, catchmentQId, mobilityQId, temporalQId, maidsQId].filter(Boolean).length;
        return NextResponse.json({
          phase: state.phase,
          progress: { step: 'queries_started', percent: 10, message: `Started ${startedCount} Athena queries...` },
        });
      } catch (err: any) {
        state = { phase: 'starting', error: err.message };
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

      // Check each query status
      const queryEntries = Object.entries(queries).filter(([, qid]) => qid);
      const statusResults = await Promise.all(
        queryEntries.map(async ([name, qid]) => {
          const s = await checkQueryStatus(qid!);
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

    // ── Phase 3: Parse visits + hourly + mobility (no geocoding) ───────
    if (state.phase === 'parsing_visits' && state.queries) {
      const reportKeys: Record<string, string> = {};

      // Parse visits
      if (state.queries.visits && !state.parsed?.visits) {
        try {
          const queryResult = await fetchQueryResults(state.queries.visits);
          const visitsByPoi = parseConsolidatedVisits(queryResult.rows, syncedJobs);
          const visitsReport = {
            megaJobId: id,
            analyzedAt: new Date().toISOString(),
            totalPois: visitsByPoi.length,
            visitsByPoi,
          };
          reportKeys.visitsByPoi = await saveConsolidatedReport(id, 'visits', visitsReport);
        } catch (err: any) {
          console.error('[MEGA] Error parsing visits:', err.message);
        }
      }

      // Parse hourly
      if (state.queries.hourly && !state.parsed?.hourly) {
        try {
          const queryResult = await fetchQueryResults(state.queries.hourly);
          const hourlyReport = parseConsolidatedHourly(id, queryResult.rows);
          reportKeys.hourly = await saveConsolidatedReport(id, 'hourly', hourlyReport);
        } catch (err: any) {
          console.error('[MEGA] Error parsing hourly:', err.message);
        }
      }

      // Parse mobility
      if (state.queries.mobility && !state.parsed?.mobility) {
        try {
          const queryResult = await fetchQueryResults(state.queries.mobility);
          const mobilityReport = parseConsolidatedMobility(id, queryResult.rows);
          reportKeys.mobility = await saveConsolidatedReport(id, 'mobility', mobilityReport);
        } catch (err: any) {
          console.error('[MEGA] Error parsing mobility:', err.message);
        }
      }

      // Parse temporal (daily pings/devices from Athena query)
      if (!state.parsed?.temporal) {
        let temporalSaved = false;
        // Try Athena query first
        if (state.queries.temporal) {
          try {
            const queryResult = await fetchQueryResults(state.queries.temporal);
            const dailyData = queryResult.rows.map((row: Record<string, any>) => ({
              date: String(row.date || ''),
              pings: parseInt(row.pings, 10) || 0,
              devices: parseInt(row.devices, 10) || 0,
            })).sort((a: any, b: any) => a.date.localeCompare(b.date));
            const temporal = buildTemporalTrends(id, [dailyData]);
            reportKeys.temporalTrends = await saveConsolidatedReport(id, 'temporal', temporal);
            temporalSaved = true;
          } catch (err: any) {
            console.error('[MEGA] Error parsing temporal Athena query:', err.message);
          }
        }
        // Fallback: build from sub-job dataset analyses
        if (!temporalSaved) {
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
              reportKeys.temporalTrends = await saveConsolidatedReport(id, 'temporal', temporal);
            }
          } catch (err: any) {
            console.error('[MEGA] Error building temporal fallback:', err.message);
          }
        }
      }

      state = { ...state, phase: 'parsing_od', parsed: { visits: true, hourly: true, mobility: true, temporal: true } };
      await putConf(CONSOLIDATION_KEY(id), state);

      return NextResponse.json({
        phase: 'parsing_od',
        progress: { step: 'parsing_geocode', percent: 60, message: 'Geocoding origins and destinations...' },
      });
    }

    // ── Phase 4: Parse OD + catchment (requires geocoding) ─────────────
    if (state.phase === 'parsing_od' && state.queries) {
      try {
        const reportKeys: Record<string, string> = {};

        // Collect all coordinates that need geocoding
        const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();

        let odClusters: ReturnType<typeof parseConsolidatedOD> | undefined;
        let catchmentRows: Record<string, any>[] | undefined;

        // Fetch OD results
        if (state.queries.od) {
          try {
            const queryResult = await fetchQueryResults(state.queries.od);
            odClusters = parseConsolidatedOD(queryResult.rows);
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

        // Fetch catchment results
        if (state.queries.catchment) {
          try {
            const queryResult = await fetchQueryResults(state.queries.catchment);
            catchmentRows = queryResult.rows;
            for (const row of catchmentRows) {
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
        const coordToZip = new Map<string, { zipCode: string; city: string; country: string }>();
        if (coordsToGeocode.size > 0) {
          // Detect country from first sub-job's data
          const firstJob = syncedJobs[0];
          const isoCountry = firstJob?.dateRange?.from ? undefined : undefined; // No country on job
          // Try to detect from POI collection
          const datasetName = firstJob?.s3DestPath?.replace(/\/$/, '').split('/').pop();
          let detectedCountry: string | undefined;
          if (datasetName) {
            const analysis = await getConf<any>(`dataset-analysis/${datasetName}`);
            detectedCountry = analysis?.country;
          }
          if (detectedCountry) {
            setCountryFilter([detectedCountry]);
          }

          const points = Array.from(coordsToGeocode.values());
          // Use 1-decimal precision for geocoding (×10 performance)
          const roundedPoints = points.map((p) => ({
            lat: Math.round(p.lat * 10) / 10,
            lng: Math.round(p.lng * 10) / 10,
            deviceCount: p.deviceCount,
          }));

          // Deduplicate rounded points
          const uniqueRounded = new Map<string, { lat: number; lng: number; deviceCount: number }>();
          for (const p of roundedPoints) {
            const key = `${p.lat},${p.lng}`;
            const ex = uniqueRounded.get(key);
            if (ex) ex.deviceCount += p.deviceCount;
            else uniqueRounded.set(key, { ...p });
          }

          const geocoded = await batchReverseGeocode(Array.from(uniqueRounded.values()));

          // Build lookup map from rounded coords
          const roundedLookup = new Map<string, { zipCode: string; city: string; country: string }>();
          for (const g of geocoded) {
            if (g.type === 'geojson_local' || g.type === 'nominatim_match') {
              // Find the original point that produced this result
              roundedLookup.set(`${g.country}|${g.postcode}`, { zipCode: g.postcode, city: g.city, country: g.country });
            }
          }

          // Map original coords to geocoded results via rounded matching
          for (const [key, p] of coordsToGeocode.entries()) {
            const roundedKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
            // Find the geocoded result for this rounded point
            const roundedP = uniqueRounded.get(roundedKey);
            if (roundedP) {
              // Find in geocoded array by index
              const idx = Array.from(uniqueRounded.keys()).indexOf(roundedKey);
              if (idx >= 0 && idx < geocoded.length) {
                const g = geocoded[idx];
                if (g.type === 'geojson_local' || g.type === 'nominatim_match') {
                  coordToZip.set(key, { zipCode: g.postcode, city: g.city, country: g.country });
                } else if (g.type === 'foreign') {
                  coordToZip.set(key, { zipCode: 'FOREIGN', city: 'FOREIGN', country: g.country });
                }
              }
            }
          }

          // Reset country filter
          setCountryFilter(null);
        }

        // Build OD report
        if (odClusters) {
          const odReport = buildODReport(id, odClusters.clusters, coordToZip);
          reportKeys.od = await saveConsolidatedReport(id, 'od', odReport);
        }

        // Build catchment report
        if (catchmentRows) {
          const catchmentReport = buildCatchmentReport(id, catchmentRows, coordToZip);
          reportKeys.catchment = await saveConsolidatedReport(id, 'catchment', catchmentReport);
        }

        // Save MAIDs query output key (CSV served directly from Athena output)
        if (state.queries?.maids) {
          reportKeys.maids = `athena-results/${state.queries.maids}.csv`;
        }

        // Update mega-job with all report keys
        await updateMegaJob(id, {
          status: 'completed',
          consolidatedReports: reportKeys,
        });

        state = { phase: 'done' };
        await putConf(CONSOLIDATION_KEY(id), state);

        return NextResponse.json({
          phase: 'done',
          progress: { step: 'complete', percent: 100, message: 'Consolidation complete' },
        });
      } catch (err: any) {
        console.error('[MEGA] Error in OD/catchment parsing:', err.message);
        // Keep in parsing_od phase so Re-consolidate can retry
        state = { ...state, phase: 'parsing_od', error: err.message };
        await putConf(CONSOLIDATION_KEY(id), state);
        await updateMegaJob(id, { status: 'error', error: `Consolidation failed: ${err.message}` });
        return NextResponse.json({
          phase: 'error',
          progress: { step: 'error', percent: 0, message: `Consolidation failed: ${err.message}. Click Re-consolidate to retry.` },
        }, { status: 500 });
      }
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
