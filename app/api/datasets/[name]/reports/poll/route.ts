import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { checkQueryStatus, fetchQueryResults } from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { getJob } from '@/lib/jobs';
import {
  startDatasetODQuery,
  startDatasetHourlyQuery,
  startDatasetCatchmentQuery,
  startDatasetMobilityQuery,
  startDatasetTemporalQuery,
  startDatasetTotalDevicesQuery,
  ensurePoiCoordsTable,
  shouldUsePoiTable,
} from '@/lib/dataset-report-queries';
import { extractPoiCoords, type PoiCoord, type DwellFilter } from '@/lib/mega-consolidation-queries';
import {
  parseConsolidatedOD,
  parseConsolidatedHourly,
  parseConsolidatedMobility,
  buildODReport,
  buildCatchmentReport,
  buildTemporalTrends,
} from '@/lib/mega-report-consolidation';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';

export const dynamic = 'force-dynamic';
export const maxDuration = 120; // Phase 1 POI table + 6 queries, Phase 4 geocoding can be slow

const STATE_KEY = (ds: string) => `dataset-report-state/${ds}`;
const REPORT_KEY = (ds: string, type: string) => `dataset-reports/${ds}/${type}`;

interface ReportState {
  phase: 'starting' | 'polling' | 'parsing' | 'geocoding' | 'done';
  queries?: Record<string, string>;
  parsed?: Record<string, boolean>;
  error?: string;
}

async function getState(ds: string): Promise<ReportState | null> {
  return await getConfig<ReportState>(STATE_KEY(ds));
}

async function saveState(ds: string, state: ReportState): Promise<void> {
  await putConfig(STATE_KEY(ds), state, { compact: true });
}

async function saveReport(ds: string, type: string, data: any): Promise<void> {
  await putConfig(REPORT_KEY(ds, type), data, { compact: true });
}

/**
 * POST /api/datasets/[name]/reports/poll
 * Multi-phase polling for OD, hourly, catchment, mobility, and temporal reports.
 * Call repeatedly until phase === 'done'.
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
    const reset = request.nextUrl.searchParams.get('reset') === 'true';
    let state = reset ? null : await getState(datasetName);

    // Parse optional POI ID filter and dwell filter from request body
    let poiIds: string[] | undefined;
    let dwellFilter: DwellFilter | undefined;
    try {
      const body = await request.json();
      if (body?.poiIds?.length) poiIds = body.poiIds;
      if (body?.dwellFilter) dwellFilter = body.dwellFilter;
    } catch { /* no body or invalid JSON */ }

    // Reset if done
    if (state?.phase === 'done') state = null;

    // ── Phase 1: Start queries ─────────────────────────────────────
    if (!state) {
      console.log(`[DS-REPORT] Phase 1: Starting queries for ${datasetName} (reset=${reset}, poiIds=${poiIds?.length || 0})`);
      // Find the job for this dataset to extract POI coordinates.
      // Use lightweight index to find job ID, then read individual job file.
      // (The monolithic config/jobs.json is 258MB+ and causes Vercel timeouts.)
      let poiCoords: PoiCoord[] = [];
      try {
        const index = await getConfig<Record<string, any>>('jobs-index');
        if (index) {
          const matchingEntry = Object.entries(index).find(
            ([_, j]: [string, any]) => j.s3DestPath && j.s3DestPath.replace(/\/$/, '').split('/').pop() === datasetName
          );
          if (matchingEntry) {
            const [matchingJobId] = matchingEntry;
            // Read full job data (has verasetPayload with POI coords)
            const matchingJob = await getJob(matchingJobId);
            if (matchingJob) {
              poiCoords = extractPoiCoords([matchingJob]);
              console.log(`[DS-REPORT] Extracted ${poiCoords.length} POI coords for ${datasetName} (job ${matchingJobId})`);

              // Filter to selected POIs if poiIds provided
              if (poiIds?.length && matchingJob.verasetPayload?.geo_radius) {
                const selectedSet = new Set(poiIds);
                const filtered: PoiCoord[] = [];
                for (const g of matchingJob.verasetPayload.geo_radius) {
                  if (selectedSet.has(g.poi_id)) {
                    filtered.push({ lat: g.latitude, lng: g.longitude, radiusM: g.distance_in_meters || 200 });
                  }
                }
                if (filtered.length > 0) {
                  poiCoords = filtered;
                  console.log(`[DS-REPORT] Filtered to ${poiCoords.length} POI coords (of ${poiIds.length} selected)`);
                }
              }
            }
          } else {
            console.warn(`[DS-REPORT] No matching job found in index for dataset ${datasetName}`);
          }
        } else {
          console.warn(`[DS-REPORT] No jobs-index found`);
        }
      } catch (e: any) {
        console.warn(`[DS-REPORT] Could not extract POI coords: ${e.message}`);
      }

      // For large POI sets, create an Athena table instead of VALUES clause
      let poiTableName: string | undefined;
      if (poiCoords.length > 0 && shouldUsePoiTable(poiCoords)) {
        try {
          poiTableName = await ensurePoiCoordsTable(datasetName, poiCoords);
          console.log(`[DS-REPORT] Using POI table ${poiTableName} for ${poiCoords.length} coords`);
        } catch (e: any) {
          console.error(`[DS-REPORT] Failed to create POI table, falling back to VALUES: ${e.message}`);
        }
      }

      const [od, hourly, catchment, mobility, temporal, totalDevices] = await Promise.all([
        startDatasetODQuery(datasetName, poiCoords, poiTableName, dwellFilter).catch((e) => { console.error('[DS-REPORT] OD failed:', e.message); return undefined; }),
        startDatasetHourlyQuery(datasetName, poiCoords, poiTableName, dwellFilter).catch((e) => { console.error('[DS-REPORT] Hourly failed:', e.message); return undefined; }),
        startDatasetCatchmentQuery(datasetName, poiCoords, poiTableName, dwellFilter).catch((e) => { console.error('[DS-REPORT] Catchment failed:', e.message); return undefined; }),
        startDatasetMobilityQuery(datasetName, poiCoords, poiTableName, dwellFilter).catch((e) => { console.error('[DS-REPORT] Mobility failed:', e.message); return undefined; }),
        startDatasetTemporalQuery(datasetName, poiIds, dwellFilter).catch((e) => { console.error('[DS-REPORT] Temporal failed:', e.message); return undefined; }),
        startDatasetTotalDevicesQuery(datasetName, poiIds, dwellFilter).catch((e) => { console.error('[DS-REPORT] TotalDevices failed:', e.message); return undefined; }),
      ]);

      const queries: Record<string, string> = {};
      if (od) queries.od = od;
      if (hourly) queries.hourly = hourly;
      if (catchment) queries.catchment = catchment;
      if (mobility) queries.mobility = mobility;
      if (temporal) queries.temporal = temporal;
      if (totalDevices) queries.totalDevices = totalDevices;

      state = { phase: 'polling', queries };
      try {
        await saveState(datasetName, state);
      } catch (saveErr: any) {
        console.error(`[DS-REPORT] Failed to save state for ${datasetName}, continuing anyway:`, saveErr.message);
      }

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'starting', percent: 5, message: `Started ${Object.keys(queries).length} queries...` },
      });
    }

    // ── Phase 2: Poll queries ──────────────────────────────────────
    if (state.phase === 'polling' && state.queries) {
      const queryEntries = Object.entries(state.queries);

      // If all queries were removed (all failed/expired), restart
      if (queryEntries.length === 0) {
        console.warn(`[DS-REPORT] All queries removed, restarting...`);
        state = null as any;
        await putConfig(STATE_KEY(datasetName), null as any, { compact: true }).catch(() => {});
        return NextResponse.json({
          phase: 'polling',
          progress: { step: 'restarting', percent: 0, message: 'Restarting queries...' },
        });
      }

      let allDone = true;

      const statusResults = await Promise.all(
        queryEntries.map(async ([name, queryId]) => {
          try {
            const s = await checkQueryStatus(queryId);
            return { name, state: s.state, error: s.error };
          } catch (err: any) {
            // Query not found in Athena (expired/invalid) → treat as failed
            const msg = err?.message || '';
            if (msg.includes('not found') || msg.includes('InvalidRequestException')) {
              console.warn(`[DS-REPORT] Query ${name} (${queryId}) not found in Athena, treating as FAILED`);
              return { name, state: 'FAILED' as const, error: 'Query expired or not found' };
            }
            return { name, state: 'FAILED' as const, error: 'Check failed' };
          }
        })
      );

      let doneCount = 0;
      let failedCount = 0;
      for (const { name: qName, state: qState, error: qError } of statusResults) {
        if (qState === 'RUNNING' || qState === 'QUEUED') allDone = false;
        if (qState === 'SUCCEEDED') doneCount++;
        if (qState === 'FAILED' || qState === 'CANCELLED') {
          doneCount++; // Count as done (will be skipped in parsing)
          failedCount++;
          console.warn(`[DS-REPORT] Query ${qName} failed: ${qError}`);
          // Remove failed queries so parsing skips them
          delete state.queries![qName];
        }
      }

      // If ALL queries failed (e.g. all expired), reset state to restart
      if (failedCount === queryEntries.length) {
        console.warn(`[DS-REPORT] All ${failedCount} queries failed/expired, resetting to restart`);
        state = { phase: 'done', error: 'All queries failed' };
        await saveState(datasetName, state);
        return NextResponse.json({
          phase: 'done',
          progress: { step: 'error', percent: 0, message: 'All queries failed or expired. Click Generate again to retry.' },
        });
      }

      if (!allDone) {
        // Save state with removed failed queries
        await saveState(datasetName, state);
        const runningCount = queryEntries.length - doneCount;
        const totalQ = queryEntries.length;
        // Build detail of which queries finished
        const completedNames = statusResults
          .filter(r => r.state === 'SUCCEEDED')
          .map(r => r.name.charAt(0).toUpperCase() + r.name.slice(1));
        const runningNames = statusResults
          .filter(r => r.state === 'RUNNING' || r.state === 'QUEUED')
          .map(r => r.name.charAt(0).toUpperCase() + r.name.slice(1));
        const detail = runningNames.length > 0
          ? `Running: ${runningNames.join(', ')}`
          : '';
        return NextResponse.json({
          phase: 'polling',
          progress: {
            step: 'polling_queries',
            percent: 10 + Math.round((doneCount / totalQ) * 30),
            message: `Athena queries: ${doneCount}/${totalQ} complete`,
            detail,
            completedNames,
            runningNames,
          },
        });
      }

      // All done → advance to parsing on NEXT call (avoid doing too much in one call → Vercel timeout)
      state = { ...state, phase: 'parsing' };
      await saveState(datasetName, state);
      return NextResponse.json({
        phase: 'parsing',
        progress: { step: 'queries_done', percent: 45, message: 'All queries done, parsing results...' },
      });
    }

    // ── Phase 3: Parse hourly + mobility + temporal (no geocoding) ──
    if (state.phase === 'parsing' && state.queries) {
      // Parse hourly
      if (state.queries.hourly) {
        try {
          const result = await fetchQueryResults(state.queries.hourly);
          const report = parseConsolidatedHourly(datasetName, result.rows);
          await saveReport(datasetName, 'hourly', report);
        } catch (err: any) {
          console.error('[DS-REPORT] Error parsing hourly:', err.message);
        }
      }

      // Parse mobility
      if (state.queries.mobility) {
        try {
          const result = await fetchQueryResults(state.queries.mobility);
          const report = parseConsolidatedMobility(datasetName, result.rows);
          await saveReport(datasetName, 'mobility', report);
        } catch (err: any) {
          console.error('[DS-REPORT] Error parsing mobility:', err.message);
        }
      }

      // Parse temporal + totalDevices
      if (state.queries.temporal) {
        try {
          const result = await fetchQueryResults(state.queries.temporal);
          const dailyData = result.rows.map((r: any) => ({
            date: r.date,
            pings: parseInt(r.pings, 10) || 0,
            devices: parseInt(r.devices, 10) || 0,
          }));
          const report = buildTemporalTrends(datasetName, [dailyData]);

          // Add total unique devices from dedicated query
          if (state.queries.totalDevices) {
            try {
              const totalResult = await fetchQueryResults(state.queries.totalDevices);
              const totalUniqueDevices = parseInt(totalResult.rows[0]?.total_unique_devices, 10) || 0;
              (report as any).totalUniqueDevices = totalUniqueDevices;
              console.log(`[DS-REPORT] Total unique devices for ${datasetName}: ${totalUniqueDevices}`);
            } catch (err: any) {
              console.error('[DS-REPORT] Error parsing totalDevices:', err.message);
            }
          }

          await saveReport(datasetName, 'temporal', report);
        } catch (err: any) {
          console.error('[DS-REPORT] Error parsing temporal:', err.message);
        }
      }

      state = { ...state, phase: 'geocoding', parsed: { hourly: true, mobility: true, temporal: true } };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'geocoding',
        progress: { step: 'geocoding', percent: 55, message: 'Geocoding origins and destinations...' },
      });
    }

    // ── Phase 4: Parse OD + catchment (with geocoding) ─────────────
    if (state.phase === 'geocoding' && state.queries) {
      try {
        // Collect coordinates for geocoding
        const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();

        let odClusters: ReturnType<typeof parseConsolidatedOD> | undefined;
        let catchmentRows: Record<string, any>[] | undefined;

        if (state.queries.od) {
          try {
            const result = await fetchQueryResults(state.queries.od);
            odClusters = parseConsolidatedOD(result.rows);
            for (const c of odClusters.clusters) {
              const oKey = `${c.originLat},${c.originLng}`;
              const ex = coordsToGeocode.get(oKey);
              coordsToGeocode.set(oKey, { lat: c.originLat, lng: c.originLng, deviceCount: (ex?.deviceCount || 0) + c.deviceDays });
              const dKey = `${c.destLat},${c.destLng}`;
              const dEx = coordsToGeocode.get(dKey);
              coordsToGeocode.set(dKey, { lat: c.destLat, lng: c.destLng, deviceCount: (dEx?.deviceCount || 0) + c.deviceDays });
            }
          } catch (err: any) {
            console.error('[DS-REPORT] Error fetching OD:', err.message);
          }
        }

        if (state.queries.catchment) {
          try {
            const result = await fetchQueryResults(state.queries.catchment);
            catchmentRows = result.rows;
            for (const row of catchmentRows) {
              const lat = parseFloat(row.origin_lat) || 0;
              const lng = parseFloat(row.origin_lng) || 0;
              const dd = parseInt(row.device_days, 10) || 0;
              const key = `${lat},${lng}`;
              const ex = coordsToGeocode.get(key);
              coordsToGeocode.set(key, { lat, lng, deviceCount: (ex?.deviceCount || 0) + dd });
            }
          } catch (err: any) {
            console.error('[DS-REPORT] Error fetching catchment:', err.message);
          }
        }

        // Batch geocode
        const coordToZip = new Map<string, { zipCode: string; city: string; country: string }>();
        if (coordsToGeocode.size > 0) {
          // Try to detect country from existing analysis
          const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
          if (analysis?.country) {
            setCountryFilter([analysis.country]);
          }

          const points = Array.from(coordsToGeocode.values());
          const roundedPoints = points.map((p) => ({
            lat: Math.round(p.lat * 10) / 10,
            lng: Math.round(p.lng * 10) / 10,
            deviceCount: p.deviceCount,
          }));

          // Deduplicate rounded
          const uniqueRounded = new Map<string, { lat: number; lng: number; deviceCount: number }>();
          for (const p of roundedPoints) {
            const key = `${p.lat},${p.lng}`;
            const ex = uniqueRounded.get(key);
            if (ex) ex.deviceCount += p.deviceCount;
            else uniqueRounded.set(key, { ...p });
          }

          const geocoded = await batchReverseGeocode(Array.from(uniqueRounded.values()));

          // Map original coords → geocode results
          for (const [key, p] of coordsToGeocode.entries()) {
            const roundedKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
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

          setCountryFilter(null);
        }

        // Build + save OD report
        if (odClusters) {
          const odReport = buildODReport(datasetName, odClusters.clusters, coordToZip);
          await saveReport(datasetName, 'od', odReport);
        }

        // Build + save catchment report
        if (catchmentRows) {
          const catchmentReport = buildCatchmentReport(datasetName, catchmentRows, coordToZip);
          await saveReport(datasetName, 'catchment', catchmentReport);
        }

        state = { phase: 'done' };
        await saveState(datasetName, state);

        return NextResponse.json({
          phase: 'done',
          progress: { step: 'complete', percent: 100, message: 'Reports generated' },
        });
      } catch (err: any) {
        console.error('[DS-REPORT] Error in geocoding phase:', err.message);
        state = { phase: 'done', error: err.message };
        await saveState(datasetName, state);
        return NextResponse.json({
          phase: 'done',
          progress: { step: 'complete', percent: 100, message: `Completed with errors: ${err.message}` },
        });
      }
    }

    // Already done
    return NextResponse.json({
      phase: 'done',
      progress: { step: 'complete', percent: 100, message: 'Reports already generated' },
    });
  } catch (error: any) {
    console.error('[DS-REPORT-POLL] Error for', datasetName, ':', error.message, error.stack?.split('\n').slice(0, 3).join(' | '));
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
