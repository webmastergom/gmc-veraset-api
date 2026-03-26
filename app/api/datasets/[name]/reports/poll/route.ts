import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { checkQueryStatus, fetchQueryResults } from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { getJob } from '@/lib/jobs';
import {
  DWELL_BUCKETS,
  startDwellCTASQuery,
  startBucketODQuery,
  startBucketHourlyQuery,
  startBucketCatchmentQuery,
  startBucketMobilityQuery,
  startBucketTemporalQuery,
  startBucketTotalDevicesQuery,
  ensurePoiCoordsTable,
  shouldUsePoiTable,
} from '@/lib/dataset-report-queries';
import { extractPoiCoords, type PoiCoord } from '@/lib/mega-consolidation-queries';
import {
  parseConsolidatedOD,
  parseConsolidatedHourly,
  parseConsolidatedMobility,
  buildODReport,
  buildCatchmentReport,
  buildTemporalTrends,
} from '@/lib/mega-report-consolidation';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { dropTempTable, cleanupTempS3 } from '@/lib/athena';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

const STATE_KEY = (ds: string) => `dataset-report-state/${ds}`;
const REPORT_KEY = (ds: string, type: string, bucket: number) =>
  `dataset-reports/${ds}/${type}-dwell-${bucket}`;

// 3 batches of 3 buckets each
const BUCKET_BATCHES: number[][] = [
  [0, 2, 5],
  [10, 15, 30],
  [60, 120, 180],
];

const QUERY_TYPES = ['od', 'hourly', 'catchment', 'mobility', 'temporal', 'totalDevices'] as const;

interface DwellReportState {
  phase: 'materialize' | 'poll_ctas' | 'start_batch' | 'poll_batch' | 'parse_batch' | 'cleanup' | 'done';
  ctasQueryId?: string;
  tempTableName?: string;
  currentBatchIndex: number;
  bucketBatches: number[][];
  queries?: Record<string, string>; // "0-od" -> queryId
  completedBuckets: number[];
  coordToZip?: Record<string, string>; // "lat,lng" -> "zip|city|country"
  error?: string;
}

async function getState(ds: string): Promise<DwellReportState | null> {
  return await getConfig<DwellReportState>(STATE_KEY(ds));
}

async function saveState(ds: string, state: DwellReportState): Promise<void> {
  await putConfig(STATE_KEY(ds), state, { compact: true });
}

async function saveReport(ds: string, type: string, bucket: number, data: any): Promise<void> {
  await putConfig(REPORT_KEY(ds, type, bucket), data, { compact: true });
}

function batchPercent(batchIndex: number, intraPercent: number): number {
  // 0-10%: materialize + poll_ctas
  // 10-40%: batch 0 (start_batch + poll_batch + parse_batch)
  // 40-70%: batch 1
  // 70-100%: batch 2
  const batchStart = 10 + batchIndex * 30;
  return Math.min(100, batchStart + Math.round(intraPercent * 0.3));
}

/**
 * POST /api/datasets/[name]/reports/poll
 * Multi-phase polling for pre-computed dwell-bucketed reports.
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

    // Parse optional POI ID filter from request body
    let poiIds: string[] | undefined;
    try {
      const body = await request.json();
      if (body?.poiIds?.length) poiIds = body.poiIds;
    } catch { /* no body or invalid JSON */ }

    // Reset if done
    if (state?.phase === 'done') state = null;

    // ── Phase: materialize ─────────────────────────────────────────
    if (!state) {
      console.log(`[DS-REPORT] Phase materialize: Starting CTAS for ${datasetName} (reset=${reset}, poiIds=${poiIds?.length || 0})`);

      // Find the job for this dataset to extract POI coordinates
      let poiCoords: PoiCoord[] = [];
      try {
        const index = await getConfig<Record<string, any>>('jobs-index');
        if (index) {
          const matchingEntry = Object.entries(index).find(
            ([_, j]: [string, any]) => j.s3DestPath && j.s3DestPath.replace(/\/$/, '').split('/').pop() === datasetName
          );
          if (matchingEntry) {
            const [matchingJobId] = matchingEntry;
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

      // Start CTAS query to materialize at-POI pings with dwell
      const { queryId, tableName } = await startDwellCTASQuery(datasetName, poiCoords, poiTableName);
      console.log(`[DS-REPORT] CTAS started: queryId=${queryId}, tempTable=${tableName}`);

      state = {
        phase: 'poll_ctas',
        ctasQueryId: queryId,
        tempTableName: tableName,
        currentBatchIndex: 0,
        bucketBatches: BUCKET_BATCHES,
        completedBuckets: [],
      };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'materialize',
        progress: {
          step: 'materialize',
          percent: 5,
          message: 'Materializing visits with dwell calculation...',
          totalBuckets: DWELL_BUCKETS.length,
        },
      });
    }

    // ── Phase: poll_ctas ───────────────────────────────────────────
    if (state.phase === 'poll_ctas') {
      if (!state.ctasQueryId) {
        state = { ...state, phase: 'done', error: 'Missing CTAS query ID' };
        await saveState(datasetName, state);
        return NextResponse.json({
          phase: 'done',
          progress: { step: 'error', percent: 0, message: 'Missing CTAS query ID' },
        });
      }

      let qStatus: { state: string; error?: string };
      try {
        qStatus = await checkQueryStatus(state.ctasQueryId);
      } catch (err: any) {
        const msg = err?.message || '';
        if (msg.includes('not found') || msg.includes('InvalidRequestException')) {
          state = { ...state, phase: 'done', error: 'CTAS query expired or not found' };
          await saveState(datasetName, state);
          return NextResponse.json({
            phase: 'done',
            progress: { step: 'error', percent: 0, message: 'CTAS query expired. Click Generate again to retry.' },
          });
        }
        throw err;
      }

      if (qStatus.state === 'FAILED' || qStatus.state === 'CANCELLED') {
        state = { ...state, phase: 'done', error: qStatus.error || 'CTAS query failed' };
        await saveState(datasetName, state);
        return NextResponse.json({
          phase: 'done',
          progress: { step: 'error', percent: 0, message: `CTAS failed: ${qStatus.error || 'unknown error'}` },
        });
      }

      if (qStatus.state === 'RUNNING' || qStatus.state === 'QUEUED') {
        return NextResponse.json({
          phase: 'poll_ctas',
          progress: {
            step: 'poll_ctas',
            percent: 10,
            message: 'Computing spatial join + dwell times...',
            detail: 'This is the most expensive step — only happens once',
            totalBuckets: DWELL_BUCKETS.length,
          },
        });
      }

      // SUCCEEDED → advance to start_batch
      state = { ...state, phase: 'start_batch' };
      await saveState(datasetName, state);
      // Fall through to start_batch
    }

    // ── Phase: start_batch ─────────────────────────────────────────
    if (state.phase === 'start_batch') {
      const batch = state.bucketBatches[state.currentBatchIndex];
      if (!batch) {
        // No more batches → cleanup
        state = { ...state, phase: 'cleanup' };
        await saveState(datasetName, state);
        // Fall through to cleanup
      } else {
        console.log(`[DS-REPORT] Starting batch ${state.currentBatchIndex}: buckets [${batch.join(', ')}]`);

        const queries: Record<string, string> = {};
        const tempTable = state.tempTableName!;

        // Start 6 queries per bucket = 18 queries total for 3 buckets
        const startPromises: Promise<void>[] = [];
        for (const bucket of batch) {
          startPromises.push(
            startBucketODQuery(datasetName, tempTable, bucket)
              .then(id => { queries[`${bucket}-od`] = id; })
              .catch(e => { console.error(`[DS-REPORT] ${bucket}-od failed to start:`, e.message); }),
            startBucketHourlyQuery(datasetName, tempTable, bucket)
              .then(id => { queries[`${bucket}-hourly`] = id; })
              .catch(e => { console.error(`[DS-REPORT] ${bucket}-hourly failed to start:`, e.message); }),
            startBucketCatchmentQuery(datasetName, tempTable, bucket)
              .then(id => { queries[`${bucket}-catchment`] = id; })
              .catch(e => { console.error(`[DS-REPORT] ${bucket}-catchment failed to start:`, e.message); }),
            startBucketMobilityQuery(datasetName, tempTable, bucket)
              .then(id => { queries[`${bucket}-mobility`] = id; })
              .catch(e => { console.error(`[DS-REPORT] ${bucket}-mobility failed to start:`, e.message); }),
            startBucketTemporalQuery(datasetName, tempTable, bucket)
              .then(id => { queries[`${bucket}-temporal`] = id; })
              .catch(e => { console.error(`[DS-REPORT] ${bucket}-temporal failed to start:`, e.message); }),
            startBucketTotalDevicesQuery(datasetName, tempTable, bucket)
              .then(id => { queries[`${bucket}-totalDevices`] = id; })
              .catch(e => { console.error(`[DS-REPORT] ${bucket}-totalDevices failed to start:`, e.message); }),
          );
        }

        await Promise.all(startPromises);

        state = { ...state, phase: 'poll_batch', queries };
        await saveState(datasetName, state);

        const pendingBuckets = DWELL_BUCKETS.filter(
          b => !state!.completedBuckets.includes(b) && !batch.includes(b)
        );

        return NextResponse.json({
          phase: 'start_batch',
          progress: {
            step: 'start_batch',
            percent: batchPercent(state.currentBatchIndex, 0),
            message: `Started ${Object.keys(queries).length} queries for buckets [${batch.join(', ')}] min`,
            completedBuckets: state.completedBuckets,
            runningBuckets: batch,
            pendingBuckets,
            totalBuckets: DWELL_BUCKETS.length,
          },
        });
      }
    }

    // ── Phase: poll_batch ──────────────────────────────────────────
    if (state.phase === 'poll_batch' && state.queries) {
      const queryEntries = Object.entries(state.queries);

      if (queryEntries.length === 0) {
        // All queries failed → advance to parse anyway (will skip everything)
        state = { ...state, phase: 'parse_batch' };
        await saveState(datasetName, state);
        // Fall through
      } else {
        const statusResults = await Promise.all(
          queryEntries.map(async ([name, queryId]) => {
            try {
              const s = await checkQueryStatus(queryId);
              return { name, state: s.state, error: s.error };
            } catch (err: any) {
              const msg = err?.message || '';
              if (msg.includes('not found') || msg.includes('InvalidRequestException')) {
                console.warn(`[DS-REPORT] Query ${name} (${queryId}) not found, treating as FAILED`);
                return { name, state: 'FAILED' as const, error: 'Query expired or not found' };
              }
              return { name, state: 'FAILED' as const, error: 'Check failed' };
            }
          })
        );

        let allDone = true;
        let doneCount = 0;
        const totalQ = queryEntries.length;

        for (const { name: qName, state: qState, error: qError } of statusResults) {
          if (qState === 'RUNNING' || qState === 'QUEUED') {
            allDone = false;
          } else {
            doneCount++;
            if (qState === 'FAILED' || qState === 'CANCELLED') {
              console.warn(`[DS-REPORT] Query ${qName} failed: ${qError}`);
              delete state.queries![qName];
            }
          }
        }

        if (!allDone) {
          await saveState(datasetName, state);

          const batch = state.bucketBatches[state.currentBatchIndex];
          const runningNames = statusResults
            .filter(r => r.state === 'RUNNING' || r.state === 'QUEUED')
            .map(r => r.name);
          const pendingBuckets = DWELL_BUCKETS.filter(
            b => !state!.completedBuckets.includes(b) && !batch.includes(b)
          );

          return NextResponse.json({
            phase: 'poll_batch',
            progress: {
              step: 'poll_batch',
              percent: batchPercent(state.currentBatchIndex, Math.round((doneCount / totalQ) * 33)),
              message: `Athena queries: ${doneCount}/${totalQ} complete`,
              detail: `Running: ${runningNames.join(', ')}`,
              completedBuckets: state.completedBuckets,
              runningBuckets: batch,
              pendingBuckets,
              totalBuckets: DWELL_BUCKETS.length,
            },
          });
        }

        // All done → advance to parse_batch
        state = { ...state, phase: 'parse_batch' };
        await saveState(datasetName, state);

        return NextResponse.json({
          phase: 'parse_batch',
          progress: {
            step: 'queries_done',
            percent: batchPercent(state.currentBatchIndex, 33),
            message: 'All queries done, parsing results...',
            completedBuckets: state.completedBuckets,
            totalBuckets: DWELL_BUCKETS.length,
          },
        });
      }
    }

    // ── Phase: parse_batch ─────────────────────────────────────────
    if (state.phase === 'parse_batch') {
      const batch = state.bucketBatches[state.currentBatchIndex];
      const queries = state.queries || {};

      // Collect all coords needing geocoding across this batch
      const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();
      const bucketODData = new Map<number, ReturnType<typeof parseConsolidatedOD>>();
      const bucketCatchmentRows = new Map<number, Record<string, any>[]>();

      for (const bucket of batch) {
        // Parse hourly
        if (queries[`${bucket}-hourly`]) {
          try {
            const result = await fetchQueryResults(queries[`${bucket}-hourly`]);
            const report = parseConsolidatedHourly(datasetName, result.rows);
            await saveReport(datasetName, 'hourly', bucket, report);
          } catch (err: any) {
            console.error(`[DS-REPORT] Error parsing ${bucket}-hourly:`, err.message);
          }
        }

        // Parse mobility
        if (queries[`${bucket}-mobility`]) {
          try {
            const result = await fetchQueryResults(queries[`${bucket}-mobility`]);
            const report = parseConsolidatedMobility(datasetName, result.rows);
            await saveReport(datasetName, 'mobility', bucket, report);
          } catch (err: any) {
            console.error(`[DS-REPORT] Error parsing ${bucket}-mobility:`, err.message);
          }
        }

        // Parse temporal + totalDevices
        if (queries[`${bucket}-temporal`]) {
          try {
            const result = await fetchQueryResults(queries[`${bucket}-temporal`]);
            const dailyData = result.rows.map((r: any) => ({
              date: r.date,
              pings: parseInt(r.pings, 10) || 0,
              devices: parseInt(r.devices, 10) || 0,
            }));
            const report = buildTemporalTrends(datasetName, [dailyData]);

            if (queries[`${bucket}-totalDevices`]) {
              try {
                const totalResult = await fetchQueryResults(queries[`${bucket}-totalDevices`]);
                const totalUniqueDevices = parseInt(totalResult.rows[0]?.total_unique_devices, 10) || 0;
                (report as any).totalUniqueDevices = totalUniqueDevices;
                console.log(`[DS-REPORT] Bucket ${bucket} total unique devices: ${totalUniqueDevices}`);
              } catch (err: any) {
                console.error(`[DS-REPORT] Error parsing ${bucket}-totalDevices:`, err.message);
              }
            }

            await saveReport(datasetName, 'temporal', bucket, report);
          } catch (err: any) {
            console.error(`[DS-REPORT] Error parsing ${bucket}-temporal:`, err.message);
          }
        }

        // Parse OD → collect coords for geocoding
        if (queries[`${bucket}-od`]) {
          try {
            const result = await fetchQueryResults(queries[`${bucket}-od`]);
            const odClusters = parseConsolidatedOD(result.rows);
            bucketODData.set(bucket, odClusters);
            for (const c of odClusters.clusters) {
              const oKey = `${c.originLat},${c.originLng}`;
              const ex = coordsToGeocode.get(oKey);
              coordsToGeocode.set(oKey, { lat: c.originLat, lng: c.originLng, deviceCount: (ex?.deviceCount || 0) + c.deviceDays });
              const dKey = `${c.destLat},${c.destLng}`;
              const dEx = coordsToGeocode.get(dKey);
              coordsToGeocode.set(dKey, { lat: c.destLat, lng: c.destLng, deviceCount: (dEx?.deviceCount || 0) + c.deviceDays });
            }
          } catch (err: any) {
            console.error(`[DS-REPORT] Error parsing ${bucket}-od:`, err.message);
          }
        }

        // Parse catchment → collect coords for geocoding
        if (queries[`${bucket}-catchment`]) {
          try {
            const result = await fetchQueryResults(queries[`${bucket}-catchment`]);
            bucketCatchmentRows.set(bucket, result.rows);
            for (const row of result.rows) {
              const lat = parseFloat(row.origin_lat) || 0;
              const lng = parseFloat(row.origin_lng) || 0;
              const dd = parseInt(row.device_days, 10) || 0;
              const key = `${lat},${lng}`;
              const ex = coordsToGeocode.get(key);
              coordsToGeocode.set(key, { lat, lng, deviceCount: (ex?.deviceCount || 0) + dd });
            }
          } catch (err: any) {
            console.error(`[DS-REPORT] Error parsing ${bucket}-catchment:`, err.message);
          }
        }
      }

      // Geocode: reuse from state if already computed, otherwise compute once
      let coordToZip = new Map<string, { zipCode: string; city: string; country: string }>();

      if (state.coordToZip) {
        // Deserialize from saved state
        for (const [key, packed] of Object.entries(state.coordToZip)) {
          const [zipCode, city, country] = (packed as string).split('|');
          coordToZip.set(key, { zipCode, city, country });
        }
      }

      // Always geocode new coords not yet in the map
      if (coordsToGeocode.size > 0) {
        const newCoords = new Map<string, { lat: number; lng: number; deviceCount: number }>();
        for (const [key, val] of coordsToGeocode.entries()) {
          if (!coordToZip.has(key)) {
            newCoords.set(key, val);
          }
        }

        if (newCoords.size > 0) {
          try {
            // Detect country from existing analysis
            const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
            if (analysis?.country) {
              setCountryFilter([analysis.country]);
            }

            const points = Array.from(newCoords.values());
            const roundedPoints = points.map(p => ({
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
            const uniqueKeys = Array.from(uniqueRounded.keys());

            // Map original coords → geocode results
            for (const [key, p] of newCoords.entries()) {
              const roundedKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
              const idx = uniqueKeys.indexOf(roundedKey);
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
          } catch (err: any) {
            console.error(`[DS-REPORT] Geocoding error:`, err.message);
            setCountryFilter(null);
          }
        }
      }

      // Build + save OD and catchment reports per bucket
      for (const bucket of batch) {
        const odClusters = bucketODData.get(bucket);
        if (odClusters) {
          const odReport = buildODReport(datasetName, odClusters.clusters, coordToZip);
          await saveReport(datasetName, 'od', bucket, odReport);
        }

        const catchmentRows = bucketCatchmentRows.get(bucket);
        if (catchmentRows) {
          const catchmentReport = buildCatchmentReport(datasetName, catchmentRows, coordToZip);
          await saveReport(datasetName, 'catchment', bucket, catchmentReport);
        }
      }

      // Serialize coordToZip to state for reuse by subsequent batches
      const serializedCoordToZip: Record<string, string> = {};
      for (const [key, val] of coordToZip.entries()) {
        serializedCoordToZip[key] = `${val.zipCode}|${val.city}|${val.country}`;
      }

      // Mark buckets as completed
      const newCompleted = [...state.completedBuckets, ...batch];

      if (state.currentBatchIndex + 1 < state.bucketBatches.length) {
        // More batches → advance to start_batch
        state = {
          ...state,
          phase: 'start_batch',
          currentBatchIndex: state.currentBatchIndex + 1,
          completedBuckets: newCompleted,
          coordToZip: serializedCoordToZip,
          queries: undefined,
        };
        await saveState(datasetName, state);

        const pendingBuckets = DWELL_BUCKETS.filter(b => !newCompleted.includes(b));
        return NextResponse.json({
          phase: 'parse_batch',
          progress: {
            step: 'batch_done',
            percent: batchPercent(state.currentBatchIndex - 1, 100),
            message: `Batch ${state.currentBatchIndex} of ${state.bucketBatches.length} complete`,
            completedBuckets: newCompleted,
            pendingBuckets,
            totalBuckets: DWELL_BUCKETS.length,
          },
        });
      }

      // All batches done → advance to cleanup
      state = {
        ...state,
        phase: 'cleanup',
        completedBuckets: newCompleted,
        coordToZip: serializedCoordToZip,
        queries: undefined,
      };
      await saveState(datasetName, state);
      // Fall through to cleanup
    }

    // ── Phase: cleanup ─────────────────────────────────────────────
    if (state.phase === 'cleanup') {
      if (state.tempTableName) {
        try {
          await dropTempTable(state.tempTableName);
          console.log(`[DS-REPORT] Dropped temp table ${state.tempTableName}`);
        } catch (err: any) {
          console.error(`[DS-REPORT] Error dropping temp table:`, err.message);
        }

        try {
          await cleanupTempS3(state.tempTableName);
          console.log(`[DS-REPORT] Cleaned up temp S3 for ${state.tempTableName}`);
        } catch (err: any) {
          console.error(`[DS-REPORT] Error cleaning up temp S3:`, err.message);
        }
      }

      state = {
        ...state,
        phase: 'done',
        coordToZip: undefined, // don't persist large geocode map in final state
      };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'done',
        progress: {
          step: 'done',
          percent: 100,
          message: 'All dwell buckets computed!',
          completedBuckets: DWELL_BUCKETS,
          totalBuckets: DWELL_BUCKETS.length,
        },
      });
    }

    // Already done
    return NextResponse.json({
      phase: 'done',
      progress: {
        step: 'done',
        percent: 100,
        message: 'All dwell buckets computed!',
        completedBuckets: DWELL_BUCKETS,
        totalBuckets: DWELL_BUCKETS.length,
      },
    });
  } catch (error: any) {
    console.error('[DS-REPORT-POLL] Error for', datasetName, ':', error.message, error.stack?.split('\n').slice(0, 3).join(' | '));
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
