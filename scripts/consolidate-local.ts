/**
 * Local consolidation script — bypasses Vercel 60s timeout.
 * Runs phases 3+4 (parse + geocode + build reports) locally.
 *
 * Usage: npx tsx scripts/consolidate-local.ts <megaJobId>
 */

import { fetchQueryResults } from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { getMegaJob, updateMegaJob, megaReportKey } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import {
  parseConsolidatedVisits,
  buildTemporalTrends,
  parseConsolidatedOD,
  buildODReport,
  parseConsolidatedHourly,
  buildCatchmentReport,
  parseConsolidatedMobility,
  buildAffinityReport,
  saveConsolidatedReport,
} from '@/lib/mega-report-consolidation';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { getPOICollection } from '@/lib/poi-storage';

const MEGA_JOB_ID = process.argv[2];
if (!MEGA_JOB_ID) {
  console.error('Usage: npx tsx scripts/consolidate-local.ts <megaJobId>');
  process.exit(1);
}

const CONSOLIDATION_KEY = `mega-consolidation-state/${MEGA_JOB_ID}`;

async function main() {
  console.log(`\n🔧 Local consolidation for mega-job: ${MEGA_JOB_ID}\n`);

  // Load state
  const state = await getConfig<any>(CONSOLIDATION_KEY);
  if (!state?.queries) {
    console.error('❌ No consolidation state found or no queries. Run consolidation from UI first to start queries.');
    process.exit(1);
  }

  console.log(`📋 Current phase: ${state.phase}`);
  console.log(`📋 Queries: ${Object.keys(state.queries).join(', ')}`);

  const megaJob = await getMegaJob(MEGA_JOB_ID);
  if (!megaJob) {
    console.error('❌ Mega-job not found');
    process.exit(1);
  }

  // Get synced sub-jobs for country detection
  const syncedJobs: Awaited<ReturnType<typeof getJob>>[] = [];
  for (const sjId of megaJob.subJobIds || []) {
    const job = await getJob(sjId);
    if (job?.status === 'SUCCESS') syncedJobs.push(job);
  }
  console.log(`📋 Synced sub-jobs: ${syncedJobs.length}`);

  const reportKeys: Record<string, string> = {};
  const errors: string[] = [];

  // ── Phase 3: Parse visits + hourly + mobility + temporal ──────────
  if (!state.parsed?.visits || !state.parsed?.hourly || !state.parsed?.temporal) {
    console.log('\n── Phase 3: Parsing visits, hourly, mobility, temporal ──');

    // Parse & save helper
    const parseAndSave = async (name: string, queryId: string, builder: (rows: any[]) => any, reportType: string) => {
      try {
        console.log(`  📥 Fetching ${name} results...`);
        const result = await fetchQueryResults(queryId);
        console.log(`  📊 ${name}: ${result.rows.length} rows`);
        const report = builder(result.rows);
        reportKeys[reportType] = await saveConsolidatedReport(MEGA_JOB_ID, reportType, report);
        console.log(`  ✅ ${name} saved`);
        return report;
      } catch (err: any) {
        console.error(`  ❌ ${name}: ${err.message}`);
        errors.push(`${name}: ${err.message}`);
        return null;
      }
    };

    // Get totalDevices first
    let totalUniqueDevices: number | undefined;
    if (state.queries.totalDevices) {
      try {
        const tdResult = await fetchQueryResults(state.queries.totalDevices);
        if (tdResult.rows.length > 0) {
          totalUniqueDevices = parseInt(tdResult.rows[0].total_unique_devices || tdResult.rows[0].total_devices || '0', 10);
          console.log(`  📊 Total unique devices: ${totalUniqueDevices}`);
        }
      } catch (err: any) {
        console.error(`  ⚠️ totalDevices: ${err.message}`);
      }
    }

    // Load POI names from collection GeoJSON
    let poiNameMap: Map<string, string> | undefined;
    const collectionId = megaJob.sourceScope?.poiCollectionIds?.[0];
    if (collectionId) {
      try {
        const geojson = await getPOICollection(collectionId);
        if (geojson?.features) {
          poiNameMap = new Map();
          for (const f of geojson.features) {
            const props = f.properties || {};
            const poiId = props.id || props.poi_id || '';
            const name = props.name || props['nombre / name'] || props.nombre || props.Name || '';
            if (poiId && name) poiNameMap.set(poiId, name);
          }
          console.log(`  📍 Loaded ${poiNameMap.size} POI names from collection ${collectionId}`);
        }
      } catch (err: any) {
        console.warn(`  ⚠️ Could not load POI names: ${err.message}`);
      }
    }

    // Visits
    if (state.queries.visits) {
      await parseAndSave('visits', state.queries.visits, (rows) => {
        const visitsByPoi = parseConsolidatedVisits(rows, syncedJobs.filter((j): j is NonNullable<typeof j> => j !== null), poiNameMap);
        const report = { megaJobId: MEGA_JOB_ID, analyzedAt: new Date().toISOString(), totalPois: visitsByPoi.length, visitsByPoi };
        if (totalUniqueDevices !== undefined) (report as any).totalUniqueDevices = totalUniqueDevices;
        return report;
      }, 'visits');
    }

    // Hourly
    if (state.queries.hourly) {
      await parseAndSave('hourly', state.queries.hourly, (rows) => {
        return parseConsolidatedHourly(MEGA_JOB_ID, rows);
      }, 'hourly');
    }

    // Mobility
    if (state.queries.mobility) {
      await parseAndSave('mobility', state.queries.mobility, (rows) => {
        return parseConsolidatedMobility(MEGA_JOB_ID, rows);
      }, 'mobility');
    }

    // Temporal
    if (state.queries.temporal) {
      await parseAndSave('temporal', state.queries.temporal, (rows) => {
        const dailyData = rows.map((row: Record<string, any>) => ({
          date: String(row.date || ''),
          pings: parseInt(row.pings, 10) || 0,
          devices: parseInt(row.devices, 10) || 0,
        })).sort((a: any, b: any) => a.date.localeCompare(b.date));
        const report = buildTemporalTrends(MEGA_JOB_ID, [dailyData]);
        if (totalUniqueDevices !== undefined) (report as any).totalUniqueDevices = totalUniqueDevices;
        return report;
      }, 'temporal');
    }

    console.log(`\n  Phase 3 done: saved=${Object.keys(reportKeys).join(',')}, errors=${errors.length}`);
  } else {
    console.log('\n── Phase 3 already done, loading saved report keys ──');
    const phase3Keys = state.phase3ReportKeys || {};
    Object.assign(reportKeys, phase3Keys);
    console.log(`  Loaded: ${Object.keys(phase3Keys).join(', ')}`);
  }

  // ── Phase 4: OD + catchment + affinity (geocoding) ────────────────
  console.log('\n── Phase 4: Geocoding + OD + catchment + affinity ──');

  // Collect coordinates from OD + catchment results
  const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();

  let odClusters: ReturnType<typeof parseConsolidatedOD> | undefined;
  let catchmentRows: Record<string, any>[] | undefined;

  // Fetch OD results
  if (state.queries.od) {
    try {
      console.log('  📥 Fetching OD results...');
      const queryResult = await fetchQueryResults(state.queries.od);
      odClusters = parseConsolidatedOD(queryResult.rows);
      console.log(`  📊 OD: ${odClusters.clusters.length} clusters`);
      for (const c of odClusters.clusters) {
        const oKey = `${c.originLat},${c.originLng}`;
        const existing = coordsToGeocode.get(oKey);
        coordsToGeocode.set(oKey, { lat: c.originLat, lng: c.originLng, deviceCount: (existing?.deviceCount || 0) + c.deviceDays });
        const dKey = `${c.destLat},${c.destLng}`;
        const dExisting = coordsToGeocode.get(dKey);
        coordsToGeocode.set(dKey, { lat: c.destLat, lng: c.destLng, deviceCount: (dExisting?.deviceCount || 0) + c.deviceDays });
      }
    } catch (err: any) {
      console.error(`  ❌ OD fetch: ${err.message}`);
      errors.push(`od: ${err.message}`);
    }
  }

  // Fetch catchment results
  if (state.queries.catchment) {
    try {
      console.log('  📥 Fetching catchment results...');
      const queryResult = await fetchQueryResults(state.queries.catchment);
      catchmentRows = queryResult.rows;
      console.log(`  📊 Catchment: ${catchmentRows.length} rows`);
      for (const row of catchmentRows) {
        const lat = parseFloat(row.origin_lat) || 0;
        const lng = parseFloat(row.origin_lng) || 0;
        const dd = parseInt(row.device_days, 10) || 0;
        const key = `${lat},${lng}`;
        const existing = coordsToGeocode.get(key);
        coordsToGeocode.set(key, { lat, lng, deviceCount: (existing?.deviceCount || 0) + dd });
      }
    } catch (err: any) {
      console.error(`  ❌ Catchment fetch: ${err.message}`);
      errors.push(`catchment: ${err.message}`);
    }
  }

  // Reverse geocode
  const coordToZip = new Map<string, { zipCode: string; city: string; country: string }>();
  if (coordsToGeocode.size > 0) {
    console.log(`\n  🌍 Geocoding ${coordsToGeocode.size} unique coordinates...`);

    // Detect country
    const firstJob = syncedJobs[0];
    const datasetName = firstJob?.s3DestPath?.replace(/\/$/, '').split('/').pop();
    let detectedCountry: string | undefined;
    if (datasetName) {
      const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
      detectedCountry = analysis?.country;
    }
    if (detectedCountry) {
      console.log(`  🏳️ Country filter: ${detectedCountry}`);
      setCountryFilter([detectedCountry]);
    }

    // Round to 1-decimal precision for performance
    const uniqueRounded = new Map<string, { lat: number; lng: number; deviceCount: number }>();
    for (const p of coordsToGeocode.values()) {
      const key = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
      const ex = uniqueRounded.get(key);
      if (ex) ex.deviceCount += p.deviceCount;
      else uniqueRounded.set(key, { lat: Math.round(p.lat * 10) / 10, lng: Math.round(p.lng * 10) / 10, deviceCount: p.deviceCount });
    }
    console.log(`  📍 Unique rounded points: ${uniqueRounded.size}`);

    const geocoded = await batchReverseGeocode(Array.from(uniqueRounded.values()));
    console.log(`  ✅ Geocoded ${geocoded.length} points`);

    // Build rounded→zip lookup
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

    // Map original coords to geocoded results
    for (const [key, p] of coordsToGeocode.entries()) {
      const roundedKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
      const zip = roundedToZip.get(roundedKey);
      if (zip) coordToZip.set(key, zip);
    }

    setCountryFilter(null);
    console.log(`  📬 Mapped ${coordToZip.size} of ${coordsToGeocode.size} coords to zip codes`);
  }

  // Build OD report
  if (odClusters) {
    console.log('\n  📊 Building OD report...');
    const odReport = buildODReport(MEGA_JOB_ID, odClusters.clusters, coordToZip);
    reportKeys.od = await saveConsolidatedReport(MEGA_JOB_ID, 'od', odReport);
    console.log(`  ✅ OD report saved (${odReport.origins.length} origins, ${odReport.destinations.length} destinations)`);
  }

  // Build catchment report
  if (catchmentRows) {
    console.log('  📊 Building catchment report...');
    const catchmentReport = buildCatchmentReport(MEGA_JOB_ID, catchmentRows, coordToZip);
    reportKeys.catchment = await saveConsolidatedReport(MEGA_JOB_ID, 'catchment', catchmentReport);
    console.log(`  ✅ Catchment report saved (${catchmentReport.byZipCode.length} zip codes)`);
  }

  // Build affinity report
  if (state.queries.affinity) {
    try {
      console.log('  📊 Building affinity report...');
      const affinityResult = await fetchQueryResults(state.queries.affinity);
      const affinityReport = buildAffinityReport(MEGA_JOB_ID, affinityResult.rows, coordToZip);
      reportKeys.affinity = await saveConsolidatedReport(MEGA_JOB_ID, 'affinity', affinityReport);
      console.log(`  ✅ Affinity report saved (${affinityReport.byZipCode.length} zip codes)`);
    } catch (err: any) {
      console.error(`  ❌ Affinity: ${err.message}`);
      errors.push(`affinity: ${err.message}`);
    }
  }

  // MAIDs
  if (state.queries.maids) {
    reportKeys.maids = `athena-results/${state.queries.maids}.csv`;
    console.log('  ✅ MAIDs CSV key saved');
  }

  // ── Finalize ──────────────────────────────────────────────────────
  console.log('\n── Finalizing ──');
  const phase3Keys = state.phase3ReportKeys || {};
  const allReportKeys = { ...phase3Keys, ...reportKeys };
  console.log(`📋 All reports: ${Object.keys(allReportKeys).join(', ')}`);

  await updateMegaJob(MEGA_JOB_ID, {
    status: 'completed',
    consolidatedReports: allReportKeys,
  });

  await putConfig(CONSOLIDATION_KEY, { phase: 'done' });

  console.log('\n✅ Consolidation complete! Reload the page in the browser.\n');
  if (errors.length > 0) {
    console.log(`⚠️ Errors (${errors.length}):`);
    for (const e of errors) console.log(`  - ${e}`);
  }
}

main().catch((err) => {
  console.error('\n💥 Fatal error:', err);
  process.exit(1);
});
