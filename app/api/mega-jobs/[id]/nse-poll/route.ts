import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import { checkQueryStatus, ensureTableForDataset } from '@/lib/athena';
import { getConfig, putConfig, s3Client, BUCKET } from '@/lib/s3-config';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import {
  startConsolidatedNseQuery,
  extractPoiCoords,
  type DwellFilter,
} from '@/lib/mega-consolidation-queries';
import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

const STATE_KEY = (id: string) => `nse-export-state/mega-${id}`;
const NSE_KEY = (cc: string) => `nse/${cc.toUpperCase()}`;

interface NseRecord {
  postal_code: string;
  population: number;
  nse: number;
}

const NSE_BRACKETS = [
  { label: '0-19 (Low)', min: 0, max: 19 },
  { label: '20-39', min: 20, max: 39 },
  { label: '40-59 (Mid)', min: 40, max: 59 },
  { label: '60-79', min: 60, max: 79 },
  { label: '80-100 (High)', min: 80, max: 100 },
];

interface BracketResult {
  label: string;
  min: number;
  max: number;
  postalCodes: number;
  population: number;
  maidCount: number;
  downloadUrl: string | null;
}

interface NseExportState {
  phase: 'querying' | 'polling' | 'geocoding' | 'done' | 'error';
  queryId?: string;
  country: string;
  megaJobId: string;
  error?: string;
  brackets?: BracketResult[];
}

/**
 * POST /api/mega-jobs/[id]/nse-poll
 *
 * Multi-phase polling endpoint for NSE MAID segmentation on mega-jobs.
 * First call body: { country: string }
 * Subsequent calls: no body needed (reads state from S3)
 *
 * Phases:
 * 1. querying: load sub-jobs, start Athena query for ad_id + origin coords
 * 2. polling: wait for Athena query
 * 3. geocoding: fetch results, geocode → postal codes, match to NSE brackets, save CSVs
 * 4. done: return bracket counts + download URLs
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { id } = await context.params;

  try {
    const megaJob = await getMegaJob(id);
    if (!megaJob) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    // Parse body to detect new request (has country)
    let body: any;
    try { body = await request.json(); } catch { body = {}; }
    const isNewRequest = !!body.country;

    let state = await getConfig<NseExportState>(STATE_KEY(id));

    // Return cached results if done and not a new request
    if (state?.phase === 'done' && !isNewRequest && state.brackets) {
      const totalMaids = state.brackets.reduce((s, b) => s + b.maidCount, 0);
      return NextResponse.json({
        phase: 'done',
        brackets: state.brackets,
        totalMaids,
        progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
      });
    }

    // Reset on error or new request
    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: start query ──────────────────────────────────────────
    if (!state) {
      const country: string = body.country || '';
      if (!country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      // Verify NSE data exists for this country
      const nseData = await getConfig<NseRecord[]>(NSE_KEY(country));
      if (!nseData?.length) {
        return NextResponse.json({ error: `No NSE data for ${country}. Upload CSV first.` }, { status: 404 });
      }

      // Load synced sub-jobs
      const subJobs = (
        await Promise.all(megaJob.subJobIds.map((jid) => getJob(jid)))
      ).filter((j): j is NonNullable<typeof j> => j !== null);
      const syncedJobs = subJobs.filter((j) => j.status === 'SUCCESS' && j.syncedAt);

      if (syncedJobs.length === 0) {
        return NextResponse.json({ error: 'No synced sub-jobs' }, { status: 400 });
      }

      // Ensure Athena tables
      for (const job of syncedJobs) {
        const dsName = job.s3DestPath?.replace(/\/$/, '').split('/').pop();
        if (dsName) await ensureTableForDataset(dsName);
      }

      // Extract POI coordinates for spatial path
      const poiCoords = extractPoiCoords(syncedJobs);

      console.log(`[MEGA-NSE-POLL] Starting for mega-job ${id}, country=${country}, ${syncedJobs.length} sub-jobs, ${poiCoords.length} POI coords`);

      const queryId = await startConsolidatedNseQuery(
        syncedJobs,
        undefined, // poiIds
        poiCoords.length > 0 ? poiCoords : undefined,
      );

      state = { phase: 'polling', queryId, country, megaJobId: id };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 10, message: 'Running Athena query for device origins...' },
      });
    }

    // ── Phase: polling ──────────────────────────────────────────────
    if (state.phase === 'polling' && state.queryId) {
      try {
        const status = await checkQueryStatus(state.queryId);

        if (status.state === 'RUNNING' || status.state === 'QUEUED') {
          const scannedGB = status.statistics?.dataScannedBytes
            ? (status.statistics.dataScannedBytes / 1e9).toFixed(1)
            : '0';
          const runtimeSec = status.statistics?.engineExecutionTimeMs
            ? Math.round(status.statistics.engineExecutionTimeMs / 1000)
            : 0;

          return NextResponse.json({
            phase: 'polling',
            progress: {
              step: 'polling',
              percent: 30,
              message: `Athena query running... ${scannedGB} GB scanned, ${runtimeSec}s`,
            },
          });
        }

        if (status.state === 'FAILED' || status.state === 'CANCELLED') {
          state = { ...state, phase: 'error', error: status.error || 'Query failed' };
          await putConfig(STATE_KEY(id), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

        // SUCCEEDED → move to geocoding
        state = { ...state, phase: 'geocoding' };
        await putConfig(STATE_KEY(id), state, { compact: true });

        return NextResponse.json({
          phase: 'geocoding',
          progress: { step: 'geocoding', percent: 55, message: 'Query complete, reverse geocoding origins...' },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'error', error: 'Query expired — please retry' };
          await putConfig(STATE_KEY(id), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        throw err;
      }
    }

    // ── Phase: geocoding + bracket matching ──────────────────────────
    if (state.phase === 'geocoding' && state.queryId) {
      // Download CSV from S3
      const csvKey = `athena-results/${state.queryId}.csv`;
      console.log(`[MEGA-NSE-POLL] Downloading results from S3: ${csvKey}`);
      const csvObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: csvKey }));
      const csvText = await csvObj.Body!.transformToString('utf-8');

      // Parse CSV: header is "ad_id","origin_lat","origin_lng"
      const lines = csvText.split('\n');
      console.log(`[MEGA-NSE-POLL] CSV: ${lines.length - 1} data lines`);

      // Pre-aggregate: group ad_ids by unique (lat,lng) coord
      const coordToAdIds = new Map<string, string[]>();
      let totalDevices = 0;
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        const parts = line.replace(/"/g, '').split(',');
        if (parts.length < 3) continue;
        const [adId, lat, lng] = parts;
        const coordKey = `${lat},${lng}`;
        let arr = coordToAdIds.get(coordKey);
        if (!arr) { arr = []; coordToAdIds.set(coordKey, arr); }
        arr.push(adId);
        totalDevices++;
      }

      console.log(`[MEGA-NSE-POLL] ${totalDevices} devices → ${coordToAdIds.size} unique coords`);

      // Load NSE data
      const nseData = await getConfig<NseRecord[]>(NSE_KEY(state.country));
      if (!nseData?.length) {
        state = { ...state, phase: 'error', error: 'NSE data disappeared' };
        await putConfig(STATE_KEY(id), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      // Geocode unique coords
      setCountryFilter([state.country]);
      const coordKeys = Array.from(coordToAdIds.keys());
      const uniquePoints = coordKeys.map(k => {
        const [lat, lng] = k.split(',').map(Number);
        return { lat, lng, deviceCount: 1 };
      });

      console.log(`[MEGA-NSE-POLL] Reverse geocoding ${uniquePoints.length} unique coords...`);
      const geocoded = await batchReverseGeocode(uniquePoints);

      // Build postal code → ad_ids mapping
      const cpToAdIds = new Map<string, string[]>();
      for (let i = 0; i < coordKeys.length; i++) {
        const geo = geocoded[i];
        if (geo.type === 'geojson_local') {
          const cp = geo.postcode?.replace(/^[A-Z]{2}[-\s]/, '') || '';
          if (cp) {
            const existing = cpToAdIds.get(cp) || [];
            const adIds = coordToAdIds.get(coordKeys[i])!;
            for (const aid of adIds) existing.push(aid);
            cpToAdIds.set(cp, existing);
          }
        }
      }

      setCountryFilter(null);

      // Build NSE bracket → postal codes lookup
      const bracketCPs: Map<string, Set<string>> = new Map();
      for (const b of NSE_BRACKETS) {
        bracketCPs.set(b.label, new Set(
          nseData.filter(r => r.nse >= b.min && r.nse <= b.max).map(r => r.postal_code)
        ));
      }

      // Compute per-bracket MAIDs and save CSVs
      const brackets: BracketResult[] = [];
      const timestamp = Date.now();

      for (const b of NSE_BRACKETS) {
        const cps = bracketCPs.get(b.label)!;
        const inBracket = nseData.filter(r => r.nse >= b.min && r.nse <= b.max);
        const population = inBracket.reduce((s, r) => s + r.population, 0);

        // Collect unique MAIDs for this bracket
        const maidSet = new Set<string>();
        for (const cp of cps) {
          const adIds = cpToAdIds.get(cp);
          if (adIds) {
            for (const aid of adIds) maidSet.add(aid);
          }
        }

        let downloadUrl: string | null = null;
        if (maidSet.size > 0) {
          const csvContent = 'ad_id\n' + Array.from(maidSet).join('\n');
          const fileName = `mega-${id}-maids-nse-${b.min}-${b.max}-${timestamp}.csv`;
          const key = `exports/${fileName}`;

          await s3Client.send(new PutObjectCommand({
            Bucket: BUCKET,
            Key: key,
            Body: csvContent,
            ContentType: 'text/csv',
          }));

          downloadUrl = `/api/mega-jobs/${id}/reports/download?type=nse&file=${encodeURIComponent(fileName)}`;
        }

        brackets.push({
          label: b.label,
          min: b.min,
          max: b.max,
          postalCodes: inBracket.length,
          population,
          maidCount: maidSet.size,
          downloadUrl,
        });

        console.log(`[MEGA-NSE-POLL] Bracket ${b.label}: ${maidSet.size} MAIDs from ${cps.size} CPs`);
      }

      const totalMaids = brackets.reduce((s, b) => s + b.maidCount, 0);
      console.log(`[MEGA-NSE-POLL] Done: ${totalMaids} total MAIDs across ${brackets.length} brackets`);

      state = { ...state, phase: 'done', brackets };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        brackets,
        totalMaids,
        progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
      });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[MEGA-NSE-POLL] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(id), {
        phase: 'error', error: error.message, country: '', megaJobId: id,
      });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
