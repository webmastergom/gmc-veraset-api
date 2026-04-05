import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { getAllJobsSummary } from '@/lib/jobs';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { registerContribution } from '@/lib/master-maids';

export const dynamic = 'force-dynamic';
export const maxDuration = 120;

const STATE_KEY = (ds: string) => `nse-export-state/${ds}`;
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
  minDwell: number;
  dateRange?: { from: string; to: string };
  error?: string;
  brackets?: BracketResult[];
}

/**
 * POST /api/datasets/[name]/export/nse-poll
 *
 * Multi-phase polling endpoint for NSE MAID analysis + export.
 * First call body: { country: string, minDwell: number }
 * Subsequent calls: no body needed (reads state from S3)
 *
 * Phases:
 * 1. querying: load NSE data, start Athena query for device origins
 * 2. polling: wait for Athena query
 * 3. geocoding: fetch results, geocode, match to NSE brackets, save per-bracket CSVs
 * 4. done: return bracket counts + download URLs
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
    // Parse body first to detect if this is a new request (has country)
    let body: any;
    try { body = await request.json(); } catch { body = {}; }
    const isNewRequest = !!body.country;

    let state = await getConfig<NseExportState>(STATE_KEY(datasetName));

    // If done and NOT a new request, return cached results
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

    // ── Phase: start ─────────────────────────────────────────────
    if (!state) {

      const country: string = body.country || '';
      const minDwell: number = body.minDwell || 0;

      if (!country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      // Verify NSE data exists
      const nseData = await getConfig<NseRecord[]>(NSE_KEY(country));
      if (!nseData?.length) {
        return NextResponse.json({ error: `No NSE data for ${country}. Upload CSV first.` }, { status: 404 });
      }

      console.log(`[NSE-POLL] Starting for ${datasetName}, country=${country}, minDwell=${minDwell}, ${nseData.length} NSE CPs`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      const dwellHaving = minDwell > 0
        ? `HAVING DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`
        : '';

      // Round origins to 1 decimal (~11km) — sufficient for postal code matching
      // and dramatically reduces unique coords for geocoding (2.6M → ~37K)
      const sql = `
        WITH poi_visitors AS (
          SELECT ad_id, date
          FROM ${table}
          CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
          WHERE poi_id IS NOT NULL AND poi_id != ''
            AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
          GROUP BY ad_id, date
          ${dwellHaving}
        ),
        origins AS (
          SELECT
            pv.ad_id,
            ROUND(MIN_BY(TRY_CAST(t.latitude AS DOUBLE), t.utc_timestamp), 1) as origin_lat,
            ROUND(MIN_BY(TRY_CAST(t.longitude AS DOUBLE), t.utc_timestamp), 1) as origin_lng
          FROM poi_visitors pv
          INNER JOIN ${table} t ON pv.ad_id = t.ad_id AND pv.date = t.date
          WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
            AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
          GROUP BY pv.ad_id
        )
        SELECT DISTINCT ad_id, origin_lat, origin_lng
        FROM origins
        WHERE origin_lat IS NOT NULL
      `;

      const queryId = await startQueryAsync(sql);
      console.log(`[NSE-POLL] Athena query started: ${queryId}`);

      // Resolve dateRange from job metadata (backend-side, no frontend dependency)
      let dateRange = body.dateRange || { from: 'unknown', to: 'unknown' };
      if (dateRange.from === 'unknown') {
        try {
          const jobs = await getAllJobsSummary();
          const job = jobs.find(j => j.s3DestPath?.includes(datasetName));
          if (job?.actualDateRange) dateRange = { from: job.actualDateRange.from, to: job.actualDateRange.to };
          else if (job?.dateRange) dateRange = { from: (job.dateRange as any).from, to: (job.dateRange as any).to };
        } catch {}
      }
      state = { phase: 'polling', queryId, country, minDwell, dateRange };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 10, message: 'Running Athena query for device origins...' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
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
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

        // SUCCEEDED → move to geocoding
        state = { ...state, phase: 'geocoding' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });

        return NextResponse.json({
          phase: 'geocoding',
          progress: { step: 'geocoding', percent: 55, message: 'Query complete, reverse geocoding origins...' },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'error', error: 'Query expired — please retry' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        throw err;
      }
    }

    // ── Phase: geocoding ─────────────────────────────────────────
    if (state.phase === 'geocoding' && state.queryId) {
      // Download CSV directly from S3 (much faster than paging Athena API for large results)
      const csvKey = `athena-results/${state.queryId}.csv`;
      console.log(`[NSE-POLL] Downloading results from S3: ${csvKey}`);
      const csvObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: csvKey }));
      const csvText = await csvObj.Body!.transformToString('utf-8');

      // Parse CSV: header is "ad_id","origin_lat","origin_lng"
      const lines = csvText.split('\n');
      const header = lines[0];
      console.log(`[NSE-POLL] CSV header: ${header}, ${lines.length - 1} data lines`);

      // Pre-aggregate: group ad_ids by unique (lat,lng) coord
      const coordToAdIds = new Map<string, string[]>();
      let totalDevices = 0;
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        // CSV values may be quoted: "ad_id","lat","lng"
        const parts = line.replace(/"/g, '').split(',');
        if (parts.length < 3) continue;
        const [adId, lat, lng] = parts;
        const coordKey = `${lat},${lng}`;
        let arr = coordToAdIds.get(coordKey);
        if (!arr) { arr = []; coordToAdIds.set(coordKey, arr); }
        arr.push(adId);
        totalDevices++;
      }

      console.log(`[NSE-POLL] ${totalDevices} devices → ${coordToAdIds.size} unique coords`);

      // Load NSE data
      const nseData = await getConfig<NseRecord[]>(NSE_KEY(state.country));
      if (!nseData?.length) {
        state = { ...state, phase: 'error', error: 'NSE data disappeared' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      // Geocode only unique coords (37K instead of 2.6M)
      setCountryFilter([state.country]);
      const coordKeys = Array.from(coordToAdIds.keys());
      const uniquePoints = coordKeys.map(k => {
        const [lat, lng] = k.split(',').map(Number);
        return { lat, lng, deviceCount: 1 };
      });

      console.log(`[NSE-POLL] Reverse geocoding ${uniquePoints.length} unique coords...`);
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
            for (const id of adIds) existing.push(id);
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
            for (const id of adIds) maidSet.add(id);
          }
        }

        let downloadUrl: string | null = null;
        if (maidSet.size > 0) {
          const csvContent = 'ad_id\n' + Array.from(maidSet).join('\n');
          const fileName = `${datasetName}-maids-nse-${b.min}-${b.max}-${timestamp}.csv`;
          const key = `exports/${fileName}`;

          await s3Client.send(new PutObjectCommand({
            Bucket: BUCKET,
            Key: key,
            Body: csvContent,
            ContentType: 'text/csv',
          }));

          downloadUrl = `/api/datasets/${datasetName}/export/download?file=${encodeURIComponent(fileName)}`;
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

        console.log(`[NSE-POLL] Bracket ${b.label}: ${maidSet.size} MAIDs from ${cps.size} CPs`);
      }

      const totalMaids = brackets.reduce((s, b) => s + b.maidCount, 0);
      console.log(`[NSE-POLL] Done: ${totalMaids} total MAIDs across ${brackets.length} brackets`);

      // Register per-bracket export CSVs as master MAID contributions
      // (reuse the existing bracket CSVs that were already written above)
      const dr = state.dateRange || { from: 'unknown', to: 'unknown' };
      try {
        const cc = state.country.toUpperCase();
        for (const b of brackets) {
          if (b.maidCount > 0 && b.downloadUrl) {
            const bracketFile = `exports/${datasetName}-maids-nse-${b.min}-${b.max}-${timestamp}.csv`;
            await registerContribution(cc, datasetName, 'nse_bracket', b.label, bracketFile, dr);
          }
        }
        console.log(`[NSE-POLL] Registered ${brackets.filter(b => b.maidCount > 0).length} bracket contributions`);
      } catch (e: any) {
        console.warn(`[NSE-POLL] Failed to register contributions: ${e.message}`);
      }

      state = { ...state, phase: 'done', brackets };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        brackets,
        totalMaids,
        progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
      });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[NSE-POLL] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(datasetName), {
        phase: 'error', error: error.message, country: '', minDwell: 0,
      });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
