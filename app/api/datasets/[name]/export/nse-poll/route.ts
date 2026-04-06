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
import { createInterface } from 'readline';
import { Readable } from 'stream';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

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
  phase: 'querying' | 'polling' | 'geocoding' | 'saving' | 'done' | 'error';
  queryId?: string;         // Full query: ad_id, origin_lat, origin_lng
  coordQueryId?: string;    // Coord-only query: DISTINCT origin_lat, origin_lng (small)
  country: string;
  minDwell: number;
  dateRange?: { from: string; to: string };
  coordToPostalCode?: Record<string, string>; // Geocoded mapping (saved in state)
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

      // Two queries:
      // 1. Full: ad_id + origin coords (large, used later for per-bracket CSV export)
      // 2. Coords-only: DISTINCT coords with device count (small ~37K rows, for geocoding)
      const originsCTE = `
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
      `;

      const fullSql = `${originsCTE} SELECT DISTINCT ad_id, origin_lat, origin_lng FROM origins WHERE origin_lat IS NOT NULL`;
      const coordSql = `${originsCTE} SELECT origin_lat, origin_lng, COUNT(*) as device_count FROM origins WHERE origin_lat IS NOT NULL GROUP BY origin_lat, origin_lng`;

      const [queryId, coordQueryId] = await Promise.all([
        startQueryAsync(fullSql),
        startQueryAsync(coordSql),
      ]);
      console.log(`[NSE-POLL] Athena queries started: full=${queryId}, coords=${coordQueryId}`);

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
      state = { phase: 'polling', queryId, coordQueryId, country, minDwell, dateRange };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 10, message: 'Running Athena query for device origins...' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    // Wait for the COORD query (small) — the full query can finish in background
    if (state.phase === 'polling' && (state.coordQueryId || state.queryId)) {
      try {
        // Check coord query (or fall back to full query for old states)
        const checkId = state.coordQueryId || state.queryId!;
        const status = await checkQueryStatus(checkId);

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
    // Use the SMALL coord CSV (~37K rows) for geocoding, not the full CSV (10M+ rows)
    if (state.phase === 'geocoding' && (state.coordQueryId || state.queryId)) {
      const csvKey = `athena-results/${state.coordQueryId || state.queryId}.csv`;
      console.log(`[NSE-POLL] Reading coord CSV from S3: ${csvKey}`);
      let csvObj;
      try {
        csvObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: csvKey }));
      } catch (err: any) {
        console.error(`[NSE-POLL] CSV not found (query expired?): ${csvKey}`);
        state = { ...state, phase: 'error', error: 'Query results expired. Please retry.' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      // Parse coord CSV — small file: origin_lat, origin_lng, device_count (~37K rows)
      const csvText = await csvObj.Body!.transformToString('utf-8');
      const lines = csvText.split('\n');
      const coordDeviceCounts = new Map<string, number>();
      let totalDevices = 0;

      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        const parts = line.replace(/"/g, '').split(',');
        if (parts.length < 2) continue;
        // Format: origin_lat,origin_lng,device_count (coord query)
        // OR: ad_id,origin_lat,origin_lng (old full query — fallback)
        let lat: string, lng: string, count: number;
        if (parts.length >= 3 && state.coordQueryId) {
          [lat, lng] = parts;
          count = parseInt(parts[2]) || 1;
        } else if (parts.length >= 3) {
          // Old format: ad_id, lat, lng — count each row as 1
          lat = parts[1]; lng = parts[2]; count = 1;
        } else continue;
        const coordKey = `${lat},${lng}`;
        coordDeviceCounts.set(coordKey, (coordDeviceCounts.get(coordKey) || 0) + count);
        totalDevices += count;
      }

      console.log(`[NSE-POLL] ${totalDevices} devices → ${coordDeviceCounts.size} unique coords`);

      // Load NSE data
      const nseData = await getConfig<NseRecord[]>(NSE_KEY(state.country));
      if (!nseData?.length) {
        state = { ...state, phase: 'error', error: 'NSE data disappeared' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      // Geocode unique coords (~37K points — fast)
      setCountryFilter([state.country]);
      const coordKeys = Array.from(coordDeviceCounts.keys());
      const uniquePoints = coordKeys.map(k => {
        const [lat, lng] = k.split(',').map(Number);
        return { lat, lng, deviceCount: coordDeviceCounts.get(k) || 1 };
      });

      console.log(`[NSE-POLL] Reverse geocoding ${uniquePoints.length} unique coords...`);
      const geocoded = await batchReverseGeocode(uniquePoints);

      // Build coord → postal code mapping
      const coordToPostalCode: Record<string, string> = {};
      for (let i = 0; i < coordKeys.length; i++) {
        const geo = geocoded[i];
        if (geo.type === 'geojson_local') {
          const cp = geo.postcode?.replace(/^[A-Z]{2}[-\s]/, '') || '';
          if (cp) coordToPostalCode[coordKeys[i]] = cp;
        }
      }

      setCountryFilter(null);

      // Build postal code → device count (using coord device counts)
      const cpDeviceCounts = new Map<string, number>();
      for (const [coord, cp] of Object.entries(coordToPostalCode)) {
        const count = coordDeviceCounts.get(coord) || 0;
        cpDeviceCounts.set(cp, (cpDeviceCounts.get(cp) || 0) + count);
      }

      // Compute per-bracket stats using device counts (no individual MAIDs needed)
      const brackets: BracketResult[] = [];
      for (const b of NSE_BRACKETS) {
        const inBracket = nseData.filter(r => r.nse >= b.min && r.nse <= b.max);
        const population = inBracket.reduce((s, r) => s + r.population, 0);
        const bracketCPs = new Set(inBracket.map(r => r.postal_code));

        let maidCount = 0;
        for (const cp of bracketCPs) {
          maidCount += cpDeviceCounts.get(cp) || 0;
        }

        // Download URL — CSVs will be generated on-demand from the full query when ready
        // For now, mark as null (full query may still be running)
        brackets.push({
          label: b.label,
          min: b.min,
          max: b.max,
          postalCodes: inBracket.length,
          population,
          maidCount,
          downloadUrl: null,
        });

        console.log(`[NSE-POLL] Bracket ${b.label}: ~${maidCount} devices from ${bracketCPs.size} CPs`);
      }

      const totalMaids = brackets.reduce((s, b) => s + b.maidCount, 0);
      console.log(`[NSE-POLL] Done: ~${totalMaids} total devices across ${brackets.length} brackets`);

      // Save coord→postalCode mapping in state for the saving phase
      state = { ...state, phase: 'saving', brackets, coordToPostalCode };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'saving',
        brackets,
        totalMaids,
        progress: { step: 'saving', percent: 80, message: `Generating per-bracket CSV files...` },
      });
    }

    // ── Phase: saving ──────────────────────────────────────────────
    // Wait for the full query to complete, then generate per-bracket CSVs
    if (state.phase === 'saving' && state.queryId) {
      // Check if full query is ready
      try {
        const fullStatus = await checkQueryStatus(state.queryId);

        if (fullStatus.state === 'RUNNING' || fullStatus.state === 'QUEUED') {
          return NextResponse.json({
            phase: 'saving',
            brackets: state.brackets,
            totalMaids: (state.brackets || []).reduce((s: number, b: BracketResult) => s + b.maidCount, 0),
            progress: { step: 'saving', percent: 85, message: `Waiting for full query to complete...` },
          });
        }

        if (fullStatus.state === 'FAILED' || fullStatus.state === 'CANCELLED') {
          // Full query failed — still return bracket counts (no download CSVs)
          console.warn(`[NSE-POLL] Full query failed: ${fullStatus.error}. Returning counts only.`);
          state = { ...state, phase: 'done' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          const totalMaids = (state.brackets || []).reduce((s: number, b: BracketResult) => s + b.maidCount, 0);
          return NextResponse.json({
            phase: 'done', brackets: state.brackets, totalMaids,
            progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed (download not available for large datasets)` },
          });
        }

        // Full query succeeded — stream CSV and generate per-bracket files
        const fullCsvKey = `athena-results/${state.queryId}.csv`;
        console.log(`[NSE-POLL] Streaming full CSV for per-bracket export: ${fullCsvKey}`);

        let fullCsvObj;
        try {
          fullCsvObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: fullCsvKey }));
        } catch {
          // CSV expired — skip download generation
          state = { ...state, phase: 'done' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          const totalMaids = (state.brackets || []).reduce((s: number, b: BracketResult) => s + b.maidCount, 0);
          return NextResponse.json({
            phase: 'done', brackets: state.brackets, totalMaids,
            progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
          });
        }

        // Load NSE data for bracket assignment
        const nseData2 = await getConfig<NseRecord[]>(NSE_KEY(state.country));
        if (!nseData2?.length) {
          state = { ...state, phase: 'done' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          const totalMaids = (state.brackets || []).reduce((s: number, b: BracketResult) => s + b.maidCount, 0);
          return NextResponse.json({ phase: 'done', brackets: state.brackets, totalMaids });
        }

        // Build postal code → NSE bracket lookup
        const cpToBracket = new Map<string, string>();
        for (const b of NSE_BRACKETS) {
          for (const r of nseData2) {
            if (r.nse >= b.min && r.nse <= b.max) {
              cpToBracket.set(r.postal_code, b.label);
            }
          }
        }

        // Stream full CSV and assign each MAID to a bracket via coord→postalCode→bracket
        const bracketMaids: Record<string, string[]> = {};
        for (const b of NSE_BRACKETS) bracketMaids[`${b.min}-${b.max}`] = [];

        const savedCoordToCP = state.coordToPostalCode || {};
        const rl = createInterface({ input: fullCsvObj.Body as Readable, crlfDelay: Infinity });
        let isHeader = true;
        for await (const line of rl) {
          if (isHeader) { isHeader = false; continue; }
          const trimmed = line.trim();
          if (!trimmed) continue;
          const parts = trimmed.replace(/"/g, '').split(',');
          if (parts.length < 3) continue;
          const [adId, lat, lng] = parts;
          const cp = savedCoordToCP[`${lat},${lng}`];
          if (!cp) continue;
          const bracket = cpToBracket.get(cp);
          if (!bracket) continue;
          const b = NSE_BRACKETS.find(x => x.label === bracket);
          if (b) bracketMaids[`${b.min}-${b.max}`].push(adId);
        }

        // Save per-bracket CSVs
        const timestamp = Date.now();
        const updatedBrackets = [...(state.brackets || [])];
        const dr = state.dateRange || { from: 'unknown', to: 'unknown' };
        const cc = state.country.toUpperCase();

        for (let i = 0; i < NSE_BRACKETS.length; i++) {
          const b = NSE_BRACKETS[i];
          const maids = bracketMaids[`${b.min}-${b.max}`];
          if (maids.length > 0) {
            // 1. Export CSV (ad_id only) for download
            const downloadCsv = 'ad_id\n' + maids.join('\n');
            const fileName = `${datasetName}-maids-nse-${b.min}-${b.max}-${timestamp}.csv`;
            const exportKey = `exports/${fileName}`;

            // 2. Consolidation CSV (5-column schema) for Master MAIDs UNION ALL
            const contribLines = maids.map(adId => `${adId},nse,${b.label},,`);
            const contribCsv = 'ad_id,attr_type,attr_value,dwell_minutes,postal_code\n' + contribLines.join('\n');
            const contribFileName = `${datasetName}-nse-${b.min}-${b.max}-${timestamp}.csv`;
            const contribKey = `master-maids/${cc}/contributions/${contribFileName}`;

            // Save both in parallel
            await Promise.all([
              s3Client.send(new PutObjectCommand({ Bucket: BUCKET, Key: exportKey, Body: downloadCsv, ContentType: 'text/csv' })),
              s3Client.send(new PutObjectCommand({ Bucket: BUCKET, Key: contribKey, Body: contribCsv, ContentType: 'text/csv' })),
            ]);

            const downloadUrl = `/api/datasets/${datasetName}/export/download?file=${encodeURIComponent(fileName)}`;
            const idx = updatedBrackets.findIndex(x => x.label === b.label);
            if (idx >= 0) {
              updatedBrackets[idx] = { ...updatedBrackets[idx], maidCount: maids.length, downloadUrl };
            }
            // Register contribution with actual MAID count and correct path
            try {
              await registerContribution(cc, datasetName, 'nse_bracket', b.label, contribKey, dr, maids.length);
            } catch {}
          }
        }

        const totalMaids = updatedBrackets.reduce((s, b) => s + b.maidCount, 0);
        state = { ...state, phase: 'done', brackets: updatedBrackets };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });

        return NextResponse.json({
          phase: 'done', brackets: updatedBrackets, totalMaids,
          progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'done' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          const totalMaids = (state.brackets || []).reduce((s: number, b: BracketResult) => s + b.maidCount, 0);
          return NextResponse.json({ phase: 'done', brackets: state.brackets, totalMaids });
        }
        throw err;
      }
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
