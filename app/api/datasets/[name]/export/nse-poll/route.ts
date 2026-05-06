import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
  runQuery,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import { getAllJobsSummary } from '@/lib/jobs';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { PutObjectCommand, GetObjectCommand, CopyObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { registerContribution } from '@/lib/master-maids';

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
  { label: '0-19 (Low)', min: 0, max: 19, key: '0_19' },
  { label: '20-39', min: 20, max: 39, key: '20_39' },
  { label: '40-59 (Mid)', min: 40, max: 59, key: '40_59' },
  { label: '60-79', min: 60, max: 79, key: '60_79' },
  { label: '80-100 (High)', min: 80, max: 100, key: '80_100' },
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

type Phase = 'querying' | 'polling' | 'geocoding'
  | 'bracket_queries' | 'bracket_polling'
  | 'done' | 'error';

interface NseExportState {
  phase: Phase;
  queryId?: string;         // CTAS query for origins table
  coordQueryId?: string;    // Coord-only query (small, for geocoding)
  ctasTable?: string;       // Athena table name from CTAS
  country: string;
  minDwell: number;
  maxDwell: number;
  hourFrom: number;
  hourTo: number;
  dateRange?: { from: string; to: string };
  coordToPostalCode?: Record<string, string>;
  brackets?: BracketResult[];
  // Bracket split phase
  bracketLookupTable?: string;
  bracketQueryIds?: Record<string, string>;  // bracket key → Athena query ID
  error?: string;
}

/**
 * POST /api/datasets/[name]/export/nse-poll
 *
 * Multi-phase polling endpoint for NSE MAID analysis + export.
 * All heavy data work happens in Athena — Node.js never touches large CSVs.
 *
 * Phases:
 * 1. querying: start CTAS (origins Parquet table) + coord query (small, for geocoding)
 * 2. polling: wait for coord query
 * 3. geocoding: reverse geocode coords, compute bracket counts, save bracket lookup CSV
 * 4. bracket_queries: wait for CTAS, create bracket lookup table, start 5 per-bracket queries
 * 5. bracket_polling: poll bracket queries, copy results to exports, register contributions
 * 6. done: return results
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
      const minDwell: number = parseInt(body.minDwell, 10) || 0;
      const maxDwell: number = parseInt(body.maxDwell, 10) || 0;
      const hourFrom: number = parseInt(body.hourFrom, 10) || 0;
      const hourTo: number = body.hourTo != null ? parseInt(body.hourTo, 10) : 23;

      if (!country) {
        return NextResponse.json({ error: 'country required' }, { status: 400 });
      }

      const nseData = await getConfig<NseRecord[]>(NSE_KEY(country));
      if (!nseData?.length) {
        return NextResponse.json({ error: `No NSE data for ${country}. Upload CSV first.` }, { status: 404 });
      }

      const filterLabel = [
        minDwell > 0 || maxDwell > 0 ? `dwell=${minDwell}-${maxDwell || '∞'}` : '',
        hourFrom > 0 || hourTo < 23 ? `hours=${hourFrom}-${hourTo}` : '',
      ].filter(Boolean).join(', ');
      console.log(`[NSE-POLL] Starting for ${datasetName}, country=${country}, ${filterLabel || 'no filters'}, ${nseData.length} NSE CPs`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);
      const cc = country.toUpperCase();

      // Build dwell HAVING clause (min and/or max)
      const dwellParts: string[] = [];
      if (minDwell > 0) dwellParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
      if (maxDwell > 0) dwellParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
      const dwellHaving = dwellParts.length > 0 ? `HAVING ${dwellParts.join(' AND ')}` : '';

      // Build hour filter
      let hourFilter = '';
      if (hourFrom > 0 || hourTo < 23) {
        if (hourFrom <= hourTo) {
          hourFilter = `AND HOUR(utc_timestamp) >= ${hourFrom} AND HOUR(utc_timestamp) <= ${hourTo}`;
        } else {
          // Cross-midnight (e.g., 22h to 6h)
          hourFilter = `AND (HOUR(utc_timestamp) >= ${hourFrom} OR HOUR(utc_timestamp) <= ${hourTo})`;
        }
      }

      // FULL-schema bypass: TRY(geo_fields['zipcode']) is NULL on BASIC, populated on FULL.
      // When populated, we skip the Node-side reverse-geocoding for that coord entirely.
      const originsCTE = `
        WITH poi_visitors AS (
          SELECT ad_id, date
          FROM ${table}
          CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
          WHERE poi_id IS NOT NULL AND poi_id != ''
            AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
            ${hourFilter}
          GROUP BY ad_id, date
          ${dwellHaving}
        ),
        origins AS (
          SELECT
            pv.ad_id,
            ROUND(MIN_BY(TRY_CAST(t.latitude AS DOUBLE), t.utc_timestamp), 1) as origin_lat,
            ROUND(MIN_BY(TRY_CAST(t.longitude AS DOUBLE), t.utc_timestamp), 1) as origin_lng,
            MIN_BY(TRY(t.geo_fields['zipcode']), t.utc_timestamp) as native_zip
          FROM poi_visitors pv
          INNER JOIN ${table} t ON pv.ad_id = t.ad_id AND pv.date = t.date
          WHERE TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
            AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
          GROUP BY pv.ad_id
        )
      `;

      // CTAS: create Parquet table with origins (Athena-native, no Node.js streaming)
      const safeDs = datasetName.replace(/[^a-z0-9]/gi, '_').toLowerCase();
      const ctasTable = `nse_origins_${safeDs}_${Date.now()}`;
      const ctasSql = `
        CREATE TABLE ${ctasTable}
        WITH (format='PARQUET', parquet_compression='SNAPPY',
              external_location='s3://${BUCKET}/athena-temp/${ctasTable}/')
        AS ${originsCTE}
        SELECT DISTINCT ad_id, origin_lat, origin_lng FROM origins WHERE origin_lat IS NOT NULL
      `;

      // Coord query: small (~37K rows) for geocoding. Column order is kept
      // backwards-compatible (lat,lng,count first); native_zip is the optional
      // 4th column and is consumed by the geocoding phase when present.
      const coordSql = `${originsCTE} SELECT origin_lat, origin_lng, COUNT(*) as device_count, MIN(native_zip) as native_zip FROM origins WHERE origin_lat IS NOT NULL GROUP BY origin_lat, origin_lng`;

      const [queryId, coordQueryId] = await Promise.all([
        startQueryAsync(ctasSql),
        startQueryAsync(coordSql),
      ]);
      console.log(`[NSE-POLL] Queries started: CTAS=${queryId} (${ctasTable}), coords=${coordQueryId}`);

      let dateRange = body.dateRange || { from: 'unknown', to: 'unknown' };
      if (dateRange.from === 'unknown') {
        try {
          const jobs = await getAllJobsSummary();
          const job = jobs.find(j => j.s3DestPath?.includes(datasetName));
          if (job?.actualDateRange) dateRange = { from: job.actualDateRange.from, to: job.actualDateRange.to };
          else if (job?.dateRange) dateRange = { from: (job.dateRange as any).from, to: (job.dateRange as any).to };
        } catch {}
      }

      state = { phase: 'polling', queryId, coordQueryId, ctasTable, country, minDwell, maxDwell, hourFrom, hourTo, dateRange };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 10, message: 'Running Athena queries for device origins...' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    if (state.phase === 'polling' && (state.coordQueryId || state.queryId)) {
      try {
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
            progress: { step: 'polling', percent: 30, message: `Athena query running... ${scannedGB} GB scanned, ${runtimeSec}s` },
          });
        }

        if (status.state === 'FAILED' || status.state === 'CANCELLED') {
          state = { ...state, phase: 'error', error: status.error || 'Query failed' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }

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
    if (state.phase === 'geocoding' && (state.coordQueryId || state.queryId)) {
      const csvKey = `athena-results/${state.coordQueryId || state.queryId}.csv`;
      let csvObj;
      try {
        csvObj = await s3Client.send(new GetObjectCommand({ Bucket: BUCKET, Key: csvKey }));
      } catch {
        state = { ...state, phase: 'error', error: 'Query results expired. Please retry.' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      const csvText = await csvObj.Body!.transformToString('utf-8');
      const lines = csvText.split('\n');
      const coordDeviceCounts = new Map<string, number>();
      const coordNativeZip = new Map<string, string>(); // FULL-schema bypass
      let totalDevices = 0;

      for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        const parts = line.replace(/"/g, '').split(',');
        if (parts.length < 2) continue;
        let lat: string, lng: string, count: number;
        let nativeZip = '';
        if (parts.length >= 3 && state.coordQueryId) {
          [lat, lng] = parts;
          count = parseInt(parts[2]) || 1;
          if (parts.length >= 4) nativeZip = (parts[3] || '').trim();
        } else if (parts.length >= 3) {
          lat = parts[1]; lng = parts[2]; count = 1;
        } else continue;
        const coordKey = `${lat},${lng}`;
        coordDeviceCounts.set(coordKey, (coordDeviceCounts.get(coordKey) || 0) + count);
        if (nativeZip && !coordNativeZip.has(coordKey)) {
          coordNativeZip.set(coordKey, nativeZip);
        }
        totalDevices += count;
      }

      console.log(`[NSE-POLL] ${totalDevices} devices → ${coordDeviceCounts.size} unique coords (${coordNativeZip.size} have native_zip)`);

      const nseData = await getConfig<NseRecord[]>(NSE_KEY(state.country));
      if (!nseData?.length) {
        state = { ...state, phase: 'error', error: 'NSE data disappeared' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }

      // Build coord → postal code, starting with FULL-schema native_zip rows.
      // Strip optional country prefix to match the NSE CSV format ("28001" not "ES-28001").
      const coordToPostalCode: Record<string, string> = {};
      for (const [coordKey, nz] of coordNativeZip.entries()) {
        const cp = nz.replace(/^[A-Z]{2}[-\s]/, '').trim();
        if (cp) coordToPostalCode[coordKey] = cp;
      }

      // Geocode the remaining coords (BASIC schema or FULL coords without zipcode)
      const coordKeysToGeocode = Array.from(coordDeviceCounts.keys()).filter(k => !coordToPostalCode[k]);
      if (coordKeysToGeocode.length > 0) {
        setCountryFilter([state.country]);
        const uniquePoints = coordKeysToGeocode.map(k => {
          const [lat, lng] = k.split(',').map(Number);
          return { lat, lng, deviceCount: coordDeviceCounts.get(k) || 1 };
        });
        console.log(`[NSE-POLL] Reverse geocoding ${uniquePoints.length} non-native coords (skipped ${coordNativeZip.size} via FULL bypass)...`);
        const geocoded = await batchReverseGeocode(uniquePoints);
        for (let i = 0; i < coordKeysToGeocode.length; i++) {
          const geo = geocoded[i];
          if (geo.type === 'geojson_local') {
            const cp = geo.postcode?.replace(/^[A-Z]{2}[-\s]/, '') || '';
            if (cp) coordToPostalCode[coordKeysToGeocode[i]] = cp;
          }
        }
        setCountryFilter(null);
      } else {
        console.log(`[NSE-POLL] All ${coordNativeZip.size} coords resolved via FULL-schema bypass — skipping reverse geocoding`);
      }

      // Build postal code → device count
      const cpDeviceCounts = new Map<string, number>();
      for (const [coord, cp] of Object.entries(coordToPostalCode)) {
        const count = coordDeviceCounts.get(coord) || 0;
        cpDeviceCounts.set(cp, (cpDeviceCounts.get(cp) || 0) + count);
      }

      // Compute per-bracket stats
      const brackets: BracketResult[] = [];
      for (const b of NSE_BRACKETS) {
        const inBracket = nseData.filter(r => r.nse >= b.min && r.nse <= b.max);
        const population = inBracket.reduce((s, r) => s + r.population, 0);
        const bracketCPs = new Set(inBracket.map(r => r.postal_code));
        let maidCount = 0;
        for (const cp of bracketCPs) maidCount += cpDeviceCounts.get(cp) || 0;
        brackets.push({ label: b.label, min: b.min, max: b.max, postalCodes: inBracket.length, population, maidCount, downloadUrl: null });
        console.log(`[NSE-POLL] Bracket ${b.label}: ~${maidCount} devices from ${bracketCPs.size} CPs`);
      }

      // Build coord → bracket lookup CSV and save to S3 (for Athena JOIN)
      // Map: postal_code → bracket_key
      const cpToBracketKey = new Map<string, string>();
      for (const b of NSE_BRACKETS) {
        for (const r of nseData) {
          if (r.nse >= b.min && r.nse <= b.max) cpToBracketKey.set(r.postal_code, b.key);
        }
      }

      // Build: origin_lat, origin_lng, bracket_key (only matched coords)
      const lookupLines = ['origin_lat,origin_lng,bracket_key'];
      for (const [coord, cp] of Object.entries(coordToPostalCode)) {
        const bk = cpToBracketKey.get(cp);
        if (bk) {
          const [lat, lng] = coord.split(',');
          lookupLines.push(`${lat},${lng},${bk}`);
        }
      }

      const cc = state.country.toUpperCase();
      const lookupKey = `nse-temp/${datasetName}/bracket_lookup/lookup.csv`;
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET, Key: lookupKey, Body: lookupLines.join('\n'), ContentType: 'text/csv',
      }));
      console.log(`[NSE-POLL] Saved bracket lookup: ${lookupLines.length - 1} coord→bracket mappings to ${lookupKey}`);

      const totalMaids = brackets.reduce((s, b) => s + b.maidCount, 0);
      console.log(`[NSE-POLL] Geocoding done: ~${totalMaids} total devices across ${brackets.length} brackets`);

      // Don't save coordToPostalCode in state anymore (can be huge) — we saved it as CSV instead
      state = { ...state, phase: 'bracket_queries', brackets };
      delete state.coordToPostalCode;
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'bracket_queries',
        brackets,
        totalMaids,
        progress: { step: 'bracket_queries', percent: 70, message: `Preparing per-bracket MAID exports...` },
      });
    }

    // ── Phase: bracket_queries ───────────────────────────────────
    // Wait for CTAS to finish, then create lookup table + start 5 per-bracket queries
    if (state.phase === 'bracket_queries' && state.queryId) {
      // Check CTAS status
      try {
        const ctasStatus = await checkQueryStatus(state.queryId);

        if (ctasStatus.state === 'RUNNING' || ctasStatus.state === 'QUEUED') {
          const totalMaids = (state.brackets || []).reduce((s: number, b) => s + b.maidCount, 0);
          return NextResponse.json({
            phase: 'bracket_queries',
            brackets: state.brackets,
            totalMaids,
            progress: { step: 'bracket_queries', percent: 75, message: 'Waiting for origins table...' },
          });
        }

        if (ctasStatus.state === 'FAILED' || ctasStatus.state === 'CANCELLED') {
          // CTAS failed — return bracket counts without downloads
          console.warn(`[NSE-POLL] CTAS failed: ${ctasStatus.error}. Returning counts only.`);
          state = { ...state, phase: 'done' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          const totalMaids = (state.brackets || []).reduce((s: number, b) => s + b.maidCount, 0);
          return NextResponse.json({
            phase: 'done', brackets: state.brackets, totalMaids,
            progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed (downloads not available)` },
          });
        }

        // CTAS SUCCEEDED — create bracket lookup external table + start bracket queries
        const ctasTable = state.ctasTable!;
        const cc = state.country.toUpperCase();
        const lookupTable = `nse_bracket_lookup_${datasetName.replace(/[^a-z0-9]/gi, '_').toLowerCase()}_${Date.now()}`;
        const lookupPath = `s3://${BUCKET}/nse-temp/${datasetName}/bracket_lookup/`;

        // Create external table for the bracket lookup CSV
        const createLookupSql = `
          CREATE EXTERNAL TABLE IF NOT EXISTS ${lookupTable} (
            origin_lat DOUBLE, origin_lng DOUBLE, bracket_key STRING
          )
          ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
          WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"')
          STORED AS TEXTFILE
          LOCATION '${lookupPath}'
          TBLPROPERTIES ('skip.header.line.count' = '1')
        `;
        await runQuery(createLookupSql);
        console.log(`[NSE-POLL] Created bracket lookup table: ${lookupTable}`);

        // Start 5 per-bracket queries — output 5-column format for Master MAIDs consolidation
        // Result CSV can be directly copied to contributions/ without Node.js processing
        const bracketQueryIds: Record<string, string> = {};
        for (const b of NSE_BRACKETS) {
          const sql = `
            SELECT DISTINCT f.ad_id,
              'nse' as attr_type,
              '${b.label}' as attr_value,
              CAST(NULL AS VARCHAR) as dwell_minutes,
              CAST(NULL AS VARCHAR) as postal_code
            FROM ${ctasTable} f
            INNER JOIN ${lookupTable} b
              ON f.origin_lat = CAST(b.origin_lat AS DOUBLE)
              AND f.origin_lng = CAST(b.origin_lng AS DOUBLE)
            WHERE b.bracket_key = '${b.key}'
          `;
          const qid = await startQueryAsync(sql);
          bracketQueryIds[b.key] = qid;
          console.log(`[NSE-POLL] Bracket query ${b.key}: ${qid}`);
        }

        state = { ...state, phase: 'bracket_polling', bracketLookupTable: lookupTable, bracketQueryIds };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });

        const totalMaids = (state.brackets || []).reduce((s: number, b) => s + b.maidCount, 0);
        return NextResponse.json({
          phase: 'bracket_polling',
          brackets: state.brackets,
          totalMaids,
          progress: { step: 'bracket_polling', percent: 80, message: 'Running per-bracket queries...' },
        });
      } catch (err: any) {
        if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
          state = { ...state, phase: 'error', error: 'CTAS query expired — please retry' };
          await putConfig(STATE_KEY(datasetName), state, { compact: true });
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        throw err;
      }
    }

    // ── Phase: bracket_polling ────────────────────────────────────
    // Poll 5 bracket queries. When all done, copy results to exports + register.
    if (state.phase === 'bracket_polling' && state.bracketQueryIds) {
      const qids = state.bracketQueryIds;
      let allDone = true;
      let anyFailed = false;

      for (const b of NSE_BRACKETS) {
        const qid = qids[b.key];
        if (!qid) continue;
        try {
          const s = await checkQueryStatus(qid);
          if (s.state === 'RUNNING' || s.state === 'QUEUED') allDone = false;
          if (s.state === 'FAILED' || s.state === 'CANCELLED') {
            anyFailed = true;
            console.warn(`[NSE-POLL] Bracket ${b.key} query failed: ${s.error}`);
          }
        } catch {
          anyFailed = true;
        }
      }

      if (!allDone && !anyFailed) {
        const totalMaids = (state.brackets || []).reduce((s: number, b) => s + b.maidCount, 0);
        return NextResponse.json({
          phase: 'bracket_polling',
          brackets: state.brackets,
          totalMaids,
          progress: { step: 'bracket_polling', percent: 85, message: 'Per-bracket queries running...' },
        });
      }

      // All done (or some failed) — copy results via S3 server-side copy (NO memory loading)
      const cc = state.country.toUpperCase();
      const dr = state.dateRange || { from: 'unknown', to: 'unknown' };
      const timestamp = Date.now();
      const updatedBrackets = [...(state.brackets || [])];

      for (const b of NSE_BRACKETS) {
        const qid = qids[b.key];
        if (!qid) continue;

        try {
          const s = await checkQueryStatus(qid);
          if (s.state !== 'SUCCEEDED') continue;

          const srcKey = `athena-results/${qid}.csv`;
          const fileName = `${datasetName}-maids-nse-${b.min}-${b.max}-${timestamp}.csv`;
          const exportKey = `exports/${fileName}`;
          const contribFileName = `${datasetName}-nse-${b.min}-${b.max}-${timestamp}.csv`;
          const contribKey = `master-maids/${cc}/contributions/${contribFileName}`;

          // Server-side copy to both exports (download) and contributions (consolidation)
          // The Athena result already has 5-column format — no Node.js processing needed
          await Promise.all([
            s3Client.send(new CopyObjectCommand({
              Bucket: BUCKET, CopySource: `${BUCKET}/${srcKey}`, Key: exportKey,
            })),
            s3Client.send(new CopyObjectCommand({
              Bucket: BUCKET, CopySource: `${BUCKET}/${srcKey}`, Key: contribKey,
            })),
          ]);

          // Use geocoding estimate for maidCount (accurate, avoids reading large CSV)
          const maidCount = (state.brackets || []).find(x => x.label === b.label)?.maidCount || 0;
          const downloadUrl = `/api/datasets/${datasetName}/export/download?file=${encodeURIComponent(fileName)}`;

          try {
            await registerContribution(cc, datasetName, 'nse_bracket', b.label, contribKey, dr, maidCount);
            console.log(`[NSE-POLL] Bracket ${b.label}: ~${maidCount.toLocaleString()} MAIDs → ${exportKey} + registered`);
          } catch (e: any) {
            console.warn(`[NSE-POLL] Failed to register bracket ${b.label}: ${e.message}`);
          }

          const idx = updatedBrackets.findIndex(x => x.label === b.label);
          if (idx >= 0) {
            updatedBrackets[idx] = { ...updatedBrackets[idx], maidCount, downloadUrl };
          }
        } catch (e: any) {
          console.warn(`[NSE-POLL] Failed to process bracket ${b.key}: ${e.message}`);
        }
      }

      // Cleanup: drop lookup table (best-effort)
      if (state.bracketLookupTable) {
        try { await runQuery(`DROP TABLE IF EXISTS ${state.bracketLookupTable}`); } catch {}
      }

      const totalMaids = updatedBrackets.reduce((s, b) => s + b.maidCount, 0);
      state = { ...state, phase: 'done', brackets: updatedBrackets };
      delete state.bracketQueryIds;
      delete state.bracketLookupTable;
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        brackets: updatedBrackets,
        totalMaids,
        progress: { step: 'done', percent: 100, message: `${totalMaids.toLocaleString()} MAIDs analyzed` },
      });
    }

    // Handle legacy 'saving' phase from old state — reset to bracket_queries
    if ((state.phase as string) === 'saving' && state.queryId) {
      // Old state had a regular query (not CTAS) — can't reuse. Reset.
      console.warn(`[NSE-POLL] Legacy 'saving' state detected — resetting to error for retry`);
      state = { ...state, phase: 'error', error: 'State expired after code update. Please retry.' };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });
      return NextResponse.json({ phase: 'error', error: state.error });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state — please retry' });

  } catch (error: any) {
    console.error(`[NSE-POLL] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(datasetName), {
        phase: 'error', error: error.message, country: '', minDwell: 0,
      } as NseExportState);
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
