import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig, s3Client, BUCKET } from '@/lib/s3-config';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import {
  computeAffinityReport,
  affinityReportToCsv,
  buildCategoryAffinityLabel,
  type BaselineZip,
} from '@/lib/affinity-builder';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { getJob } from '@/lib/jobs';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const STATE_KEY = (ds: string) => `category-affinity-state/dataset-${ds}`;

interface AffinityExportState {
  phase: 'querying' | 'polling' | 'processing' | 'done' | 'error';
  ctasTable?: string;
  country?: string;
  groupKey?: string;
  categories?: string[];
  matchMode?: 'OR' | 'AND';
  slug?: string;
  queryId?: string;
  error?: string;
  result?: {
    csvKey: string;
    slug: string;
    label: string;
    totalZips: number;
    totalDevicesWithZip: number;
  };
}

function buildAffinitySlug(
  groupKey: string | undefined,
  categories: string[] | undefined,
  matchMode?: 'OR' | 'AND',
): string {
  const base = (groupKey && groupKey !== 'custom' ? groupKey : (categories?.[0] || 'custom'))
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 32) || 'custom';
  const n = categories?.length ?? 0;
  const modeBit = matchMode === 'AND' && n >= 2 ? '_and' : '';
  const ts = Math.floor(Date.now() / 1000).toString(36);
  return `${base}${modeBit}_${n}cat_${ts}`;
}

const buildAffinityLabel = buildCategoryAffinityLabel;

/**
 * Build the affinity-export SQL for a single-dataset CTAS. Mirrors the
 * megajob version but joins one table instead of UNION-ALL-ing several.
 */
function buildCategoryAffinitySQL(ctasTable: string, table: string): string {
  return `
    WITH cat_maids AS (
      SELECT ad_id,
        AVG(dwell_minutes) as avg_dwell,
        COUNT(*) as cat_count
      FROM ${ctasTable}
      GROUP BY ad_id
    ),
    all_pings AS (
      SELECT
        ad_id,
        date,
        utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng,
        TRY(geo_fields['zipcode']) as native_zip,
        TRY(geo_fields['city']) as native_city
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND ad_id IN (SELECT ad_id FROM cat_maids)
    ),
    first_pings AS (
      SELECT
        ad_id, date,
        MIN_BY(lat, utc_timestamp) as origin_lat,
        MIN_BY(lng, utc_timestamp) as origin_lng,
        MIN_BY(native_zip, utc_timestamp) as native_zip,
        MIN_BY(native_city, utc_timestamp) as native_city
      FROM all_pings
      GROUP BY ad_id, date
    ),
    device_homes_agg AS (
      SELECT ad_id,
        ROUND(origin_lat, 1) as home_lat,
        ROUND(origin_lng, 1) as home_lng,
        ARBITRARY(native_zip) as native_zip,
        ARBITRARY(native_city) as native_city,
        COUNT(DISTINCT date) as days_at_loc
      FROM first_pings
      WHERE origin_lat IS NOT NULL
      GROUP BY ad_id, ROUND(origin_lat, 1), ROUND(origin_lng, 1)
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
    ),
    device_stats AS (
      SELECT
        dh.ad_id,
        dh.home_lat as origin_lat,
        dh.home_lng as origin_lng,
        dh.native_zip,
        dh.native_city,
        dh.days_at_loc as visit_days,
        cm.avg_dwell as avg_dwell,
        cm.cat_count as freq
      FROM device_homes dh
      INNER JOIN cat_maids cm ON dh.ad_id = cm.ad_id
    )
    SELECT
      origin_lat,
      origin_lng,
      MIN(native_zip) as native_zip,
      MIN(native_city) as native_city,
      COUNT(DISTINCT ad_id) as unique_devices,
      SUM(visit_days) as total_visit_days,
      AVG(avg_dwell) as avg_dwell_minutes,
      AVG(freq) as avg_frequency
    FROM device_stats
    GROUP BY origin_lat, origin_lng
    ORDER BY unique_devices DESC
    LIMIT 50000
  `;
}

/**
 * POST /api/datasets/[name]/export/category-affinity-poll
 *
 * Single-dataset analogue of the megajob category-affinity-poll endpoint.
 * Same multi-phase flow, same shared affinity builder, same CSV shape —
 * only the SQL JOINs a single Athena table instead of a UNION ALL.
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
    let body: any = {};
    try { body = await request.json(); } catch {}
    const isNewRequest = !!body.ctasTable;

    let state = await getConfig<AffinityExportState>(STATE_KEY(datasetName));

    if (state?.phase === 'done' && !isNewRequest && state.result) {
      return NextResponse.json({
        phase: 'done',
        result: state.result,
        progress: { step: 'done', percent: 100, message: 'Affinity CSV ready' },
      });
    }
    if (state?.phase === 'error' || isNewRequest) state = null;

    if (!state) {
      const ctasTable: string = body.ctasTable;
      if (!ctasTable) {
        return NextResponse.json({ error: 'ctasTable required' }, { status: 400 });
      }
      // Resolve country from the job if not in body
      let country = (body.country || '').toUpperCase();
      if (!country) {
        try {
          const job = await getJob(datasetName);
          country = ((job as any)?.country || '').toUpperCase();
        } catch {}
      }
      const groupKey: string | undefined = typeof body.groupKey === 'string' ? body.groupKey : undefined;
      const categories: string[] | undefined = Array.isArray(body.categories) ? body.categories : undefined;
      const matchMode: 'OR' | 'AND' = body.matchMode === 'AND' ? 'AND' : 'OR';
      const slug = buildAffinitySlug(groupKey, categories, matchMode);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);
      const sql = buildCategoryAffinitySQL(ctasTable, table);
      const queryId = await startQueryAsync(sql);
      console.log(`[DS-CATEGORY-AFFINITY] Started queryId=${queryId} for dataset=${datasetName}, slug=${slug}, ctas=${ctasTable}, matchMode=${matchMode}`);

      state = { phase: 'polling', ctasTable, country, groupKey, categories, matchMode, slug, queryId };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 15, message: 'Computing per-zip affinity from category MAIDs…' },
      });
    }

    if (state.phase === 'polling' && state.queryId) {
      const status = await checkQueryStatus(state.queryId);
      if (status.state === 'FAILED' || status.state === 'CANCELLED') {
        state = { ...state, phase: 'error', error: status.error || 'Query failed' };
        await putConfig(STATE_KEY(datasetName), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }
      if (status.state !== 'SUCCEEDED') {
        return NextResponse.json({
          phase: 'polling',
          progress: { step: 'querying', percent: 40, message: `Athena: ${status.state}…` },
        });
      }
      state = { ...state, phase: 'processing' };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });
    }

    if (state.phase === 'processing' && state.queryId && state.ctasTable) {
      const obj = await s3Client.send(new GetObjectCommand({
        Bucket: BUCKET,
        Key: `athena-results/${state.queryId}.csv`,
      }));
      const csvText = await obj.Body!.transformToString('utf-8');
      const lines = csvText.split('\n').filter((l) => l.length > 0);

      const parseCsvLine = (line: string): string[] => {
        const cells: string[] = [];
        let cur = '';
        let inQuote = false;
        for (let i = 0; i < line.length; i++) {
          const c = line[i];
          if (inQuote) {
            if (c === '"') {
              if (line[i + 1] === '"') { cur += '"'; i++; }
              else { inQuote = false; }
            } else { cur += c; }
          } else {
            if (c === ',') { cells.push(cur); cur = ''; }
            else if (c === '"') { inQuote = true; }
            else { cur += c; }
          }
        }
        cells.push(cur);
        return cells;
      };
      const header = parseCsvLine(lines[0]);
      const rows: any[] = [];
      for (let i = 1; i < lines.length; i++) {
        const cells = parseCsvLine(lines[i]);
        const rec: Record<string, string> = {};
        for (let j = 0; j < header.length; j++) rec[header[j]] = cells[j] ?? '';
        rows.push(rec);
      }
      console.log(`[DS-CATEGORY-AFFINITY] Parsed ${rows.length} rows from result CSV`);

      // Build coordToZip: native_zip first, then reverse-geocode the rest
      // (same pattern as the megajob route — without this, BASIC schema or
      // sparse FULL data collapses to UNKNOWN).
      const coordToZip = new Map<string, { zipCode: string; city: string; country: string }>();
      const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();
      for (const row of rows) {
        const lat = parseFloat(row.origin_lat);
        const lng = parseFloat(row.origin_lng);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
        const key = `${lat},${lng}`;
        const nativeZip = String(row.native_zip || '').trim();
        if (nativeZip) {
          if (!coordToZip.has(key)) {
            coordToZip.set(key, {
              zipCode: nativeZip,
              city: String(row.native_city || '').trim() || 'UNKNOWN',
              country: state.country || 'UNKNOWN',
            });
          }
        } else if (!coordsToGeocode.has(key)) {
          coordsToGeocode.set(key, {
            lat, lng,
            deviceCount: parseInt(row.unique_devices, 10) || 1,
          });
        }
      }
      console.log(`[DS-CATEGORY-AFFINITY] native_zip resolved=${coordToZip.size}, pending=${coordsToGeocode.size}`);

      if (coordsToGeocode.size > 0) {
        try {
          if (state.country) setCountryFilter([state.country]);
          const roundedMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
          for (const p of coordsToGeocode.values()) {
            const rl = Math.round(p.lat * 10) / 10;
            const rn = Math.round(p.lng * 10) / 10;
            const rk = `${rl},${rn}`;
            const ex = roundedMap.get(rk);
            if (ex) ex.deviceCount += p.deviceCount;
            else roundedMap.set(rk, { lat: rl, lng: rn, deviceCount: p.deviceCount });
          }
          const geocoded = await batchReverseGeocode(Array.from(roundedMap.values()));
          const rKeys = Array.from(roundedMap.keys());
          let matched = 0;
          for (const [key, p] of coordsToGeocode.entries()) {
            if (coordToZip.has(key)) continue;
            const roundKey = `${Math.round(p.lat * 10) / 10},${Math.round(p.lng * 10) / 10}`;
            const idx = rKeys.indexOf(roundKey);
            if (idx >= 0 && idx < geocoded.length) {
              const g = geocoded[idx];
              if (g.type === 'geojson_local' || g.type === 'nominatim_match') {
                coordToZip.set(key, { zipCode: g.postcode, city: g.city, country: g.country });
                matched++;
              }
            }
          }
          console.log(`[DS-CATEGORY-AFFINITY] reverse-geocoded ${matched}/${coordsToGeocode.size} coords (country=${state.country || 'auto'})`);
        } catch (e: any) {
          console.warn(`[DS-CATEGORY-AFFINITY] reverse-geocode failed: ${e?.message || e}`);
        } finally {
          setCountryFilter(null);
        }
      }

      // Load main dataset affinity as baseline so we score lift over
      // population density (not raw density). Falls back to legacy heat
      // mode if no main affinity has been generated yet.
      let baseline: BaselineZip[] | undefined;
      try {
        const main = await getConfig<any>(`dataset-reports/${datasetName}/affinity`);
        const baseRows: any[] = Array.isArray(main?.byZipCode) ? main.byZipCode : [];
        baseline = baseRows
          .filter((z) =>
            Number.isFinite(z?.lat) && Number.isFinite(z?.lng) &&
            (z?.uniqueDevices ?? 0) > 0,
          )
          .map((z) => ({
            zipCode: String(z.zipCode ?? z.postalCode ?? ''),
            lat: z.lat,
            lng: z.lng,
            uniqueDevices: z.uniqueDevices,
          }));
        if (baseline.length === 0) baseline = undefined;
        console.log(`[DS-CATEGORY-AFFINITY] baseline: ${baseline ? baseline.length + ' zips' : 'none — falling back to legacy heat mode'}`);
      } catch (e: any) {
        console.warn(`[DS-CATEGORY-AFFINITY] baseline load failed: ${e?.message || e}`);
      }

      const report = await computeAffinityReport(
        `dataset-${datasetName}-category-affinity`,
        rows,
        coordToZip,
        state.country,
        baseline,
      );
      const useful = report.byZipCode.filter((r) => r.uniqueDevices > 0 || r.affinityIndex > 0);
      const csvBody = affinityReportToCsv({ ...report, byZipCode: useful });

      const slug = state.slug || buildAffinitySlug(state.groupKey, state.categories, state.matchMode);
      const label = buildAffinityLabel(state.groupKey, state.categories, state.matchMode);
      const ts = Math.floor(Date.now() / 1000).toString(36);
      const csvKey = `athena-results/dataset-${datasetName}_category_affinity_${ts}.csv`;
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET,
        Key: csvKey,
        Body: csvBody,
        ContentType: 'text/csv',
      }));

      const totalDevicesWithZip = useful.filter((r) => r.uniqueDevices > 0).reduce((s, r) => s + r.uniqueDevices, 0);

      // Persist the report under config/dataset-reports/{name}/category-affinity/{slug}.json
      // so the dataset page can list + render it via the new select menu.
      await putConfig(`dataset-reports/${datasetName}/category-affinity/${slug}`, {
        slug,
        label,
        groupKey: state.groupKey || null,
        categories: state.categories || [],
        matchMode: state.matchMode || 'OR',
        country: state.country || null,
        generatedAt: new Date().toISOString(),
        totalZips: useful.length,
        totalDevicesWithZip,
        csvKey,
        byZipCode: useful,
      }, { compact: true });

      const result = { csvKey, slug, label, totalZips: useful.length, totalDevicesWithZip };
      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(datasetName), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        result,
        progress: { step: 'done', percent: 100, message: `Affinity CSV ready · ${useful.length} zips` },
      });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state' });
  } catch (error: any) {
    console.error(`[DS-CATEGORY-AFFINITY] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(datasetName), { phase: 'error', error: error.message } as any, { compact: true });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { name: datasetName } = await context.params;
  const state = await getConfig<AffinityExportState>(STATE_KEY(datasetName));
  if (!state?.result?.csvKey) {
    return NextResponse.json({ error: 'No affinity CSV available — run the export first' }, { status: 404 });
  }
  const obj = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: state.result.csvKey,
  }));
  const text = await obj.Body!.transformToString('utf-8');
  return new NextResponse(text, {
    headers: {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="${datasetName}-category-affinity.csv"`,
    },
  });
}
