import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig, s3Client, BUCKET } from '@/lib/s3-config';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import { computeAffinityReport, affinityReportToCsv } from '@/lib/affinity-builder';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const STATE_KEY = (id: string) => `category-affinity-state/mega-${id}`;

interface AffinityExportState {
  phase: 'querying' | 'polling' | 'processing' | 'done' | 'error';
  ctasTable?: string;
  country?: string;
  groupKey?: string;
  categories?: string[];
  slug?: string;
  queryId?: string;
  affinityQueryId?: string;
  error?: string;
  result?: {
    csvKey: string;
    /** Slug used to persist the report at
     *  config/mega-reports/{megaJobId}/category-affinity/{slug}.json — drives
     *  the dropdown menu on the Affinity Heatmap map. */
    slug: string;
    label: string;
    totalZips: number;
    totalDevicesWithZip: number;
  };
}

/** Build a filesystem-safe slug from a category group + count + timestamp.
 *  Used as the JSON file name under config/mega-reports/{id}/category-affinity/. */
function buildAffinitySlug(groupKey: string | undefined, categories: string[] | undefined): string {
  const base = (groupKey && groupKey !== 'custom' ? groupKey : (categories?.[0] || 'custom'))
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 32) || 'custom';
  const n = categories?.length ?? 0;
  const ts = Math.floor(Date.now() / 1000).toString(36);
  return `${base}_${n}cat_${ts}`;
}

function buildAffinityLabel(groupKey: string | undefined, categories: string[] | undefined): string {
  const head = groupKey && groupKey !== 'custom' ? groupKey : (categories?.[0] || 'Custom');
  const niceHead = head.replace(/[_-]+/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
  const n = categories?.length ?? 0;
  return n > 1 ? `${niceHead} (${n} categories)` : niceHead;
}

/**
 * Build the affinity-export SQL. Joins the category-MAIDs CTAS table
 * against the megajob's sub-job ping tables to derive a per-zip
 * aggregate row compatible with `computeAffinityReport`.
 *
 * Strategy (mirrors buildAffinitySQL from the dataset poll route):
 *   1. cat_maids:        per-device avg dwell + #categories from the CTAS.
 *   2. all_pings:        UNION ALL of sub-job pings filtered to ad_ids in cat_maids.
 *   3. first_pings:      first ping of each day (proxies "home" per day).
 *   4. device_homes:     dedup home location per device (mode by days_at_loc).
 *   5. device_stats:     join homes back to cat_maids → engagement.
 *   6. Final SELECT:     aggregate by (origin_lat, origin_lng) → affinity-ready rows.
 *
 * Column shape matches what computeAffinityReport expects:
 *   origin_lat, origin_lng, native_zip, native_city,
 *   unique_devices, total_visit_days, avg_dwell_minutes, avg_frequency
 */
function buildCategoryAffinitySQL(ctasTable: string, subTables: string[]): string {
  // Each sub-job's all_pings SELECT — utc_date and timestamp come from the
  // Veraset parquet schema. We join on ad_id IN cat_maids to keep the scan
  // tight (Athena pushes the filter into the parquet scan when possible).
  // Column is `date` (the partition key), NOT `utc_date`. utc_timestamp
  // is the precise ping time; date is the YYYY-MM-DD partition string.
  const subPingsUnion = subTables.map((t) => `
    SELECT
      ad_id,
      date,
      utc_timestamp,
      TRY_CAST(latitude AS DOUBLE) as lat,
      TRY_CAST(longitude AS DOUBLE) as lng,
      TRY(geo_fields['zipcode']) as native_zip,
      TRY(geo_fields['city']) as native_city
    FROM ${t}
    WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
      AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
      AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
      AND ad_id IN (SELECT ad_id FROM cat_maids)
  `).join(' UNION ALL ');

  return `
    WITH cat_maids AS (
      SELECT ad_id,
        AVG(dwell_minutes) as avg_dwell,
        COUNT(*) as cat_count
      FROM ${ctasTable}
      GROUP BY ad_id
    ),
    all_pings AS (
      ${subPingsUnion}
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
 * POST /api/mega-jobs/[id]/export/category-affinity-poll
 *
 * Multi-phase polling endpoint that turns the category-MAIDs CTAS into
 * a zip-level affinity CSV with the canonical 8-column format. First
 * call: body { ctasTable } kicks off the Athena query. Subsequent
 * calls: state-driven polling. Final response: { result.csvKey }.
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

    let body: any = {};
    try { body = await request.json(); } catch {}
    const isNewRequest = !!body.ctasTable;

    let state = await getConfig<AffinityExportState>(STATE_KEY(id));

    // If done and NOT new, return cached
    if (state?.phase === 'done' && !isNewRequest && state.result) {
      return NextResponse.json({
        phase: 'done',
        result: state.result,
        progress: { step: 'done', percent: 100, message: 'Affinity CSV ready' },
      });
    }

    if (state?.phase === 'error' || isNewRequest) state = null;

    // ── Phase: start ─────────────────────────────────────────────
    if (!state) {
      const ctasTable: string = body.ctasTable;
      if (!ctasTable) {
        return NextResponse.json({ error: 'ctasTable required' }, { status: 400 });
      }

      // Resolve sub-job tables from the megajob
      const subJobs = (
        await Promise.all(megaJob.subJobIds.map((jid) => getJob(jid)))
      ).filter((j): j is NonNullable<typeof j> => j !== null);
      const subDatasetNames = subJobs
        .filter((j) => j.status === 'SUCCESS' && j.syncedAt)
        .map((j) => j.s3DestPath?.replace(/\/$/, '').split('/').pop())
        .filter((n): n is string => !!n);

      if (subDatasetNames.length === 0) {
        return NextResponse.json({ error: 'No synced sub-jobs to query' }, { status: 400 });
      }

      const country = (body.country || megaJob.country || '').toUpperCase();
      const groupKey: string | undefined = typeof body.groupKey === 'string' ? body.groupKey : undefined;
      const categories: string[] | undefined = Array.isArray(body.categories) ? body.categories : undefined;
      const slug = buildAffinitySlug(groupKey, categories);

      await Promise.all(subDatasetNames.map((ds) => ensureTableForDataset(ds)));
      const subTables = subDatasetNames.map((ds) => getTableName(ds));
      const sql = buildCategoryAffinitySQL(ctasTable, subTables);
      const queryId = await startQueryAsync(sql);
      console.log(`[MEGA-CATEGORY-AFFINITY] Started queryId=${queryId} for megajob=${id}, slug=${slug}, ctas=${ctasTable}, sub-tables=${subTables.length}`);

      state = {
        phase: 'polling',
        ctasTable,
        country,
        groupKey,
        categories,
        slug,
        queryId,
      };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'query_started', percent: 15, message: 'Computing per-zip affinity from category MAIDs…' },
      });
    }

    // ── Phase: polling ───────────────────────────────────────────
    if (state.phase === 'polling' && state.queryId) {
      const status = await checkQueryStatus(state.queryId);
      if (status.state === 'FAILED' || status.state === 'CANCELLED') {
        state = { ...state, phase: 'error', error: status.error || 'Query failed' };
        await putConfig(STATE_KEY(id), state, { compact: true });
        return NextResponse.json({ phase: 'error', error: state.error });
      }
      if (status.state !== 'SUCCEEDED') {
        return NextResponse.json({
          phase: 'polling',
          progress: { step: 'querying', percent: 40, message: `Athena: ${status.state}…` },
        });
      }
      state = { ...state, phase: 'processing' };
      await putConfig(STATE_KEY(id), state, { compact: true });
      // Fall through to processing
    }

    // ── Phase: processing — read results, build affinity, save CSV ──
    if (state.phase === 'processing' && state.queryId && state.ctasTable) {
      // Stream the Athena CSV result directly from S3 (the queryId's
      // result file). No re-run cost.
      const { GetObjectCommand } = await import('@aws-sdk/client-s3');
      const obj = await s3Client.send(new GetObjectCommand({
        Bucket: BUCKET,
        Key: `athena-results/${state.queryId}.csv`,
      }));
      const csvText = await obj.Body!.transformToString('utf-8');
      const lines = csvText.split('\n').filter((l) => l.length > 0);
      const header = lines[0].split(',').map((c) => c.replace(/^"|"$/g, ''));
      const rows: any[] = [];
      for (let i = 1; i < lines.length; i++) {
        const cells = lines[i].match(/(?:"((?:[^"]|"")*)")|([^,]*)/g)?.map((c) =>
          c.replace(/^"|"$/g, '').replace(/""/g, '"')
        ) || [];
        const rec: Record<string, string> = {};
        for (let j = 0; j < header.length; j++) rec[header[j]] = cells[j] ?? '';
        rows.push(rec);
      }
      console.log(`[MEGA-CATEGORY-AFFINITY] Parsed ${rows.length} rows from result CSV`);

      // Build affinity using the shared lib (Gaussian heat field + log normalize).
      // coordToZip empty: rely on FULL-schema native_zip when present;
      // otherwise the zip falls back to 'UNKNOWN' and gets filtered out of
      // the heat field. (For unresolved zips a future enhancement could
      // batch reverse-geocode here, but for category exports the FULL
      // schema bypass is typically already populated.)
      const report = await computeAffinityReport(
        `mega-${id}-category-affinity`,
        rows,
        new Map(),
        state.country,
      );

      // Drop the long tail of zero-engagement rows to keep CSV tight.
      const useful = report.byZipCode.filter((r) => r.uniqueDevices > 0 || r.affinityIndex > 0);
      const csvText2 = affinityReportToCsv({ ...report, byZipCode: useful });

      const ts = Math.floor(Date.now() / 1000).toString(36);
      const csvKey = `athena-results/${id}_category_affinity_${ts}.csv`;
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET,
        Key: csvKey,
        Body: csvText2,
        ContentType: 'text/csv',
      }));

      const totalDevicesWithZip = useful
        .filter((r) => r.uniqueDevices > 0)
        .reduce((s, r) => s + r.uniqueDevices, 0);

      // Persist the report under config/mega-reports/{id}/category-affinity/{slug}.json
      // so the megajob page can list + render it via the new select menu on
      // the Affinity Heatmap card.
      const slug = state.slug || buildAffinitySlug(state.groupKey, state.categories);
      const label = buildAffinityLabel(state.groupKey, state.categories);
      const reportJson = {
        slug,
        label,
        groupKey: state.groupKey || null,
        categories: state.categories || [],
        country: state.country || null,
        generatedAt: new Date().toISOString(),
        totalZips: useful.length,
        totalDevicesWithZip,
        csvKey,
        byZipCode: useful,
      };
      await putConfig(`mega-reports/${id}/category-affinity/${slug}`, reportJson, { compact: true });

      const result = {
        csvKey,
        slug,
        label,
        totalZips: useful.length,
        totalDevicesWithZip,
      };
      state = { ...state, phase: 'done', result };
      await putConfig(STATE_KEY(id), state, { compact: true });

      return NextResponse.json({
        phase: 'done',
        result,
        progress: { step: 'done', percent: 100, message: `Affinity CSV ready · ${useful.length} zips` },
      });
    }

    return NextResponse.json({ phase: 'error', error: 'Unknown state' });
  } catch (error: any) {
    console.error(`[MEGA-CATEGORY-AFFINITY] Error:`, error.message);
    try {
      await putConfig(STATE_KEY(id), { phase: 'error', error: error.message } as any, { compact: true });
    } catch {}
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

/**
 * GET /api/mega-jobs/[id]/export/category-affinity-poll?file=...
 *
 * Stream the generated CSV from S3. Allows the modal to use a normal
 * <a download> link without exposing raw S3 URLs.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { id } = await context.params;
  const state = await getConfig<AffinityExportState>(STATE_KEY(id));
  if (!state?.result?.csvKey) {
    return NextResponse.json({ error: 'No affinity CSV available — run the export first' }, { status: 404 });
  }
  const { GetObjectCommand } = await import('@aws-sdk/client-s3');
  const obj = await s3Client.send(new GetObjectCommand({
    Bucket: BUCKET,
    Key: state.result.csvKey,
  }));
  const text = await obj.Body!.transformToString('utf-8');
  return new NextResponse(text, {
    headers: {
      'Content-Type': 'text/csv; charset=utf-8',
      'Content-Disposition': `attachment; filename="mega-${id}-category-affinity.csv"`,
    },
  });
}
