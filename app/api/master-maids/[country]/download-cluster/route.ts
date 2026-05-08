import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getCountryContributions } from '@/lib/master-maids';
import { startQueryAndWait } from '@/lib/athena';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const maxDuration = 60; // hobby cap; clusters under ~3M MAIDs finish well under

/**
 * GET /api/master-maids/[country]/download-cluster?type=<attrType>&value=<attrValue>
 *
 * Streams a CSV with one MAID per line for a single (attributeType, attributeValue)
 * cluster. Used by the per-row Download button on the Master MAIDs page.
 *
 * Strategy:
 * - Find all contributions matching (country, attributeType, attributeValue).
 *   Most clusters have ONE contribution; cross-dataset / re-imports may have
 *   multiple — we UNION their ad_ids.
 * - Run an Athena query that SELECTs DISTINCT ad_id from the matched tables.
 *   Athena writes the result CSV to s3://bucket/athena-results/<queryId>.csv.
 * - Stream that CSV directly back to the user.
 *
 * Why not UNLOAD: UNLOAD writes to a directory of multiple part files; we'd
 * have to list-and-concatenate. A regular SELECT writes a SINGLE CSV which
 * is much simpler to stream. For million-row clusters Athena still produces
 * one file (~30-60MB).
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ country: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { country } = await context.params;
  const cc = country.toUpperCase();
  const type = request.nextUrl.searchParams.get('type') || '';
  const value = request.nextUrl.searchParams.get('value') || '';

  if (!type) {
    return NextResponse.json({ error: 'type query param required' }, { status: 400 });
  }

  try {
    const entry = await getCountryContributions(cc);
    if (!entry) {
      return NextResponse.json({ error: 'No master MAID data for this country' }, { status: 404 });
    }

    const matches = entry.contributions.filter(
      (c) => c.attributeType === type && (c.attributeValue || '') === value
    );

    if (matches.length === 0) {
      return NextResponse.json(
        { error: `No contributions found for ${type}=${value || '(empty)'}` },
        { status: 404 }
      );
    }

    // Build the SELECT — UNION across all matching contribution tables.
    // We DISTINCT to dedup if the same MAID appears in multiple contributions.
    const tables = matches.map((m) => m.athenaTable).filter(Boolean);
    if (tables.length === 0) {
      return NextResponse.json(
        { error: 'Matched contributions have no Athena tables (legacy CSV-only)' },
        { status: 422 }
      );
    }

    const sql =
      tables.length === 1
        ? `SELECT DISTINCT ad_id FROM ${tables[0]}`
        : `SELECT DISTINCT ad_id FROM (\n  ${tables
            .map((t) => `SELECT ad_id FROM ${t}`)
            .join('\n  UNION ALL\n  ')}\n)`;

    console.log(
      `[MASTER-MAIDS-DL-CLUSTER] ${cc} ${type}=${value || '(empty)'} → ${matches.length} contribution(s) (${tables.length} tables)`
    );

    // Run the query and wait. Athena writes the result CSV to:
    //   s3://bucket/athena-results/<queryId>.csv
    const { queryId, outputCsvKey } = await startQueryAndWait(sql);
    console.log(`[MASTER-MAIDS-DL-CLUSTER] queryId=${queryId} csv=${outputCsvKey}`);

    // Stream the CSV directly. Athena prepends a header row "ad_id" (quoted)
    // which the user can easily strip if they want pure MAIDs only.
    const obj = await s3Client.send(
      new GetObjectCommand({ Bucket: BUCKET, Key: outputCsvKey })
    );

    const stream = obj.Body as ReadableStream;
    const safeValue = (value || 'all').replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 60);
    const fileName = `master-maids-${cc}-${type}-${safeValue}-${new Date()
      .toISOString()
      .slice(0, 10)}.csv`;

    return new Response(stream as any, {
      headers: {
        'Content-Type': 'text/csv; charset=utf-8',
        'Content-Disposition': `attachment; filename="${fileName}"`,
        'Content-Length': String(obj.ContentLength || ''),
        'Cache-Control': 'no-store',
      },
    });
  } catch (error: any) {
    console.error(
      `[MASTER-MAIDS-DL-CLUSTER] error ${cc} ${type}=${value}:`,
      error.message
    );
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
