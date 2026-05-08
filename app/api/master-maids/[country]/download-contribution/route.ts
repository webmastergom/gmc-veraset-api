import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getCountryContributions } from '@/lib/master-maids';
import { startQueryAndWait } from '@/lib/athena';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * GET /api/master-maids/[country]/download-contribution?id=<contributionId>
 *
 * Streams a CSV with the MAIDs from a SINGLE contribution (one row in the
 * Contributions table of the master-maids page). Unlike download-cluster
 * (which UNIONs all contributions sharing the same attr_type+attr_value),
 * this endpoint is scoped to one specific CTAS — useful when an attribute
 * has been re-imported by multiple sources and you only want one.
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
  const id = request.nextUrl.searchParams.get('id') || '';
  if (!id) {
    return NextResponse.json({ error: 'id query param required' }, { status: 400 });
  }

  try {
    const entry = await getCountryContributions(cc);
    if (!entry) {
      return NextResponse.json({ error: 'No master MAID data for this country' }, { status: 404 });
    }

    const contrib = entry.contributions.find((c) => c.id === id);
    if (!contrib) {
      return NextResponse.json({ error: `Contribution ${id} not found` }, { status: 404 });
    }
    if (!contrib.athenaTable) {
      return NextResponse.json(
        { error: 'Legacy CSV-only contribution — download not supported (use country-level download)' },
        { status: 422 }
      );
    }

    console.log(
      `[MASTER-MAIDS-DL-CONTRIB] ${cc} id=${id} table=${contrib.athenaTable} type=${contrib.attributeType} value="${contrib.attributeValue || '(empty)'}"`
    );

    const sql = `SELECT DISTINCT ad_id FROM ${contrib.athenaTable}`;
    const { queryId, outputCsvKey } = await startQueryAndWait(sql);
    console.log(`[MASTER-MAIDS-DL-CONTRIB] queryId=${queryId} csv=${outputCsvKey}`);

    const obj = await s3Client.send(
      new GetObjectCommand({ Bucket: BUCKET, Key: outputCsvKey })
    );
    const stream = obj.Body as ReadableStream;
    const safeValue = (contrib.attributeValue || 'all')
      .replace(/[^a-zA-Z0-9._-]/g, '_')
      .slice(0, 50);
    const fileName = `master-maids-${cc}-${contrib.attributeType}-${safeValue}-${id.slice(-8)}-${new Date()
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
  } catch (e: any) {
    console.error(`[MASTER-MAIDS-DL-CONTRIB] error ${cc} id=${id}:`, e?.message || e);
    return NextResponse.json({ error: e?.message || String(e) }, { status: 500 });
  }
}
