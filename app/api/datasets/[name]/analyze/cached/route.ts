import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { GetObjectCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';

/**
 * GET /api/datasets/[name]/analyze/cached
 *
 * Read-only lookup for the dataset's basic-analysis result. Returns
 * `{ summary }` if a completed analysis is cached at
 * config/analysis-state/{name}.json, else 404.
 *
 * Does NOT kick off a new analysis. Used by the job-detail page to
 * surface audience size without running queries on its own.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ name: string }> },
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }
  const { name } = await context.params;
  try {
    const res = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: `config/analysis-state/${name}.json`,
    }));
    const body = await res.Body?.transformToString();
    if (!body) {
      return NextResponse.json({ error: 'Not found' }, { status: 404 });
    }
    const state = JSON.parse(body);
    if (state.status !== 'completed' || !state.result?.summary) {
      return NextResponse.json({ status: state.status }, { status: 404 });
    }
    return NextResponse.json({ summary: state.result.summary });
  } catch (e: any) {
    if (e?.name === 'NoSuchKey') {
      return NextResponse.json({ error: 'Not found' }, { status: 404 });
    }
    return NextResponse.json({ error: e.message }, { status: 500 });
  }
}
