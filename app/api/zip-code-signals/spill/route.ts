import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig } from '@/lib/s3-config';
import type { PostalMaidResult } from '@/lib/postal-maid-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

const SPILL_PREFIX = 'postal-maid-spill/';

/**
 * GET /api/zip-code-signals/spill?key=postal-maid-spill/...
 * Fetch a full Zip Code Signals result written to S3 when the SSE payload was too large.
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const key = request.nextUrl.searchParams.get('key')?.trim() || '';
  if (!key.startsWith(SPILL_PREFIX) || key.includes('..') || key.length > 200) {
    return NextResponse.json({ error: 'Invalid key' }, { status: 400 });
  }

  const data = await getConfig<PostalMaidResult>(key);
  if (!data) {
    return NextResponse.json(
      { error: 'Not found or expired (config may have been rotated)' },
      { status: 404 },
    );
  }

  return NextResponse.json(data);
}
