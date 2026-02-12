import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getDeviceMovements } from '@/lib/dataset-movements';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 120;

/**
 * GET /api/datasets/[name]/movements?dateFrom=YYYY-MM-DD&dateTo=YYYY-MM-DD&sample=50
 * Returns movement trajectories for a random sample of devices (max 50).
 */
export async function GET(
  req: NextRequest,
  context: { params: Promise<{ name: string }> | { name: string } }
) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await (typeof context.params === 'object' && context.params instanceof Promise
    ? context.params
    : Promise.resolve(context.params as { name: string }));
  const datasetName = params.name;

  const searchParams = req.nextUrl.searchParams;
  const dateFrom = searchParams.get('dateFrom');
  const dateTo = searchParams.get('dateTo');
  const sample = parseInt(searchParams.get('sample') || '50', 10);

  if (!dateFrom || !dateTo) {
    return NextResponse.json(
      { error: 'dateFrom and dateTo (YYYY-MM-DD) are required' },
      { status: 400 }
    );
  }

  try {
    const result = await getDeviceMovements(datasetName, dateFrom, dateTo, sample);
    return NextResponse.json(result);
  } catch (error: any) {
    console.error(`[MOVEMENTS] GET /api/datasets/${datasetName}/movements error:`, error);
    return NextResponse.json(
      { error: 'Failed to fetch movements', details: error.message },
      { status: 500 }
    );
  }
}
