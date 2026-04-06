import { NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getMasterIndex } from '@/lib/master-maids';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/master-maids
 *
 * List all countries with master MAID stats.
 * Returns a summary per country: contribution count, last consolidated date, stats.
 */
export async function GET(request: Request) {
  if (!isAuthenticated(request as any)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    const index = await getMasterIndex();

    const countries = Object.entries(index).map(([code, entry]) => ({
      country: code,
      contributionCount: entry.contributions.length,
      lastConsolidatedAt: entry.lastConsolidatedAt,
      totalMaids: entry.stats?.totalMaids ?? null,
      attributeCount: entry.stats?.byAttribute?.length ?? 0,
      datasetCount: entry.stats?.byDataset ? Object.keys(entry.stats.byDataset).length : new Set(entry.contributions.map(c => c.sourceDataset)).size,
      // Date range from contributions
      dateRange: entry.contributions.length > 0
        ? {
            from: entry.contributions
              .map(c => c.dateRange?.from)
              .filter(d => d && d !== 'unknown')
              .sort()[0] || null,
            to: entry.contributions
              .map(c => c.dateRange?.to)
              .filter(d => d && d !== 'unknown')
              .sort()
              .reverse()[0] || null,
          }
        : null,
    }));

    // Sort by MAID count descending (consolidated first, then by contributions)
    countries.sort((a, b) => (b.totalMaids ?? -1) - (a.totalMaids ?? -1));

    const globalTotal = countries.reduce((sum, c) => sum + (c.totalMaids ?? 0), 0);

    return NextResponse.json({ countries, globalTotal });
  } catch (error: any) {
    console.error('[MASTER-MAIDS] Error listing countries:', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
