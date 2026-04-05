import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getCountryContributions, removeContribution } from '@/lib/master-maids';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/master-maids/[country]
 *
 * Get detailed stats and contributions for a specific country.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ country: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    const { country } = await context.params;
    const entry = await getCountryContributions(country);

    if (!entry) {
      return NextResponse.json({ error: 'No master MAID data for this country' }, { status: 404 });
    }

    return NextResponse.json({
      country: country.toUpperCase(),
      ...entry,
    });
  } catch (error: any) {
    console.error('[MASTER-MAIDS] Error getting country detail:', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

/**
 * DELETE /api/master-maids/[country]?id=contributionId
 *
 * Remove a specific contribution from the registry.
 */
export async function DELETE(
  request: NextRequest,
  context: { params: Promise<{ country: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  try {
    const { country } = await context.params;
    const contributionId = request.nextUrl.searchParams.get('id');

    if (!contributionId) {
      return NextResponse.json({ error: 'id query parameter required' }, { status: 400 });
    }

    const removed = await removeContribution(country, contributionId);
    if (!removed) {
      return NextResponse.json({ error: 'Contribution not found' }, { status: 404 });
    }

    return NextResponse.json({ success: true });
  } catch (error: any) {
    console.error('[MASTER-MAIDS] Error removing contribution:', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
