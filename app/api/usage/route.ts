import { NextResponse } from 'next/server';
import { getUsage } from '@/lib/usage';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const usage = await getUsage();
    // Cache usage for 5 minutes — monthly quota doesn't change frequently.
    // stale-while-revalidate lets the browser use the cached value while
    // fetching a fresh copy in the background.
    return NextResponse.json(usage, {
      headers: {
        'Cache-Control': 'private, max-age=300, stale-while-revalidate=600',
      },
    });
  } catch (error: any) {
    console.error('GET /api/usage error:', error);
    return NextResponse.json(
      {
        error: 'Failed to fetch usage',
        details: error.message,
        month: new Date().toISOString().slice(0, 7),
        used: 0,
        limit: 200,
        remaining: 200,
        percentage: 0,
      },
      { status: 500 }
    );
  }
}
