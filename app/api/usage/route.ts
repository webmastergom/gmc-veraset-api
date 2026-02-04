import { NextResponse } from 'next/server';
import { getUsage } from '@/lib/usage';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET() {
  try {
    const usage = await getUsage();
    return NextResponse.json(usage);
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
