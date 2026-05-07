/**
 * GET /api/personas/report?runId=...
 * Return the cached PersonaReport for a finished run.
 */

import { NextRequest, NextResponse } from 'next/server';
import { getConfig } from '@/lib/s3-config';
import { type PersonaReport, type PersonaState } from '@/lib/persona-types';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest): Promise<NextResponse> {
  const runId = request.nextUrl.searchParams.get('runId');
  if (!runId) return NextResponse.json({ error: 'runId required' }, { status: 400 });

  // Prefer the dedicated report file (saved early so frontend can render
  // scorecard while exports finish).
  const report = await getConfig<PersonaReport>(`persona-reports/${runId}`);
  if (report) return NextResponse.json({ report });

  // Fallback: state file in case report wasn't saved separately.
  const state = await getConfig<PersonaState>(`persona-state/${runId}`);
  if (state?.report) return NextResponse.json({ report: state.report });
  return NextResponse.json({ error: 'Report not found' }, { status: 404 });
}
