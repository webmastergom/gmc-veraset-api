/**
 * GET /api/personas/runs
 * List past persona runs by reading the persona-state/ S3 prefix.
 */

import { NextRequest, NextResponse } from 'next/server';
import { ListObjectsV2Command } from '@aws-sdk/client-s3';
import { getConfig, s3Client, BUCKET } from '@/lib/s3-config';
import { type PersonaState } from '@/lib/persona-types';
import { getMegaJob } from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';

export const dynamic = 'force-dynamic';

interface RunSummary {
  runId: string;
  phase: string;
  generatedAt: string;
  megaJobIds: string[];
  megaJobNames: string[];
  jobIds: string[];
  jobNames: string[];
  totalDevices?: number;
  personaCount?: number;
  error?: string;
}

export async function GET(_request: NextRequest): Promise<NextResponse> {
  try {
    // List all state files under config/persona-state/
    const list = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: 'config/persona-state/',
        MaxKeys: 200,
      })
    );

    const runIds: string[] = [];
    for (const obj of list.Contents || []) {
      if (!obj.Key) continue;
      const m = obj.Key.match(/persona-state\/([^/]+)\.json$/);
      if (m) runIds.push(m[1]);
    }

    const runs: RunSummary[] = [];
    for (const runId of runIds) {
      try {
        const state = await getConfig<PersonaState>(`persona-state/${runId}`);
        if (!state) continue;
        const megaJobNames: string[] = [];
        for (const id of state.config.megaJobIds) {
          const mj = await getMegaJob(id);
          megaJobNames.push(mj?.name || id);
        }
        const jobNames: string[] = [];
        for (const id of state.config.jobIds || []) {
          const j = await getJob(id);
          jobNames.push(j?.name || id);
        }
        runs.push({
          runId,
          phase: state.phase,
          generatedAt: state.report?.generatedAt || state.updatedAt,
          megaJobIds: state.config.megaJobIds,
          megaJobNames,
          jobIds: state.config.jobIds || [],
          jobNames,
          totalDevices: state.report?.scorecard.totalDevices,
          personaCount: state.report?.personas.length,
          error: state.error,
        });
      } catch {}
    }

    runs.sort((a, b) => (b.generatedAt || '').localeCompare(a.generatedAt || ''));
    return NextResponse.json({ runs });
  } catch (e: any) {
    console.error('[PERSONAS-RUNS] error:', e?.message);
    return NextResponse.json({ error: e?.message || String(e) }, { status: 500 });
  }
}
