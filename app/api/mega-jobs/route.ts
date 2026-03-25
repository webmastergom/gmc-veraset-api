import { NextRequest, NextResponse } from 'next/server';
import {
  getAllMegaJobs,
  createMegaJob,
  computeSplitPlan,
  type MegaJob,
} from '@/lib/mega-jobs';
import { getJob } from '@/lib/jobs';
import { canCreateMegaJob } from '@/lib/usage';
import {
  createMegaJobAutoSplitSchema,
  createMegaJobManualGroupSchema,
} from '@/lib/validation';
import { z } from 'zod';

export const dynamic = 'force-dynamic';
export const maxDuration = 30;

/**
 * GET /api/mega-jobs
 * List all mega-jobs (lightweight index).
 */
export async function GET(): Promise<NextResponse> {
  try {
    const megaJobs = await getAllMegaJobs();
    return NextResponse.json(megaJobs);
  } catch (error: any) {
    console.error('[MEGA-JOBS GET]', error.message);
    return NextResponse.json({ error: 'Failed to list mega-jobs' }, { status: 500 });
  }
}

/**
 * POST /api/mega-jobs
 * Create a mega-job in either auto-split or manual-group mode.
 * Body must include { mode: 'auto-split' | 'manual-group', ...fields }
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  try {
    const body = await request.json();
    const mode = body.mode as string;

    // Resolve API key name for origin tracking on sub-jobs
    let apiKeyName: string | undefined;
    const headerKey = request.headers.get('X-API-Key');
    if (headerKey) {
      try {
        const { validateApiKey } = await import('@/lib/api-keys');
        const keyResult = await validateApiKey(headerKey);
        if (keyResult.valid && keyResult.keyId) {
          const { getApiKeyById } = await import('@/lib/api-keys');
          const keyInfo = await getApiKeyById(keyResult.keyId);
          apiKeyName = keyInfo?.name;
        }
      } catch { /* non-critical */ }
    }

    if (mode === 'auto-split') {
      return await handleAutoSplit(body, apiKeyName);
    }
    if (mode === 'manual-group') {
      return await handleManualGroup(body);
    }

    return NextResponse.json(
      { error: 'Invalid mode. Must be "auto-split" or "manual-group".' },
      { status: 400 }
    );
  } catch (error: any) {
    console.error('[MEGA-JOBS POST]', error.message);
    return NextResponse.json({ error: error.message || 'Failed to create mega-job' }, { status: 500 });
  }
}

// ── Auto-split ────────────────────────────────────────────────────────

async function handleAutoSplit(body: any, apiKeyName?: string): Promise<NextResponse> {
  // Validate
  let data: z.infer<typeof createMegaJobAutoSplitSchema>;
  try {
    data = createMegaJobAutoSplitSchema.parse(body);
  } catch (error: any) {
    const msg = error instanceof z.ZodError
      ? error.errors.map((e: any) => `${e.path.join('.') || 'root'}: ${e.message}`).join(', ')
      : 'Validation failed';
    return NextResponse.json({ error: msg }, { status: 400 });
  }

  // Fetch POI collections to get combined count
  const { getConfig } = await import('@/lib/s3-config');
  type CollectionRecord = { id: string; poiCount: number; geojsonPath: string; name: string };
  type Collections = Record<string, CollectionRecord>;
  const allCollections = await getConfig<Collections>('poi-collections');

  const resolvedCollections: CollectionRecord[] = [];
  for (const colId of data.poiCollectionIds) {
    const col = allCollections?.[colId];
    if (!col) {
      return NextResponse.json(
        { error: `POI collection "${colId}" not found` },
        { status: 404 }
      );
    }
    resolvedCollections.push(col);
  }

  const totalPois = resolvedCollections.reduce((sum, c) => sum + c.poiCount, 0);
  const collectionNames = resolvedCollections.map((c) => c.name).join(' + ');

  // Compute split plan
  const splitPlan = computeSplitPlan(data.dateRange, totalPois);

  // Check usage quota
  const quota = await canCreateMegaJob(splitPlan.totalSubJobs);
  if (!quota.allowed) {
    return NextResponse.json({ error: quota.reason, remaining: quota.remaining }, { status: 429 });
  }

  // Create mega-job in "planning" status
  const megaJob = await createMegaJob({
    name: data.name,
    description: data.description,
    country: data.country,
    mode: 'auto-split',
    sourceScope: {
      poiCollectionIds: data.poiCollectionIds,
      dateRange: data.dateRange,
      radius: data.radius,
      schema: data.schema,
      type: data.type,
    },
    splits: splitPlan,
    subJobIds: [],
    status: 'planning',
    progress: { created: 0, synced: 0, failed: 0, total: splitPlan.totalSubJobs },
    ...(apiKeyName ? { apiKeyName } : {}),
  });

  return NextResponse.json({
    megaJob,
    splitPreview: {
      dateChunks: splitPlan.dateChunks,
      poiChunks: splitPlan.poiChunks.map((c) => ({
        ...c,
        label: `POIs ${c.startIndex + 1}–${c.endIndex}`,
      })),
      totalSubJobs: splitPlan.totalSubJobs,
      poiCollectionName: collectionNames,
      totalPois: totalPois,
      quotaRemaining: quota.remaining,
    },
  }, { status: 201 });
}

// ── Manual group ──────────────────────────────────────────────────────

async function handleManualGroup(body: any): Promise<NextResponse> {
  let data: z.infer<typeof createMegaJobManualGroupSchema>;
  try {
    data = createMegaJobManualGroupSchema.parse(body);
  } catch (error: any) {
    const msg = error instanceof z.ZodError
      ? error.errors.map((e: any) => `${e.path.join('.') || 'root'}: ${e.message}`).join(', ')
      : 'Validation failed';
    return NextResponse.json({ error: msg }, { status: 400 });
  }

  // Validate all sub-jobs exist and are SUCCESS + synced
  const errors: string[] = [];
  for (const jobId of data.subJobIds) {
    const job = await getJob(jobId);
    if (!job) {
      errors.push(`Job ${jobId} not found`);
    } else if (job.status !== 'SUCCESS') {
      errors.push(`Job ${job.name || jobId} is ${job.status}, must be SUCCESS`);
    } else if (!job.syncedAt) {
      errors.push(`Job ${job.name || jobId} is not synced yet`);
    }
  }

  if (errors.length > 0) {
    return NextResponse.json({ error: 'Invalid sub-jobs', details: errors }, { status: 400 });
  }

  // Create mega-job — goes straight to "running" since all sub-jobs already exist
  const megaJob = await createMegaJob({
    name: data.name,
    description: data.description,
    mode: 'manual-group',
    subJobIds: data.subJobIds,
    status: 'running',
    progress: {
      created: data.subJobIds.length,
      synced: data.subJobIds.length,
      failed: 0,
      total: data.subJobIds.length,
    },
  });

  return NextResponse.json({ megaJob }, { status: 201 });
}
