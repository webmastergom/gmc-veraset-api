import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { dropHomeTable } from '@/lib/home-detector';
import { s3Client, BUCKET } from '@/lib/s3-config';
import {
  ListObjectsV2Command,
  DeleteObjectsCommand,
  HeadObjectCommand,
} from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * POST /api/datasets/[name]/home-detect/redetect
 *
 * Full dataset reset. Wipes every derived artifact for a dataset and
 * leaves only the Veraset-synced parquets and the job/UI config
 * untouched. After this returns, the next "Run analysis" click will
 * auto-trigger a fresh home detection and rebuild every report from
 * scratch.
 *
 * Targets (each independent, never aborts the others):
 *   • home_table     — Glue catalog entry for `home_{ds}` + parquets
 *                      under `home-locations/{ds}/`
 *   • home_parquets  — defensive re-sweep of `home-locations/{ds}/`
 *                      in case `home_table`'s S3 wipe partially failed
 *   • reports        — every object under `config/dataset-reports/{ds}/`
 *                      including subdirectories (category-affinity/,
 *                      zip-code-signals/, every filter variant)
 *   • report_cache   — staging area `dataset-report-cache/{ds}/` used
 *                      by the multiphase parsing path
 *   • dataset_analysis, report_state, catchment_state, analysis_state
 *                    — single-key per-dataset state JSONs
 *
 * After every step runs, a verification scan re-lists every prefix
 * and HEADs every single-key, and any survivor is surfaced in the
 * response's `remaining` array. That guarantees the caller can tell
 * whether `ok: true` means "really clean" or "we tried and X survived".
 *
 * Idempotent: running it on a half-deleted state finishes the cleanup
 * rather than erroring out. Running it twice on a clean dataset is a
 * no-op that returns `{ ok: true, remaining: [] }`.
 */

type StepStatus = 'deleted' | 'noop' | 'failed';

type StepResult = {
  step: string;
  status: StepStatus;
  /** Items deleted in this step. 0 means noop or failed-before-any-delete. */
  count: number;
  /** Set when status === 'failed'. */
  error?: string;
};

async function listAllKeys(prefix: string): Promise<string[]> {
  const out: string[] = [];
  let token: string | undefined;
  do {
    const r = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET, Prefix: prefix, ContinuationToken: token,
    }));
    for (const o of r.Contents || []) if (o.Key) out.push(o.Key);
    token = r.IsTruncated ? r.NextContinuationToken : undefined;
  } while (token);
  return out;
}

async function deleteKeysBatched(keys: string[]): Promise<{ deleted: number; firstError?: string }> {
  let deleted = 0;
  let firstError: string | undefined;
  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000).map((Key) => ({ Key }));
    const r = await s3Client.send(new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: { Objects: batch, Quiet: false },
    }));
    deleted += batch.length - (r.Errors?.length || 0);
    if (r.Errors?.length && !firstError) {
      const e = r.Errors[0];
      firstError = `${e.Key}: ${e.Code} ${e.Message}`;
    }
  }
  return { deleted, firstError };
}

async function wipePrefix(stepName: string, prefix: string): Promise<StepResult> {
  try {
    const keys = await listAllKeys(prefix);
    if (keys.length === 0) return { step: stepName, status: 'noop', count: 0 };
    const { deleted, firstError } = await deleteKeysBatched(keys);
    if (firstError) {
      return { step: stepName, status: 'failed', count: deleted, error: firstError };
    }
    return { step: stepName, status: 'deleted', count: deleted };
  } catch (e: any) {
    return { step: stepName, status: 'failed', count: 0, error: e?.message || String(e) };
  }
}

async function wipeSingleKey(stepName: string, key: string): Promise<StepResult> {
  // HEAD first so we can distinguish 'already gone' (noop) from 'we deleted it'.
  try {
    await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
  } catch (e: any) {
    if (e?.$metadata?.httpStatusCode === 404 || e?.name === 'NotFound') {
      return { step: stepName, status: 'noop', count: 0 };
    }
    return { step: stepName, status: 'failed', count: 0, error: e?.message || String(e) };
  }
  try {
    const r = await s3Client.send(new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: { Objects: [{ Key: key }], Quiet: false },
    }));
    if (r.Errors?.length) {
      const e = r.Errors[0];
      return { step: stepName, status: 'failed', count: 0, error: `${e.Code} ${e.Message}` };
    }
    return { step: stepName, status: 'deleted', count: 1 };
  } catch (e: any) {
    return { step: stepName, status: 'failed', count: 0, error: e?.message || String(e) };
  }
}

async function dropHome(stepName: string, ds: string): Promise<StepResult> {
  try {
    await dropHomeTable(ds);
    return { step: stepName, status: 'deleted', count: 1 };
  } catch (e: any) {
    return { step: stepName, status: 'failed', count: 0, error: e?.message || String(e) };
  }
}

async function keyExists(key: string): Promise<boolean> {
  try {
    await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
    return true;
  } catch (e: any) {
    if (e?.$metadata?.httpStatusCode === 404 || e?.name === 'NotFound') return false;
    throw e;
  }
}

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> },
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { name: datasetName } = await context.params;

  const prefixTargets: Array<{ step: string; prefix: string }> = [
    { step: 'reports', prefix: `config/dataset-reports/${datasetName}/` },
    { step: 'home_parquets', prefix: `home-locations/${datasetName}/` },
    { step: 'report_cache', prefix: `dataset-report-cache/${datasetName}/` },
  ];
  const singleKeyTargets: Array<{ step: string; key: string }> = [
    { step: 'dataset_analysis', key: `config/dataset-analysis/${datasetName}.json` },
    { step: 'report_state', key: `config/dataset-report-state/${datasetName}.json` },
    { step: 'catchment_state', key: `config/catchment-state/${datasetName}.json` },
    { step: 'analysis_state', key: `config/analysis-state/${datasetName}.json` },
  ];

  // Step 1: drop the Glue catalog entry + home parquets via the shared
  // helper. We still run wipePrefix('home_parquets', ...) afterward as
  // belt-and-suspenders for the case where Glue dropped fine but the
  // S3 wipe inside dropHomeTable failed mid-way.
  const steps: StepResult[] = [];
  steps.push(await dropHome('home_table', datasetName));

  // Step 2: wipe each prefix. Each runs in its own try/catch and never
  // aborts the next one.
  for (const t of prefixTargets) {
    steps.push(await wipePrefix(t.step, t.prefix));
  }

  // Step 3: delete each single state key.
  for (const t of singleKeyTargets) {
    steps.push(await wipeSingleKey(t.step, t.key));
  }

  // Step 4: verification scan. Re-list every prefix and HEAD every
  // single-key target. Anything still alive goes into `remaining` so
  // the caller can tell the truth from the optimistic step status.
  const remaining: string[] = [];
  try {
    for (const t of prefixTargets) {
      const survivors = await listAllKeys(t.prefix);
      remaining.push(...survivors);
    }
    for (const t of singleKeyTargets) {
      if (await keyExists(t.key)) remaining.push(t.key);
    }
  } catch (e: any) {
    console.error(`[DATASET-RESET] ${datasetName} verification error:`, e?.message || e);
  }

  const totalDeleted = steps.reduce((n, s) => n + s.count, 0);
  const anyStepFailed = steps.some((s) => s.status === 'failed');
  const ok = remaining.length === 0 && !anyStepFailed;

  console.log(
    `[DATASET-RESET] ${datasetName}: ok=${ok} totalDeleted=${totalDeleted} ` +
    `remaining=${remaining.length} failedSteps=${steps.filter((s) => s.status === 'failed').map((s) => s.step).join(',') || 'none'}`,
  );

  return NextResponse.json({
    ok,
    dataset: datasetName,
    steps,
    remaining,
    totalDeleted,
    summary: ok
      ? `Dataset reset clean — ${totalDeleted} object(s) deleted. Click Analyze to rebuild.`
      : `Reset incomplete: ${remaining.length} survivor(s), ${steps.filter((s) => s.status === 'failed').length} step(s) failed. See steps[] and remaining[] for details.`,
  }, { status: ok ? 200 : 207 /* Multi-Status */ });
}
