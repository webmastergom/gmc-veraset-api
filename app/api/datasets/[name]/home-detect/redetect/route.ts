import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { dropHomeTable } from '@/lib/home-detector';
import { s3Client, BUCKET } from '@/lib/s3-config';
import {
  ListObjectsV2Command,
  DeleteObjectsCommand,
  DeleteObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * POST /api/datasets/[name]/home-detect/redetect
 *
 * Full dataset reset. Each cleanup target runs independently and the
 * result of every step is returned to the caller, so a partial failure
 * is never silent.
 *
 * Targets:
 *   • home_table         — Glue catalog entry + `home-locations/{ds}/`
 *   • home_parquets      — defensive re-sweep of `home-locations/{ds}/`
 *   • reports            — `config/dataset-reports/{ds}/` (recursive)
 *   • report_cache       — `dataset-report-cache/{ds}/` (recursive)
 *   • dataset_analysis,
 *     report_state,
 *     catchment_state,
 *     analysis_state     — single-key per-dataset state JSONs
 *
 * IAM resilience — the Vercel IAM user historically lacks
 * `s3:DeleteObject` on some prefixes. When a hard delete fails we fall
 * back to a 0-byte PutObject overwrite ("soft delete"). Downstream
 * readers either fail to JSON-parse the empty file (catchment /
 * affinity / etc.) or skip 0-byte parquets, so soft-deletes behave
 * identically to hard deletes from the user's perspective. The
 * response distinguishes the two so we can see when the IAM is the
 * bottleneck.
 *
 * Verification — after every step runs we re-HEAD/list the targets
 * and report any object that is still present AND non-empty as a
 * survivor. A 0-byte object counts as cleared, not as a survivor.
 */

type StepStatus = 'cleared' | 'noop' | 'failed';

type StepResult = {
  step: string;
  status: StepStatus;
  /** How many keys ended up cleared (hard-deleted or soft-deleted). */
  cleared: number;
  /** Of `cleared`, how many were 0-byte soft-deletes (IAM fallback). */
  softDeleted: number;
  /** Set when status === 'failed'. */
  error?: string;
};

async function listAllKeys(prefix: string): Promise<Array<{ key: string; size: number }>> {
  const out: Array<{ key: string; size: number }> = [];
  let token: string | undefined;
  do {
    const r = await s3Client.send(new ListObjectsV2Command({
      Bucket: BUCKET, Prefix: prefix, ContinuationToken: token,
    }));
    for (const o of r.Contents || []) {
      if (o.Key) out.push({ key: o.Key, size: o.Size || 0 });
    }
    token = r.IsTruncated ? r.NextContinuationToken : undefined;
  } while (token);
  return out;
}

/** Real S3 delete for one key. Returns null on success, error string on failure. */
async function tryHardDelete(key: string): Promise<string | null> {
  try {
    await s3Client.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: key }));
    return null;
  } catch (e: any) {
    return e?.name || e?.Code || e?.message || 'unknown delete error';
  }
}

/** 0-byte PutObject overwrite. Same pattern as app/api/settings/staging/route.ts. */
async function softDelete(key: string): Promise<string | null> {
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: '',
      ContentType: 'application/octet-stream',
    }));
    return null;
  } catch (e: any) {
    return e?.name || e?.Code || e?.message || 'unknown soft-delete error';
  }
}

/** Clear one key: hard-delete first, soft-delete on failure. */
async function clearKey(key: string): Promise<'hard' | 'soft' | 'failed'> {
  const hardErr = await tryHardDelete(key);
  if (hardErr === null) return 'hard';
  const softErr = await softDelete(key);
  if (softErr === null) return 'soft';
  return 'failed';
}

async function wipePrefix(stepName: string, prefix: string): Promise<StepResult> {
  try {
    const all = await listAllKeys(prefix);
    const live = all.filter((o) => o.size > 0);
    if (live.length === 0) return { step: stepName, status: 'noop', cleared: 0, softDeleted: 0 };
    let hardCount = 0;
    let softCount = 0;
    let firstError: string | undefined;
    // Parallel clears, capped at 10 concurrent (S3 is happy with this).
    for (let i = 0; i < live.length; i += 10) {
      const slice = live.slice(i, i + 10);
      const outcomes = await Promise.all(slice.map((o) => clearKey(o.key).then((r) => ({ key: o.key, r }))));
      for (const o of outcomes) {
        if (o.r === 'hard') hardCount++;
        else if (o.r === 'soft') softCount++;
        else if (!firstError) firstError = `${o.key}: both hard and soft delete failed`;
      }
    }
    const cleared = hardCount + softCount;
    if (firstError) {
      return { step: stepName, status: 'failed', cleared, softDeleted: softCount, error: firstError };
    }
    return { step: stepName, status: 'cleared', cleared, softDeleted: softCount };
  } catch (e: any) {
    return { step: stepName, status: 'failed', cleared: 0, softDeleted: 0, error: e?.message || String(e) };
  }
}

async function wipeSingleKey(stepName: string, key: string): Promise<StepResult> {
  try {
    const head = await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
    if ((head.ContentLength || 0) === 0) {
      return { step: stepName, status: 'noop', cleared: 0, softDeleted: 0 };
    }
  } catch (e: any) {
    if (e?.$metadata?.httpStatusCode === 404 || e?.name === 'NotFound') {
      return { step: stepName, status: 'noop', cleared: 0, softDeleted: 0 };
    }
    return { step: stepName, status: 'failed', cleared: 0, softDeleted: 0, error: e?.message || String(e) };
  }
  const r = await clearKey(key);
  if (r === 'failed') {
    return { step: stepName, status: 'failed', cleared: 0, softDeleted: 0, error: `${key}: both hard and soft delete failed` };
  }
  return { step: stepName, status: 'cleared', cleared: 1, softDeleted: r === 'soft' ? 1 : 0 };
}

async function dropHome(stepName: string, ds: string): Promise<StepResult> {
  try {
    await dropHomeTable(ds);
    return { step: stepName, status: 'cleared', cleared: 1, softDeleted: 0 };
  } catch (e: any) {
    return { step: stepName, status: 'failed', cleared: 0, softDeleted: 0, error: e?.message || String(e) };
  }
}

async function liveKeyExists(key: string): Promise<boolean> {
  try {
    const head = await s3Client.send(new HeadObjectCommand({ Bucket: BUCKET, Key: key }));
    return (head.ContentLength || 0) > 0;
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
    { step: 'audience_counter_state', key: `config/audience-counter-state/${datasetName}.json` },
  ];

  const steps: StepResult[] = [];
  // Glue + S3 in one shared helper. If Glue drop throws (IAM), we still
  // run the home_parquets sweep below as a belt-and-suspenders.
  steps.push(await dropHome('home_table', datasetName));
  for (const t of prefixTargets) {
    steps.push(await wipePrefix(t.step, t.prefix));
  }
  for (const t of singleKeyTargets) {
    steps.push(await wipeSingleKey(t.step, t.key));
  }

  // Verification: re-list every prefix, re-HEAD every single key. Only
  // objects with size > 0 count as survivors — 0-byte soft-deletes are
  // treated as cleared.
  const remaining: string[] = [];
  try {
    for (const t of prefixTargets) {
      const all = await listAllKeys(t.prefix);
      for (const o of all) if (o.size > 0) remaining.push(o.key);
    }
    for (const t of singleKeyTargets) {
      if (await liveKeyExists(t.key)) remaining.push(t.key);
    }
  } catch (e: any) {
    console.error(`[DATASET-RESET] ${datasetName} verification error:`, e?.message || e);
  }

  const totalCleared = steps.reduce((n, s) => n + s.cleared, 0);
  const totalSoftDeleted = steps.reduce((n, s) => n + s.softDeleted, 0);
  const anyStepFailed = steps.some((s) => s.status === 'failed');
  const ok = remaining.length === 0 && !anyStepFailed;

  console.log(
    `[DATASET-RESET] ${datasetName}: ok=${ok} cleared=${totalCleared} ` +
    `(soft=${totalSoftDeleted}) remaining=${remaining.length} ` +
    `failedSteps=${steps.filter((s) => s.status === 'failed').map((s) => s.step).join(',') || 'none'}`,
  );

  return NextResponse.json({
    ok,
    dataset: datasetName,
    steps,
    remaining,
    totalCleared,
    totalSoftDeleted,
    summary: ok
      ? `Dataset reset clean — ${totalCleared} object(s) cleared` +
        (totalSoftDeleted ? ` (${totalSoftDeleted} via IAM-fallback soft-delete)` : '') +
        `. Click Analyze to rebuild.`
      : `Reset incomplete: ${remaining.length} survivor(s), ${steps.filter((s) => s.status === 'failed').length} step(s) failed. See steps[] and remaining[] for details.`,
  }, { status: ok ? 200 : 207 });
}
