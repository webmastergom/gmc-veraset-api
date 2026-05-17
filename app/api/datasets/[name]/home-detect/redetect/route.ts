import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { dropHomeTable } from '@/lib/home-detector';
import { s3Client, BUCKET } from '@/lib/s3-config';
import { ListObjectsV2Command, DeleteObjectsCommand } from '@aws-sdk/client-s3';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * POST /api/datasets/[name]/home-detect/redetect
 *
 * Admin operation: drop the TC-WK-19-7 home-locations table for a
 * dataset and clear the catchment / affinity reports that were built
 * against it, plus the polling state files. After this returns, the
 * next click on "Run analysis" will auto-trigger a fresh home
 * detection (see /reports/poll's home_detection phase) using the
 * current home-detection SQL.
 *
 * Use this when you suspect the existing home table is biased — e.g.
 * a previous run with the GPS-only filter that pushed urban residents
 * out into the suburbs.
 *
 * Returns: { ok: true, deleted: { homeTable, reports, state } }
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> },
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { name: datasetName } = await context.params;

  try {
    // 1. Drop the home table (Glue catalog entry + parquet under s3://.../home-locations/{ds}/)
    await dropHomeTable(datasetName);

    // 2. Delete every saved catchment + affinity report (all filter variants:
    //    catchment.json, catchment-gps-cs2.json, catchment-dwell-30-60.json, etc.)
    const reportsPrefix = `config/dataset-reports/${datasetName}/`;
    const reportKeysToDelete: string[] = [];
    let continuationToken: string | undefined;
    do {
      const list = await s3Client.send(new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: reportsPrefix,
        ContinuationToken: continuationToken,
      }));
      for (const obj of list.Contents || []) {
        const key = obj.Key!;
        // Match either "catchment*.json" or "affinity*.json" right under the
        // dataset prefix (do NOT touch deeper paths like category-affinity/*).
        const tail = key.slice(reportsPrefix.length);
        if (!tail.includes('/') && (tail.startsWith('catchment') || tail.startsWith('affinity')) && tail.endsWith('.json')) {
          reportKeysToDelete.push(key);
        }
      }
      continuationToken = list.IsTruncated ? list.NextContinuationToken : undefined;
    } while (continuationToken);

    if (reportKeysToDelete.length > 0) {
      // DeleteObjects caps at 1000 keys per call — chunk if needed.
      for (let i = 0; i < reportKeysToDelete.length; i += 1000) {
        const batch = reportKeysToDelete.slice(i, i + 1000).map((Key) => ({ Key }));
        await s3Client.send(new DeleteObjectsCommand({
          Bucket: BUCKET,
          Delete: { Objects: batch },
        }));
      }
    }

    // 3. Delete polling state files so the next /reports/poll call
    //    re-enters from a clean slate (which triggers the home-detection phase).
    const stateKeys = [
      `config/dataset-report-state/${datasetName}.json`,
      `config/catchment-state/${datasetName}.json`,
    ];
    await s3Client.send(new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: { Objects: stateKeys.map((Key) => ({ Key })), Quiet: true },
    }));

    console.log(`[HOME-REDETECT] ${datasetName}: dropped home table, removed ${reportKeysToDelete.length} report file(s), cleared state`);

    return NextResponse.json({
      ok: true,
      deleted: {
        homeTable: true,
        reports: reportKeysToDelete.length,
        state: stateKeys.length,
      },
      message: 'Home table cleared. Click "Run analysis" to trigger fresh home detection.',
    });
  } catch (err: any) {
    console.error(`[HOME-REDETECT] ${datasetName} error:`, err.message);
    return NextResponse.json(
      { error: err.message || 'Re-detect home failed' },
      { status: 500 },
    );
  }
}
