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
 * Full dataset reset. Wipes every derived artifact for a dataset
 * (home table, all reports, basic analysis, all polling state) and
 * leaves only the Veraset-synced parquets and the job/UI config
 * untouched. After this returns, the next "Run analysis" click will
 * auto-trigger a fresh home detection (see /reports/poll's
 * home_detection phase) and rebuild every report from scratch.
 *
 * What gets deleted:
 *   1. Home table: Glue catalog entry + `home-locations/{ds}/` parquet
 *   2. Every file under `config/dataset-reports/{ds}/` (all 6 report
 *      types, every filter variant, and any subdirectories like
 *      `category-affinity/*`)
 *   3. `config/dataset-analysis/{ds}.json` — per-dataset basic analysis
 *   4. All polling/state files keyed by the dataset:
 *        - `config/dataset-report-state/{ds}.json`
 *        - `config/catchment-state/{ds}.json`
 *        - `config/analysis-state/{ds}.json`
 *
 * What stays:
 *   • Raw parquet data under `{datasetName}/...`
 *   • Job config, mega-jobs index, geocode caches, POI collections
 *   • Exports under `exports/{ds}/`
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

    // 2. Wipe EVERY object under config/dataset-reports/{ds}/ — all 6
    //    report types, all filter variants, and any subdirectories
    //    (category-affinity/*, zip-code-signals/*, etc).
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
        if (obj.Key) reportKeysToDelete.push(obj.Key);
      }
      continuationToken = list.IsTruncated ? list.NextContinuationToken : undefined;
    } while (continuationToken);

    if (reportKeysToDelete.length > 0) {
      // DeleteObjects caps at 1000 keys per call — chunk if needed.
      for (let i = 0; i < reportKeysToDelete.length; i += 1000) {
        const batch = reportKeysToDelete.slice(i, i + 1000).map((Key) => ({ Key }));
        await s3Client.send(new DeleteObjectsCommand({
          Bucket: BUCKET,
          Delete: { Objects: batch, Quiet: true },
        }));
      }
    }

    // 3. Delete the per-dataset state + analysis files so the next
    //    /reports/poll call re-enters from a clean slate (which
    //    triggers the home-detection phase) and the next Analyze
    //    click re-runs the basic dataset analysis.
    const stateKeys = [
      `config/dataset-analysis/${datasetName}.json`,
      `config/dataset-report-state/${datasetName}.json`,
      `config/catchment-state/${datasetName}.json`,
      `config/analysis-state/${datasetName}.json`,
    ];
    await s3Client.send(new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: { Objects: stateKeys.map((Key) => ({ Key })), Quiet: true },
    }));

    console.log(`[DATASET-RESET] ${datasetName}: dropped home table, removed ${reportKeysToDelete.length} report file(s), cleared ${stateKeys.length} state file(s)`);

    return NextResponse.json({
      ok: true,
      deleted: {
        homeTable: true,
        reports: reportKeysToDelete.length,
        state: stateKeys.length,
      },
      message: 'Dataset reset to fresh-from-Veraset state. Click "Run analysis" to rebuild everything from scratch.',
    });
  } catch (err: any) {
    console.error(`[DATASET-RESET] ${datasetName} error:`, err.message);
    return NextResponse.json(
      { error: err.message || 'Dataset reset failed' },
      { status: 500 },
    );
  }
}
