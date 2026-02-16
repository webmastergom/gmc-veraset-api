import { NextRequest, NextResponse } from 'next/server';
import { getAllJobs } from '@/lib/jobs';
import { listS3Objects, parseS3Path, normalizePrefix } from '@/lib/s3';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 120;

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/**
 * POST /api/datasets/[name]/audit
 * Compare our S3 dataset folder with Veraset source. Returns asymmetries (missing in dest, extra in dest).
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  try {
    const params = await context.params;
    const datasetName = params.name;

    if (!datasetName) {
      return NextResponse.json(
        { error: 'Dataset name is required' },
        { status: 400 }
      );
    }

    const jobs = await getAllJobs();
    const job = jobs.find((j) => {
      if (!j.s3DestPath) return false;
      const path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
      const folder = path.split('/').filter(Boolean)[0] || path.replace(/\/$/, '');
      return folder === datasetName;
    });

    if (!job) {
      return NextResponse.json(
        { error: 'No job found for this dataset', datasetName },
        { status: 404 }
      );
    }

    if (!job.s3SourcePath) {
      return NextResponse.json(
        { error: 'Job has no Veraset source path', jobId: job.jobId },
        { status: 400 }
      );
    }

    const sourcePath = parseS3Path(job.s3SourcePath);
    const destPath = {
      bucket: BUCKET,
      key: normalizePrefix(datasetName),
    };

    const [sourceObjects, destObjects] = await Promise.all([
      listS3Objects(sourcePath.bucket, sourcePath.key),
      listS3Objects(destPath.bucket, destPath.key),
    ]);

    // Filter to only .parquet files (same as card display) - ignore metadata/index files
    const sourceParquet = sourceObjects.filter((o) => o.Key.endsWith('.parquet'));
    const destParquet = destObjects.filter((o) => o.Key.endsWith('.parquet'));

    const sourcePrefix = normalizePrefix(sourcePath.key);
    const destPrefix = normalizePrefix(destPath.key);

    const toRelative = (key: string, prefix: string) =>
      prefix ? key.replace(prefix, '').replace(/^\/+/, '') : key;

    const sourceRel = new Set(
      sourceParquet.map((o) => toRelative(o.Key, sourcePrefix))
    );
    const destRel = new Set(
      destParquet.map((o) => toRelative(o.Key, destPrefix))
    );

    const missingInDest = sourceParquet
      .map((o) => toRelative(o.Key, sourcePrefix))
      .filter((rel) => !destRel.has(rel));
    const extraInDest = destParquet
      .map((o) => toRelative(o.Key, destPrefix))
      .filter((rel) => !sourceRel.has(rel));

    const symmetric =
      missingInDest.length === 0 && extraInDest.length === 0;

    return NextResponse.json({
      datasetName,
      jobId: job.jobId,
      jobName: job.name,
      sourcePath: job.s3SourcePath,
      destPath: `s3://${BUCKET}/${destPath.key}`,
      sourceCount: sourceParquet.length,
      destCount: destParquet.length,
      missingInDestCount: missingInDest.length,
      extraInDestCount: extraInDest.length,
      missingInDest: missingInDest.slice(0, 200),
      extraInDest: extraInDest.slice(0, 200),
      symmetric,
      message: symmetric
        ? 'Dataset matches Veraset source.'
        : `Asymmetry: ${missingInDest.length} missing in our S3, ${extraInDest.length} extra in our S3. Re-sync to fix.`,
    });
  } catch (error: any) {
    console.error('[AUDIT]', error);
    return NextResponse.json(
      {
        error: 'Audit failed',
        details: error.message || 'Unknown error',
      },
      { status: 500 }
    );
  }
}
