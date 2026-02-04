import { NextRequest, NextResponse } from "next/server";
import { getJob, markJobSynced } from "@/lib/jobs";
import { listS3Objects, copyS3Object, parseS3Path } from "@/lib/s3";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function POST(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const body = await request.json();
    const { destPath } = body;

    if (!destPath) {
      return NextResponse.json(
        { error: 'destPath is required' },
        { status: 400 }
      );
    }

    // Get job details
    const job = await getJob(params.id);
    
    if (!job) {
      return NextResponse.json(
        { error: 'Job not found' },
        { status: 404 }
      );
    }

    if (job.status !== 'SUCCESS') {
      return NextResponse.json(
        { error: 'Job must be SUCCESS before syncing' },
        { status: 400 }
      );
    }

    if (!job.s3SourcePath) {
      return NextResponse.json(
        { error: 'Job has no source path' },
        { status: 400 }
      );
    }

    // Parse S3 paths
    const sourcePath = parseS3Path(job.s3SourcePath);
    const destPathParsed = parseS3Path(destPath);

    // List source objects
    const sourceObjects = await listS3Objects(sourcePath.bucket, sourcePath.key);

    if (sourceObjects.length === 0) {
      return NextResponse.json(
        { error: 'No objects found in source path' },
        { status: 404 }
      );
    }

    // Copy objects
    let copied = 0;
    let totalBytes = 0;
    const errors: string[] = [];

    for (const obj of sourceObjects) {
      if (!obj.Key) continue;

      try {
        const destKey = `${destPathParsed.key}${obj.Key.replace(sourcePath.key, '')}`;
        await copyS3Object(
          sourcePath.bucket,
          obj.Key,
          destPathParsed.bucket,
          destKey
        );
        copied++;
        totalBytes += obj.Size || 0;
      } catch (err: any) {
        errors.push(`Failed to copy ${obj.Key}: ${err.message || 'Unknown error'}`);
      }
    }

    // Update job record
    await markJobSynced(params.id, destPath, copied, totalBytes);

    return NextResponse.json({
      success: true,
      copied,
      totalBytes,
      errors: errors.length > 0 ? errors : undefined,
    });
  } catch (error: any) {
    console.error('POST /api/jobs/[id]/sync error:', error);
    return NextResponse.json(
      { error: 'Failed to sync job', details: error.message },
      { status: 500 }
    );
  }
}
