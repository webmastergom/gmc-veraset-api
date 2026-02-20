import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/jobs';
import { listS3Objects, parseS3Path } from '@/lib/s3';
import { checkRateLimit, getClientIdentifier } from '@/lib/security';
import { determineSyncStatus } from '@/lib/sync/determine-sync-status';
import type { SyncStatusResponse } from '@/lib/sync-types';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

function statusHeaders(rateLimit: { remaining: number; resetAt: number }) {
  return {
    'Cache-Control': 'no-cache',
    'X-RateLimit-Limit': '20',
    'X-RateLimit-Remaining': String(rateLimit.remaining - 1),
    'X-RateLimit-Reset': String(Math.floor(rateLimit.resetAt / 1000)),
  };
}

/**
 * GET /api/jobs/[id]/sync/status
 * Returns canonical sync status from job. No side effects (no repair in GET).
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const jobId = params?.id;
    if (!jobId) {
      return NextResponse.json(
        { status: 'error', message: 'Job ID required', progress: 0, total: 0, totalBytes: 0, copied: 0, copiedBytes: 0 } as SyncStatusResponse,
        { status: 400 }
      );
    }
    const clientId = getClientIdentifier(request);
    const rateLimit = checkRateLimit(`sync-status:${clientId}:${jobId}`, 20, 60000);

    if (!rateLimit.allowed) {
      return NextResponse.json(
        {
          status: 'error',
          message: 'Too many requests. Please wait before checking sync status again.',
        } as SyncStatusResponse,
        {
          status: 429,
          headers: {
            ...statusHeaders(rateLimit),
            'Retry-After': String(Math.ceil((rateLimit.resetAt - Date.now()) / 1000)),
          },
        }
      );
    }

    const job = await getJob(jobId);
    if (!job) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    if (!job.s3SourcePath) {
      const response = determineSyncStatus(job);
      return NextResponse.json(response, {
        status: 200,
        headers: { ...statusHeaders(rateLimit), 'Cache-Control': 'public, max-age=60' },
      });
    }

    if (!job.s3DestPath) {
      const response = determineSyncStatus(job);
      return NextResponse.json(response, {
        status: 200,
        headers: { ...statusHeaders(rateLimit), 'Cache-Control': 'public, max-age=30' },
      });
    }

    // When we have dest path and expected totals, use stored progress only (no S3 listing in GET)
    let response: SyncStatusResponse = determineSyncStatus(job);

    // Backward compatibility: if no expected totals yet (e.g. very first poll), list source once
    const hasExpected = (job.expectedObjectCount ?? 0) > 0 || (job.expectedTotalBytes ?? 0) > 0;
    if (!hasExpected && (job.objectCount ?? 0) === 0) {
      try {
        const sourcePath = parseS3Path(job.s3SourcePath);
        const sourceObjects = await listS3Objects(sourcePath.bucket, sourcePath.key);
        const totalObjects = sourceObjects.length;
        const totalBytes = sourceObjects.reduce((sum, o) => sum + o.Size, 0);
        response = {
          ...response,
          total: totalObjects,
          totalBytes,
          message: totalObjects > 0 ? `Syncing... 0/${totalObjects} objects` : response.message,
        };
      } catch (err) {
        console.warn('[SYNC STATUS] List source failed:', err instanceof Error ? err.message : err);
      }
    }

    const cacheControl = response.status === 'syncing' ? 'no-cache' : 'public, max-age=60';
    return NextResponse.json(response, {
      status: 200,
      headers: { ...statusHeaders(rateLimit), 'Cache-Control': cacheControl },
    });
  } catch (error: unknown) {
    console.error('GET /api/jobs/[id]/sync/status error:', error);
    return NextResponse.json(
      {
        status: 'error',
        message: error instanceof Error ? error.message : 'Failed to check sync status',
        progress: 0,
        total: 0,
        totalBytes: 0,
        copied: 0,
        copiedBytes: 0,
      } as SyncStatusResponse,
      { status: 500 }
    );
  }
}
