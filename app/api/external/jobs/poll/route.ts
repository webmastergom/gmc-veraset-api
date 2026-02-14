import { NextRequest, NextResponse } from 'next/server';
import { waitUntil } from '@vercel/functions';
import { getAllJobs, getJob, updateJobStatus, markJobSynced } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { notifyWebhook } from '@/lib/webhooks';
import { logger } from '@/lib/logger';
import { listS3Objects, copyS3Object, parseS3Path } from '@/lib/s3';
import { BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/external/jobs/poll
 * Batch-check all non-terminal jobs against Veraset API and auto-sync
 * any that have completed. Cloud-garritz calls this periodically.
 *
 * Returns the list of jobs whose status changed.
 */
export async function POST(request: NextRequest) {
  try {
    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // 2. Get all jobs, filter non-terminal
    const allJobs = await getAllJobs();
    const pendingJobs = allJobs.filter(
      (j) => j.status === 'QUEUED' || j.status === 'RUNNING' || j.status === 'SCHEDULED'
    );

    if (pendingJobs.length === 0) {
      return NextResponse.json({ updated: [], pending: 0 });
    }

    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) {
      return NextResponse.json(
        { error: 'VERASET_API_KEY not configured' },
        { status: 500 }
      );
    }

    // 3. Check each pending job against Veraset (sequentially to avoid rate limits)
    const updated: Array<{
      job_id: string;
      name: string | null;
      old_status: string;
      new_status: string;
      synced: boolean;
    }> = [];

    for (const job of pendingJobs) {
      try {
        const verasetRes = await fetch(
          `https://platform.prd.veraset.tech/v1/job/${job.jobId}`,
          {
            cache: 'no-store',
            headers: {
              'X-API-Key': verasetApiKey,
              'Content-Type': 'application/json',
            },
          }
        );

        if (!verasetRes.ok) continue;

        const verasetData = await verasetRes.json();
        const newStatus = verasetData.status || verasetData.data?.status;

        if (!newStatus || newStatus === job.status) continue;

        // Status changed — update locally
        await updateJobStatus(
          job.jobId,
          newStatus,
          verasetData.error_message || verasetData.data?.error_message
        );

        logger.log(`[poll] Job ${job.jobId} status: ${job.status} -> ${newStatus}`);

        let synced = false;

        // Auto-sync if SUCCESS and not yet synced
        if (newStatus === 'SUCCESS' && !job.s3DestPath && job.s3SourcePath) {
          try {
            const sourcePath = parseS3Path(job.s3SourcePath);
            const destKey = `${job.jobId}/`;
            const sourceObjects = await listS3Objects(sourcePath.bucket, sourcePath.key);

            let copied = 0;
            let totalBytes = 0;

            for (const obj of sourceObjects) {
              if (!obj.Key) continue;
              try {
                const relativeKey = obj.Key.replace(sourcePath.key, '');
                await copyS3Object(sourcePath.bucket, obj.Key, BUCKET, `${destKey}${relativeKey}`);
                copied++;
                totalBytes += obj.Size || 0;
              } catch {
                // Continue on individual file errors
              }
            }

            if (copied > 0) {
              await markJobSynced(job.jobId, `s3://${BUCKET}/${destKey}`, copied, totalBytes);
              synced = true;
              logger.log(`[poll] Auto-synced job ${job.jobId}: ${copied} files, ${totalBytes} bytes`);
            }
          } catch (syncErr: any) {
            logger.warn(`[poll] Auto-sync failed for ${job.jobId}`, { error: syncErr.message });
          }
        }

        // Webhook notification (fire-and-forget)
        if (job.webhookUrl) {
          waitUntil(
            notifyWebhook(
              { ...job, status: newStatus },
              job.status,
              newStatus
            ).catch(err => logger.warn('Webhook notification failed', { error: err.message }))
          );
        }

        updated.push({
          job_id: job.jobId,
          name: job.name || null,
          old_status: job.status,
          new_status: newStatus,
          synced,
        });
      } catch (err: any) {
        logger.warn(`[poll] Failed to check job ${job.jobId}`, { error: err.message });
      }
    }

    return NextResponse.json({
      updated,
      pending: pendingJobs.length - updated.length,
    });
  } catch (error: any) {
    logger.error('POST /api/external/jobs/poll error:', error);
    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message },
      { status: 500 }
    );
  }
}
