import { NextRequest, NextResponse } from 'next/server';
import { getJob, updateJobStatus, markJobSynced } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { notifyWebhook } from '@/lib/webhooks';
import { logger } from '@/lib/logger';
import { listS3Objects, copyS3Object, parseS3Path } from '@/lib/s3';
import { BUCKET } from '@/lib/s3-config';
import { tableExists, createTableForDataset, getTableName, runQuery } from '@/lib/athena';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * GET /api/external/jobs/[datasetName]/status
 * Get job status with auto-refresh from Veraset API.
 * The datasetName param here is actually a jobId.
 */
export async function GET(
  request: NextRequest,
  { params }: { params: { datasetName: string } }
) {
  const jobId = params.datasetName;

  try {
    // 1. Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // 2. Find job
    const job = await getJob(jobId);
    if (!job) {
      return NextResponse.json(
        { error: 'Not Found', message: `Job '${jobId}' not found.` },
        { status: 404 }
      );
    }

    // 3. If not terminal, check Veraset for updated status
    const terminalStatuses = ['SUCCESS', 'FAILED'];
    let currentStatus = job.status;

    if (!terminalStatuses.includes(job.status)) {
      try {
        const verasetApiKey = process.env.VERASET_API_KEY?.trim();
        const verasetRes = await fetch(`https://platform.prd.veraset.tech/v1/job/${jobId}`, {
          cache: 'no-store',
          headers: {
            'X-API-Key': verasetApiKey || '',
            'Content-Type': 'application/json',
          },
        });

        if (verasetRes.ok) {
          const verasetData = await verasetRes.json();
          const newStatus = verasetData.status || verasetData.data?.status;

          if (newStatus && newStatus !== job.status) {
            const oldStatus = job.status;
            await updateJobStatus(
              jobId,
              newStatus,
              verasetData.error_message || verasetData.data?.error_message
            );
            currentStatus = newStatus;

            logger.log(`Job ${jobId} status: ${oldStatus} -> ${newStatus}`);

            // Auto-sync when job becomes SUCCESS
            if (newStatus === 'SUCCESS' && !job.s3DestPath) {
              await autoSyncJob(jobId, job.s3SourcePath);
            }

            // Notify webhook if registered (fire-and-forget)
            notifyWebhook(
              { ...job, status: newStatus },
              oldStatus,
              newStatus
            ).catch(err => logger.warn('Webhook notification failed', { error: err.message }));
          }
        }
      } catch (err: any) {
        logger.warn(`Failed to check Veraset status for ${jobId}`, { error: err.message });
      }
    }

    // 4. Build response based on status
    if (currentStatus === 'SUCCESS') {
      const results = await getJobResults(jobId, job);
      return NextResponse.json({
        job_id: jobId,
        status: currentStatus,
        created_at: job.createdAt,
        completed_at: job.updatedAt,
        results,
        // Original POIs with coordinates for downstream validation
        pois: job.externalPois || [],
      });
    }

    if (currentStatus === 'FAILED') {
      return NextResponse.json({
        job_id: jobId,
        status: currentStatus,
        created_at: job.createdAt,
        error: job.errorMessage || 'Job processing failed',
        pois: job.externalPois || [],
      });
    }

    // QUEUED or RUNNING
    return NextResponse.json({
      job_id: jobId,
      status: currentStatus,
      created_at: job.createdAt,
      updated_at: job.updatedAt,
      pois: job.externalPois || [],
    });

  } catch (error: any) {
    logger.error(`GET /api/external/jobs/${jobId}/status error:`, error);
    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message },
      { status: 500 }
    );
  }
}

/**
 * Auto-sync job data from Veraset S3 to our S3 bucket
 */
async function autoSyncJob(jobId: string, s3SourcePath?: string): Promise<void> {
  if (!s3SourcePath) return;

  try {
    const sourcePath = parseS3Path(s3SourcePath);
    const destKey = `${jobId}/`;
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
      await markJobSynced(jobId, `s3://${BUCKET}/${destKey}`, copied, totalBytes);
      logger.log(`Auto-synced job ${jobId}: ${copied} files, ${totalBytes} bytes`);
    }
  } catch (err: any) {
    logger.warn(`Auto-sync failed for ${jobId}`, { error: err.message });
  }
}

/**
 * Get job results (metrics) from Athena
 */
async function getJobResults(jobId: string, job: any): Promise<Record<string, any>> {
  const results: Record<string, any> = {
    date_range: job.dateRange,
    poi_count: job.poiCount,
    catchment: {
      available: true,
      url: `/api/external/jobs/${jobId}/catchment`,
    },
  };

  // Get dataset name from s3DestPath
  let datasetName: string | null = null;
  if (job.s3DestPath) {
    const s3Path = job.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
    datasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');
  }

  if (!datasetName) return results;

  try {
    if (!(await tableExists(datasetName))) {
      await createTableForDataset(datasetName);
    }

    const tableName = getTableName(datasetName);

    // Query total pings and devices
    const summaryResult = await runQuery(`
      SELECT COUNT(*) as total_pings, COUNT(DISTINCT ad_id) as total_devices
      FROM ${tableName}
    `);
    if (summaryResult.rows.length > 0) {
      results.total_pings = Number(summaryResult.rows[0].total_pings) || 0;
      results.total_devices = Number(summaryResult.rows[0].total_devices) || 0;
    }

    // Query per-POI summary
    const poiMapping = job.poiMapping || {};
    const poiNames = job.poiNames || {};

    const poiResult = await runQuery(`
      SELECT poi_ids[1] as poi_id, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
      FROM ${tableName}
      WHERE poi_ids[1] IS NOT NULL
      GROUP BY poi_ids[1]
      ORDER BY pings DESC
    `);
    results.poi_summary = poiResult.rows.map((row: any) => {
      const verasetId = String(row.poi_id);
      const originalId = poiMapping[verasetId] || verasetId;
      return {
        poi_id: originalId,
        name: poiNames[verasetId] || originalId,
        pings: Number(row.pings) || 0,
        devices: Number(row.devices) || 0,
      };
    });
  } catch (err: any) {
    logger.warn(`Failed to get results for job ${jobId}`, { error: err.message });
    results.total_pings = null;
    results.total_devices = null;
    results.poi_summary = [];
  }

  return results;
}
