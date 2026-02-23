import { NextRequest, NextResponse } from "next/server";
import { getJob, updateJobStatus } from "@/lib/jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/jobs/refresh?ids=id1,id2,id3
 *
 * Check Veraset for specific jobs and update S3.
 * Pass job IDs as comma-separated query param to SKIP the expensive getAllJobs() call.
 * Without ?ids, falls back to reading all jobs (slow, ~11s on cold start).
 *
 * Processes jobs one at a time with a 3s timeout per Veraset call.
 * Stops before hitting Vercel's 10s limit.
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  const startTime = Date.now();
  const MAX_TOTAL_MS = 8000;

  try {
    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) {
      return NextResponse.json({ error: 'VERASET_API_KEY not configured' }, { status: 500 });
    }

    // Fast path: specific job IDs passed as query param (skips getAllJobs)
    const idsParam = request.nextUrl.searchParams.get('ids');

    let jobsToCheck: Array<{ jobId: string; name: string; status: string }> = [];

    if (idsParam) {
      const ids = idsParam.split(',').map(id => id.trim()).filter(Boolean);
      console.log(`[REFRESH] Fast path: checking ${ids.length} specific jobs`);

      // Read each job individually (each ~1s, much faster than getAllJobs ~11s)
      for (const id of ids) {
        if (Date.now() - startTime > MAX_TOTAL_MS) break;
        const job = await getJob(id);
        if (job && (job.status === 'QUEUED' || job.status === 'RUNNING' || job.status === 'SCHEDULED')) {
          jobsToCheck.push({ jobId: job.jobId, name: job.name, status: job.status });
        }
      }
    } else {
      // Slow fallback: read all jobs
      const { getAllJobs } = await import('@/lib/jobs');
      const allJobs = await getAllJobs();
      jobsToCheck = allJobs
        .filter(j => j.status === 'QUEUED' || j.status === 'RUNNING' || j.status === 'SCHEDULED')
        .map(j => ({ jobId: j.jobId, name: j.name, status: j.status }));
    }

    console.log(`[REFRESH] ${jobsToCheck.length} non-terminal jobs to check [${Date.now()-startTime}ms]`);

    if (jobsToCheck.length === 0) {
      return NextResponse.json({ message: 'No non-terminal jobs to refresh', updated: 0 });
    }

    const results: Array<{ jobId: string; name: string; before: string; after: string; error?: string }> = [];
    let stoppedEarly = false;

    for (const job of jobsToCheck) {
      const elapsed = Date.now() - startTime;
      if (elapsed > MAX_TOTAL_MS) {
        stoppedEarly = true;
        break;
      }

      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 3000);

        const res = await fetch(
          `https://platform.prd.veraset.tech/v1/job/${job.jobId}`,
          {
            cache: 'no-store',
            signal: controller.signal,
            headers: { 'X-API-Key': verasetApiKey, 'Content-Type': 'application/json' },
          }
        );
        clearTimeout(timeout);

        if (res.ok) {
          const data = await res.json();
          const newStatus = data.status || data.data?.status;
          const errorMsg = data.error_message || data.data?.error_message;

          if (newStatus && newStatus !== job.status) {
            console.log(`[REFRESH] ${job.jobId}: ${job.status} -> ${newStatus}`);
            await updateJobStatus(job.jobId, newStatus, errorMsg);
            results.push({ jobId: job.jobId, name: job.name, before: job.status, after: newStatus });
          } else {
            results.push({ jobId: job.jobId, name: job.name, before: job.status, after: job.status });
          }
        } else {
          results.push({ jobId: job.jobId, name: job.name, before: job.status, after: job.status, error: `Veraset ${res.status}` });
        }
      } catch (err: any) {
        const isAbort = err.name === 'AbortError';
        results.push({ jobId: job.jobId, name: job.name, before: job.status, after: job.status, error: isAbort ? 'timeout' : err.message });
      }
    }

    const updated = results.filter(r => r.before !== r.after);
    const remaining = jobsToCheck.length - results.length;
    const elapsedTotal = Date.now() - startTime;

    console.log(`[REFRESH] Done in ${elapsedTotal}ms. Updated ${updated.length}/${results.length}. ${remaining} remaining.`);

    return NextResponse.json({
      message: stoppedEarly
        ? `Checked ${results.length} of ${jobsToCheck.length} jobs (stopped at ${elapsedTotal}ms). Call again for remaining ${remaining}.`
        : `Refreshed ${updated.length} of ${jobsToCheck.length} non-terminal jobs`,
      updated: updated.length,
      checked: results.length,
      total: jobsToCheck.length,
      remaining,
      stoppedEarly,
      elapsedMs: elapsedTotal,
      results,
    });

  } catch (error: any) {
    console.error('[REFRESH] Error:', error);
    return NextResponse.json(
      { error: 'Failed to refresh jobs', details: error.message },
      { status: 500 }
    );
  }
}
