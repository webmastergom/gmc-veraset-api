import { NextRequest, NextResponse } from "next/server";
import { getAllJobs, updateJobStatus } from "@/lib/jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/jobs/refresh
 * Check Veraset for non-terminal jobs and update S3.
 *
 * Processes jobs one at a time with a 4s timeout per Veraset call.
 * Tracks elapsed time and stops before hitting Vercel's 10s limit.
 * Call multiple times if you have many stale jobs.
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
  const startTime = Date.now();
  const MAX_TOTAL_MS = 8000; // Stop processing after 8s to stay within 10s limit

  try {
    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) {
      return NextResponse.json({ error: 'VERASET_API_KEY not configured' }, { status: 500 });
    }

    const allJobs = await getAllJobs();
    const nonTerminal = allJobs.filter(
      (j) => j.status === 'QUEUED' || j.status === 'RUNNING' || j.status === 'SCHEDULED'
    );

    console.log(`[REFRESH] Found ${nonTerminal.length} non-terminal jobs to check`);

    if (nonTerminal.length === 0) {
      return NextResponse.json({ message: 'No non-terminal jobs to refresh', updated: 0 });
    }

    const results: Array<{ jobId: string; name: string; before: string; after: string; error?: string }> = [];
    let stoppedEarly = false;

    for (const job of nonTerminal) {
      // Check if we're running out of time BEFORE starting another API call
      const elapsed = Date.now() - startTime;
      if (elapsed > MAX_TOTAL_MS) {
        console.log(`[REFRESH] Stopping early after ${elapsed}ms (processed ${results.length}/${nonTerminal.length})`);
        stoppedEarly = true;
        break;
      }

      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 4000); // 4s per job

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
            console.log(`[REFRESH] ${job.jobId} (${job.name}): ${job.status} -> ${newStatus}`);
            await updateJobStatus(job.jobId, newStatus, errorMsg);
            results.push({ jobId: job.jobId, name: job.name, before: job.status, after: newStatus });
          } else {
            results.push({ jobId: job.jobId, name: job.name, before: job.status, after: job.status });
          }
        } else {
          const errText = await res.text().catch(() => '');
          console.warn(`[REFRESH] Veraset ${res.status} for ${job.jobId}: ${errText.substring(0, 200)}`);
          results.push({ jobId: job.jobId, name: job.name, before: job.status, after: job.status, error: `Veraset ${res.status}` });
        }
      } catch (err: any) {
        const isAbort = err.name === 'AbortError';
        console.warn(`[REFRESH] ${isAbort ? 'Timeout' : 'Error'} checking ${job.jobId}:`, err.message);
        results.push({ jobId: job.jobId, name: job.name, before: job.status, after: job.status, error: isAbort ? 'timeout' : err.message });
      }
    }

    const updated = results.filter(r => r.before !== r.after);
    const remaining = nonTerminal.length - results.length;
    const elapsedTotal = Date.now() - startTime;

    console.log(`[REFRESH] Done in ${elapsedTotal}ms. Updated ${updated.length}/${results.length} checked. ${remaining} remaining.`);

    return NextResponse.json({
      message: stoppedEarly
        ? `Checked ${results.length} of ${nonTerminal.length} jobs (stopped at ${elapsedTotal}ms to avoid timeout). Call again to process remaining ${remaining}.`
        : `Refreshed ${updated.length} of ${nonTerminal.length} non-terminal jobs`,
      updated: updated.length,
      checked: results.length,
      total: nonTerminal.length,
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
