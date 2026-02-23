import { NextRequest, NextResponse } from "next/server";
import { getAllJobs, updateJobStatus } from "@/lib/jobs";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/jobs/refresh
 * One-shot: check Veraset for ALL non-terminal jobs and update S3.
 * Call this when stale jobs need their status refreshed in bulk.
 *
 * Changed from POST to GET because POST requests systematically return
 * 405 Method Not Allowed on the current Vercel deployment.
 */
export async function GET(request: NextRequest): Promise<NextResponse> {
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

    // Check each job sequentially to avoid overwhelming Veraset API
    // Use 6s timeout per job to stay within Vercel's 10s function limit
    // (we process sequentially, so we can only check ~1 job before timeout)
    // For bulk refreshes with many jobs, call this endpoint multiple times
    for (const job of nonTerminal) {
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 6000); // 6s timeout per job

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
        console.warn(`[REFRESH] Error checking ${job.jobId}:`, err.message);
        results.push({ jobId: job.jobId, name: job.name, before: job.status, after: job.status, error: err.message });
      }
    }

    const updated = results.filter(r => r.before !== r.after);
    console.log(`[REFRESH] Done. Updated ${updated.length}/${nonTerminal.length} jobs`);

    return NextResponse.json({
      message: `Refreshed ${updated.length} of ${nonTerminal.length} non-terminal jobs`,
      updated: updated.length,
      total: nonTerminal.length,
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
