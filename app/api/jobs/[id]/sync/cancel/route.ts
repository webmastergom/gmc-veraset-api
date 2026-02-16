import { NextRequest, NextResponse } from 'next/server';
import { getJob, cancelSyncJob, releaseSyncLock } from '@/lib/jobs';
import { abortSync } from '@/lib/sync/sync-abort-registry';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * POST /api/jobs/[id]/sync/cancel
 * Cooperatively stop sync: signal in-process sync to abort, release lock, then update DB.
 * Releasing the lock allows resync even when the previous sync crashed without releasing.
 */
export async function POST(
  _request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;

    const job = await getJob(params.id);
    if (!job) {
      return NextResponse.json({ error: 'Job not found' }, { status: 404 });
    }

    const aborted = abortSync(params.id);
    if (aborted) {
      console.log(`[SYNC] Abort signal sent for job ${params.id}`);
    }

    await releaseSyncLock(params.id);
    console.log(`[SYNC] Lock released for job ${params.id} (allows resync)`);

    const { action } = await cancelSyncJob(params.id);

    return NextResponse.json({
      success: true,
      action,
      message:
        action === 'completed'
          ? 'Sync marked as complete (data was already there)'
          : 'Sync stopped. You can start a new sync.',
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to stop sync';
    console.error('POST /api/jobs/[id]/sync/cancel error:', error);
    return NextResponse.json(
      { error: 'Failed to stop sync', details: message },
      { status: 500 }
    );
  }
}
