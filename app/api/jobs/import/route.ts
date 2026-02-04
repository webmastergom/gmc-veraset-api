import { NextRequest, NextResponse } from 'next/server'
import { createJob } from '@/lib/jobs'

export const dynamic = 'force-dynamic'
export const revalidate = 0

/**
 * Import an external job (created outside the app)
 * External jobs do NOT count toward monthly API quota
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { jobId, name, type, poiCount, dateRange, s3SourcePath, s3DestPath, status, summaryMetrics, objectCount, totalBytes } = body

    // Validate required fields
    if (!jobId || !name || !dateRange) {
      return NextResponse.json(
        { error: 'Missing required fields: jobId, name, dateRange' },
        { status: 400 }
      )
    }

    // Create job marked as external - does NOT increment usage
    const job = await createJob({
      jobId,
      name,
      type: type || 'pings',
      poiCount: poiCount || 1,
      dateRange: typeof dateRange === 'object' ? dateRange : {
        from: dateRange.from_date || dateRange.from,
        to: dateRange.to_date || dateRange.to,
      },
      radius: 10,
      schema: 'BASIC',
      status: status || 'SUCCESS',
      s3SourcePath: s3SourcePath || `s3://veraset-prd-platform-us-west-2/output/garritz/${jobId}/`,
      s3DestPath: s3DestPath || null,
      syncedAt: s3DestPath ? new Date().toISOString() : null,
      objectCount,
      totalBytes,
      summaryMetrics,
      external: true, // Key difference: marked as external
    })

    // Do NOT call incrementUsage() for external jobs

    return NextResponse.json({ 
      success: true, 
      job,
      message: 'External job imported successfully (not counted toward quota)'
    })

  } catch (error: any) {
    console.error('POST /api/jobs/import error:', error)
    return NextResponse.json(
      { error: 'Failed to import external job', details: error.message },
      { status: 500 }
    )
  }
}
