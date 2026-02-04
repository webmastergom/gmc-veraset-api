import { NextRequest, NextResponse } from "next/server";
import { auditAllPOICollections, auditAllJobs } from "@/lib/audit";

export const dynamic = 'force-dynamic';
export const revalidate = 0;

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const type = searchParams.get('type') || 'all';
    
    if (type === 'pois') {
      const results = await auditAllPOICollections();
      return NextResponse.json({
        type: 'pois',
        results,
        summary: {
          total: results.length,
          withIssues: results.filter(r => r.issues.length > 0).length,
          totalDiscrepancy: results.reduce((sum, r) => sum + Math.abs(r.discrepancy), 0),
        },
      });
    } else if (type === 'jobs') {
      const results = await auditAllJobs();
      return NextResponse.json({
        type: 'jobs',
        results,
        summary: {
          total: results.length,
          withIssues: results.filter(r => r.issues.length > 0).length,
          totalDiscrepancy: results.reduce((sum, r) => sum + Math.abs(r.discrepancy), 0),
        },
      });
    } else {
      // Audit both
      const [poiResults, jobResults] = await Promise.all([
        auditAllPOICollections(),
        auditAllJobs(),
      ]);
      
      return NextResponse.json({
        type: 'all',
        pois: {
          results: poiResults,
          summary: {
            total: poiResults.length,
            withIssues: poiResults.filter(r => r.issues.length > 0).length,
            totalDiscrepancy: poiResults.reduce((sum, r) => sum + Math.abs(r.discrepancy), 0),
          },
        },
        jobs: {
          results: jobResults,
          summary: {
            total: jobResults.length,
            withIssues: jobResults.filter(r => r.issues.length > 0).length,
            totalDiscrepancy: jobResults.reduce((sum, r) => sum + Math.abs(r.discrepancy), 0),
          },
        },
      });
    }
  } catch (error: any) {
    console.error('Audit error:', error);
    return NextResponse.json(
      { error: 'Failed to run audit', details: error.message },
      { status: 500 }
    );
  }
}
