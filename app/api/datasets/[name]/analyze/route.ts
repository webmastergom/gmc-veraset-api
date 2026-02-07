import { NextRequest, NextResponse } from 'next/server';
import { analyzeDataset, AnalysisFilters } from '@/lib/dataset-analyzer';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300; // Allow up to 5 minutes for Athena queries (Vercel max)

// Export POST handler - must be named export
export async function POST(
  request: NextRequest,
  { params }: { params: { name: string } }
) {
  try {
    const datasetName = params.name;

    if (!datasetName) {
      return NextResponse.json(
        { error: 'Dataset name is required' },
        { status: 400 }
      );
    }

    const body = await request.json().catch(() => ({}));
    const filters: AnalysisFilters = body.filters || {};

    const result = await analyzeDataset(datasetName, filters);

    return NextResponse.json(result);

  } catch (error: any) {
    const datasetName = params?.name || 'unknown';
    console.error(`POST /api/datasets/${datasetName}/analyze error:`, error);
    console.error('Error stack:', error.stack);
    
    // Provide more helpful error messages
    let errorMessage = error.message || 'Unknown error';
    let statusCode = 500;
    
    if (errorMessage.includes('AWS credentials not configured')) {
      statusCode = 503;
      errorMessage = 'AWS credentials not configured. Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.';
    } else if (errorMessage.includes('database') || errorMessage.includes('Database')) {
      statusCode = 503;
      errorMessage = `Athena database 'veraset' not found. Please create it in AWS Glue: aws glue create-database --database-input '{"Name": "veraset"}'`;
    } else if (errorMessage.includes('Access denied')) {
      statusCode = 403;
    }
    
    return NextResponse.json(
      { 
        error: 'Analysis failed', 
        details: errorMessage,
        hint: errorMessage.includes('database') ? 'Run: aws glue create-database --database-input \'{"Name": "veraset"}\'' : undefined
      },
      { status: statusCode }
    );
  }
}
