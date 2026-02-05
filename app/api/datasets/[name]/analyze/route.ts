import { NextRequest, NextResponse } from 'next/server';
import { analyzeDataset, AnalysisFilters } from '@/lib/dataset-analyzer';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300; // Allow up to 5 minutes for Athena queries (Vercel max)

// Handle unsupported methods - return clear error message
export async function GET(
  req: NextRequest,
  { params }: { params: { name: string } }
) {
  return NextResponse.json(
    { 
      error: 'Method Not Allowed', 
      message: 'This endpoint only accepts POST requests. Use POST to analyze the dataset.',
      hint: 'Make sure you are using POST method with a JSON body containing optional filters.'
    },
    { status: 405, headers: { 'Allow': 'POST' } }
  );
}

// Handle other unsupported methods
export async function PUT() {
  return NextResponse.json(
    { error: 'Method Not Allowed', message: 'This endpoint only accepts POST requests.' },
    { status: 405, headers: { 'Allow': 'POST' } }
  );
}

export async function DELETE() {
  return NextResponse.json(
    { error: 'Method Not Allowed', message: 'This endpoint only accepts POST requests.' },
    { status: 405, headers: { 'Allow': 'POST' } }
  );
}

export async function PATCH() {
  return NextResponse.json(
    { error: 'Method Not Allowed', message: 'This endpoint only accepts POST requests.' },
    { status: 405, headers: { 'Allow': 'POST' } }
  );
}

export async function POST(
  req: NextRequest,
  { params }: { params: { name: string } }
) {
  try {
    const body = await req.json().catch(() => ({}));
    const filters: AnalysisFilters = body.filters || {};

    const result = await analyzeDataset(params.name, filters);

    return NextResponse.json(result);

  } catch (error: any) {
    console.error(`POST /api/datasets/${params.name}/analyze error:`, error);
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
