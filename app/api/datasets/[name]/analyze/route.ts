import { NextRequest, NextResponse } from 'next/server';
import { analyzeDataset, AnalysisFilters } from '@/lib/dataset-analyzer';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300;

/**
 * POST /api/datasets/[name]/analyze
 * Analyze a dataset with optional filters
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> | { name: string } }
): Promise<NextResponse> {
  let datasetName: string | undefined;
  
  try {
    // Handle params - Next.js 14+ may pass params as Promise, 13 uses direct object
    let params: { name: string };
    if (context.params instanceof Promise) {
      params = await context.params;
    } else {
      params = context.params;
    }
    datasetName = params.name;

    if (!datasetName || typeof datasetName !== 'string') {
      return NextResponse.json(
        { 
          error: 'Dataset name is required',
          received: datasetName 
        },
        { status: 400 }
      );
    }

    // Parse request body
    let body: { filters?: AnalysisFilters } = {};
    try {
      body = await request.json();
    } catch (e) {
      // Body is optional, continue with empty filters
      console.log('No body provided, using empty filters');
    }

    const filters: AnalysisFilters = body.filters || {};

    console.log(`[ANALYZE] Starting analysis for dataset: ${datasetName}`, {
      filters,
      timestamp: new Date().toISOString()
    });

    // Run analysis
    const result = await analyzeDataset(datasetName, filters);

    console.log(`[ANALYZE] Analysis completed successfully for: ${datasetName}`);

    return NextResponse.json(result, {
      status: 200,
      headers: {
        'Content-Type': 'application/json',
      },
    });

  } catch (error: any) {
    const errorDatasetName = datasetName || 'unknown';
    console.error(`[ANALYZE ERROR] POST /api/datasets/${errorDatasetName}/analyze:`, {
      error: error.message,
      stack: error.stack,
      name: error.name,
    });
    
    // Determine status code and error message
    let errorMessage = error.message || 'Unknown error occurred';
    let statusCode = 500;
    
    if (errorMessage.includes('AWS credentials not configured') || 
        errorMessage.includes('AWS_ACCESS_KEY_ID') ||
        errorMessage.includes('AWS_SECRET_ACCESS_KEY')) {
      statusCode = 503;
      errorMessage = 'AWS credentials not configured. Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in Vercel environment variables.';
    } else if (errorMessage.includes('database') || 
               errorMessage.includes('Database') ||
               errorMessage.includes('veraset')) {
      statusCode = 503;
      errorMessage = `Athena database 'veraset' not found. Please create it in AWS Glue:\n\naws glue create-database --database-input '{"Name": "veraset"}' --region us-west-2`;
    } else if (errorMessage.includes('Access denied') || 
               errorMessage.includes('not authorized') ||
               errorMessage.includes('AccessDeniedException')) {
      statusCode = 403;
      errorMessage = `Athena access denied. Please ensure your AWS IAM user has the following permissions:\n\n` +
        `- Athena: StartQueryExecution, GetQueryExecution, GetQueryResults\n` +
        `- Glue: GetDatabase, CreateTable, GetTable, BatchCreatePartition\n` +
        `- S3: GetObject, PutObject, ListBucket (on garritz-veraset-data-us-west-2)\n\n` +
        `See ATHENA_SETUP.md for detailed instructions.`;
    } else if (errorMessage.includes('table') || errorMessage.includes('Table')) {
      statusCode = 404;
      errorMessage = `Table not found for dataset: ${errorDatasetName}. The dataset may not exist or may not have been processed yet.`;
    }
    
    return NextResponse.json(
      { 
        error: 'Analysis failed',
        details: errorMessage,
        dataset: errorDatasetName,
        hint: errorMessage.includes('database') 
          ? 'Run: aws glue create-database --database-input \'{"Name": "veraset"}\' --region us-west-2'
          : undefined
      },
      { 
        status: statusCode,
        headers: {
          'Content-Type': 'application/json',
        }
      }
    );
  }
}
