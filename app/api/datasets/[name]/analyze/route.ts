import { NextRequest, NextResponse } from 'next/server';
import { runFullAnalysis } from '@/lib/dataset-analysis';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 600; // 10 minutes - allows time to wait for all partitions (10 attempts * 30s = 5min max wait)

/**
 * POST /api/datasets/[name]/analyze
 * Analyzes the dataset reading all days (partitions) without exception.
 * Returns dailyData (one row per day), visitsByPoi, and summary.
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
): Promise<NextResponse> {
  let datasetName: string | undefined;

  try {
    const params = await context.params;
    datasetName = params.name;

    if (!datasetName || typeof datasetName !== 'string') {
      return NextResponse.json(
        { error: 'Dataset name is required', received: datasetName },
        { status: 400 }
      );
    }

    const result = await runFullAnalysis(datasetName);
    return NextResponse.json(result, {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error: any) {
    const name = datasetName || 'unknown';
    const errorMsg = error?.message || String(error) || 'Analysis failed';
    console.error(`[ANALYZE] POST /api/datasets/${name}/analyze error:`, errorMsg);
    console.error(`[ANALYZE] Error stack:`, error?.stack);

    let statusCode = 500;
    let message = errorMsg;
    let actionableSteps: string[] = [];

    // Categorize errors and provide actionable guidance
    if (
      message.includes('AWS credentials not configured') ||
      message.includes('AWS_ACCESS_KEY_ID') ||
      message.includes('AWS_SECRET_ACCESS_KEY')
    ) {
      statusCode = 503;
      message = 'AWS credentials not configured.';
      actionableSteps = [
        'Set AWS_ACCESS_KEY_ID environment variable',
        'Set AWS_SECRET_ACCESS_KEY environment variable',
        'Verify credentials have access to S3 and Athena services',
      ];
    } else if (message.includes('database') || message.includes('veraset') || message.includes('Database')) {
      statusCode = 503;
      message = `Athena database 'veraset' not found.`;
      actionableSteps = [
        'Create the database in AWS Glue Console',
        'Or run: CREATE DATABASE IF NOT EXISTS veraset; in Athena',
        'Verify AWS Glue permissions are configured',
      ];
    } else if (
      message.includes('Access denied') ||
      message.includes('not authorized') ||
      message.includes('AccessDeniedException') ||
      message.includes('Access Denied')
    ) {
      statusCode = 403;
      message = 'Access denied to AWS services.';
      actionableSteps = [
        'Check IAM user/role has Athena QueryExecution permissions',
        'Check IAM user/role has Glue GetPartitions and GetTable permissions',
        'Check IAM user/role has S3 ListObjects and GetObject permissions',
        'Verify bucket policies allow access',
      ];
    } else if (message.includes('table') || message.includes('Table') || message.includes('does not exist')) {
      statusCode = 404;
      message = `Table not found or not accessible for dataset: ${name}.`;
      actionableSteps = [
        'Verify the dataset exists in S3',
        'Check that partitions are properly formatted (date=YYYY-MM-DD)',
        'Try running sync again to ensure data is available',
      ];
    } else if (message.includes('partition') || message.includes('Partition')) {
      statusCode = 500;
      message = `Partition error: ${message}`;
      actionableSteps = [
        'Verify S3 partitions are correctly formatted',
        'Check that partition dates match expected format (date=YYYY-MM-DD)',
        'Try running MSCK REPAIR TABLE manually in Athena',
      ];
    } else if (message.includes('timeout') || message.includes('Timeout')) {
      statusCode = 504;
      message = 'Analysis query timed out.';
      actionableSteps = [
        'Large datasets may take longer to analyze',
        'Try reducing the date range',
        'Check Athena query execution limits',
        'Contact support if the issue persists',
      ];
    } else if (message.includes('No partitions found') || message.includes('No partitions')) {
      statusCode = 404;
      message = `No partitions found for dataset: ${name}.`;
      actionableSteps = [
        'Verify the dataset has been synced from Veraset',
        'Check that S3 path contains partition folders (date=YYYY-MM-DD)',
        'Ensure sync completed successfully',
      ];
    }

    return NextResponse.json(
      {
        error: 'Analysis failed',
        details: message,
        dataset: name,
        ...(actionableSteps.length > 0 && { actionableSteps }),
        timestamp: new Date().toISOString(),
      },
      { status: statusCode, headers: { 'Content-Type': 'application/json' } }
    );
  }
}
