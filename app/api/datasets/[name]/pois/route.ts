import { NextRequest, NextResponse } from 'next/server';
import { runQuery, createTableForDataset, tableExists, getTableName } from '@/lib/athena';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60; // Allow up to 60s for this query

/**
 * GET /api/datasets/[name]/pois
 * Get list of all unique POIs in a dataset for filtering
 */
export async function GET(
  req: Request,
  { params }: { params: { name: string } }
) {
  try {
    const datasetName = params.name;
    const tableName = getTableName(datasetName);

    // Check if AWS credentials are configured
    if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
      return NextResponse.json(
        { error: 'AWS credentials not configured' },
        { status: 503 }
      );
    }

    // Ensure table exists
    if (!(await tableExists(datasetName))) {
      await createTableForDataset(datasetName);
    }

    // Query to get all unique POIs with their stats (UNNEST for complete coverage)
    const sql = `
      SELECT
        poi_id,
        COUNT(*) as pings,
        COUNT(DISTINCT ad_id) as devices
      FROM ${tableName}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
      GROUP BY poi_id
      ORDER BY COUNT(DISTINCT ad_id) DESC
    `;

    console.log(`🔍 Fetching POIs for dataset ${datasetName}...`);
    const result = await runQuery(sql);

    const pois = result.rows.map((row: any) => ({
      poiId: row.poi_id,
      pings: parseInt(String(row.pings)) || 0,
      devices: parseInt(String(row.devices)) || 0,
    }));

    console.log(`✅ Found ${pois.length} unique POIs in dataset ${datasetName}`);

    return NextResponse.json({
      pois,
      total: pois.length,
    });

  } catch (error: any) {
    console.error(`GET /api/datasets/${params.name}/pois error:`, error);
    
    let errorMessage = error.message || 'Unknown error';
    let statusCode = 500;
    
    if (errorMessage.includes('AWS credentials not configured')) {
      statusCode = 503;
    } else if (errorMessage.includes('database') || errorMessage.includes('Database')) {
      statusCode = 503;
    } else if (errorMessage.includes('Access denied')) {
      statusCode = 403;
    }
    
    return NextResponse.json(
      { 
        error: 'Failed to fetch POIs', 
        details: errorMessage
      },
      { status: statusCode }
    );
  }
}
