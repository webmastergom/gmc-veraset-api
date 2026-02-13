import { NextRequest, NextResponse } from 'next/server';
import { runQuery, createTableForDataset, tableExists, getTableName } from '@/lib/athena';

const VERASET_BASE_URL = 'https://platform.prd.veraset.tech';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 60;

/**
 * GET /api/datasets/[name]/pois/enrich
 * Get POIs from dataset and enrich them with details from Veraset API
 * This uses /v1/poi/pois to get information about POIs that visited the dataset
 */
export async function GET(
  req: NextRequest,
  { params }: { params: { name: string } }
) {
  try {
    const apiKey = process.env.VERASET_API_KEY?.trim();
    
    if (!apiKey) {
      return NextResponse.json(
        { error: 'VERASET_API_KEY not configured' },
        { status: 500 }
      );
    }

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

    const datasetPois = result.rows.map((row: any) => ({
      poiId: row.poi_id,
      pings: parseInt(String(row.pings)) || 0,
      devices: parseInt(String(row.devices)) || 0,
    }));

    console.log(`✅ Found ${datasetPois.length} unique POIs in dataset ${datasetName}`);

    // If no POIs found, return early
    if (datasetPois.length === 0) {
      return NextResponse.json({
        pois: [],
        total: 0,
        enriched: false,
      });
    }

    // Extract POI IDs to query Veraset API
    const poiIds = datasetPois.map(p => p.poiId).filter(Boolean);
    
    // Query Veraset API to get POI details for the POIs in our dataset
    // According to Veraset docs: /v1/poi/pois can be queried to get POI information
    // We'll query with the POI IDs we found in the dataset
    try {
      // Query Veraset API - we can pass POI IDs as query parameters if supported
      // Or query all and filter (less efficient but works)
      // TODO: Check Veraset API docs for best way to query specific POI IDs
      const verasetResponse = await fetch(`${VERASET_BASE_URL}/v1/poi/pois`, {
        method: 'GET',
        headers: {
          'X-API-Key': apiKey,
          'Content-Type': 'application/json',
        },
      });

      if (verasetResponse.ok) {
        const verasetData = await verasetResponse.json();
        const verasetPois = verasetData.pois || verasetData.data || [];
        
        // Create a map of POI ID to Veraset POI data
        // Match by poi_id or id field
        const poiDetailsMap = new Map<string, any>();
        verasetPois.forEach((poi: any) => {
          const poiId = poi.poi_id || poi.id || poi.place_key;
          if (poiId) {
            // Store by both string and number representation for matching
            poiDetailsMap.set(String(poiId), poi);
            if (typeof poiId === 'number') {
              poiDetailsMap.set(String(poiId), poi);
            }
          }
        });

        // Enrich dataset POIs with Veraset data
        const enrichedPois = datasetPois.map(datasetPoi => {
          const verasetPoi = poiDetailsMap.get(String(datasetPoi.poiId));
          
          return {
            ...datasetPoi,
            // Add Veraset POI details if found
            name: verasetPoi?.name || verasetPoi?.poi_name || verasetPoi?.brand || datasetPoi.poiId,
            category: verasetPoi?.category,
            subcategory: verasetPoi?.subcategory,
            brand: verasetPoi?.brand,
            address: verasetPoi?.address,
            city: verasetPoi?.city,
            state: verasetPoi?.state,
            zipcode: verasetPoi?.zipcode || verasetPoi?.zip,
            country: verasetPoi?.country,
            place_key: verasetPoi?.place_key || verasetPoi?.placeKey,
            enriched: !!verasetPoi,
          };
        });

        return NextResponse.json({
          pois: enrichedPois,
          total: enrichedPois.length,
          enriched: true,
          enrichedCount: enrichedPois.filter(p => p.enriched).length,
        });
      } else {
        // If Veraset API fails, return POIs without enrichment
        console.warn('Failed to enrich POIs from Veraset API, returning basic POI data');
        return NextResponse.json({
          pois: datasetPois,
          total: datasetPois.length,
          enriched: false,
        });
      }
    } catch (error: any) {
      console.error('Error enriching POIs from Veraset:', error);
      // Return POIs without enrichment if Veraset API fails
      return NextResponse.json({
        pois: datasetPois,
        total: datasetPois.length,
        enriched: false,
        error: 'Failed to enrich POIs from Veraset API',
      });
    }

  } catch (error: any) {
    console.error(`GET /api/datasets/${params.name}/pois/enrich error:`, error);
    
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
        error: 'Failed to fetch and enrich POIs', 
        details: errorMessage
      },
      { status: statusCode }
    );
  }
}
