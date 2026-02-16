import { NextRequest, NextResponse } from 'next/server';
import { getAllJobs } from '@/lib/jobs';
import { validateApiKeyFromRequest } from '@/lib/api-auth';
import { runFullAnalysis } from '@/lib/dataset-analysis';
import { getPOICollection } from '@/lib/s3-config';
import { BUCKET } from '@/lib/s3-config';
import { runQuery, createTableForDataset, tableExists, getTableName } from '@/lib/athena';

export const dynamic = 'force-dynamic';
export const revalidate = 0;
export const maxDuration = 300; // Allow up to 5 minutes for Athena queries

/**
 * GET /api/external/jobs/[datasetName]/analysis
 * Get dataset analysis data for a specific dataset
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ datasetName: string }> }
) {
  const params = await context.params;
  const datasetName = params.datasetName;

  try {
    // Validate API key
    const auth = await validateApiKeyFromRequest(request);
    if (!auth.valid) {
      return NextResponse.json(
        { error: 'Unauthorized', message: auth.error },
        { status: 401 }
      );
    }

    // Find job by dataset name
    const allJobs = await getAllJobs();
    const job = allJobs.find(j => {
      if (!j.s3DestPath || j.status !== 'SUCCESS') return false;
      const s3Path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
      const jobDatasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');
      return jobDatasetName === datasetName;
    });

    if (!job) {
      return NextResponse.json(
        { error: 'Not Found', message: `Dataset '${datasetName}' not found or not available.` },
        { status: 404 }
      );
    }

    // Run analysis (all days, no exceptions)
    let analysisResult;
    try {
      analysisResult = await runFullAnalysis(datasetName);
    } catch (error: any) {
      console.error(`Analysis failed for ${datasetName}:`, error);
      return NextResponse.json(
        {
          error: 'Internal Server Error',
          message: 'Failed to analyze dataset',
          details: error.message,
        },
        { status: 500 }
      );
    }

    // Transform dailyData into separate arrays
    const dailyPings = analysisResult.dailyData.map(day => ({
      date: day.date,
      pings: day.pings,
    }));

    const dailyDevices = analysisResult.dailyData.map(day => ({
      date: day.date,
      devices: day.devices,
    }));

    // Get active POIs with coordinates
    const pois: Array<{
      poiId: string;
      name: string;
      latitude: number;
      longitude: number;
    }> = [];

    try {
      // Get all unique POIs with activity from the dataset
      // Query Athena directly to get all active POIs (not just top 20)
      const tableName = getTableName(datasetName);
      
      // Ensure table exists before querying
      let activePoiIds: string[] = [];
      try {
        if (!(await tableExists(datasetName))) {
          await createTableForDataset(datasetName);
        }
        
        // UNNEST poi_ids to discover ALL POI IDs across the full array
        const poisQuery = `
          SELECT DISTINCT poi_id
          FROM ${tableName}
          CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
          WHERE poi_id IS NOT NULL AND poi_id != ''
        `;
        
        const poisResult = await runQuery(poisQuery);
        activePoiIds = poisResult.rows.map((row: any) => String(row.poi_id || '')).filter(Boolean);
      } catch (error) {
        console.warn(`Could not query all POIs, using visitsByPoi as fallback:`, error);
        activePoiIds = analysisResult.visitsByPoi.map(p => p.poiId);
      }

      // Get POI mapping and names from job metadata
      const poiMapping = job.poiMapping || {};
      const poiNames = job.poiNames || {};

      // Get GeoJSON if available
      let geojson: any = null;
      if (job.poiCollectionId) {
        try {
          geojson = await getPOICollection(job.poiCollectionId);
        } catch (error) {
          console.warn(`Could not load GeoJSON for collection ${job.poiCollectionId}:`, error);
        }
      }

      // Build POI array with coordinates
      for (const verasetPoiId of activePoiIds) {
        const originalPoiId = poiMapping[verasetPoiId] || verasetPoiId;
        const poiName = poiNames[verasetPoiId] || originalPoiId;

        // Try to find coordinates in GeoJSON
        let latitude: number | null = null;
        let longitude: number | null = null;

        if (geojson && geojson.features) {
          // Search for the feature by ID
          const feature = geojson.features.find((f: any) => {
            const featureId = f.id || f.properties?.id || f.properties?.poi_id || f.properties?.identifier;
            return String(featureId) === String(originalPoiId);
          });

          if (feature && feature.geometry && feature.geometry.type === 'Point') {
            const coords = feature.geometry.coordinates;
            if (Array.isArray(coords) && coords.length >= 2) {
              longitude = coords[0];
              latitude = coords[1];
            }
          }
        }

        // Only include POIs with valid coordinates
        if (latitude !== null && longitude !== null) {
          pois.push({
            poiId: verasetPoiId,
            name: poiName,
            latitude,
            longitude,
          });
        }
      }
    } catch (error: any) {
      console.warn(`Error processing POIs for ${datasetName}:`, error);
      // Continue without POIs rather than failing the entire request
    }

    // Return formatted response
    return NextResponse.json({
      datasetName,
      jobName: job.name,
      analysis: {
        dailyPings,
        dailyDevices,
        pois,
      },
    });
  } catch (error: any) {
    console.error(`GET /api/external/jobs/${datasetName}/analysis error:`, error);
    return NextResponse.json(
      { error: 'Internal Server Error', message: error.message },
      { status: 500 }
    );
  }
}
