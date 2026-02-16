import { NextRequest, NextResponse } from 'next/server';
import { getAllJobs } from '@/lib/jobs';
import { isAuthenticated } from '@/lib/auth';
import { getPOICollection } from '@/lib/s3-config';

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/datasets/[name]/pois/positions
 * Returns POI positions (lat, lng) for the dataset's analysis POIs.
 * Used for map overlay.
 */
export async function GET(
  req: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(req)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const params = await context.params;
  const datasetName = params.name;

  try {
    const jobs = await getAllJobs();
    const job = jobs.find((j) => {
      if (!j.s3DestPath) return false;
      const s3Path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '').trim();
      const parts = s3Path.split('/').filter(Boolean);
      const jobFolderName = parts[0] || parts.pop() || s3Path.replace(/\/$/, '');
      return jobFolderName === datasetName;
    });

    const positions: Array<{ poiId: string; lat: number; lng: number; name?: string }> = [];

    const geoRadius = job?.verasetPayload?.geo_radius || job?.auditTrail?.userInput?.verasetConfig?.geo_radius;
    if (geoRadius && Array.isArray(geoRadius)) {
      for (const poi of geoRadius) {
        const lat = Number(poi.latitude);
        const lng = Number(poi.longitude);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
        const poiId = poi.poi_id || '';
        const name = job?.poiNames?.[poiId];
        positions.push({ poiId, lat, lng, name });
      }
    }

    if (positions.length === 0 && job?.externalPois?.length) {
      for (const p of job.externalPois) {
        if (Number.isFinite(p.latitude) && Number.isFinite(p.longitude)) {
          positions.push({ poiId: p.id, lat: p.latitude, lng: p.longitude, name: p.name });
        }
      }
    }

    if (positions.length === 0 && job?.poiCollectionId) {
      try {
        const geojson = await getPOICollection(job.poiCollectionId);
        const features = geojson?.features || [];
        const poiMapping = job.poiMapping || {};
        const poiNames = job.poiNames || {};
        for (const f of features) {
          const geom = f.geometry;
          const props = f.properties || {};
          const originalId = props.id || props.name || f.id || '';
          const verasetId = Object.entries(poiMapping).find(([, v]) => v === originalId)?.[0] || originalId;
          let coords: number[] = [];
          if (geom?.type === 'Point' && Array.isArray(geom.coordinates)) {
            coords = geom.coordinates;
          } else if (geom?.type === 'MultiPoint' && geom.coordinates?.[0]) {
            coords = geom.coordinates[0];
          } else if (geom?.type === 'Polygon' && geom.coordinates?.[0]?.[0]) {
            coords = geom.coordinates[0][0];
          } else if (geom?.type === 'MultiPolygon' && geom.coordinates?.[0]?.[0]?.[0]) {
            coords = geom.coordinates[0][0][0];
          }
          if (coords.length >= 2) {
            const lng = coords[0];
            const lat = coords[1];
            const name = props.name || poiNames[verasetId] || originalId;
            positions.push({ poiId: verasetId || originalId, lat, lng, name });
          }
        }
      } catch {
        // ignore
      }
    }

    return NextResponse.json({ positions });
  } catch (error: any) {
    console.error(`[POIS POSITIONS] GET /api/datasets/${datasetName}/pois/positions:`, error);
    return NextResponse.json(
      { error: 'Failed to fetch POI positions', details: error.message },
      { status: 500 }
    );
  }
}
