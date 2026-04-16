/**
 * POI Collection storage — local file fallback + S3.
 *
 * Extracted from s3-config.ts so that importing s3-config.ts does NOT pull in
 * the `fs` / `path` modules.  Node File Tracing (NFT) follows static imports
 * of `fs` and traces every file that `fs.existsSync()` / `fsp.readFile()` could
 * touch — including the large GeoJSON files under `./POIs/`.  By isolating
 * these imports here, only routes that actually need local-file POI access pay
 * the bundle-size cost.
 */

import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';
import { s3Client, BUCKET, getConfig } from './s3-config';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { getJobByDatasetName } from './jobs';

/**
 * Get POI collection GeoJSON from S3, with fallback to local files.
 * Local fallback uses async fs.readFile instead of blocking readFileSync.
 */
export async function getPOICollection(collectionId: string): Promise<any | null> {
  try {
    // Get collection metadata to find the correct GeoJSON path
    const collections = await getConfig<Record<string, any>>('poi-collections') || {};
    const collection = collections[collectionId];

    // Use geojsonPath from collection metadata if available, otherwise construct from ID
    const geojsonKey = collection?.geojsonPath || `pois/${collectionId}.geojson`;

    // Try S3 first if credentials are configured
    console.log(`[getPOICollection] id=${collectionId}, key=${geojsonKey}, hasConfig=${!!collection}, bucket=${BUCKET}`);
    if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
      try {
        const command = new GetObjectCommand({
          Bucket: BUCKET,
          Key: geojsonKey,
        });

        console.log(`[getPOICollection] Fetching S3: ${BUCKET}/${geojsonKey}`);
        const response = await s3Client.send(command);
        const body = await response.Body?.transformToString();

        if (body) {
          console.log(`[getPOICollection] Success: ${body.length} bytes from S3`);
          return JSON.parse(body);
        }
        console.warn(`[getPOICollection] Empty body from S3`);
      } catch (s3Error: any) {
        console.error(`[getPOICollection] S3 error: name=${s3Error.name}, msg=${s3Error.message}, status=${s3Error.$metadata?.httpStatusCode}`);
        // Fall through to local file check
      }
    } else {
      console.warn(`[getPOICollection] No AWS credentials`);
    }

    // Fallback: Try to read from local POIs directory
    // Map collection IDs to local file names (with underscores)
    const localFileMap: Record<string, string> = {
      'spain-ecigarette-combined': 'spain_ecigarette_combined.geojson',
      'spain-tobacco-combined': 'spain_tobacco_combined.geojson',
      'spain-nicotine-full': 'overture_spain_nicotine_clean.geojson', // Use the combined nicotine file
    };

    const localFileName = localFileMap[collectionId] || `${collectionId.replace(/-/g, '_')}.geojson`;
    const localPath = path.join(process.cwd(), 'POIs', localFileName);

    if (fs.existsSync(localPath)) {
      console.log(`📁 Using local GeoJSON file: ${localPath}`);
      const fileContent = await fsp.readFile(localPath, 'utf-8');
      return JSON.parse(fileContent);
    }

    if (!collection) {
      console.warn(`POI collection ${collectionId} not found in config and no local file found`);
    } else {
      console.warn(`GeoJSON file not found in S3 or locally for collection ${collectionId}`);
    }
    return null;
  } catch (error: any) {
    console.error(`Error reading GeoJSON for collection ${collectionId}:`, error);
    throw error;
  }
}

/**
 * Save POI collection GeoJSON to S3
 */
export async function putPOICollection(collectionId: string, geojson: any): Promise<void> {
  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS credentials not configured');
  }

  const command = new PutObjectCommand({
    Bucket: BUCKET,
    Key: `pois/${collectionId}.geojson`,
    Body: JSON.stringify(geojson, null, 2),
    ContentType: 'application/json',
  });

  await s3Client.send(command);
}

// ─────────────────────────────────────────────────────────────────────
// Dataset → POI positions helper
// Used by /api/datasets/[name]/pois/positions and /api/compare to fetch
// POI lat/lng/name for map rendering. Mirrors the 3-path lookup:
//   1. job.verasetPayload.geo_radius (pre-enriched)
//   2. job.externalPois
//   3. POI collection GeoJSON + job.poiMapping
// ─────────────────────────────────────────────────────────────────────

export interface DatasetPoiPosition {
  poiId: string;
  lat: number;
  lng: number;
  name?: string;
}

/**
 * @param prefetchedJob Optional job object the caller already has. If omitted,
 *                      looks up the job via getJobByDatasetName (index + 1 file).
 *                      Avoids the N-parallel-S3-GET pattern of getAllJobs.
 */
export async function getPOIPositionsForDataset(
  datasetName: string,
  prefetchedJob?: any | null,
): Promise<DatasetPoiPosition[]> {
  const job = prefetchedJob ?? await getJobByDatasetName(datasetName);
  if (!job) return [];

  // Build a name lookup from EVERY source we have — names can live in:
  //   - job.poiNames[poiId]              (set by sync/enrichment)
  //   - job.externalPois[].{id,name}     (external API uploads)
  //   - job.poiCollectionId GeoJSON      (POI collections)
  // Merge them all so whichever path produces the positions gets a name.
  const nameById = new Map<string, string>();

  const poiNames = (job as any)?.poiNames || {};
  for (const [id, n] of Object.entries(poiNames)) {
    if (n) nameById.set(String(id), String(n));
  }

  const externalPois: any[] = (job as any)?.externalPois || [];
  for (const p of externalPois) {
    if (p?.id && p?.name) nameById.set(String(p.id), String(p.name));
  }

  const positions: DatasetPoiPosition[] = [];

  // Path 1: verasetPayload.geo_radius has authoritative lat/lng + poi_id
  const geoRadius = (job as any)?.verasetPayload?.geo_radius || (job as any)?.auditTrail?.userInput?.verasetConfig?.geo_radius;
  if (geoRadius && Array.isArray(geoRadius)) {
    const poiMapping = (job as any)?.poiMapping || {};
    for (const poi of geoRadius) {
      const lat = Number(poi.latitude);
      const lng = Number(poi.longitude);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) continue;
      const poiId = poi.poi_id || '';
      // poiMapping maps Veraset-side ids (geo_radius_X) → original ids (poi-1, etc.)
      const originalId = (poiMapping as Record<string, string>)[poiId];
      const name = nameById.get(poiId) || (originalId ? nameById.get(originalId) : undefined);
      positions.push({ poiId, lat, lng, name });
    }
  }

  // Path 2: externalPois as primary source when geo_radius is absent
  if (positions.length === 0 && externalPois.length) {
    for (const p of externalPois) {
      if (Number.isFinite(p.latitude) && Number.isFinite(p.longitude)) {
        positions.push({ poiId: p.id, lat: p.latitude, lng: p.longitude, name: p.name });
      }
    }
  }

  // Path 3: POI collection GeoJSON
  if (positions.length === 0 && (job as any)?.poiCollectionId) {
    try {
      const geojson = await getPOICollection((job as any).poiCollectionId);
      const features = geojson?.features || [];
      const poiMapping = (job as any).poiMapping || {};
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
          const name = props.name || nameById.get(verasetId as string) || nameById.get(String(originalId)) || undefined;
          positions.push({ poiId: (verasetId as string) || originalId, lat, lng, name });
        }
      }
    } catch {
      // ignore
    }
  }

  return positions;
}
