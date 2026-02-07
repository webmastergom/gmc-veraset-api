// POI Collection GeoJSON endpoint (Pages Router)
// This duplicates app/api/pois/collections/[id]/geojson/route.ts
// because the App Router version doesn't work on Vercel for this nested dynamic route

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-west-2',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { id: collectionId } = req.query;

  if (!collectionId) {
    return res.status(400).json({ error: 'Collection ID is required' });
  }

  try {
    console.log(`[GeoJSON-Pages] Fetching collection: ${collectionId}`);

    // Try S3
    if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
      // First try to get config for geojsonPath
      let geojsonKey = `pois/${collectionId}.geojson`;

      try {
        const configCmd = new GetObjectCommand({
          Bucket: BUCKET,
          Key: 'config/poi-collections.json',
        });
        const configRes = await s3Client.send(configCmd);
        const configBody = await configRes.Body?.transformToString();
        if (configBody) {
          const collections = JSON.parse(configBody);
          if (collections[collectionId]?.geojsonPath) {
            geojsonKey = collections[collectionId].geojsonPath;
          }
        }
      } catch (e) {
        // Config not found, use default key
      }

      console.log(`[GeoJSON-Pages] Fetching S3: ${BUCKET}/${geojsonKey}`);

      try {
        const command = new GetObjectCommand({
          Bucket: BUCKET,
          Key: geojsonKey,
        });
        const response = await s3Client.send(command);
        const body = await response.Body?.transformToString();

        if (body) {
          console.log(`[GeoJSON-Pages] Success: ${body.length} bytes`);
          const geojson = JSON.parse(body);

          res.setHeader('Content-Type', 'application/json');
          res.setHeader('Cache-Control', 'no-store');
          return res.status(200).json(geojson);
        }
      } catch (s3Error) {
        console.error(`[GeoJSON-Pages] S3 error: ${s3Error.name} - ${s3Error.message}`);
      }
    }

    console.error(`[GeoJSON-Pages] Collection ${collectionId} not found`);
    return res.status(404).json({ error: 'POI collection not found', id: collectionId });
  } catch (error) {
    console.error('[GeoJSON-Pages] Error:', error);
    return res.status(500).json({ error: 'Failed to fetch POI collection' });
  }
}
