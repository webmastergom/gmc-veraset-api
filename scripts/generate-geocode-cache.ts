/**
 * Pre-generate geocode cache CSVs for large countries.
 *
 * Produces a CSV with lat_key,lng_key,zipcode for every 0.1° grid cell
 * within the country's bounding box. These are uploaded to S3 and used
 * by the activation flow instead of loading 40MB+ GeoJSON files at runtime.
 *
 * Usage:
 *   npx tsx scripts/generate-geocode-cache.ts [CC...]
 *   npx tsx scripts/generate-geocode-cache.ts MX       # Single country
 *   npx tsx scripts/generate-geocode-cache.ts          # All large countries
 */

import * as fs from 'fs';
import * as path from 'path';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getZipcode, ensureCountriesLoaded, setCountryFilter } from '../lib/reverse-geocode';

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
const REGION = process.env.AWS_REGION || 'us-west-2';
const PRECISION = 10; // 1 decimal: lat_key = ROUND(lat, 1) * 10

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

// Countries with GeoJSON > 10MB — need pre-computed cache for Vercel 60s limit
const LARGE_COUNTRIES: Record<string, { minLat: number; maxLat: number; minLng: number; maxLng: number }> = {
  US: { minLat: 24, maxLat: 50, minLng: -125, maxLng: -66 },  // Continental US only
  MX: { minLat: 14, maxLat: 33, minLng: -118, maxLng: -86 },
  AR: { minLat: -56, maxLat: -21, minLng: -74, maxLng: -25 },
  ES: { minLat: 27, maxLat: 44, minLng: -19, maxLng: 5 },
  DO: { minLat: 17, maxLat: 20, minLng: -73, maxLng: -68 },
  CR: { minLat: 5, maxLat: 12, minLng: -88, maxLng: -82 },
  PA: { minLat: 7, maxLat: 10, minLng: -84, maxLng: -77 },
  CL: { minLat: -56, maxLat: -17, minLng: -76, maxLng: -66 },
  FR: { minLat: 42, maxLat: 52, minLng: -5, maxLng: 9 },
  DE: { minLat: 47, maxLat: 56, minLng: 5, maxLng: 16 },
  IT: { minLat: 35, maxLat: 48, minLng: 6, maxLng: 19 },
  CO: { minLat: -5, maxLat: 14, minLng: -82, maxLng: -66 },
  PE: { minLat: -19, maxLat: 1, minLng: -82, maxLng: -68 },
  UK: { minLat: 49, maxLat: 61, minLng: -8, maxLng: 2 },
};

// Country center points for ensureCountriesLoaded
const COUNTRY_CENTERS: Record<string, { lat: number; lng: number }> = {
  US: { lat: 39.83, lng: -98.58 },
  MX: { lat: 23.63, lng: -102.55 },
  AR: { lat: -38.42, lng: -63.62 },
  ES: { lat: 40.46, lng: -3.75 },
  DO: { lat: 18.74, lng: -70.16 },
  CR: { lat: 9.75, lng: -83.75 },
  PA: { lat: 8.54, lng: -80.78 },
  CL: { lat: -35.68, lng: -71.54 },
  FR: { lat: 46.23, lng: 2.21 },
  DE: { lat: 51.16, lng: 10.45 },
  IT: { lat: 41.87, lng: 12.57 },
  CO: { lat: 4.57, lng: -74.30 },
  PE: { lat: -9.19, lng: -75.02 },
  UK: { lat: 55.38, lng: -3.44 },
};

async function generateCache(cc: string) {
  const bbox = LARGE_COUNTRIES[cc];
  if (!bbox) {
    console.error(`Unknown country: ${cc}`);
    return;
  }

  console.log(`\n=== Generating cache for ${cc} ===`);
  console.log(`  BBox: lat ${bbox.minLat}..${bbox.maxLat}, lng ${bbox.minLng}..${bbox.maxLng}`);

  // Load GeoJSON for this country only
  setCountryFilter([cc]);
  const center = COUNTRY_CENTERS[cc];
  await ensureCountriesLoaded([center]);
  console.log(`  GeoJSON loaded`);

  // Generate grid cells
  const minLatKey = Math.floor(bbox.minLat * PRECISION);
  const maxLatKey = Math.ceil(bbox.maxLat * PRECISION);
  const minLngKey = Math.floor(bbox.minLng * PRECISION);
  const maxLngKey = Math.ceil(bbox.maxLng * PRECISION);

  const totalCells = (maxLatKey - minLatKey + 1) * (maxLngKey - minLngKey + 1);
  console.log(`  Grid: ${maxLatKey - minLatKey + 1} lat × ${maxLngKey - minLngKey + 1} lng = ${totalCells.toLocaleString()} cells`);

  const lines: string[] = [];
  let matched = 0;
  let checked = 0;

  for (let latKey = minLatKey; latKey <= maxLatKey; latKey++) {
    for (let lngKey = minLngKey; lngKey <= maxLngKey; lngKey++) {
      checked++;
      const lat = latKey / PRECISION;
      const lng = lngKey / PRECISION;
      const result = getZipcode(lat, lng);
      if (result?.postcode) {
        lines.push(`${latKey},${lngKey},${result.postcode}`);
        matched++;
      }
    }
    if (checked % 10000 === 0) {
      process.stdout.write(`\r  Progress: ${checked.toLocaleString()}/${totalCells.toLocaleString()} (${matched.toLocaleString()} matched)`);
    }
  }
  console.log(`\r  Progress: ${checked.toLocaleString()}/${totalCells.toLocaleString()} (${matched.toLocaleString()} matched)`);

  setCountryFilter(null);

  // Write locally
  const localDir = path.join(process.cwd(), 'data', 'geocode-cache');
  fs.mkdirSync(localDir, { recursive: true });
  const csvContent = lines.join('\n') + '\n';
  const localPath = path.join(localDir, `${cc}.csv`);
  fs.writeFileSync(localPath, csvContent);
  console.log(`  Local: ${localPath} (${(csvContent.length / 1024).toFixed(1)} KB, ${lines.length} entries)`);

  // Upload to S3
  const s3Key = `config/geocode-cache/${cc}.csv`;
  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: s3Key,
    Body: csvContent,
    ContentType: 'text/csv',
  }));
  console.log(`  S3: s3://${BUCKET}/${s3Key}`);
}

async function main() {
  const args = process.argv.slice(2);
  const countries = args.length > 0
    ? args.map(a => a.toUpperCase())
    : Object.keys(LARGE_COUNTRIES);

  console.log(`Generating geocode cache for: ${countries.join(', ')}`);
  console.log(`Precision: ${PRECISION} (1 decimal = ~11km)`);

  for (const cc of countries) {
    await generateCache(cc);
  }

  console.log('\nDone!');
}

main().catch(console.error);
