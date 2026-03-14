import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

const GEOJSON_DIR = path.join(process.cwd(), 'data', 'geojson');

// Large countries that may exceed memory/timeout — skip full GeoJSON load
const SKIP_COUNTRIES = new Set(['US', 'MX']);

interface ZipDeviceDays {
  zipCode: string;
  deviceDays: number;
  city: string;
}

/**
 * Extract postal code from GeoJSON feature properties (mirrors reverse-geocode.ts parseProperties)
 */
function extractPostalCode(country: string, props: Record<string, any>): string {
  return String(props.postal_code || '');
}

/**
 * POST /api/geojson/catchment
 * Body: { entries: Array<{ zipCode, deviceDays, city, country }> }
 * Returns: GeoJSON FeatureCollection with matched polygons + deviceDays in properties
 */
export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const entries: Array<{ zipCode: string; deviceDays: number; city: string; country: string }> = body.entries || [];

    if (!entries.length) {
      return NextResponse.json({ type: 'FeatureCollection', features: [] });
    }

    // Group entries by country
    const byCountry = new Map<string, ZipDeviceDays[]>();
    for (const e of entries) {
      const cc = (e.country || '').toUpperCase();
      if (!cc || cc === 'UNKNOWN' || SKIP_COUNTRIES.has(cc)) continue;
      if (!byCountry.has(cc)) byCountry.set(cc, []);
      byCountry.get(cc)!.push({ zipCode: e.zipCode, deviceDays: e.deviceDays, city: e.city });
    }

    const allFeatures: any[] = [];

    for (const [country, zipEntries] of byCountry) {
      const filePath = path.join(GEOJSON_DIR, `${country}.geojson`);
      if (!fs.existsSync(filePath)) {
        console.log(`[GEOJSON-CATCHMENT] No GeoJSON file for ${country}, skipping`);
        continue;
      }

      // Build lookup: zipCode → { deviceDays, city }
      const lookup = new Map<string, { deviceDays: number; city: string }>();
      for (const z of zipEntries) {
        const existing = lookup.get(z.zipCode);
        if (existing) {
          existing.deviceDays += z.deviceDays;
        } else {
          lookup.set(z.zipCode, { deviceDays: z.deviceDays, city: z.city });
        }
      }

      console.log(`[GEOJSON-CATCHMENT] Loading ${country}.geojson for ${lookup.size} zip codes...`);
      const raw = fs.readFileSync(filePath, 'utf-8');
      const geojson = JSON.parse(raw);
      const features = geojson.features || [];

      let matched = 0;
      for (const feature of features) {
        const pc = extractPostalCode(country, feature.properties || {});
        const entry = lookup.get(pc);
        if (entry) {
          // Simplify: only send geometry + essential props
          allFeatures.push({
            type: 'Feature',
            geometry: feature.geometry,
            properties: {
              zipCode: pc,
              city: entry.city,
              country,
              deviceDays: entry.deviceDays,
            },
          });
          matched++;
        }
      }
      console.log(`[GEOJSON-CATCHMENT] Matched ${matched}/${lookup.size} zip codes for ${country}`);
    }

    return NextResponse.json({
      type: 'FeatureCollection',
      features: allFeatures,
    });
  } catch (err: any) {
    console.error('[GEOJSON-CATCHMENT] Error:', err.message);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
