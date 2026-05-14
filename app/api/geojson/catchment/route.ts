import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

export const maxDuration = 60;

const GEOJSON_DIR = path.join(process.cwd(), 'data', 'geojson');

// Only countries we genuinely can't ship are skipped here. US is gigantic
// (~150 MB) and not included in the data dir. MX used to be skipped at
// 40 MB but the function now has 3008 MB allocated (vercel.json), so it
// fits comfortably alongside the other countries.
const SKIP_COUNTRIES = new Set(['US']);

// When a row's country is missing/UNKNOWN we still want to render a tile
// instead of falling back to a circle. Try matching the zip across the
// candidate countries below in order — first hit wins. This list is small
// on purpose (only countries we have GeoJSON for); large catalogs like MX
// are skipped during the fallback to avoid scanning 36k features per zip.
const FALLBACK_COUNTRIES_FOR_UNKNOWN = ['SV', 'HN', 'GT', 'CR', 'NI', 'PA', 'CO', 'EC', 'CL', 'AR', 'ES', 'FR', 'DE', 'BE', 'IT', 'IE', 'UK', 'DO'];

interface ZipDeviceDays {
  zipCode: string;
  deviceDays: number;
  city: string;
}

/**
 * Normalize a postal code for matching: strip whitespace, uppercase. We
 * intentionally KEEP country prefixes like "SV010101" / "HN1102" because
 * the source GeoJSON files use the same format — stripping would cause
 * mismatch for those countries.
 */
function normZip(z: string): string {
  return String(z || '').trim().toUpperCase();
}

/**
 * Extract postal code from GeoJSON feature properties. Tries the canonical
 * `postal_code` first, then a small set of common alternates seen in
 * legacy/external GeoJSON catalogs.
 */
function extractPostalCode(_country: string, props: Record<string, any>): string {
  return normZip(
    props.postal_code ||
    props.postalCode ||
    props.zipcode ||
    props.zip ||
    props.cp ||
    props.d_cp ||
    props.code ||
    ''
  );
}

/** Load a country's GeoJSON file (returns null if missing). */
function loadCountryGeoJSON(country: string): any | null {
  const filePath = path.join(GEOJSON_DIR, `${country}.geojson`);
  if (!fs.existsSync(filePath)) return null;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(raw);
  } catch (err: any) {
    console.warn(`[GEOJSON-CATCHMENT] Failed to load ${country}.geojson: ${err.message}`);
    return null;
  }
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

    // Group entries by country. Entries with missing/UNKNOWN country go into
    // a pool we'll try against the fallback countries.
    const byCountry = new Map<string, ZipDeviceDays[]>();
    const unknownPool: ZipDeviceDays[] = [];
    for (const e of entries) {
      const cc = (e.country || '').toUpperCase();
      const zd: ZipDeviceDays = { zipCode: normZip(e.zipCode), deviceDays: e.deviceDays, city: e.city };
      if (!cc || cc === 'UNKNOWN') {
        unknownPool.push(zd);
        continue;
      }
      if (SKIP_COUNTRIES.has(cc)) continue;
      if (!byCountry.has(cc)) byCountry.set(cc, []);
      byCountry.get(cc)!.push(zd);
    }

    const allFeatures: any[] = [];

    // ── Pass 1: known countries ────────────────────────────────────────
    for (const [country, zipEntries] of byCountry) {
      const lookup = new Map<string, { deviceDays: number; city: string }>();
      for (const z of zipEntries) {
        const existing = lookup.get(z.zipCode);
        if (existing) existing.deviceDays += z.deviceDays;
        else lookup.set(z.zipCode, { deviceDays: z.deviceDays, city: z.city });
      }
      const geojson = loadCountryGeoJSON(country);
      if (!geojson) {
        console.log(`[GEOJSON-CATCHMENT] No GeoJSON file for ${country}, skipping`);
        continue;
      }
      console.log(`[GEOJSON-CATCHMENT] Loading ${country}.geojson for ${lookup.size} zip codes...`);
      const features = geojson.features || [];
      let matched = 0;
      for (const feature of features) {
        const pc = extractPostalCode(country, feature.properties || {});
        const entry = lookup.get(pc);
        if (entry) {
          allFeatures.push({
            type: 'Feature',
            geometry: feature.geometry,
            properties: { zipCode: pc, city: entry.city, country, deviceDays: entry.deviceDays },
          });
          matched++;
        }
      }
      console.log(`[GEOJSON-CATCHMENT] Matched ${matched}/${lookup.size} zip codes for ${country}`);
    }

    // ── Pass 2: rows with UNKNOWN country — try fallback countries ─────
    if (unknownPool.length > 0) {
      console.log(`[GEOJSON-CATCHMENT] ${unknownPool.length} entries have UNKNOWN country, trying fallback list`);
      const remaining = new Map<string, { deviceDays: number; city: string }>();
      for (const z of unknownPool) {
        const existing = remaining.get(z.zipCode);
        if (existing) existing.deviceDays += z.deviceDays;
        else remaining.set(z.zipCode, { deviceDays: z.deviceDays, city: z.city });
      }
      for (const country of FALLBACK_COUNTRIES_FOR_UNKNOWN) {
        if (remaining.size === 0) break;
        const geojson = loadCountryGeoJSON(country);
        if (!geojson) continue;
        const features = geojson.features || [];
        let matched = 0;
        for (const feature of features) {
          const pc = extractPostalCode(country, feature.properties || {});
          const entry = remaining.get(pc);
          if (entry) {
            allFeatures.push({
              type: 'Feature',
              geometry: feature.geometry,
              properties: { zipCode: pc, city: entry.city, country, deviceDays: entry.deviceDays },
            });
            remaining.delete(pc);
            matched++;
          }
        }
        if (matched > 0) {
          console.log(`[GEOJSON-CATCHMENT] Fallback matched ${matched} UNKNOWN-country zips in ${country} (${remaining.size} still unresolved)`);
        }
      }
    }

    return NextResponse.json({ type: 'FeatureCollection', features: allFeatures });
  } catch (err: any) {
    console.error('[GEOJSON-CATCHMENT] Error:', err.message);
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
