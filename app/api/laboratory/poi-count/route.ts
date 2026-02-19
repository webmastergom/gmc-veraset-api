import { NextRequest, NextResponse } from 'next/server';
import { runQuery } from '@/lib/athena';
import type { PoiCategory } from '@/lib/laboratory-types';
import { POI_CATEGORIES } from '@/lib/laboratory-types';
import { toIsoCountry } from '@/lib/country-inference';

export const dynamic = 'force-dynamic';

const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';

/**
 * POST /api/laboratory/poi-count
 * Returns the number of POIs matching the given categories + country.
 * Body: { categories: string[], country: string }
 */
export async function POST(request: NextRequest): Promise<NextResponse> {
  let body: { categories: string[]; country: string };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 });
  }

  const { categories, country } = body;
  if (!categories?.length || !country) {
    return NextResponse.json({ error: 'categories and country are required' }, { status: 400 });
  }

  // Validate categories
  const validCats = categories.filter(c => POI_CATEGORIES.includes(c as PoiCategory));
  if (validCats.length === 0) {
    return NextResponse.json({ count: 0, categories: [] });
  }

  const poiTableName = 'lab_pois_gmc';

  // Ensure POI table exists
  try {
    await runQuery(`
      CREATE EXTERNAL TABLE IF NOT EXISTS ${poiTableName} (
        id STRING,
        name STRING,
        category STRING,
        city STRING,
        postal_code STRING,
        country STRING,
        latitude DOUBLE,
        longitude DOUBLE
      )
      STORED AS PARQUET
      LOCATION 's3://${BUCKET}/pois_gmc/'
    `);
  } catch (error: any) {
    if (!error.message?.includes('already exists')) {
      console.warn(`[POI-COUNT] Warning creating POI table:`, error.message);
    }
  }

  const catFilter = validCats.map(c => `'${c}'`).join(',');
  const query = `
    SELECT category, COUNT(*) as cnt
    FROM ${poiTableName}
    WHERE category IN (${catFilter})
      AND country = '${toIsoCountry(country)}'
    GROUP BY category
  `;

  try {
    const result = await runQuery(query);
    let total = 0;
    const breakdown: Record<string, number> = {};
    for (const row of result.rows) {
      const cat = String(row.category);
      const cnt = parseInt(String(row.cnt)) || 0;
      breakdown[cat] = cnt;
      total += cnt;
    }
    return NextResponse.json({ count: total, breakdown });
  } catch (error: any) {
    console.error(`[POI-COUNT] Query failed:`, error.message);
    return NextResponse.json({ error: 'Failed to count POIs' }, { status: 500 });
  }
}
