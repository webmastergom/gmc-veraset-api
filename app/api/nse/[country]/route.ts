import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { getConfig, putConfig } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

const NSE_KEY = (cc: string) => `nse/${cc.toUpperCase()}`;

export interface NseRecord {
  postal_code: string;
  population: number;
  nse: number;
  region1: string;
  region2: string;
  region3: string;
}

function parseNseCsv(csv: string): NseRecord[] {
  const lines = csv.trim().split('\n');
  if (lines.length < 2) throw new Error('CSV must have a header row and at least one data row');

  const header = lines[0].toLowerCase().replace(/["\s]/g, '').split(',');
  const cpIdx = header.findIndex(h => ['postal_code', 'postalcode', 'cp', 'codigo_postal', 'zip', 'zipcode'].includes(h));
  const popIdx = header.findIndex(h => ['population', 'poblacion', 'pop'].includes(h));
  const nseIdx = header.findIndex(h => ['nse', 'socioeconomic', 'nivel', 'level'].includes(h));
  const r1Idx = header.findIndex(h => ['region1', 'estado', 'state', 'province'].includes(h));
  const r2Idx = header.findIndex(h => ['region2', 'municipio', 'municipality', 'county'].includes(h));
  const r3Idx = header.findIndex(h => ['region3', 'localidad', 'locality', 'city'].includes(h));

  if (cpIdx === -1) throw new Error('Missing postal_code column');
  if (nseIdx === -1) throw new Error('Missing nse column');

  const records: NseRecord[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',').map(c => c.replace(/^"|"$/g, '').trim());
    if (!cols[cpIdx]) continue;
    records.push({
      postal_code: cols[cpIdx],
      population: popIdx >= 0 ? (parseInt(cols[popIdx], 10) || 0) : 0,
      nse: parseFloat(cols[nseIdx]) || 0,
      region1: r1Idx >= 0 ? (cols[r1Idx] || '') : '',
      region2: r2Idx >= 0 ? (cols[r2Idx] || '') : '',
      region3: r3Idx >= 0 ? (cols[r3Idx] || '') : '',
    });
  }
  return records;
}

/**
 * GET /api/nse/{CC}
 * Returns parsed NSE data for a country, or 404 if not uploaded.
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ country: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { country } = await context.params;
  const data = await getConfig<NseRecord[]>(NSE_KEY(country));

  if (!data) {
    return NextResponse.json({ error: 'NSE data not found for this country' }, { status: 404 });
  }

  return NextResponse.json({ country: country.toUpperCase(), records: data.length, data });
}

/**
 * POST /api/nse/{CC}
 * Upload NSE CSV content (for small files < 3MB).
 * Body: { csv: string } (raw CSV content)
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ country: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { country } = await context.params;

  try {
    const { csv } = await request.json();
    if (!csv || typeof csv !== 'string') {
      return NextResponse.json({ error: 'csv field is required (string)' }, { status: 400 });
    }

    const records = parseNseCsv(csv);
    if (records.length === 0) {
      return NextResponse.json({ error: 'No valid records found in CSV' }, { status: 400 });
    }

    await putConfig(NSE_KEY(country), records);

    return NextResponse.json({
      country: country.toUpperCase(),
      records: records.length,
      message: `Uploaded ${records.length} postal codes for ${country.toUpperCase()}`,
    });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 400 });
  }
}
