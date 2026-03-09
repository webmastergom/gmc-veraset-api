import { NextRequest, NextResponse } from 'next/server';
import { getConfig, putConfig } from '@/lib/s3-config';
import type { CountryDatasetEntry } from '@/lib/country-dataset-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

const CONFIG_KEY = 'country-dataset-config';

export interface CountryDatasetConfig {
  entries: Record<string, CountryDatasetEntry>;
  updatedAt: string;
}

/** GET /api/settings/country-datasets — Read current config */
export async function GET() {
  try {
    const config = await getConfig<CountryDatasetConfig>(CONFIG_KEY);
    return NextResponse.json(config || { entries: {}, updatedAt: new Date().toISOString() });
  } catch {
    return NextResponse.json({ entries: {}, updatedAt: new Date().toISOString() });
  }
}

/** PUT /api/settings/country-datasets — Save full config */
export async function PUT(request: NextRequest) {
  try {
    const body = await request.json();
    const entries: Record<string, CountryDatasetEntry> = body.entries || {};

    // Validate entries
    for (const [code, entry] of Object.entries(entries)) {
      if (code.length !== 2) {
        return NextResponse.json({ error: `Invalid country code: "${code}". Must be 2 letters.` }, { status: 400 });
      }
      if (!entry.dataset || typeof entry.dataset !== 'string') {
        return NextResponse.json({ error: `Missing dataset for country "${code}".` }, { status: 400 });
      }
      if (!entry.label || typeof entry.label !== 'string') {
        return NextResponse.json({ error: `Missing label for country "${code}".` }, { status: 400 });
      }
    }

    const config: CountryDatasetConfig = {
      entries,
      updatedAt: new Date().toISOString(),
    };

    await putConfig(CONFIG_KEY, config);
    return NextResponse.json(config);
  } catch (error: any) {
    return NextResponse.json({ error: error.message || 'Failed to save config' }, { status: 500 });
  }
}
