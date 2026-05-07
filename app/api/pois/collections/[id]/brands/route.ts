/**
 * Brand editor API for a POI collection.
 *
 * GET — runs the same 3-layer brand resolution that personas does
 *   (override property → frequency-discovered → BRAND_RULES) and
 *   returns the result per POI plus a brand summary. The user can then
 *   eyeball the auto-discovery, spot garbage / collisions, and fix.
 *
 * POST — accepts `{ updates: { [poiId]: brand } }` and writes the
 *   chosen brand into `properties.brand` of each matching POI feature.
 *   The next persona run picks them up via Layer 1 (override).
 */

import { NextRequest, NextResponse } from 'next/server';
import { getPOICollection, putPOICollection } from '@/lib/poi-storage';
import { discoverBrands, type PoiNameLookup } from '@/lib/persona-brand-discovery';
import { resolveBrand } from '@/lib/persona-brand-lookup';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/** Same case-insensitive brand-property keys recognised by the personas pipeline. */
const BRAND_PROP_KEYS = [
  'brand', 'chain',
  'cadena', 'marca', 'franquicia',
  'concesionaria', 'concesionario',
  'operador', 'operator',
  'enseigne',
];

function pickBrandProp(props: Record<string, any> | undefined): string {
  if (!props) return '';
  const lower: Record<string, string> = {};
  for (const k of Object.keys(props)) {
    const v = props[k];
    if (typeof v === 'string' && v.trim()) lower[k.toLowerCase()] = v.trim();
  }
  for (const key of BRAND_PROP_KEYS) {
    if (lower[key]) return lower[key];
  }
  return '';
}

interface BrandedPoi {
  poiId: string;
  name: string;
  currentBrand: string;
  /** Where the current brand came from: explicit override, frequency discovery, hardcoded rules, or 'other'. */
  source: 'override' | 'discovered' | 'rules' | 'other';
}

interface BrandSummary {
  brand: string;
  count: number;
}

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  const { id } = await context.params;
  const geojson = await getPOICollection(id);
  if (!geojson) {
    return NextResponse.json({ error: 'POI collection not found' }, { status: 404 });
  }

  // Prepare poiInputs in the same shape the personas pipeline uses, so the
  // editor preview matches what'd actually be applied at run time.
  const poiInputs: PoiNameLookup[] = [];
  const overrideMap: Record<string, string> = {};
  for (const f of (geojson.features || []) as any[]) {
    const poiId = String(f.properties?.id ?? f.id ?? '').trim();
    if (!poiId) continue;
    const name = String(f.properties?.name || f.properties?.label || '').trim();
    const override = pickBrandProp(f.properties);
    if (override) overrideMap[poiId] = override;
    poiInputs.push({ poiId, name, brandOverride: override || undefined });
  }

  const discovery = discoverBrands(poiInputs);

  const pois: BrandedPoi[] = [];
  const counts = new Map<string, number>();
  for (const p of poiInputs) {
    let currentBrand = discovery.poiToBrand.get(p.poiId) || 'other';
    let source: BrandedPoi['source'] =
      overrideMap[p.poiId] ? 'override' : currentBrand !== 'other' ? 'discovered' : 'other';
    if (currentBrand === 'other' && p.name) {
      const rb = resolveBrand(p.name);
      if (rb !== 'other') {
        currentBrand = rb;
        source = 'rules';
      }
    }
    pois.push({ poiId: p.poiId, name: p.name, currentBrand, source });
    counts.set(currentBrand, (counts.get(currentBrand) || 0) + 1);
  }

  const summary: BrandSummary[] = Array.from(counts.entries())
    .map(([brand, count]) => ({ brand, count }))
    .sort((a, b) => b.count - a.count);

  return NextResponse.json({
    collectionId: id,
    poiCount: pois.length,
    pois,
    summary,
    /** Discovered candidates with frequency, for the suggestion dropdown. */
    candidates: discovery.candidates,
  });
}

interface ApplyBody {
  /** poiId → brand. Empty string clears the override (falls back to discovery/rules). */
  updates: Record<string, string>;
}

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  const { id } = await context.params;
  let body: ApplyBody;
  try {
    body = (await request.json()) as ApplyBody;
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }
  if (!body || typeof body.updates !== 'object' || body.updates === null) {
    return NextResponse.json({ error: 'updates object required' }, { status: 400 });
  }

  const geojson = await getPOICollection(id);
  if (!geojson) {
    return NextResponse.json({ error: 'POI collection not found' }, { status: 404 });
  }

  const updates = body.updates;
  let touched = 0;
  for (const f of (geojson.features || []) as any[]) {
    const poiId = String(f.properties?.id ?? f.id ?? '').trim();
    if (!poiId || !(poiId in updates)) continue;
    f.properties = f.properties || {};
    const next = String(updates[poiId] ?? '').trim();
    if (next === '') {
      // Clear override — remove all known brand-property keys (case-insensitive)
      // so the auto-discovery layer takes over again.
      for (const k of Object.keys(f.properties)) {
        if (BRAND_PROP_KEYS.includes(k.toLowerCase())) delete f.properties[k];
      }
    } else {
      // Canonicalize: store under lowercase 'brand' key. Remove any other brand
      // aliases so we don't have conflicting properties.
      for (const k of Object.keys(f.properties)) {
        if (BRAND_PROP_KEYS.includes(k.toLowerCase()) && k !== 'brand') {
          delete f.properties[k];
        }
      }
      f.properties.brand = next;
    }
    touched++;
  }

  if (touched === 0) {
    return NextResponse.json({ error: 'No matching POIs in updates' }, { status: 400 });
  }

  await putPOICollection(id, geojson);

  return NextResponse.json({ success: true, touched });
}
