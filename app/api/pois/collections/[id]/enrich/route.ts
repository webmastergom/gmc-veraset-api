import { NextRequest, NextResponse } from 'next/server';
import { getPOICollection, putPOICollection, getConfig, putConfig } from '@/lib/s3-config';
import { enrichPOICollection, applyEnrichmentToGeoJSON } from '@/lib/poi-enrichment';
import type { EnrichmentMatch } from '@/lib/poi-enrichment';

export const dynamic = 'force-dynamic';
export const maxDuration = 300; // 5 minutes

/**
 * POST /api/pois/collections/[id]/enrich
 * Enrich a POI collection with Veraset placekeys.
 *
 * Body options:
 * - { action: "enrich" } Run enrichment (default)
 * - { action: "apply", matches: [...] } Apply enrichment results to GeoJSON
 * - { action: "enrich", limit: 100 } Enrich only first N POIs (for testing)
 */
export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const collectionId = params.id;
    const apiKey = process.env.VERASET_API_KEY?.trim();
    
    if (!apiKey) {
      return NextResponse.json(
        { error: 'VERASET_API_KEY not configured' },
        { status: 500 }
      );
    }

    const body = await request.json().catch(() => ({}));
    const action = body.action || 'enrich';

    // Load the GeoJSON collection
    const geojson = await getPOICollection(collectionId);
    if (!geojson) {
      return NextResponse.json(
        { error: 'POI collection not found', collectionId },
        { status: 404 }
      );
    }

    const features = geojson.features || [];
    if (!features.length) {
      return NextResponse.json(
        { error: 'POI collection is empty' },
        { status: 400 }
      );
    }

    if (action === 'apply') {
      // Apply enrichment results
      const matches: EnrichmentMatch[] = body.matches;
      if (!matches?.length) {
        return NextResponse.json(
          { error: 'No matches provided' },
          { status: 400 }
        );
      }

      const enrichedGeoJSON = applyEnrichmentToGeoJSON(geojson, matches);

      // Save enriched collection (overwrite)
      await putPOICollection(collectionId, enrichedGeoJSON);

      // Update collection metadata
      const collections = await getConfig<Record<string, any>>('poi-collections') || {};
      if (collections[collectionId]) {
        const matchedCount = matches.filter(m => m.status === 'matched').length;
        collections[collectionId].enrichedAt = new Date().toISOString();
        collections[collectionId].enrichedMatchRate = matchedCount / features.length;
        collections[collectionId].enrichedCount = matchedCount;
        await putConfig('poi-collections', collections);
      }

      return NextResponse.json({
        success: true,
        collectionId,
        totalFeatures: features.length,
        enrichedCount: matches.filter(m => m.status === 'matched').length,
      });
    }

    // Default: Run enrichment
    const limit = body.limit || features.length;
    const featuresToEnrich = features.slice(0, limit);

    console.log(`Starting enrichment for ${featuresToEnrich.length} POIs in collection ${collectionId}`);

    const result = await enrichPOICollection(
      featuresToEnrich,
      apiKey,
      (current, total) => {
        if (current % 50 === 0 || current === total) {
          console.log(`  Enrichment progress: ${current}/${total} (${Math.round(current / total * 100)}%)`);
        }
      }
    );

    result.collectionId = collectionId;

    return NextResponse.json(result);

  } catch (error: any) {
    console.error('Enrichment error:', error);
    return NextResponse.json(
      { error: 'Enrichment failed', details: error.message },
      { status: 500 }
    );
  }
}
