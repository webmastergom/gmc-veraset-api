/**
 * POST /api/mega-jobs/[id]/repair-chunk
 *
 * Surgical sub-job creation for a specific (dateChunkIdx, poiChunkIdx) pair
 * — used to fill gaps left by the create-poll race condition (where two
 * concurrent calls both created the same chunk and another chunk was
 * skipped). Body: `{ dateChunkIdx: number, poiChunkIdx?: number }`.
 *
 * Mirrors create-poll's Veraset payload construction but does NOT advance
 * `progress.created` from the counter — it explicitly creates the chunk
 * the caller asks for, then appends the resulting jobId to subJobIds.
 *
 * Idempotent: if a sub-job for the same megaJobIndex already exists in the
 * megajob's subJobIds, returns 409 instead of duplicating.
 */

import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob, updateMegaJob } from '@/lib/mega-jobs';
import { createJob, getJob } from '@/lib/jobs';
import { incrementUsage } from '@/lib/usage';
import { invalidateCache } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  const t0 = Date.now();
  try {
    const { id } = await context.params;
    let body: any = {};
    try { body = await request.json(); } catch {}

    const dateChunkIdx = parseInt(body?.dateChunkIdx, 10);
    const poiChunkIdx = parseInt(body?.poiChunkIdx ?? '0', 10) || 0;
    if (isNaN(dateChunkIdx) || dateChunkIdx < 0) {
      return NextResponse.json({ error: 'dateChunkIdx required (non-negative integer)' }, { status: 400 });
    }

    invalidateCache(`mega-jobs/${id}`);
    const megaJob = await getMegaJob(id);
    if (!megaJob) return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    if (megaJob.mode !== 'auto-split') {
      return NextResponse.json({ error: 'Only auto-split mega-jobs can be repaired' }, { status: 400 });
    }
    if (!megaJob.sourceScope || !megaJob.splits) {
      return NextResponse.json({ error: 'Mega-job has no split plan' }, { status: 400 });
    }

    const dateChunk = megaJob.splits.dateChunks[dateChunkIdx];
    const poiChunk = megaJob.splits.poiChunks[poiChunkIdx];
    if (!dateChunk) return NextResponse.json({ error: `dateChunkIdx ${dateChunkIdx} out of range (have ${megaJob.splits.dateChunks.length})` }, { status: 400 });
    if (!poiChunk) return NextResponse.json({ error: `poiChunkIdx ${poiChunkIdx} out of range` }, { status: 400 });

    const targetMegaJobIndex = dateChunkIdx * megaJob.splits.poiChunks.length + poiChunkIdx;

    // Idempotency: check whether a sub-job for this megaJobIndex already exists.
    for (const jid of (megaJob.subJobIds || [])) {
      const existing = await getJob(jid);
      if (existing && (existing as any).megaJobIndex === targetMegaJobIndex) {
        return NextResponse.json({
          error: `Chunk already exists`,
          existingJobId: jid,
          megaJobIndex: targetMegaJobIndex,
        }, { status: 409 });
      }
    }

    const scope = megaJob.sourceScope;
    console.log(`[MEGA-REPAIR ${id}] Creating chunk D${dateChunkIdx}-P${poiChunkIdx} (megaJobIndex=${targetMegaJobIndex}): ${dateChunk.from}→${dateChunk.to}`);

    // ── Build geo_radius from POI collections ──
    const { getPOICollection } = await import('@/lib/poi-storage');
    const collectionIds = scope.poiCollectionIds || (scope.poiCollectionId ? [scope.poiCollectionId] : []);
    const validFeatures: any[] = [];
    for (const colId of collectionIds) {
      const geojson = await getPOICollection(colId);
      if (!geojson?.features) {
        return NextResponse.json({ error: `POI collection "${colId}" GeoJSON not found` }, { status: 404 });
      }
      const features = geojson.features.filter(
        (f: any) =>
          f.geometry?.type === 'Point' &&
          Array.isArray(f.geometry.coordinates) &&
          f.geometry.coordinates.length >= 2
      );
      validFeatures.push(...features);
    }
    const chunkFeatures = validFeatures.slice(poiChunk.startIndex, poiChunk.endIndex);
    const geoRadius = chunkFeatures.map((f: any, i: number) => ({
      poi_id: f.properties?.id || f.id || `poi_${poiChunk.startIndex + i}`,
      latitude: f.geometry.coordinates[1],
      longitude: f.geometry.coordinates[0],
      distance_in_meters: scope.radius,
    }));
    const poiMapping: Record<string, string> = {};
    const poiNames: Record<string, string> = {};
    geoRadius.forEach((poi: any, i: number) => {
      const verasetId = `geo_radius_${i}`;
      poiMapping[verasetId] = poi.poi_id;
      const name = chunkFeatures[i]?.properties?.name;
      if (name) poiNames[verasetId] = name;
    });

    const verasetPayload = {
      date_range: { from_date: dateChunk.from, to_date: dateChunk.to },
      schema_type: scope.schema,
      geo_radius: geoRadius,
    };

    // ── Veraset call ──
    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) return NextResponse.json({ error: 'VERASET_API_KEY not configured' }, { status: 500 });

    const endpoints: Record<string, string> = {
      pings: '/v1/movement/job/pings',
      devices: '/v1/movement/job/devices',
      aggregate: '/v1/movement/job/aggregate',
      cohort: '/v1/movement/job/cohort',
      pings_by_device: '/v1/movement/job/pings_by_device',
    };
    const endpoint = endpoints[scope.type] || endpoints.pings;
    const verasetUrl = `https://platform.prd.veraset.tech${endpoint}`;

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 40000);
    const response = await fetch(verasetUrl, {
      method: 'POST',
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json', 'X-API-Key': verasetApiKey },
      body: JSON.stringify(verasetPayload),
    });
    clearTimeout(timeout);

    const responseText = await response.text();
    let verasetData: any;
    try { verasetData = JSON.parse(responseText); } catch { verasetData = { raw: responseText.substring(0, 500) }; }

    if (!response.ok) {
      console.error(`[MEGA-REPAIR ${id}] Veraset error ${response.status}:`, responseText.substring(0, 300));
      return NextResponse.json({
        error: `Veraset ${response.status}: ${verasetData.error_message || 'unknown'}`,
      }, { status: 502 });
    }

    const jobId = verasetData.job_id || verasetData.data?.job_id;
    if (!jobId) return NextResponse.json({ error: 'No job_id from Veraset' }, { status: 502 });

    // Job name reflects the chunk position
    const subJobName = megaJob.splits.dateChunks.length > 1 && megaJob.splits.poiChunks.length > 1
      ? `${megaJob.name} [D${dateChunkIdx + 1}-P${poiChunkIdx + 1}]`
      : megaJob.splits.dateChunks.length > 1
        ? `${megaJob.name} [${dateChunk.from}→${dateChunk.to}]`
        : `${megaJob.name} [POIs ${poiChunk.startIndex + 1}–${poiChunk.endIndex}]`;

    await createJob({
      jobId,
      name: subJobName,
      type: scope.type,
      poiCount: chunkFeatures.length,
      poiCollectionId: collectionIds[0],
      dateRange: { from: dateChunk.from, to: dateChunk.to },
      radius: scope.radius,
      schema: scope.schema as 'BASIC' | 'FULL' | 'ENHANCED' | 'N/A',
      status: 'QUEUED',
      s3SourcePath: `s3://veraset-prd-platform-us-west-2/output/garritz/${jobId}/`,
      external: !!megaJob.apiKeyName,
      apiKeyName: megaJob.apiKeyName,
      country: megaJob.country,
      poiMapping,
      poiNames,
      megaJobId: id,
      megaJobIndex: targetMegaJobIndex,
      verasetPayload: {
        date_range: verasetPayload.date_range,
        schema_type: verasetPayload.schema_type,
        geo_radius: verasetPayload.geo_radius,
      },
    });

    await incrementUsage(jobId);

    // Re-read megaJob to avoid clobbering concurrent updates, append + write.
    invalidateCache(`mega-jobs/${id}`);
    const fresh = await getMegaJob(id);
    if (!fresh) return NextResponse.json({ error: 'Mega-job vanished mid-repair' }, { status: 500 });
    const newSubJobIds = [...(fresh.subJobIds || []), jobId];
    const newProgress = { ...fresh.progress, created: newSubJobIds.length };
    if (newProgress.created >= newProgress.total && fresh.status === 'creating') {
      await updateMegaJob(id, { subJobIds: newSubJobIds, progress: newProgress, status: 'running' });
    } else {
      await updateMegaJob(id, { subJobIds: newSubJobIds, progress: newProgress });
    }

    console.log(`[MEGA-REPAIR ${id}] Created ${jobId} for D${dateChunkIdx}-P${poiChunkIdx} [${Date.now() - t0}ms]`);

    return NextResponse.json({
      createdJobId: jobId,
      subJobName,
      megaJobIndex: targetMegaJobIndex,
      dateChunk,
    });
  } catch (error: any) {
    const isAbort = error.name === 'AbortError';
    console.error(`[MEGA-REPAIR] ${isAbort ? 'TIMEOUT' : 'ERROR'}:`, error.message);
    return NextResponse.json(
      { error: isAbort ? 'Veraset API timeout' : error.message },
      { status: isAbort ? 504 : 500 }
    );
  }
}
