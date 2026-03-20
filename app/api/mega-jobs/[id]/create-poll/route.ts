import { NextRequest, NextResponse } from 'next/server';
import { getMegaJob, updateMegaJob, getChunkIndices } from '@/lib/mega-jobs';
import { createJob } from '@/lib/jobs';
import { incrementUsage } from '@/lib/usage';

export const dynamic = 'force-dynamic';
export const maxDuration = 60;

/**
 * POST /api/mega-jobs/[id]/create-poll
 * Creates ONE sub-job per call via Veraset API.
 * Frontend polls until progress.created === progress.total.
 *
 * On first call: transitions from 'planning' to 'creating'.
 * On last sub-job: transitions to 'running'.
 */
export async function POST(
  _request: NextRequest,
  context: { params: Promise<{ id: string }> }
): Promise<NextResponse> {
  const t0 = Date.now();

  try {
    const { id } = await context.params;
    const megaJob = await getMegaJob(id);

    if (!megaJob) {
      return NextResponse.json({ error: 'Mega-job not found' }, { status: 404 });
    }

    if (megaJob.mode !== 'auto-split') {
      return NextResponse.json({ error: 'Only auto-split mega-jobs need create-poll' }, { status: 400 });
    }

    if (!megaJob.sourceScope || !megaJob.splits) {
      return NextResponse.json({ error: 'Mega-job has no split plan' }, { status: 400 });
    }

    // Already done creating?
    if (megaJob.progress.created >= megaJob.progress.total) {
      return NextResponse.json({
        megaJob,
        message: 'All sub-jobs already created',
        done: true,
      });
    }

    // Transition from planning → creating on first call
    if (megaJob.status === 'planning') {
      await updateMegaJob(id, { status: 'creating' });
      megaJob.status = 'creating';
    }

    // Determine which sub-job to create next
    const nextIndex = megaJob.progress.created;
    const { dateChunkIdx, poiChunkIdx } = getChunkIndices(
      nextIndex,
      megaJob.splits.poiChunks.length
    );

    const dateChunk = megaJob.splits.dateChunks[dateChunkIdx];
    const poiChunk = megaJob.splits.poiChunks[poiChunkIdx];
    const scope = megaJob.sourceScope;

    console.log(
      `[MEGA-JOB ${id}] Creating sub-job ${nextIndex + 1}/${megaJob.progress.total}: ` +
      `dates=${dateChunk.from}→${dateChunk.to} pois=${poiChunk.startIndex}–${poiChunk.endIndex}`
    );

    // Load POI GeoJSON and slice
    const { getPOICollection } = await import('@/lib/poi-storage');
    const geojson = await getPOICollection(scope.poiCollectionId);
    if (!geojson?.features) {
      return NextResponse.json({ error: 'POI collection GeoJSON not found' }, { status: 404 });
    }

    // Filter to valid Point features and slice for this chunk
    const validFeatures = geojson.features.filter(
      (f: any) =>
        f.geometry?.type === 'Point' &&
        Array.isArray(f.geometry.coordinates) &&
        f.geometry.coordinates.length >= 2
    );

    const chunkFeatures = validFeatures.slice(poiChunk.startIndex, poiChunk.endIndex);

    // Build geo_radius array
    const geoRadius = chunkFeatures.map((f: any, i: number) => ({
      poi_id: f.properties?.id || f.id || `poi_${poiChunk.startIndex + i}`,
      latitude: f.geometry.coordinates[1],
      longitude: f.geometry.coordinates[0],
      distance_in_meters: scope.radius,
    }));

    // Build POI mapping and names
    const poiMapping: Record<string, string> = {};
    const poiNames: Record<string, string> = {};
    geoRadius.forEach((poi: any, i: number) => {
      const verasetId = `geo_radius_${i}`;
      poiMapping[verasetId] = poi.poi_id;
      const name = chunkFeatures[i]?.properties?.name;
      if (name) poiNames[verasetId] = name;
    });

    // Build Veraset payload
    const verasetPayload = {
      date_range: { from_date: dateChunk.from, to_date: dateChunk.to },
      schema_type: scope.schema,
      geo_radius: geoRadius,
    };

    // Call Veraset API
    const verasetApiKey = process.env.VERASET_API_KEY?.trim();
    if (!verasetApiKey) {
      return NextResponse.json({ error: 'VERASET_API_KEY not configured' }, { status: 500 });
    }

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
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': verasetApiKey,
      },
      body: JSON.stringify(verasetPayload),
    });
    clearTimeout(timeout);

    const responseText = await response.text();
    let verasetData: any;
    try { verasetData = JSON.parse(responseText); } catch { verasetData = { raw: responseText.substring(0, 500) }; }

    if (!response.ok) {
      console.error(`[MEGA-JOB ${id}] Veraset error ${response.status}:`, responseText.substring(0, 300));
      // Don't fail the whole mega-job on one sub-job failure — record and continue
      megaJob.progress.failed += 1;
      await updateMegaJob(id, {
        progress: megaJob.progress,
        error: `Sub-job ${nextIndex + 1} failed: ${verasetData.error_message || response.status}`,
      });
      // Skip this sub-job and advance created counter so next poll creates the next one
      megaJob.progress.created += 1;
      await updateMegaJob(id, { progress: megaJob.progress });

      return NextResponse.json({
        megaJob: await getMegaJob(id),
        subJobError: `Veraset ${response.status}: ${verasetData.error_message || 'unknown'}`,
        done: megaJob.progress.created >= megaJob.progress.total,
      });
    }

    const jobId = verasetData.job_id || verasetData.data?.job_id;
    if (!jobId) {
      return NextResponse.json({ error: 'No job_id from Veraset' }, { status: 502 });
    }

    // Save sub-job
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
      poiCollectionId: scope.poiCollectionId,
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
      megaJobIndex: nextIndex,
      verasetPayload: {
        date_range: verasetPayload.date_range,
        schema_type: verasetPayload.schema_type,
        geo_radius: verasetPayload.geo_radius,
      },
    });

    await incrementUsage(jobId);

    // Update mega-job progress
    megaJob.subJobIds.push(jobId);
    megaJob.progress.created += 1;

    const allCreated = megaJob.progress.created >= megaJob.progress.total;
    if (allCreated) {
      megaJob.status = 'running';
    }

    await updateMegaJob(id, {
      subJobIds: megaJob.subJobIds,
      progress: megaJob.progress,
      status: megaJob.status,
    });

    console.log(
      `[MEGA-JOB ${id}] Sub-job ${nextIndex + 1}/${megaJob.progress.total} created: ${jobId} [${Date.now() - t0}ms]`
    );

    return NextResponse.json({
      megaJob: await getMegaJob(id),
      createdJobId: jobId,
      subJobName,
      done: allCreated,
    });
  } catch (error: any) {
    const isAbort = error.name === 'AbortError';
    console.error(`[MEGA-JOB create-poll] ${isAbort ? 'TIMEOUT' : 'ERROR'}:`, error.message);
    return NextResponse.json(
      { error: isAbort ? 'Veraset API timeout' : error.message },
      { status: isAbort ? 504 : 500 }
    );
  }
}
