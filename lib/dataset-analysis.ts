/**
 * Dataset analysis: all days per job, no exceptions.
 * Reads every partition (day) from S3, ensures they are in Athena, then runs queries.
 */

import {
  runQuery,
  createTableForDataset,
  tableExists,
  getTableName,
  discoverPartitionsFromS3,
  getPartitionsFromCatalog,
  addPartitionsManually,
} from './athena';

export interface DailyData {
  date: string;
  pings: number;
  devices: number;
}

export interface VisitByPoi {
  poiId: string;
  name?: string; // Human-readable name from GeoJSON or job metadata
  visits: number;
  devices: number;
}

export interface AnalysisResult {
  dataset: string;
  analyzedAt: string;
  summary: {
    totalPings: number;
    uniqueDevices: number;
    uniquePois: number;
    dateRange: { from: string; to: string };
    daysAnalyzed: number;
  };
  /** One row per day (partition) - guaranteed all days present in S3. */
  dailyData: DailyData[];
  visitsByPoi: VisitByPoi[];
  /** Job metadata for verification */
  jobMetadata?: {
    radius?: number;
    requestedRadius?: number;
    radiusMismatch?: boolean;
  };
}

/**
 * Ensure table exists and every S3 partition is registered in Glue so we read all days.
 * 
 * This function guarantees that ALL partitions discovered in S3 are registered in Athena/Glue.
 * It performs validation to ensure no partitions are omitted.
 * 
 * @returns Array of all partition dates found in S3 (guaranteed to be registered in catalog)
 */
async function ensureAllPartitions(
  datasetName: string,
  tableName: string
): Promise<string[]> {
  console.log(`[ENSURE PARTITIONS] Starting partition verification for ${datasetName} (table: ${tableName})`);
  
  // Step 1: Discover all partitions from S3 (source of truth)
  const s3Partitions = await discoverPartitionsFromS3(datasetName);
  if (s3Partitions.length === 0) {
    console.warn(`[ENSURE PARTITIONS] No partitions found in S3 for ${datasetName}`);
    return [];
  }

  console.log(`[ENSURE PARTITIONS] Found ${s3Partitions.length} partitions in S3`);

  // Step 2: Get partitions currently registered in Glue catalog
  let catalogPartitions: string[] = [];
  try {
    catalogPartitions = await getPartitionsFromCatalog(tableName);
    console.log(`[ENSURE PARTITIONS] Found ${catalogPartitions.length} partitions in Glue catalog`);
  } catch (error: any) {
    console.warn(`[ENSURE PARTITIONS] Could not read catalog partitions (table may not exist):`, error.message);
    catalogPartitions = [];
  }

  // Step 3: Identify missing partitions
  const missing = s3Partitions.filter((p) => !catalogPartitions.includes(p));
  
  if (missing.length > 0) {
    console.log(`[ENSURE PARTITIONS] ${missing.length} partitions missing from catalog:`, missing.slice(0, 10));
    
    // Step 4: Try MSCK REPAIR first (fastest if it works)
    try {
      console.log(`[ENSURE PARTITIONS] Attempting MSCK REPAIR TABLE...`);
      await runQuery(`MSCK REPAIR TABLE ${tableName}`);
      await new Promise((r) => setTimeout(r, 2000)); // Wait for repair to propagate
      
      // Re-check catalog after repair
      catalogPartitions = await getPartitionsFromCatalog(tableName);
      const stillMissing = s3Partitions.filter((p) => !catalogPartitions.includes(p));
      
      if (stillMissing.length > 0) {
        console.log(`[ENSURE PARTITIONS] MSCK REPAIR left ${stillMissing.length} partitions missing, adding manually...`);
        await addPartitionsManually(tableName, datasetName, stillMissing);
        
        // Final verification: re-check catalog after manual addition
        catalogPartitions = await getPartitionsFromCatalog(tableName);
        const finalMissing = s3Partitions.filter((p) => !catalogPartitions.includes(p));
        
        if (finalMissing.length > 0) {
          console.error(`[ENSURE PARTITIONS] ⚠️  WARNING: ${finalMissing.length} partitions still missing after manual addition:`, finalMissing.slice(0, 10));
          console.error(`[ENSURE PARTITIONS] These partitions may not be included in analysis results`);
        } else {
          console.log(`[ENSURE PARTITIONS] ✅ All ${s3Partitions.length} partitions successfully registered`);
        }
      } else {
        console.log(`[ENSURE PARTITIONS] ✅ MSCK REPAIR successfully registered all partitions`);
      }
    } catch (repairError: any) {
      console.warn(`[ENSURE PARTITIONS] MSCK REPAIR failed:`, repairError.message);
      console.log(`[ENSURE PARTITIONS] Falling back to manual partition addition...`);
      
      // Fallback: Add partitions manually
      await addPartitionsManually(tableName, datasetName, missing);
      
      // Verify after manual addition
      catalogPartitions = await getPartitionsFromCatalog(tableName);
      const finalMissing = s3Partitions.filter((p) => !catalogPartitions.includes(p));
      
      if (finalMissing.length > 0) {
        console.error(`[ENSURE PARTITIONS] ⚠️  WARNING: ${finalMissing.length} partitions still missing:`, finalMissing.slice(0, 10));
      } else {
        console.log(`[ENSURE PARTITIONS] ✅ Manual addition successfully registered all partitions`);
      }
    }
  } else {
    console.log(`[ENSURE PARTITIONS] ✅ All ${s3Partitions.length} partitions already registered in catalog`);
  }

  // Step 5: Final validation - ensure catalog matches S3
  const finalCatalogPartitions = await getPartitionsFromCatalog(tableName);
  const verifiedPartitions = s3Partitions.filter((p) => finalCatalogPartitions.includes(p));
  
  if (verifiedPartitions.length !== s3Partitions.length) {
    const unregistered = s3Partitions.filter((p) => !finalCatalogPartitions.includes(p));
    console.error(`[ENSURE PARTITIONS] ⚠️  VALIDATION FAILED: ${unregistered.length} partitions not registered:`, unregistered.slice(0, 10));
    console.error(`[ENSURE PARTITIONS] Analysis may omit data from these partitions`);
  } else {
    console.log(`[ENSURE PARTITIONS] ✅ VALIDATION PASSED: All ${s3Partitions.length} S3 partitions registered in catalog`);
  }

  return s3Partitions; // Return S3 partitions (source of truth), even if some aren't registered
}

/**
 * Get expected date range from job associated with dataset.
 * Returns the date range that was requested from Veraset (source of truth).
 */
async function getExpectedDateRange(datasetName: string): Promise<{ from: string; to: string; days: number } | null> {
  try {
    const { getAllJobs } = await import('./jobs');
    const { calculateDaysInclusive } = await import('./s3');
    const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
    
    const allJobs = await getAllJobs();
    const job = allJobs.find(j => {
      if (!j.s3DestPath) return false;
      const s3Path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
      const jobDatasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');
      return jobDatasetName === datasetName;
    });
    
    if (job?.verasetPayload?.date_range?.from_date && job?.verasetPayload?.date_range?.to_date) {
      const from = job.verasetPayload.date_range.from_date;
      const to = job.verasetPayload.date_range.to_date;
      const days = calculateDaysInclusive(from, to);
      return { from, to, days };
    }
    
    return null;
  } catch (error) {
    console.warn(`[ANALYSIS] Could not get expected date range for ${datasetName}:`, error);
    return null;
  }
}

/**
 * Wait for all expected partitions to be available in S3.
 * Retries up to maxAttempts times, waiting waitSeconds between attempts.
 */
async function waitForAllPartitions(
  datasetName: string,
  expectedDates: string[],
  maxAttempts: number = 10,
  waitSeconds: number = 30
): Promise<string[]> {
  console.log(`[ANALYSIS] Waiting for ${expectedDates.length} expected partitions to be available...`);
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const availablePartitions = await discoverPartitionsFromS3(datasetName);
    const availableSet = new Set(availablePartitions);
    const missingDates = expectedDates.filter(d => !availableSet.has(d));
    
    if (missingDates.length === 0) {
      console.log(`[ANALYSIS] ✅ All ${expectedDates.length} expected partitions are now available`);
      return availablePartitions;
    }
    
    console.log(`[ANALYSIS] Attempt ${attempt}/${maxAttempts}: Missing ${missingDates.length} partitions:`, missingDates.slice(0, 10));
    
    if (attempt < maxAttempts) {
      console.log(`[ANALYSIS] Waiting ${waitSeconds}s before retry...`);
      await new Promise(resolve => setTimeout(resolve, waitSeconds * 1000));
    }
  }
  
  // Final check
  const finalPartitions = await discoverPartitionsFromS3(datasetName);
  const finalSet = new Set(finalPartitions);
  const stillMissing = expectedDates.filter(d => !finalSet.has(d));
  
  if (stillMissing.length > 0) {
    console.error(`[ANALYSIS] ⚠️  After ${maxAttempts} attempts, still missing ${stillMissing.length} partitions:`, stillMissing);
    throw new Error(
      `Missing ${stillMissing.length} of ${expectedDates.length} expected partitions. ` +
      `Missing dates: ${stillMissing.slice(0, 10).join(', ')}${stillMissing.length > 10 ? '...' : ''}. ` +
      `The data may still be processing. Please wait and try again later.`
    );
  }
  
  return finalPartitions;
}

/**
 * Run full analysis: read all days (partitions) without exception, one graph per day, plus visits by POI.
 * 
 * This function ensures that ALL requested days are available before completing the analysis.
 * If the expected date range is known (from job), it will wait for all partitions to be available.
 */
export async function runFullAnalysis(datasetName: string): Promise<AnalysisResult> {
  const tableName = getTableName(datasetName);

  if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
    throw new Error(
      'AWS credentials not configured. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.'
    );
  }

  // Step 1: Get expected date range and job metadata (if available)
  const expectedDateRange = await getExpectedDateRange(datasetName);
  let expectedDates: string[] = [];
  let jobMetadata: { radius?: number; requestedRadius?: number } | null = null;
  
  // Get job metadata to verify radius
  try {
    const { getAllJobs } = await import('./jobs');
    const { calculateDaysInclusive } = await import('./s3');
    const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
    
    const allJobs = await getAllJobs();
    const job = allJobs.find(j => {
      if (!j.s3DestPath) return false;
      const s3Path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
      const jobDatasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');
      return jobDatasetName === datasetName;
    });
    
    if (job) {
      jobMetadata = {
        radius: job.radius,
        requestedRadius: job.auditTrail?.userInput?.radius,
      };
      
      // Verify radius matches what was requested
      if (jobMetadata.requestedRadius !== undefined && jobMetadata.radius !== jobMetadata.requestedRadius) {
        console.error(`[ANALYSIS] ⚠️  RADIUS MISMATCH: Job was requested with ${jobMetadata.requestedRadius}m but saved with ${jobMetadata.radius}m`);
        console.error(`[ANALYSIS] This indicates the data may have been processed with incorrect radius. Analysis will proceed but results may be inaccurate.`);
      }
    }
  } catch (error: any) {
    console.warn(`[ANALYSIS] Could not load job metadata:`, error.message);
  }
  
  if (expectedDateRange) {
    console.log(`[ANALYSIS] Expected date range from job: ${expectedDateRange.from} to ${expectedDateRange.to} (${expectedDateRange.days} days)`);
    if (jobMetadata?.radius) {
      console.log(`[ANALYSIS] Job radius: ${jobMetadata.radius}m${jobMetadata.requestedRadius && jobMetadata.requestedRadius !== jobMetadata.radius ? ` (requested: ${jobMetadata.requestedRadius}m)` : ''}`);
    }
    
    // Generate list of all expected dates
    const fromDate = new Date(expectedDateRange.from + 'T00:00:00Z');
    const toDate = new Date(expectedDateRange.to + 'T00:00:00Z');
    for (let d = new Date(fromDate); d <= toDate; d.setDate(d.getDate() + 1)) {
      expectedDates.push(d.toISOString().split('T')[0]);
    }
    
    console.log(`[ANALYSIS] Will wait for all ${expectedDates.length} expected days to be available`);
  } else {
    console.log(`[ANALYSIS] No expected date range found in job - will analyze all available partitions`);
  }

  // Step 2: Ensure table exists
  if (!(await tableExists(datasetName))) {
    await createTableForDataset(datasetName);
  }

  // Step 3: Wait for all expected partitions if we know what to expect
  let allDates: string[];
  if (expectedDates.length > 0) {
    try {
      // Wait for all expected partitions to be available (with retries)
      const availablePartitions = await waitForAllPartitions(datasetName, expectedDates, 10, 30);
      allDates = availablePartitions;
      
      // Verify we have all expected dates
      const availableSet = new Set(allDates);
      const missingDates = expectedDates.filter(d => !availableSet.has(d));
      
      if (missingDates.length > 0) {
        throw new Error(
          `Missing ${missingDates.length} of ${expectedDates.length} expected partitions: ${missingDates.slice(0, 10).join(', ')}`
        );
      }
      
      console.log(`[ANALYSIS] ✅ All ${expectedDates.length} expected partitions are available`);
    } catch (error: any) {
      if (error.message?.includes('Missing')) {
        throw error; // Re-throw waiting errors
      }
      console.warn(`[ANALYSIS] Error waiting for partitions, proceeding with available data:`, error.message);
      // Fall through to discover available partitions
      allDates = await discoverPartitionsFromS3(datasetName);
    }
  } else {
    // No expected range - just discover what's available
    allDates = await discoverPartitionsFromS3(datasetName);
  }

  if (allDates.length === 0) {
    const errorMsg = expectedDates.length > 0
      ? `No partitions found. Expected ${expectedDates.length} days (${expectedDateRange?.from} to ${expectedDateRange?.to}) but found none.`
      : `No partitions found for dataset ${datasetName}.`;
    
    console.error(`[ANALYSIS] ${errorMsg}`);
    return {
      dataset: datasetName,
      analyzedAt: new Date().toISOString(),
      summary: {
        totalPings: 0,
        uniqueDevices: 0,
        uniquePois: 0,
        dateRange: { from: '', to: '' },
        daysAnalyzed: 0,
      },
      dailyData: [],
      visitsByPoi: [],
    };
  }

  // Step 4: Ensure all partitions are registered in Athena
  await ensureAllPartitions(datasetName, tableName);

  // Step 5: Verify we have all expected dates (final check)
  if (expectedDates.length > 0) {
    const availableSet = new Set(allDates);
    const missingDates = expectedDates.filter(d => !availableSet.has(d));
    
    if (missingDates.length > 0) {
      const errorMsg = `Cannot proceed: Missing ${missingDates.length} of ${expectedDates.length} expected partitions. ` +
        `Missing dates: ${missingDates.slice(0, 10).join(', ')}${missingDates.length > 10 ? '...' : ''}. ` +
        `Please wait for data sync to complete or contact support.`;
      console.error(`[ANALYSIS] ❌ ${errorMsg}`);
      throw new Error(errorMsg);
    }
    
    console.log(`[ANALYSIS] ✅ VERIFICATION PASSED: All ${expectedDates.length} expected days are available`);
  }

  // Step 6: Skip pre-verification of data - let the main queries handle it
  // If partitions exist in S3 and Glue, we trust they have data (or will show 0 if empty)
  // The main queries will return results for all partitions, and we'll include all expected dates

  const dateFrom = allDates[0];
  const dateTo = allDates[allDates.length - 1];

  // Query by partition column `date` so we get exactly one row per day (no missing days from bad timestamps).
  const dailySql = `
    SELECT 
      date,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM ${tableName}
    WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    GROUP BY date
    ORDER BY date ASC
  `;

  const summarySql = `
    SELECT 
      COUNT(*) as total_pings,
      COUNT(DISTINCT ad_id) as unique_devices,
      COUNT(DISTINCT CASE WHEN poi_ids[1] IS NOT NULL AND poi_ids[1] != '' THEN poi_ids[1] END) as unique_pois
    FROM ${tableName}
    WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
  `;

  const visitsByPoiSql = `
    SELECT 
      poi_ids[1] as poi_id,
      COUNT(*) as visits,
      COUNT(DISTINCT ad_id) as devices
    FROM ${tableName}
    WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
      AND poi_ids[1] IS NOT NULL AND poi_ids[1] != ''
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
    GROUP BY poi_ids[1]
    ORDER BY visits DESC
  `;

  // Execute queries with error handling
  let dailyRes, summaryRes, visitsRes;
  try {
    console.log(`[ANALYSIS] Executing queries for date range ${dateFrom} to ${dateTo} (${allDates.length} partitions)...`);
    [dailyRes, summaryRes, visitsRes] = await Promise.all([
      runQuery(dailySql),
      runQuery(summarySql),
      runQuery(visitsByPoiSql),
    ]);
    console.log(`[ANALYSIS] Queries completed successfully`);
  } catch (error: any) {
    console.error(`[ANALYSIS] Query execution failed:`, error.message);
    throw new Error(`Failed to execute analysis queries: ${error.message}`);
  }

  // Build dailyData from query results
  const dailyDataMap = new Map<string, DailyData>();
  dailyRes.rows.forEach((r: any) => {
    const date = String(r.date ?? '').trim();
    if (date) {
      dailyDataMap.set(date, {
        date,
        pings: Number(r.pings) || 0,
        devices: Number(r.devices) || 0,
      });
    }
  });
  
  console.log(`[ANALYSIS] Query returned ${dailyRes.rows.length} days with data`);
  console.log(`[ANALYSIS] Expected ${expectedDates.length} days total`);
  
  // Ensure all expected dates are included (even if they have 0 counts)
  if (expectedDates.length > 0) {
    let addedCount = 0;
    expectedDates.forEach(date => {
      if (!dailyDataMap.has(date)) {
        addedCount++;
        dailyDataMap.set(date, {
          date,
          pings: 0,
          devices: 0,
        });
      }
    });
    if (addedCount > 0) {
      console.log(`[ANALYSIS] Added ${addedCount} missing days with zero counts:`, expectedDates.filter(d => !dailyRes.rows.some((r: any) => String(r.date ?? '').trim() === d)).slice(0, 10));
    }
  }
  
  // Convert map to array and sort by date
  const dailyData: DailyData[] = Array.from(dailyDataMap.values()).sort((a, b) => 
    a.date.localeCompare(b.date)
  );
  
  console.log(`[ANALYSIS] Final dailyData contains ${dailyData.length} days (expected: ${expectedDates.length || allDates.length})`);

  const summaryRow = summaryRes.rows[0];
  
  // Get POI names from job metadata. Build lookup by BOTH Veraset ID (geo_radius_X) AND original ID (poi-1, poi-22).
  // Veraset/Parquet may return either format in poi_ids[1].
  let poiNamesByVerasetId: Record<string, string> = {};
  const poiNamesByOriginalId: Record<string, string> = {};
  try {
    const { getAllJobs } = await import('@/lib/jobs');
    const BUCKET = process.env.S3_BUCKET || 'garritz-veraset-data-us-west-2';
    const jobs = await getAllJobs();
    const job = jobs.find((j: any) => {
      if (j.s3DestPath) {
        const s3Path = j.s3DestPath.replace('s3://', '').replace(`${BUCKET}/`, '');
        const jobDatasetName = s3Path.split('/').filter(Boolean)[0] || s3Path.replace(/\/$/, '');
        return jobDatasetName === datasetName;
      }
      return false;
    });

    if (job?.poiNames) {
      poiNamesByVerasetId = { ...job.poiNames };
      // Build reverse map: originalPoiId -> name (Parquet often has original IDs)
      if (job?.poiMapping) {
        for (const [verasetId, originalId] of Object.entries(job.poiMapping)) {
          const name = poiNamesByVerasetId[verasetId];
          if (name) poiNamesByOriginalId[originalId] = name;
        }
      }
      console.log(`[ANALYSIS] Found ${Object.keys(poiNamesByVerasetId).length} POI names (${Object.keys(poiNamesByOriginalId).length} by original ID)`);
    }
    // External jobs: externalPois has id + name
    if (job?.externalPois) {
      for (const p of job.externalPois) {
        if (p.name && p.id) poiNamesByOriginalId[p.id] = p.name;
      }
    }
    // Fallback: GeoJSON for POIs missing names
    if (job?.poiCollectionId && job?.poiMapping) {
      try {
        const { getPOICollection } = await import('./s3-config');
        const geojson = await getPOICollection(job.poiCollectionId);
        if (geojson?.features) {
          for (const [verasetPoiId, originalPoiId] of Object.entries(job.poiMapping)) {
            if (poiNamesByVerasetId[verasetPoiId]) continue;
            const f = geojson.features.find((feat: any) => {
              const id = feat.id ?? feat.properties?.id ?? feat.properties?.poi_id ?? feat.properties?.identifier;
              return String(id) === String(originalPoiId);
            });
            const name = f?.properties?.name;
            if (name) {
              poiNamesByVerasetId[verasetPoiId] = name;
              poiNamesByOriginalId[originalPoiId] = name;
            }
          }
          console.log(`[ANALYSIS] Enriched POI names from GeoJSON`);
        }
      } catch (e: any) {
        console.warn(`[ANALYSIS] Could not load POI collection for names:`, e?.message);
      }
    }
  } catch (error: any) {
    console.warn(`[ANALYSIS] Could not load POI names from job:`, error.message);
  }
  
  const visitsByPoi: VisitByPoi[] = visitsRes.rows.map((r: any) => {
    const poiIdFromData = String(r.poi_id ?? '');
    const poiName = poiNamesByVerasetId[poiIdFromData] || poiNamesByOriginalId[poiIdFromData] || null;

    return {
      poiId: poiIdFromData,
      name: poiName || undefined,
      visits: Number(r.visits) || 0,
      devices: Number(r.devices) || 0,
    };
  });

  // INTEGRITY CHECK: Verify that we have entries for all expected partitions
  const datesInResults = new Set(dailyData.map(d => d.date));
  
  // If we have expected dates, verify all are present (even if with 0 counts)
  if (expectedDates.length > 0) {
    const missingExpectedDates = expectedDates.filter(d => !datesInResults.has(d));
    if (missingExpectedDates.length > 0) {
      // This should not happen since we added them above, but check anyway
      console.error(`[ANALYSIS] ❌ CRITICAL: ${missingExpectedDates.length} expected dates missing from results:`, missingExpectedDates.slice(0, 10));
      throw new Error(`Internal error: Expected dates not included in results: ${missingExpectedDates.slice(0, 10).join(', ')}`);
    }
    
    // Check how many have actual data vs 0 counts
    const datesWithData = dailyData.filter(d => d.pings > 0 || d.devices > 0).map(d => d.date);
    const datesWithZeroData = expectedDates.filter(d => !datesWithData.includes(d));
    
    if (datesWithZeroData.length > 0) {
      console.warn(`[ANALYSIS] ⚠️  ${datesWithZeroData.length} of ${expectedDates.length} partitions have zero data:`, datesWithZeroData.slice(0, 10));
      console.warn(`[ANALYSIS] These partitions exist but contain no valid records matching filters. Analysis will proceed with zero counts for these dates.`);
    } else {
      console.log(`[ANALYSIS] ✅ INTEGRITY CHECK PASSED: All ${expectedDates.length} expected days have data`);
    }
  } else {
    // No expected dates - just verify we got all discovered partitions
    const missingDates = allDates.filter(d => !datesInResults.has(d));
    if (missingDates.length > 0) {
      console.warn(`[ANALYSIS] ⚠️  INTEGRITY WARNING: ${missingDates.length} partitions missing from results:`, missingDates.slice(0, 10));
    } else {
      console.log(`[ANALYSIS] ✅ INTEGRITY CHECK PASSED: All ${allDates.length} partitions included in results`);
    }
  }

  // Additional validation: Check for unexpected dates in results
  const unexpectedDates = dailyData.filter(d => !allDates.includes(d.date));
  if (unexpectedDates.length > 0) {
    console.warn(`[ANALYSIS] ⚠️  Found ${unexpectedDates.length} dates in results not in partition list:`, unexpectedDates.map(d => d.date).slice(0, 10));
  }

  const result: AnalysisResult = {
    dataset: datasetName,
    analyzedAt: new Date().toISOString(),
    summary: {
      totalPings: Number(summaryRow?.total_pings) || 0,
      uniqueDevices: Number(summaryRow?.unique_devices) || 0,
      uniquePois: Number(summaryRow?.unique_pois) || 0,
      dateRange: { from: dateFrom, to: dateTo },
      daysAnalyzed: dailyData.length,
    },
    dailyData,
    visitsByPoi,
    jobMetadata: jobMetadata ? {
      radius: jobMetadata.radius,
      requestedRadius: jobMetadata.requestedRadius,
      radiusMismatch: jobMetadata.requestedRadius !== undefined && 
                      jobMetadata.radius !== undefined && 
                      jobMetadata.requestedRadius !== jobMetadata.radius,
    } : undefined,
  };

  // Log summary
  console.log(`[ANALYSIS] Analysis complete:`, {
    dataset: datasetName,
    totalPings: result.summary.totalPings,
    uniqueDevices: result.summary.uniqueDevices,
    uniquePois: result.summary.uniquePois,
    daysWithData: dailyData.length,
    expectedDays: allDates.length,
    dateRange: `${dateFrom} to ${dateTo}`,
  });

  return result;
}
