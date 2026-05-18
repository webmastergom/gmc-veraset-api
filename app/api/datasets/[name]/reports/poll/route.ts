import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import {
  startQueryAsync,
  checkQueryStatus,
  fetchQueryResults,
  ensureTableForDataset,
  getTableName,
} from '@/lib/athena';
import { getConfig, putConfig } from '@/lib/s3-config';
import {
  parseConsolidatedOD,
  parseConsolidatedHourly,
  parseConsolidatedMobility,
  buildODReport,
  buildCatchmentReport,
  buildTemporalTrends,
} from '@/lib/mega-report-consolidation';
import { batchReverseGeocode, setCountryFilter } from '@/lib/reverse-geocode';
import { homeTableExists, homeTableName, startHomeDetection, pollHomeDetection } from '@/lib/home-detector';
import { MIN_DISTINCT_DAYS_FOR_HUMAN_MAID } from '@/lib/bot-filter';

export const dynamic = 'force-dynamic';
export const maxDuration = 300;

const STATE_KEY = (ds: string) => `dataset-report-state/${ds}`;
function REPORT_KEY(ds: string, type: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  let key = `dataset-reports/${ds}/${type}`;
  if (minDwell > 0 || maxDwell > 0) key += `-dwell-${minDwell}-${maxDwell}`;
  if (hourFrom > 0 || hourTo < 23) key += `-h${hourFrom}-${hourTo}`;
  if (minVisits > 1) key += `-v${minVisits}`;
  if (gpsOnly) key += `-gps`;
  if (maxCircleScore > 0) key += `-cs${maxCircleScore}`;
  if (daysOfWeek && daysOfWeek.length > 0 && daysOfWeek.length < 7) {
    const sorted = [...daysOfWeek].sort((a, b) => a - b);
    key += `-d${sorted.join('')}`;
  }
  if (discardEmployees) key += `-noemp`;
  if (discardResidents) key += `-nores`;
  return key;
}

// ── Types ─────────────────────────────────────────────────────────────

interface ReportState {
  /**
   * Parsing used to be one monolithic phase that fetched OD + catchment
   * + affinity rows, geocoded everything in memory, and saved all
   * reports in a single request. On 50+ GB datasets the geocoding step
   * alone exceeded the request budget and the state stayed stuck in
   * 'parsing' forever. We now split that work into three sub-phases:
   *
   *   parsing         → save the fast reports (hourly/dayhour/temporal/
   *                     mobility/totalDevices), fetch the geocoding-bound
   *                     rows from Athena and cache them to S3.
   *   parsing_geocode → load cached rows, run batchReverseGeocode once,
   *                     cache coord→zip map to S3.
   *   parsing_save    → load cached rows + coord→zip, build & save the
   *                     OD / catchment / affinity reports, clean up
   *                     cache, transition to done.
   *
   * Each sub-phase finishes in well under the 5-min Vercel maxDuration,
   * so the state machine always advances on every poll instead of
   * silently rolling back.
   */
  phase: 'start' | 'home_detection' | 'polling' | 'parsing' | 'parsing_geocode' | 'parsing_save' | 'done' | 'error';
  queries: Record<string, string>;  // name → queryId
  /** Athena queryId of the TC-WK-19-7 home-detection CTAS that runs
   *  before the 8 report queries when the home table is missing. */
  homeDetectionQueryId?: string;
  datasetSizeGB?: number;
  /** Tracks per-step state shared across the parsing_* sub-phases.
   *  Set during the 'parsing' phase; consumed by 'parsing_geocode'
   *  and 'parsing_save'. The actual row payloads live in S3 cache
   *  files (see CACHE_KEY below) to keep this state file small. */
  parsingCache?: {
    /** Country detected for this dataset (used to scope geocoding). */
    detectedCountry?: string;
    /** Number of coords queued for reverse-geocoding. */
    coordsToGeocode?: number;
    /** Number of coords successfully geocoded (computed in parsing_geocode). */
    geocodedCount?: number;
  };
  minDwell?: number;
  maxDwell?: number;
  hourFrom?: number;
  hourTo?: number;
  minVisits?: number;
  gpsOnly?: boolean;
  maxCircleScore?: number;
  daysOfWeek?: number[];
  discardEmployees?: boolean;
  discardResidents?: boolean;
  error?: string;
}

/** S3 keys used by the parsing_* sub-phases to pass row payloads
 *  and the coord→zip map between requests. Cleaned up on transition
 *  to 'done' (or left around for diagnostics on transition to 'error'). */
const CACHE_KEY = {
  odRows: (ds: string) => `dataset-report-cache/${ds}/od-rows`,
  catchmentRows: (ds: string) => `dataset-report-cache/${ds}/catchment-rows`,
  affinityRows: (ds: string) => `dataset-report-cache/${ds}/affinity-rows`,
  coordsToGeocode: (ds: string) => `dataset-report-cache/${ds}/coords-to-geocode`,
  coordToZip: (ds: string) => `dataset-report-cache/${ds}/coord-to-zip`,
};

async function getState(ds: string): Promise<ReportState | null> {
  return await getConfig<ReportState>(STATE_KEY(ds));
}

async function saveState(ds: string, state: ReportState): Promise<void> {
  await putConfig(STATE_KEY(ds), state, { compact: true });
}

// ── Query builders using poi_ids (no spatial join) ────────────────────

const ACCURACY = 500;
const PREC = 4;
const GRID = 0.01;
const POI_GMC_TABLE = 'lab_pois_gmc';

/** Build SQL fragment to filter pings by hour of day. Handles cross-midnight ranges (e.g., 22h→6h).
 *  Wraps tsCol with at_timezone(...) when a localTz is provided so the user's
 *  hourFrom/hourTo are interpreted in LOCAL time, not UTC. */
function hourFilterSQL(hourFrom: number, hourTo: number, tsCol = 'utc_timestamp', localTz?: string): string {
  if (hourFrom === 0 && hourTo === 23) return '';
  const lts = localTz && localTz !== 'UTC' ? `at_timezone(${tsCol}, '${localTz}')` : tsCol;
  if (hourFrom <= hourTo) {
    return `AND HOUR(${lts}) >= ${hourFrom} AND HOUR(${lts}) <= ${hourTo}`;
  }
  // Cross-midnight (e.g., 22h to 6h)
  return `AND (HOUR(${lts}) >= ${hourFrom} OR HOUR(${lts}) <= ${hourTo})`;
}

/**
 * FULL-schema GPS-only filter. Returns empty string when off; otherwise an
 * `AND (...)` predicate that keeps NULL rows (BASIC schema) and rows where
 * `quality_fields['ping_origin_type'] = 'gps'`.
 */
function gpsOnlyFilterSQL(gpsOnly: boolean): string {
  if (!gpsOnly) return '';
  return `AND (TRY(quality_fields['ping_origin_type']) IS NULL OR TRY(quality_fields['ping_origin_type']) = 'gps')`;
}

/**
 * FULL-schema ping_circle_score threshold. 0 = off. Lower values = tighter
 * uncertainty (more precise pings). Keeps NULL rows (BASIC fallback).
 */
function circleScoreFilterSQL(maxCircleScore: number): string {
  if (!maxCircleScore || maxCircleScore <= 0) return '';
  return `AND (TRY(quality_fields['ping_circle_score']) IS NULL OR TRY_CAST(quality_fields['ping_circle_score'] AS DOUBLE) IS NULL OR TRY_CAST(quality_fields['ping_circle_score'] AS DOUBLE) <= ${maxCircleScore})`;
}

/**
 * Employee detection — excludes probable staff WITHOUT catching nearby
 * residents (whose home falls inside the POI radius).
 *
 * A naive day-count + dwell heuristic flags both equally, so we add two
 * intra-day signals that diverge between the two:
 *   - work_share: fraction of pings in 8h-20h (employees concentrated,
 *     residents distributed across the day).
 *   - has_overnight: any ping in 2h-5h (residents do this daily,
 *     employees don't).
 *
 * Criteria (all must hold):
 *   ≥15 distinct days, ≥240 min avg per-day dwell,
 *   ≥0.6 work-hours share, ≤0.3 overnight share.
 */
const EMPLOYEE_MIN_DAYS = 15;
const EMPLOYEE_MIN_AVG_DWELL = 240;
function employeeExclusionSQL(table: string, discardEmployees: boolean): string {
  if (!discardEmployees) return '';
  return `AND ad_id NOT IN (
        SELECT ad_id FROM (
          SELECT ad_id, date,
            DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as _emp_dwell_min,
            AVG(IF(HOUR(utc_timestamp) >= 8 AND HOUR(utc_timestamp) < 20, 1.0, 0.0)) as _emp_work_share,
            MAX(IF(HOUR(utc_timestamp) >= 2 AND HOUR(utc_timestamp) < 5, 1, 0)) as _emp_has_overnight
          FROM ${table}
          CROSS JOIN UNNEST(poi_ids) AS _emp_t(_emp_pid)
          WHERE _emp_pid IS NOT NULL AND _emp_pid != ''
            AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
          GROUP BY ad_id, date
        ) _per_day
        GROUP BY ad_id
        HAVING COUNT(*) >= ${EMPLOYEE_MIN_DAYS}
          AND AVG(_emp_dwell_min) >= ${EMPLOYEE_MIN_AVG_DWELL}
          AND AVG(_emp_work_share) >= 0.6
          AND AVG(_emp_has_overnight) <= 0.3
      )`;
}

/**
 * Resident detection — excludes devices that LIVE inside the POI radius.
 * Mirror of employeeExclusionSQL: same day-count + dwell thresholds,
 * but the disposing signal is high overnight share (≥0.5 of days have a
 * ping in 2h-5h). Composes with the employee filter — turning on both
 * removes both groups.
 */
const RESIDENT_MIN_DAYS = 15;
const RESIDENT_MIN_AVG_DWELL = 240;
const RESIDENT_MIN_OVERNIGHT = 0.5;
function residentExclusionSQL(table: string, discardResidents: boolean): string {
  if (!discardResidents) return '';
  return `AND ad_id NOT IN (
        SELECT ad_id FROM (
          SELECT ad_id, date,
            DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as _res_dwell_min,
            MAX(IF(HOUR(utc_timestamp) >= 2 AND HOUR(utc_timestamp) < 5, 1, 0)) as _res_has_overnight
          FROM ${table}
          CROSS JOIN UNNEST(poi_ids) AS _res_t(_res_pid)
          WHERE _res_pid IS NOT NULL AND _res_pid != ''
            AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
          GROUP BY ad_id, date
        ) _per_day
        GROUP BY ad_id
        HAVING COUNT(*) >= ${RESIDENT_MIN_DAYS}
          AND AVG(_res_dwell_min) >= ${RESIDENT_MIN_AVG_DWELL}
          AND AVG(_res_has_overnight) >= ${RESIDENT_MIN_OVERNIGHT}
      )`;
}

/**
 * Day-of-week filter. ISO 8601: 1=Mon..7=Sun (matches Athena DAY_OF_WEEK).
 * Empty/all-7 = no filter. Returns '' or `AND DAY_OF_WEEK(<tsCol>) IN (1,3,5)`.
 */
function dayOfWeekFilterSQL(daysOfWeek: number[] | undefined, tsCol = 'utc_timestamp'): string {
  if (!daysOfWeek || daysOfWeek.length === 0 || daysOfWeek.length === 7) return '';
  const valid = Array.from(new Set(daysOfWeek.filter((d) => Number.isInteger(d) && d >= 1 && d <= 7)));
  if (valid.length === 0 || valid.length === 7) return '';
  valid.sort((a, b) => a - b);
  return `AND DAY_OF_WEEK(${tsCol}) IN (${valid.join(',')})`;
}

/**
 * Common CTE: filter pings at POIs with dwell calculation.
 * Dwell = time span (minutes) between first and last ping per device-day at POI.
 * Returns per-device-day aggregated rows with dwell_minutes.
 * @param minDwell - minimum dwell time in minutes (0 = no filter)
 * @param maxDwell - maximum dwell time in minutes (0 = no filter)
 * @param hourFrom - start hour filter (0-23, default 0)
 * @param hourTo - end hour filter (0-23, default 23)
 */
function atPoiWithDwellCTE(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  const havingParts: string[] = [];
  if (minDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
  if (maxDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
  const dwellFilter = havingParts.length > 0 ? `\n    HAVING ${havingParts.join(' AND ')}` : '';
  const hFilter = hourFilterSQL(hourFrom, hourTo);
  const gFilter = gpsOnlyFilterSQL(gpsOnly);
  const sFilter = circleScoreFilterSQL(maxCircleScore);
  const dFilter = dayOfWeekFilterSQL(daysOfWeek);
  const eFilter = employeeExclusionSQL(table, discardEmployees);
  const rFilter = residentExclusionSQL(table, discardResidents);
  const visitFilterCTE = minVisits > 1 ? `visit_day_filter AS (
      SELECT ad_id FROM (
        SELECT ad_id, COUNT(DISTINCT date) as vd
        FROM ${table}
        CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
        WHERE poi_id IS NOT NULL AND poi_id != ''
          AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
          ${gFilter}
          ${sFilter}
          ${dFilter}
          ${eFilter}
        ${rFilter}
        GROUP BY ad_id
        HAVING COUNT(DISTINCT date) >= ${minVisits}
      ) t_vc
    ),
    ` : '';
  const visitWhere = minVisits > 1 ? `AND ad_id IN (SELECT ad_id FROM visit_day_filter)` : '';
  return `${visitFilterCTE}raw_poi_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        ${hFilter}
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
        ${rFilter}
        ${visitWhere}
    ),
    at_poi AS (
      SELECT ad_id, date,
        MIN(utc_timestamp) as first_ping,
        MAX(utc_timestamp) as last_ping,
        ROUND(MIN_BY(lat, utc_timestamp), ${PREC}) as lat,
        ROUND(MIN_BY(lng, utc_timestamp), ${PREC}) as lng,
        DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) as dwell_minutes,
        COUNT(*) as ping_count
      FROM raw_poi_pings
      GROUP BY ad_id, date${dwellFilter}
    )`;
}

/** Simple CTE for queries that just need the raw pings (hourly, temporal).
 * When minDwell/maxDwell > 0, only includes device-days with dwell in range.
 * When hourFrom/hourTo specified, only includes pings within the time window. */
function atPoiCTE(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  const hFilter = hourFilterSQL(hourFrom, hourTo);
  const hFilterT = hourFilterSQL(hourFrom, hourTo, 't.utc_timestamp');
  const gFilter = gpsOnlyFilterSQL(gpsOnly);
  const sFilter = circleScoreFilterSQL(maxCircleScore);
  const dFilter = dayOfWeekFilterSQL(daysOfWeek);
  const dFilterT = dayOfWeekFilterSQL(daysOfWeek, 't.utc_timestamp');
  const eFilter = employeeExclusionSQL(table, discardEmployees);
  const rFilter = residentExclusionSQL(table, discardResidents);
  // Note: when gpsOnly is on, the predicate references quality_fields directly
  // on the base table (no alias). The aliased version is needed for the joined
  // form below.
  const gFilterT = gpsOnly
    ? `AND (TRY(t.quality_fields['ping_origin_type']) IS NULL OR TRY(t.quality_fields['ping_origin_type']) = 'gps')`
    : '';
  const sFilterT = (maxCircleScore && maxCircleScore > 0)
    ? `AND (TRY(t.quality_fields['ping_circle_score']) IS NULL OR TRY_CAST(t.quality_fields['ping_circle_score'] AS DOUBLE) IS NULL OR TRY_CAST(t.quality_fields['ping_circle_score'] AS DOUBLE) <= ${maxCircleScore})`
    : '';
  // Membership test "is this ping at any POI" used to be expressed as
  // `CROSS JOIN UNNEST(poi_ids) AS t(poi_id) WHERE poi_id IS NOT NULL
  //  AND poi_id != ''` followed by `SELECT DISTINCT` to fold the
  // resulting (ping × N_pois) inflation back to one row per ping.
  // That worked at small scale but on Mexico (~200 GB, ~5 POIs/ping
  // average) it ran ~1780s and hit Athena's 30-minute query timeout
  // for hourly / dayhour / temporal / totalDevices. Switching to
  // `CARDINALITY(poi_ids) > 0` skips the row inflation entirely —
  // same set of pings, ~5× less intermediate work, ~5× faster.
  // (The downstream aggregators don't need per-POI granularity; they
  // aggregate by time bucket or by home location.)
  const visitFilterCTE = minVisits > 1 ? `visit_day_filter AS (
      SELECT ad_id, COUNT(DISTINCT date) as vd
      FROM ${table}
      WHERE CARDINALITY(poi_ids) > 0
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
      ${rFilter}
      GROUP BY ad_id
      HAVING COUNT(DISTINCT date) >= ${minVisits}
    ),
    ` : '';
  const visitWhere = minVisits > 1 ? `AND ad_id IN (SELECT ad_id FROM visit_day_filter)` : '';
  const visitWhereT = minVisits > 1 ? `AND t.ad_id IN (SELECT ad_id FROM visit_day_filter)` : '';
  if (minDwell > 0 || maxDwell > 0) {
    // Need to compute dwell first, then filter, then expand back to individual pings
    const havingParts: string[] = [];
    if (minDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
    if (maxDwell > 0) havingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
    return `${visitFilterCTE}poi_device_days AS (
      SELECT ad_id, date
      FROM ${table}
      WHERE CARDINALITY(poi_ids) > 0
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        ${hFilter}
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
        ${rFilter}
        ${visitWhere}
      GROUP BY ad_id, date
      HAVING ${havingParts.join(' AND ')}
    ),
    at_poi AS (
      SELECT t.ad_id, t.date, t.utc_timestamp,
        TRY_CAST(t.latitude AS DOUBLE) as lat,
        TRY_CAST(t.longitude AS DOUBLE) as lng
      FROM ${table} t
      INNER JOIN poi_device_days dd ON t.ad_id = dd.ad_id AND t.date = dd.date
      WHERE CARDINALITY(t.poi_ids) > 0
        AND TRY_CAST(t.latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(t.longitude AS DOUBLE) IS NOT NULL
        AND (t.horizontal_accuracy IS NULL OR TRY_CAST(t.horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        ${hFilterT}
        ${gFilterT}
        ${sFilterT}
        ${dFilterT}
    )`;
  }
  return `${visitFilterCTE}at_poi AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE CARDINALITY(poi_ids) > 0
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        ${hFilter}
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
        ${rFilter}
        ${visitWhere}
    )`;
}

function buildHourlySQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  return `WITH ${atPoiCTE(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)}
    SELECT HOUR(utc_timestamp) as touch_hour,
      COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY 1 ORDER BY 1`;
}

/**
 * Day-of-week × hour heatmap. DAY_OF_WEEK on the timestamp returns 1..7
 * (1=Monday..7=Sunday, ISO 8601). Using utc_timestamp directly avoids
 * casting the varchar `date` partition column.
 */
function buildDayHourSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  return `WITH ${atPoiCTE(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)}
    SELECT DAY_OF_WEEK(utc_timestamp) as dow,
      HOUR(utc_timestamp) as hour,
      COUNT(*) as pings,
      COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY 1, 2 ORDER BY 1, 2`;
}

function buildTemporalSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  return `WITH ${atPoiCTE(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)}
    SELECT date, COUNT(*) as pings, COUNT(DISTINCT ad_id) as devices
    FROM at_poi GROUP BY date ORDER BY date`;
}

function buildTotalDevicesSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  return `WITH ${atPoiCTE(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)}
    SELECT COUNT(DISTINCT ad_id) as total_unique_devices FROM at_poi`;
}

function buildODSQL(table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  const gFilter = gpsOnlyFilterSQL(gpsOnly);
  const sFilter = circleScoreFilterSQL(maxCircleScore);
  const dFilter = dayOfWeekFilterSQL(daysOfWeek);
  const eFilter = employeeExclusionSQL(table, discardEmployees);
  const rFilter = residentExclusionSQL(table, discardResidents);
  return `WITH ${atPoiWithDwellCTE(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)},
    poi_visits AS (
      SELECT ad_id, date, first_ping as first_poi_visit
      FROM at_poi
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
        ${rFilter}
    ),
    device_day AS (
      SELECT p.ad_id, p.date,
        MIN_BY(p.lat, p.utc_timestamp) as origin_lat,
        MIN_BY(p.lng, p.utc_timestamp) as origin_lng,
        MAX_BY(p.lat, p.utc_timestamp) as dest_lat,
        MAX_BY(p.lng, p.utc_timestamp) as dest_lng,
        HOUR(MIN(p.utc_timestamp)) as departure_hour,
        HOUR(v.first_poi_visit) as poi_arrival_hour
      FROM all_pings p
      INNER JOIN poi_visits v ON p.ad_id = v.ad_id AND p.date = v.date
      GROUP BY p.ad_id, p.date, v.first_poi_visit
    )
    SELECT
      ROUND(origin_lat, ${PREC}) as origin_lat,
      ROUND(origin_lng, ${PREC}) as origin_lng,
      ROUND(dest_lat, ${PREC}) as dest_lat,
      ROUND(dest_lng, ${PREC}) as dest_lng,
      departure_hour, poi_arrival_hour,
      COUNT(*) as device_days
    FROM device_day
    GROUP BY 1,2,3,4,5,6
    ORDER BY device_days DESC
    LIMIT 100000`;
}

async function buildCatchmentSQL(datasetName: string, table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): Promise<string> {
  // Catchment ALWAYS uses the TC-WK-19-7 home-table path
  // (METHODOLOGY.md §2.3, Pappalardo et al. EPJ Data Sci 2023). The
  // legacy first-ping-of-day proxy is methodologically biased and is
  // not an allowed fallback — fail loud so the caller runs home
  // detection first.
  if (!(await homeTableExists(datasetName))) {
    throw new Error(
      `Catchment requires the TC-WK-19-7 home-locations table for "${datasetName}", but it is missing. ` +
      `Run home detection first (npx tsx scripts/run-home-detection.ts ${datasetName}) and retry.`,
    );
  }
  const homeTbl = homeTableName(datasetName);
  // departure_hour comes from the home-detection methodology, which
  // does not model per-day departure times — we surface 0 so the
  // downstream "departure by hour" view degrades to a single bucket
  // rather than fabricating a value.
  return `WITH ${atPoiWithDwellCTE(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)},
    poi_visitors AS (SELECT DISTINCT ad_id FROM at_poi)
    SELECT
      ROUND(h.home_lat, ${PREC}) as origin_lat,
      ROUND(h.home_lng, ${PREC}) as origin_lng,
      0 as departure_hour,
      MIN(h.home_zip) as native_zip,
      MIN(h.home_city) as native_city,
      COUNT(*) as device_days
    FROM ${homeTbl} h
    INNER JOIN poi_visitors v ON h.ad_id = v.ad_id
    GROUP BY ROUND(h.home_lat, ${PREC}), ROUND(h.home_lng, ${PREC})
    ORDER BY device_days DESC
    LIMIT 100000`;
}

/**
 * Affinity query: per-origin-cluster, compute total visits, unique devices,
 * avg dwell, and visit frequency. Affinity ALWAYS uses the TC-WK-19-7
 * home-table path so its per-device home assignment matches catchment;
 * fails loud if the home table is missing.
 */
async function buildAffinitySQL(datasetName: string, table: string, minDwell = 0, maxDwell = 0, hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): Promise<string> {
  if (!(await homeTableExists(datasetName))) {
    throw new Error(
      `Affinity requires the TC-WK-19-7 home-locations table for "${datasetName}", but it is missing. ` +
      `Run home detection first (npx tsx scripts/run-home-detection.ts ${datasetName}) and retry.`,
    );
  }
  const homeTbl = homeTableName(datasetName);
  return `WITH ${atPoiWithDwellCTE(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)},
    device_stats AS (
      SELECT
        a.ad_id,
        ROUND(h.home_lat, ${PREC}) as origin_lat,
        ROUND(h.home_lng, ${PREC}) as origin_lng,
        h.home_zip as native_zip,
        h.home_city as native_city,
        COUNT(DISTINCT a.date) as visit_days,
        AVG(a.dwell_minutes) as avg_dwell,
        SUM(a.ping_count) as total_pings
      FROM at_poi a
      INNER JOIN ${homeTbl} h ON a.ad_id = h.ad_id
      GROUP BY a.ad_id, ROUND(h.home_lat, ${PREC}), ROUND(h.home_lng, ${PREC}), h.home_zip, h.home_city
    )
    SELECT
      origin_lat,
      origin_lng,
      MIN(native_zip) as native_zip,
      MIN(native_city) as native_city,
      COUNT(DISTINCT ad_id) as unique_devices,
      SUM(visit_days) as total_visit_days,
      AVG(avg_dwell) as avg_dwell_minutes,
      AVG(visit_days) as avg_frequency
    FROM device_stats
    GROUP BY origin_lat, origin_lng
    ORDER BY unique_devices DESC
    LIMIT 50000`;
}

interface PoiCoordForMobility {
  lat: number;
  lng: number;
  radiusM: number;
}

function buildMobilitySQL(table: string, minDwell = 0, maxDwell = 0, poiCoords?: PoiCoordForMobility[], hourFrom = 0, hourTo = 23, minVisits = 1, gpsOnly = false, maxCircleScore = 0, daysOfWeek: number[] = [], discardEmployees = false, discardResidents = false): string {
  // Mobility: POI categories visited before/after target POI visit.
  //
  // Strategy (same as mega-job consolidation):
  // 1. Identify visit_time using spatial proximity to POI coords (if available)
  //    or fallback to median timestamp of poi_id pings
  // 2. Find all pings ±2h of visit
  // 3. Geohash-bucket join with Overture POIs (0.01° grid, 3×3 expansion)
  // 4. Filter by distance ≤ 200m, keep closest per (ad_id, date, timing, category)
  // 5. Aggregate by timing (before/after) + category

  const GRID_STEP = 0.01; // ~1.1km geohash grid
  const BBOX_MARGIN = 0.5; // ~55km margin around POIs for nearby-activity search
  const gFilter = gpsOnlyFilterSQL(gpsOnly);
  const sFilter = circleScoreFilterSQL(maxCircleScore);
  const dFilter = dayOfWeekFilterSQL(daysOfWeek);
  const eFilter = employeeExclusionSQL(table, discardEmployees);
  const rFilter = residentExclusionSQL(table, discardResidents);

  // Compute geographic bounding box from POI coords to limit the Overture POI
  // table scan and the all_pings scan. Without this, we join millions of global
  // POIs × millions of pings → billions of Haversine computations → 1400s+ queries.
  let bboxFilter = '';
  let bboxFilterPings = '';
  if (poiCoords?.length) {
    const lats = poiCoords.map(p => p.lat);
    const lngs = poiCoords.map(p => p.lng);
    const minLat = Math.min(...lats) - BBOX_MARGIN;
    const maxLat = Math.max(...lats) + BBOX_MARGIN;
    const minLng = Math.min(...lngs) - BBOX_MARGIN;
    const maxLng = Math.max(...lngs) + BBOX_MARGIN;
    bboxFilter = `AND latitude BETWEEN ${minLat} AND ${maxLat} AND longitude BETWEEN ${minLng} AND ${maxLng}`;
    bboxFilterPings = `AND lat BETWEEN ${minLat} AND ${maxLat} AND lng BETWEEN ${minLng} AND ${maxLng}`;
  }

  // Optional min-visits filter CTE
  const mobilityVisitFilterCTE = minVisits > 1 ? `visit_day_filter AS (
      SELECT ad_id FROM (
        SELECT ad_id, COUNT(DISTINCT date) as vd
        FROM ${table}
        CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
        WHERE poi_id IS NOT NULL AND poi_id != ''
          AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
          ${gFilter}
          ${sFilter}
          ${dFilter}
          ${eFilter}
        ${rFilter}
        GROUP BY ad_id
        HAVING COUNT(DISTINCT date) >= ${minVisits}
      ) t_vc
    ),
    ` : '';
  const mobilityVisitWhere = minVisits > 1 ? `AND ad_id IN (SELECT ad_id FROM visit_day_filter)` : '';

  // Build visit_time CTE — spatial when POI coords available
  let visitTimeCTE: string;
  if (poiCoords?.length) {
    // Spatial proximity: identify actual at-POI pings via Haversine distance
    const poiValues = poiCoords
      .map(p => `(${p.lat}, ${p.lng}, ${p.radiusM})`)
      .join(', ');

    const hasDwellFilter = minDwell > 0 || maxDwell > 0;
    const dwellWhereParts: string[] = [];
    if (minDwell > 0) dwellWhereParts.push(`dwell_minutes >= ${minDwell}`);
    if (maxDwell > 0) dwellWhereParts.push(`dwell_minutes <= ${maxDwell}`);
    const dwellCTEs = hasDwellFilter ? `,
    visit_dwell AS (
      SELECT ad_id, date,
        ROUND(DATE_DIFF('second', MIN(utc_timestamp), MAX(utc_timestamp)) / 60.0, 1) as dwell_minutes
      FROM at_poi_pings
      GROUP BY ad_id, date
    ),
    dwell_filtered AS (
      SELECT ad_id, date FROM visit_dwell
      WHERE ${dwellWhereParts.join(' AND ')}
    )` : '';

    const visitorSource = hasDwellFilter
      ? `at_poi_pings a INNER JOIN dwell_filtered df ON a.ad_id = df.ad_id AND a.date = df.date`
      : `at_poi_pings`;
    const visitorPrefix = hasDwellFilter ? 'a.' : '';

    visitTimeCTE = `
    all_pings_raw AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
        ${rFilter}
    ),
    target_pois AS (
      SELECT * FROM (VALUES ${poiValues}) AS t(poi_lat, poi_lng, poi_radius_m)
    ),
    at_poi_pings AS (
      SELECT p.ad_id, p.date, p.utc_timestamp, p.lat, p.lng
      FROM all_pings_raw p
      CROSS JOIN target_pois tp
      WHERE 111320 * SQRT(
        POW(p.lat - tp.poi_lat, 2) +
        POW((p.lng - tp.poi_lng) * COS(RADIANS((p.lat + tp.poi_lat) / 2)), 2)
      ) <= tp.poi_radius_m
        ${hourFilterSQL(hourFrom, hourTo, 'p.utc_timestamp')}
        ${minVisits > 1 ? 'AND p.ad_id IN (SELECT ad_id FROM visit_day_filter)' : ''}
    )${dwellCTEs},
    target_visits AS (
      SELECT
        ${visitorPrefix}ad_id,
        ${visitorPrefix}date,
        MIN(${visitorPrefix}utc_timestamp) as visit_time
      FROM ${visitorSource}
      GROUP BY ${visitorPrefix}ad_id, ${visitorPrefix}date
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp, lat, lng FROM all_pings_raw
    )`;
  } else {
    // Fallback: use poi_ids with median timestamp as visit_time proxy
    const dwellHavingParts: string[] = [];
    if (minDwell > 0) dwellHavingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) >= ${minDwell}`);
    if (maxDwell > 0) dwellHavingParts.push(`DATE_DIFF('minute', MIN(utc_timestamp), MAX(utc_timestamp)) <= ${maxDwell}`);
    const dwellHaving = dwellHavingParts.length > 0 ? `HAVING ${dwellHavingParts.join(' AND ')}` : '';

    visitTimeCTE = `
    target_visits AS (
      SELECT ad_id, date,
        FROM_UNIXTIME(APPROX_PERCENTILE(TO_UNIXTIME(utc_timestamp), 0.5)) as visit_time
      FROM ${table}
      CROSS JOIN UNNEST(poi_ids) AS t(poi_id)
      WHERE poi_id IS NOT NULL AND poi_id != ''
        AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
        ${hourFilterSQL(hourFrom, hourTo)}
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
        ${rFilter}
        ${mobilityVisitWhere}
      GROUP BY ad_id, date
      ${dwellHaving}
    ),
    all_pings AS (
      SELECT ad_id, date, utc_timestamp,
        TRY_CAST(latitude AS DOUBLE) as lat,
        TRY_CAST(longitude AS DOUBLE) as lng
      FROM ${table}
      WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
        AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
        AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL
        AND (horizontal_accuracy IS NULL OR TRY_CAST(horizontal_accuracy AS DOUBLE) < ${ACCURACY})
        ${gFilter}
        ${sFilter}
        ${dFilter}
        ${eFilter}
        ${rFilter}
    )`;
  }

  // Pre-aggregate nearby_pings per (ad_id, date, timing, lat_bucket, lng_bucket)
  // and collapse the matched set with DISTINCT instead of ROW_NUMBER.
  // Same two-step optimization the megajob mobility query already uses (commits
  // ad0d178 + 4f3c98a). For typical 20-50 GB datasets this drops mobility from
  // ~30 min (timeout) down to a few minutes.
  return `WITH
    ${mobilityVisitFilterCTE}${visitTimeCTE},
    nearby_pings AS (
      SELECT
        a.ad_id,
        a.date,
        CASE WHEN a.utc_timestamp < t.visit_time THEN 'before' ELSE 'after' END as timing,
        CAST(FLOOR(a.lat / ${GRID_STEP}) AS BIGINT) as lat_bucket,
        CAST(FLOOR(a.lng / ${GRID_STEP}) AS BIGINT) as lng_bucket,
        AVG(a.lat) as lat,
        AVG(a.lng) as lng
      FROM all_pings a
      INNER JOIN target_visits t ON a.ad_id = t.ad_id AND a.date = t.date
      WHERE ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) <= 120
        AND ABS(DATE_DIFF('minute', a.utc_timestamp, t.visit_time)) > 0
        ${bboxFilterPings}
      GROUP BY
        a.ad_id, a.date,
        CASE WHEN a.utc_timestamp < t.visit_time THEN 'before' ELSE 'after' END,
        CAST(FLOOR(a.lat / ${GRID_STEP}) AS BIGINT),
        CAST(FLOOR(a.lng / ${GRID_STEP}) AS BIGINT)
    ),
    poi_buckets AS (
      SELECT
        id as poi_id,
        category,
        latitude as poi_lat,
        longitude as poi_lng,
        CAST(FLOOR(latitude / ${GRID_STEP}) AS BIGINT) + dlat as lat_bucket,
        CAST(FLOOR(longitude / ${GRID_STEP}) AS BIGINT) + dlng as lng_bucket
      FROM ${POI_GMC_TABLE}
      CROSS JOIN (VALUES (-1), (0), (1)) AS t1(dlat)
      CROSS JOIN (VALUES (-1), (0), (1)) AS t2(dlng)
      WHERE category IS NOT NULL
        ${bboxFilter}
    ),
    matched AS (
      SELECT DISTINCT
        p.ad_id,
        p.date,
        p.timing,
        b.category
      FROM nearby_pings p
      INNER JOIN poi_buckets b
        ON p.lat_bucket = b.lat_bucket
        AND p.lng_bucket = b.lng_bucket
      WHERE 111320 * SQRT(
          POW(p.lat - b.poi_lat, 2) +
          POW((p.lng - b.poi_lng) * COS(RADIANS((p.lat + b.poi_lat) / 2)), 2)
        ) <= 200
    )
    SELECT
      timing,
      category,
      COUNT(DISTINCT CONCAT(ad_id, '-', date)) as device_days,
      COUNT(*) as hits
    FROM matched
    GROUP BY timing, category
    ORDER BY timing, device_days DESC`;
}

// ── Affinity index computation (shared with megajob category-affinity export) ──

import { computeAffinityReport as computeAffinityReportShared, type AffinityByZip } from '@/lib/affinity-builder';

/** Wrapper that preserves the legacy { datasetName } shape on the report
 *  object. New callers should use the shared `computeAffinityReportShared`
 *  directly. */
async function computeAffinityReport(
  datasetName: string,
  rows: any[],
  coordToZip: Map<string, { zipCode: string; city: string; country: string }>,
  country?: string,
): Promise<{ analyzedAt: string; datasetName: string; byZipCode: AffinityByZip[] }> {
  const report = await computeAffinityReportShared(datasetName, rows, coordToZip, country);
  return { analyzedAt: report.analyzedAt, datasetName, byZipCode: report.byZipCode };
}


// ── Main handler ──────────────────────────────────────────────────────

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ name: string }> }
) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const { name: datasetName } = await context.params;

  try {
    let state = await getState(datasetName);

    // Parse request body for dwell interval + hour filters + min visits + gpsOnly
    let minDwell = 0;
    let maxDwell = 0;
    let hourFrom = 0;
    let hourTo = 23;
    // Bot-filter floor: never show 1-day ghost MAIDs (ad fraud, IDFA
    // rotation, web pixels). Empirical audit (2026-05-18) shows ≥30 %
    // of distinct ad_ids per market are 1-day, 1-3 ping, 1-cell. See
    // lib/bot-filter.ts. The user can still set a stricter minVisits
    // via the UI (e.g. 5 for "regulars") but cannot go below 2.
    let minVisits = MIN_DISTINCT_DAYS_FOR_HUMAN_MAID;
    let gpsOnly = false;
    let maxCircleScore = 0;
    let daysOfWeek: number[] = [];
    let discardEmployees = false;
    let discardResidents = false;
    try {
      const body = await request.json();
      if (body?.minDwell) minDwell = parseInt(body.minDwell, 10) || 0;
      if (body?.maxDwell) maxDwell = parseInt(body.maxDwell, 10) || 0;
      if (body?.hourFrom != null) hourFrom = Math.max(0, Math.min(23, parseInt(body.hourFrom, 10) || 0));
      if (body?.hourTo != null) hourTo = Math.max(0, Math.min(23, parseInt(body.hourTo, 10) || 23));
      if (body?.minVisits) minVisits = Math.max(parseInt(body.minVisits, 10) || MIN_DISTINCT_DAYS_FOR_HUMAN_MAID, MIN_DISTINCT_DAYS_FOR_HUMAN_MAID);
      if (body?.gpsOnly === true) gpsOnly = true;
      if (typeof body?.maxCircleScore === 'number' && body.maxCircleScore > 0) {
        maxCircleScore = body.maxCircleScore;
      }
      if (Array.isArray(body?.daysOfWeek)) {
        daysOfWeek = body.daysOfWeek
          .map((d: any) => parseInt(d, 10))
          .filter((d: number) => Number.isInteger(d) && d >= 1 && d <= 7);
      }
      if (body?.discardEmployees === true) discardEmployees = true;
      if (body?.discardResidents === true) discardResidents = true;
    } catch { /* no body */ }

    // Reset if done, error, or filters changed
    if (state?.phase === 'done' || state?.phase === 'error') state = null;
    if (state && ((state.minDwell ?? 0) !== minDwell || (state.maxDwell ?? 0) !== maxDwell)) {
      console.log(`[DS-REPORT] Dwell filter changed (${state.minDwell ?? 0}-${state.maxDwell ?? 0} → ${minDwell}-${maxDwell}), resetting`);
      state = null;
    }
    if (state && ((state.hourFrom ?? 0) !== hourFrom || (state.hourTo ?? 23) !== hourTo)) {
      console.log(`[DS-REPORT] Hour filter changed (${state.hourFrom ?? 0}-${state.hourTo ?? 23} → ${hourFrom}-${hourTo}), resetting`);
      state = null;
    }
    if (state && (state.minVisits ?? 1) !== minVisits) {
      console.log(`[DS-REPORT] Min visits changed (${state.minVisits ?? 1} → ${minVisits}), resetting`);
      state = null;
    }
    if (state && Boolean(state.gpsOnly) !== gpsOnly) {
      console.log(`[DS-REPORT] gpsOnly filter changed (${Boolean(state.gpsOnly)} → ${gpsOnly}), resetting`);
      state = null;
    }
    if (state && (state.maxCircleScore ?? 0) !== maxCircleScore) {
      console.log(`[DS-REPORT] maxCircleScore changed (${state.maxCircleScore ?? 0} → ${maxCircleScore}), resetting`);
      state = null;
    }
    if (state) {
      const stateDow = [...(state.daysOfWeek || [])].sort().join(',');
      const newDow = [...daysOfWeek].sort().join(',');
      if (stateDow !== newDow) {
        console.log(`[DS-REPORT] daysOfWeek changed (${stateDow || 'all'} → ${newDow || 'all'}), resetting`);
        state = null;
      }
    }
    if (state && Boolean(state.discardEmployees) !== discardEmployees) {
      console.log(`[DS-REPORT] discardEmployees changed (${Boolean(state.discardEmployees)} → ${discardEmployees}), resetting`);
      state = null;
    }
    if (state && Boolean(state.discardResidents) !== discardResidents) {
      console.log(`[DS-REPORT] discardResidents changed (${Boolean(state.discardResidents)} → ${discardResidents}), resetting`);
      state = null;
    }

    // ── Phase: home_detection (auto-trigger TC-WK-19-7 when missing) ──
    //
    // Catchment + affinity REQUIRE the per-dataset TC-WK-19-7 home-locations
    // table (METHODOLOGY.md §2.3). If it is missing on first entry, kick
    // off home detection here and poll it as its own state-machine phase
    // before the 8 report queries launch. The CTAS auto-creates the Glue
    // catalog entry on success, so a subsequent homeTableExists() returns
    // true and we fall through into the existing "Phase: start" logic.
    if (!state) {
      if (!(await homeTableExists(datasetName))) {
        console.log(`[DS-REPORT] Home table missing for ${datasetName} — auto-triggering home detection (TC-WK-19-7)`);
        await ensureTableForDataset(datasetName);
        const { queryId, outputTable } = await startHomeDetection(datasetName);
        console.log(`[DS-REPORT] Home detection started for ${datasetName}: query=${queryId} → table=${outputTable}`);
        state = {
          phase: 'home_detection',
          queries: {},
          homeDetectionQueryId: queryId,
          minDwell: minDwell || undefined,
          maxDwell: maxDwell || undefined,
          hourFrom: hourFrom > 0 ? hourFrom : undefined,
          hourTo: hourTo < 23 ? hourTo : undefined,
          minVisits: minVisits > 1 ? minVisits : undefined,
          gpsOnly: gpsOnly || undefined,
          maxCircleScore: maxCircleScore > 0 ? maxCircleScore : undefined,
          daysOfWeek: (daysOfWeek.length > 0 && daysOfWeek.length < 7) ? daysOfWeek : undefined,
          discardEmployees: discardEmployees || undefined,
          discardResidents: discardResidents || undefined,
        };
        await saveState(datasetName, state);
        return NextResponse.json({
          phase: 'home_detection',
          progress: {
            step: 'home_detection_started',
            percent: 2,
            message: 'Detecting home locations (TC-WK-19-7)…',
          },
        });
      }
    }

    if (state?.phase === 'home_detection') {
      const qid = state.homeDetectionQueryId;
      if (!qid) {
        // Inconsistent saved state — reset and let the next call start fresh.
        console.warn(`[DS-REPORT] home_detection state missing queryId for ${datasetName}, resetting`);
        state = null;
      } else {
        let pollResult: { state: 'running' | 'done' | 'error'; error?: string };
        try {
          pollResult = await pollHomeDetection(qid);
        } catch (err: any) {
          // Athena occasionally loses metadata for old queries.
          const msg = err?.message || String(err);
          if (msg.includes('not found') || msg.includes('InvalidRequestException')) {
            pollResult = { state: 'error', error: 'Home-detection query expired' };
          } else {
            pollResult = { state: 'error', error: msg };
          }
        }

        if (pollResult.state === 'running') {
          let scannedGB = 0;
          let runtimeSec = 0;
          try {
            const status = await checkQueryStatus(qid);
            if (status.statistics?.dataScannedBytes) scannedGB = status.statistics.dataScannedBytes / 1e9;
            if (status.statistics?.engineExecutionTimeMs) runtimeSec = Math.round(status.statistics.engineExecutionTimeMs / 1000);
          } catch { /* swallow — stats are best-effort */ }
          return NextResponse.json({
            phase: 'home_detection',
            progress: {
              step: 'home_detection',
              percent: 5,
              message: `Detecting home locations… ${scannedGB.toFixed(0)} GB scanned · ${runtimeSec}s`,
            },
          });
        }
        if (pollResult.state === 'error') {
          state = { ...state, phase: 'error', error: `Home detection failed: ${pollResult.error || 'unknown'}` };
          await saveState(datasetName, state);
          return NextResponse.json({ phase: 'error', error: state.error });
        }
        // pollResult.state === 'done' — the CTAS finished. The Glue
        // catalog entry should now exist; fall through to the start
        // phase below by clearing state so the !state branch fires.
        console.log(`[DS-REPORT] Home detection complete for ${datasetName} — launching report queries`);
        state = null;
      }
    }

    // ── Phase: start ────────────────────────────────────────────
    if (!state) {
      const dwellLabel = (minDwell > 0 || maxDwell > 0) ? `, dwell=${minDwell}-${maxDwell || '∞'}` : '';
      const hourLabel = (hourFrom > 0 || hourTo < 23) ? `, hours=${hourFrom}-${hourTo}` : '';
      const visitLabel = minVisits > 1 ? `, minVisits=${minVisits}` : '';
      console.log(`[DS-REPORT] Starting reports for ${datasetName} (${dwellLabel}${hourLabel}${visitLabel})`);

      await ensureTableForDataset(datasetName);
      const table = getTableName(datasetName);

      // Try to load POI coords from the job associated with this dataset
      let poiCoords: PoiCoordForMobility[] | undefined;
      try {
        const { getJob } = await import('@/lib/jobs');
        const { extractPoiCoords } = await import('@/lib/mega-consolidation-queries');
        const index = await getConfig<Record<string, any>>('jobs-index');
        if (index) {
          const jobEntry = Object.entries(index).find(
            ([_, j]: [string, any]) => j.s3DestPath?.replace(/\/$/, '').split('/').pop() === datasetName
          );
          if (jobEntry) {
            const job = await getJob(jobEntry[0]);
            if (job) {
              poiCoords = extractPoiCoords([job]);
              if (poiCoords.length > 0) {
                console.log(`[DS-REPORT] Loaded ${poiCoords.length} POI coords for spatial mobility`);
              } else {
                poiCoords = undefined;
              }
            }
          }
        }
      } catch (e: any) {
        console.warn(`[DS-REPORT] Could not load POI coords: ${e.message}`);
      }

      // Launch all 8 queries in parallel
      const queries: Record<string, string> = {};
      await Promise.all([
        startQueryAsync(buildODSQL(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)).then(id => { queries.od = id; }).catch(e => console.error('[DS-REPORT] od:', e.message)),
        startQueryAsync(buildHourlySQL(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)).then(id => { queries.hourly = id; }).catch(e => console.error('[DS-REPORT] hourly:', e.message)),
        startQueryAsync(buildDayHourSQL(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)).then(id => { queries.dayhour = id; }).catch(e => console.error('[DS-REPORT] dayhour:', e.message)),
        buildCatchmentSQL(datasetName, table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)
          .then(sql => startQueryAsync(sql))
          .then(id => { queries.catchment = id; })
          .catch(e => console.error('[DS-REPORT] catchment:', e.message)),
        startQueryAsync(buildTemporalSQL(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)).then(id => { queries.temporal = id; }).catch(e => console.error('[DS-REPORT] temporal:', e.message)),
        startQueryAsync(buildTotalDevicesSQL(table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)).then(id => { queries.totalDevices = id; }).catch(e => console.error('[DS-REPORT] totalDevices:', e.message)),
        startQueryAsync(buildMobilitySQL(table, minDwell, maxDwell, poiCoords, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)).then(id => { queries.mobility = id; }).catch(e => console.error('[DS-REPORT] mobility:', e.message)),
        buildAffinitySQL(datasetName, table, minDwell, maxDwell, hourFrom, hourTo, minVisits, gpsOnly, maxCircleScore, daysOfWeek, discardEmployees, discardResidents)
          .then(sql => startQueryAsync(sql))
          .then(id => { queries.affinity = id; })
          .catch(e => console.error('[DS-REPORT] affinity:', e.message)),
      ]);

      // Get dataset size for progress estimation
      let datasetSizeGB = 0;
      try {
        const index = await getConfig<Record<string, any>>('jobs-index');
        if (index) {
          const entry = Object.entries(index).find(
            ([_, j]: [string, any]) => j.s3DestPath?.replace(/\/$/, '').split('/').pop() === datasetName
          );
          if (entry) {
            datasetSizeGB = Math.round(((entry[1] as any).totalBytes || 0) / 1e9);
          }
        }
      } catch {}

      console.log(`[DS-REPORT] Launched ${Object.keys(queries).length} queries for ${datasetName} (${datasetSizeGB} GB)`);

      state = {
        phase: 'polling', queries, datasetSizeGB,
        minDwell: minDwell || undefined,
        maxDwell: maxDwell || undefined,
        hourFrom: hourFrom > 0 ? hourFrom : undefined,
        hourTo: hourTo < 23 ? hourTo : undefined,
        minVisits: minVisits > 1 ? minVisits : undefined,
        gpsOnly: gpsOnly || undefined,
        maxCircleScore: maxCircleScore > 0 ? maxCircleScore : undefined,
        daysOfWeek: (daysOfWeek.length > 0 && daysOfWeek.length < 7) ? daysOfWeek : undefined,
        discardEmployees: discardEmployees || undefined,
        discardResidents: discardResidents || undefined,
      };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'polling',
        progress: { step: 'queries_started', percent: 10, message: `Running ${Object.keys(queries).length} Athena queries...` },
      });
    }

    // ── Phase: polling ──────────────────────────────────────────
    if (state.phase === 'polling') {
      const entries = Object.entries(state.queries);
      if (entries.length === 0) {
        state = { ...state, phase: 'error', error: 'No queries started' };
        await saveState(datasetName, state);
        return NextResponse.json({ phase: 'error', error: 'No queries started' });
      }

      const statuses = await Promise.all(
        entries.map(async ([name, queryId]) => {
          try {
            const s = await checkQueryStatus(queryId);
            return { name, state: s.state, error: s.error, stats: s.statistics };
          } catch (err: any) {
            if (err?.message?.includes('not found') || err?.message?.includes('InvalidRequestException')) {
              return { name, state: 'FAILED' as const, error: 'Query expired', stats: undefined };
            }
            return { name, state: 'FAILED' as const, error: err.message, stats: undefined };
          }
        })
      );

      let allDone = true;
      let doneCount = 0;
      let totalScannedGB = 0;
      const dsGB = state.datasetSizeGB || 0;
      const queryDetails: string[] = [];

      for (const s of statuses) {
        const scannedGB = s.stats?.dataScannedBytes ? (s.stats.dataScannedBytes / 1e9) : 0;
        totalScannedGB += scannedGB;
        const runtimeSec = s.stats?.engineExecutionTimeMs ? Math.round(s.stats.engineExecutionTimeMs / 1000) : 0;

        if (s.state === 'RUNNING' || s.state === 'QUEUED') {
          allDone = false;
          const pct = dsGB > 0 ? Math.min(99, Math.round((scannedGB / dsGB) * 100)) : 0;
          queryDetails.push(`⏳ ${s.name}: ${scannedGB.toFixed(0)}/${dsGB} GB (${pct}%) · ${runtimeSec}s`);
        } else {
          doneCount++;
          if (s.state === 'SUCCEEDED') {
            queryDetails.push(`✅ ${s.name}: ${scannedGB.toFixed(0)} GB in ${runtimeSec}s`);
          } else {
            queryDetails.push(`❌ ${s.name}: ${s.error || 'failed'}`);
            console.warn(`[DS-REPORT] ${s.name} failed: ${s.error}`);
          }
        }
      }

      // Overall percent: scan phase (10-60%) + completion phase (60-65%)
      let overallPercent = 10;
      if (dsGB > 0 && entries.length > 0) {
        const avgScanPct = Math.min(100, (totalScannedGB / (dsGB * entries.length)) * 100);
        overallPercent = 10 + Math.round(avgScanPct * 0.5);
      }
      overallPercent = Math.max(overallPercent, 10 + Math.round((doneCount / entries.length) * 50));

      if (!allDone) {
        return NextResponse.json({
          phase: 'polling',
          progress: {
            step: 'polling',
            percent: overallPercent,
            message: `Athena queries: ${doneCount}/${entries.length} complete · ${totalScannedGB.toFixed(0)} GB scanned`,
            detail: queryDetails.join('\n'),
          },
        });
      }

      // All done → parse
      state = { ...state, phase: 'parsing' };
      await saveState(datasetName, state);
      return NextResponse.json({
        phase: 'parsing',
        progress: { step: 'parsing', percent: 65, message: 'Queries done, parsing results...' },
      });
    }

    // ── Phase: parsing (save fast reports + cache heavy rows for geocoding) ──
    //
    // Splits the legacy monolithic "parse + geocode + save" into 3 sub-
    // phases so a 50+ GB dataset never blows the request budget on the
    // geocoding step. This phase handles the fast, no-geocoding reports
    // and stages the catchment / affinity / OD rows in S3 for the next
    // phase to consume.
    if (state.phase === 'parsing') {
      const queries = state.queries;
      const dMin = state.minDwell || 0;
      const dMax = state.maxDwell || 0;
      const hFrom = state.hourFrom ?? 0;
      const hTo = state.hourTo ?? 23;
      const mVisits = state.minVisits ?? 1;
      const gOnly = !!state.gpsOnly;
      const cScore = state.maxCircleScore || 0;
      const dow = state.daysOfWeek || [];
      const noEmp = !!state.discardEmployees;
      const noRes = !!state.discardResidents;
      const rk = (type: string) => REPORT_KEY(datasetName, type, dMin, dMax, hFrom, hTo, mVisits, gOnly, cScore, dow, noEmp, noRes);
      const coordsToGeocode = new Map<string, { lat: number; lng: number; deviceCount: number }>();

      // Parse hourly
      if (queries.hourly) {
        try {
          const r = await fetchQueryResults(queries.hourly);
          await putConfig(`${rk('hourly')}`, parseConsolidatedHourly(datasetName, r.rows), { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] hourly parse:', e.message); }
      }

      // Parse dayhour (day-of-week × hour heatmap; 7×24 cells, dow 1=Mon..7=Sun)
      if (queries.dayhour) {
        try {
          const r = await fetchQueryResults(queries.dayhour);
          const cells = (r.rows || []).map((row: any) => ({
            dow: parseInt(row.dow, 10) || 0,
            hour: parseInt(row.hour, 10) || 0,
            pings: parseInt(row.pings, 10) || 0,
            devices: parseInt(row.devices, 10) || 0,
          }));
          await putConfig(`${rk('dayhour')}`, {
            datasetName,
            analyzedAt: new Date().toISOString(),
            cells,
          }, { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] dayhour parse:', e.message); }
      }

      // Parse temporal + totalDevices
      if (queries.temporal) {
        try {
          const r = await fetchQueryResults(queries.temporal);
          const daily = r.rows.map((row: any) => ({
            date: row.date,
            pings: parseInt(row.pings, 10) || 0,
            devices: parseInt(row.devices, 10) || 0,
          }));
          const report: any = buildTemporalTrends(datasetName, [daily]);
          if (queries.totalDevices) {
            try {
              const tr = await fetchQueryResults(queries.totalDevices);
              report.totalUniqueDevices = parseInt(tr.rows[0]?.total_unique_devices, 10) || 0;
            } catch {}
          }
          await putConfig(`${rk('temporal')}`, report, { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] temporal parse:', e.message); }
      }

      // Parse mobility (POI categories before/after visit)
      if (queries.mobility) {
        try {
          const r = await fetchQueryResults(queries.mobility);
          const beforeMap = new Map<string, { deviceDays: number; hits: number }>();
          const afterMap = new Map<string, { deviceDays: number; hits: number }>();
          for (const row of r.rows) {
            const cat = row.category;
            const dd = parseInt(row.device_days, 10) || 0;
            const h = parseInt(row.hits, 10) || 0;
            const map = row.timing === 'before' ? beforeMap : afterMap;
            const ex = map.get(cat);
            if (ex) { ex.deviceDays += dd; ex.hits += h; }
            else map.set(cat, { deviceDays: dd, hits: h });
          }
          const toArr = (m: Map<string, any>) =>
            Array.from(m.entries())
              .map(([category, v]) => ({ category, ...v }))
              .sort((a, b) => b.deviceDays - a.deviceDays);
          await putConfig(`${rk('mobility')}`, {
            analyzedAt: new Date().toISOString(),
            datasetName,
            before: toArr(beforeMap),
            after: toArr(afterMap),
            categories: toArr(beforeMap), // legacy compat
          }, { compact: true });
        } catch (e: any) { console.error('[DS-REPORT] mobility parse:', e.message); }
      }

      // Reverse-geocode every catchment/affinity coord via local GeoJSON.
      // We deliberately do NOT use Veraset's geo_fields['zipcode'] anymore:
      // the home table's home_zip is ARBITRARY(native_zip) per ~1.1 km bucket,
      // which collapses all residents of a dense urban bucket onto a single
      // zip code. Polygon lookup against the per-MAID centroid distributes
      // residents across distinct postal codes within the same bucket, which
      // is essential for dense German cities (München, Frankfurt, Stuttgart)
      // where hundreds of MAIDs share one bucket.
      const coordToZip = new Map<string, { zipCode: string; city: string; country: string }>();

      // Parse OD
      let odClusters: any = null;
      if (queries.od) {
        try {
          const r = await fetchQueryResults(queries.od);
          odClusters = parseConsolidatedOD(r.rows);
          for (const c of odClusters.clusters) {
            coordsToGeocode.set(`${c.originLat},${c.originLng}`, { lat: c.originLat, lng: c.originLng, deviceCount: c.deviceDays });
            coordsToGeocode.set(`${c.destLat},${c.destLng}`, { lat: c.destLat, lng: c.destLng, deviceCount: c.deviceDays });
          }
        } catch (e: any) { console.error('[DS-REPORT] od parse:', e.message); }
      }

      // Parse catchment — queue every coord for polygon reverse-geocode.
      let catchmentRows: any[] | null = null;
      if (queries.catchment) {
        try {
          const r = await fetchQueryResults(queries.catchment);
          catchmentRows = r.rows;
          for (const row of r.rows) {
            const lat = parseFloat(row.origin_lat) || 0;
            const lng = parseFloat(row.origin_lng) || 0;
            const key = `${lat},${lng}`;
            coordsToGeocode.set(key, { lat, lng, deviceCount: parseInt(row.device_days, 10) || 0 });
          }
        } catch (e: any) { console.error('[DS-REPORT] catchment parse:', e.message); }
      }

      // Parse affinity rows — queue every coord for polygon reverse-geocode.
      let affinityRows: any[] | null = null;
      if (queries.affinity) {
        try {
          const r = await fetchQueryResults(queries.affinity);
          affinityRows = r.rows;
          for (const row of r.rows) {
            const lat = parseFloat(row.origin_lat) || 0;
            const lng = parseFloat(row.origin_lng) || 0;
            const key = `${lat},${lng}`;
            if (!coordsToGeocode.has(key)) {
              coordsToGeocode.set(key, { lat, lng, deviceCount: parseInt(row.unique_devices, 10) || 0 });
            }
          }
        } catch (e: any) { console.error('[DS-REPORT] affinity parse:', e.message); }
      }

      // Stage the heavy rows + (empty) coord→zip map + coords-to-geocode
      // in S3. The next sub-phase (parsing_geocode) reads them back so
      // we don't re-fetch from Athena. coordToZip starts empty here and
      // is fully populated by polygon lookup in parsing_geocode.
      let detectedCountry: string | undefined;
      try {
        const analysis = await getConfig<any>(`dataset-analysis/${datasetName}`);
        detectedCountry = analysis?.country;
      } catch {}

      await Promise.all([
        odClusters
          ? putConfig(CACHE_KEY.odRows(datasetName), odClusters, { compact: true })
          : Promise.resolve(),
        catchmentRows
          ? putConfig(CACHE_KEY.catchmentRows(datasetName), catchmentRows, { compact: true })
          : Promise.resolve(),
        affinityRows
          ? putConfig(CACHE_KEY.affinityRows(datasetName), affinityRows, { compact: true })
          : Promise.resolve(),
        putConfig(CACHE_KEY.coordsToGeocode(datasetName), Array.from(coordsToGeocode.entries()), { compact: true }),
        putConfig(CACHE_KEY.coordToZip(datasetName), Array.from(coordToZip.entries()), { compact: true }),
      ]);

      state = {
        ...state,
        phase: 'parsing_geocode',
        parsingCache: {
          detectedCountry,
          coordsToGeocode: coordsToGeocode.size,
        },
      };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'parsing_geocode',
        progress: {
          step: 'parsing_geocode',
          percent: 75,
          message: coordsToGeocode.size > 0
            ? `Geocoding ${coordsToGeocode.size.toLocaleString()} coordinates...`
            : 'Building origin reports...',
        },
      });
    }

    // ── Phase: parsing_geocode (reverse-geocode collected coords) ──
    //
    // Loads the staged coord set from S3, runs batchReverseGeocode once
    // for the whole dataset (after 3-decimal rounding ≈ 110 m to share
    // work across truly-co-located coords while still distinguishing
    // distinct postal codes within a city), and persists the resulting
    // coord→zip map back to S3 for the parsing_save phase to consume.
    //
    // Precision rationale: home_lat = AVG(lat) per-MAID inside a ~1.1 km
    // bucket, so MAID centroids within the same bucket vary at the
    // ~100–500 m scale. 1-decimal rounding (~11 km) destroyed that
    // variance and collapsed dense German cities (München, Frankfurt,
    // Stuttgart) onto 2–6 PLZs out of 40–85 real ones. 3-decimal lets
    // the 8 k German PLZ polygons actually disambiguate.
    if (state.phase === 'parsing_geocode') {
      const detectedCountry = state.parsingCache?.detectedCountry;
      const coordsEntries = (await getConfig<Array<[string, { lat: number; lng: number; deviceCount: number }]>>(CACHE_KEY.coordsToGeocode(datasetName))) || [];
      const coordToZipEntries = (await getConfig<Array<[string, { zipCode: string; city: string; country: string }]>>(CACHE_KEY.coordToZip(datasetName))) || [];
      const coordToZip = new Map(coordToZipEntries);

      if (coordsEntries.length === 0) {
        console.log(`[DS-REPORT] No coords to geocode for ${datasetName}`);
      } else {
        try {
          if (detectedCountry) setCountryFilter([detectedCountry]);

          // Bucket to 3-decimal cells (~110 m) to deduplicate co-located
          // coords without collapsing distinct PLZ-scale neighbours.
          const roundedMap = new Map<string, { lat: number; lng: number; deviceCount: number }>();
          for (const [, p] of coordsEntries) {
            const rl = Math.round(p.lat * 1000) / 1000;
            const rn = Math.round(p.lng * 1000) / 1000;
            const key = `${rl},${rn}`;
            const ex = roundedMap.get(key);
            if (ex) ex.deviceCount += p.deviceCount;
            else roundedMap.set(key, { lat: rl, lng: rn, deviceCount: p.deviceCount });
          }
          console.log(`[DS-REPORT] Geocoding ${coordsEntries.length.toLocaleString()} coords (${roundedMap.size.toLocaleString()} unique buckets at 3-decimal precision)`);

          const geocoded = await batchReverseGeocode(Array.from(roundedMap.values()));
          const rKeys = Array.from(roundedMap.keys());

          for (const [key, p] of coordsEntries) {
            if (coordToZip.has(key)) continue;
            const roundKey = `${Math.round(p.lat * 1000) / 1000},${Math.round(p.lng * 1000) / 1000}`;
            const idx = rKeys.indexOf(roundKey);
            if (idx >= 0 && idx < geocoded.length) {
              const g = geocoded[idx];
              if (g.type === 'geojson_local' || g.type === 'nominatim_match') {
                coordToZip.set(key, { zipCode: g.postcode, city: g.city, country: g.country });
              }
            }
          }
          setCountryFilter(null);
        } catch (e: any) {
          console.error('[DS-REPORT] geocoding:', e.message);
          setCountryFilter(null);
          // Don't fail the whole pipeline — coordToZip may be partial but the
          // save phase will still produce reports (with UNKNOWN for unmatched
          // coords) rather than getting stuck here forever.
        }
      }

      await putConfig(CACHE_KEY.coordToZip(datasetName), Array.from(coordToZip.entries()), { compact: true });

      state = {
        ...state,
        phase: 'parsing_save',
        parsingCache: {
          ...state.parsingCache,
          geocodedCount: coordToZip.size,
        },
      };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: 'parsing_save',
        progress: {
          step: 'parsing_save',
          percent: 90,
          message: `Geocoded ${coordToZip.size.toLocaleString()} coords. Saving reports...`,
        },
      });
    }

    // ── Phase: parsing_save (build & save OD / catchment / affinity reports) ──
    //
    // Loads the cached rows and coord→zip map from S3 and writes the final
    // reports. Each save is wrapped in its own try/catch so one failing
    // report (e.g. computeAffinityReport blowing up on degenerate input)
    // doesn't poison the others — the user at least gets the reports that
    // did serialize.
    if (state.phase === 'parsing_save') {
      const dMin = state.minDwell || 0;
      const dMax = state.maxDwell || 0;
      const hFrom = state.hourFrom ?? 0;
      const hTo = state.hourTo ?? 23;
      const mVisits = state.minVisits ?? 1;
      const gOnly = !!state.gpsOnly;
      const cScore = state.maxCircleScore || 0;
      const dow = state.daysOfWeek || [];
      const noEmp = !!state.discardEmployees;
      const noRes = !!state.discardResidents;
      const rk = (type: string) => REPORT_KEY(datasetName, type, dMin, dMax, hFrom, hTo, mVisits, gOnly, cScore, dow, noEmp, noRes);
      const detectedCountry = state.parsingCache?.detectedCountry;

      const [odClusters, catchmentRows, affinityRows, coordToZipEntries] = await Promise.all([
        getConfig<any>(CACHE_KEY.odRows(datasetName)),
        getConfig<any[]>(CACHE_KEY.catchmentRows(datasetName)),
        getConfig<any[]>(CACHE_KEY.affinityRows(datasetName)),
        getConfig<Array<[string, { zipCode: string; city: string; country: string }]>>(CACHE_KEY.coordToZip(datasetName)),
      ]);
      const coordToZip = new Map(coordToZipEntries || []);

      const saveErrors: string[] = [];

      if (odClusters) {
        try {
          await putConfig(`${rk('od')}`, buildODReport(datasetName, odClusters.clusters, coordToZip), { compact: true });
        } catch (e: any) {
          console.error('[DS-REPORT] od save:', e.message);
          saveErrors.push(`od: ${e.message}`);
        }
      }
      if (catchmentRows) {
        try {
          await putConfig(`${rk('catchment')}`, buildCatchmentReport(datasetName, catchmentRows, coordToZip), { compact: true });
        } catch (e: any) {
          console.error('[DS-REPORT] catchment save:', e.message);
          saveErrors.push(`catchment: ${e.message}`);
        }
      }
      if (affinityRows) {
        try {
          const affinityReport = await computeAffinityReport(datasetName, affinityRows, coordToZip, detectedCountry);
          await putConfig(`${rk('affinity')}`, affinityReport, { compact: true });
          console.log(`[DS-REPORT] Affinity report: ${affinityReport.byZipCode.length} postal codes`);
        } catch (e: any) {
          console.error('[DS-REPORT] affinity save:', e.message);
          saveErrors.push(`affinity: ${e.message}`);
        }
      }

      // Best-effort cleanup of the per-run cache files. The state's
      // s3Client doesn't have s3:DeleteObject denied on dataset-report-cache
      // paths in the production policy, but we tolerate failures here
      // because the cache files are name-spaced per (ds, filter combo)
      // and will be overwritten on the next run anyway.
      try {
        await Promise.all([
          putConfig(CACHE_KEY.odRows(datasetName), null, { compact: true }),
          putConfig(CACHE_KEY.catchmentRows(datasetName), null, { compact: true }),
          putConfig(CACHE_KEY.affinityRows(datasetName), null, { compact: true }),
          putConfig(CACHE_KEY.coordsToGeocode(datasetName), null, { compact: true }),
          putConfig(CACHE_KEY.coordToZip(datasetName), null, { compact: true }),
        ]);
      } catch (e: any) {
        console.warn(`[DS-REPORT] cache cleanup warning:`, e?.message || e);
      }

      state = {
        ...state,
        phase: saveErrors.length > 0 ? 'error' : 'done',
        error: saveErrors.length > 0 ? `Some reports failed to save: ${saveErrors.join('; ')}` : undefined,
      };
      await saveState(datasetName, state);

      return NextResponse.json({
        phase: state.phase,
        progress: {
          step: state.phase,
          percent: state.phase === 'done' ? 100 : 95,
          message: state.phase === 'done'
            ? 'Reports generated successfully'
            : `Reports partially generated. ${saveErrors.length} report(s) failed — see error details.`,
        },
        error: state.error,
      });
    }

    return NextResponse.json({ phase: state?.phase || 'unknown' });
  } catch (err: any) {
    console.error(`[DS-REPORT] ${datasetName}:`, err.message);
    return NextResponse.json({ error: err.message, phase: 'error' }, { status: 500 });
  }
}
