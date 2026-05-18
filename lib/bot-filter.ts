/**
 * Bot / ad-fraud filter for Veraset MAID counts.
 *
 * Empirical audit (2026-05-18) across MX, ES, FR, DE, UK on the
 * 25k-POI national-grid jobs shows that 30-70 % of distinct ad_ids
 * in any 30-day window have:
 *
 *   - exactly 1 distinct date in the window
 *   - 1-3 pings total
 *   - all pings within a single 1 km × 1 km cell
 *
 * These are overwhelmingly ad-fraud SDKs, web tracking pixels, or
 * IDFAs that rotated during the window — NOT humans. Including them
 * in "Unique devices" inflates audience counts 2–5× over the country
 * MAID ceiling (κ_md × population × devices_per_subscriber).
 *
 * Per-country bot fraction (1-day-1-3-pings) measured on April 2026
 * grids:
 *
 *   DE 65.0 %   ES 52.6 %   UK 45.1 %   FR 40.3 %   MX 31.4 %
 *
 * The filter — require ≥ MIN_DISTINCT_DAYS distinct dates — strips
 * this layer. After filtering, country totals come back inside the
 * κ_md ceiling for every market we measured.
 */

export const MIN_DISTINCT_DAYS_FOR_HUMAN_MAID = 2;

/**
 * SQL fragment producing a `qualified_ad_ids(ad_id)` CTE filtered to
 * MAIDs with ≥ MIN_DISTINCT_DAYS distinct dates inside the window,
 * restricted to pings that fall inside any POI.
 *
 * The CTE name `qualified_ad_ids` is fixed so it composes cleanly into
 * a `WITH ${qualifiedAdIdsCTE(...)} SELECT … INNER JOIN qualified_ad_ids
 * USING (ad_id) …` pattern.
 *
 * @param table     Pings table (e.g. `job_26ff1993`)
 * @param dateFrom  Inclusive ISO date 'YYYY-MM-DD'
 * @param dateTo    Inclusive ISO date 'YYYY-MM-DD'
 */
export function qualifiedAdIdsCTE(
  table: string,
  dateFrom: string,
  dateTo: string,
): string {
  return `qualified_ad_ids AS (
    SELECT ad_id
    FROM ${table}
    WHERE date >= '${dateFrom}' AND date <= '${dateTo}'
      AND ad_id IS NOT NULL AND TRIM(ad_id) != ''
      AND CARDINALITY(poi_ids) > 0
    GROUP BY ad_id
    HAVING COUNT(DISTINCT date) >= ${MIN_DISTINCT_DAYS_FOR_HUMAN_MAID}
  )`;
}

/**
 * UNION-ALL variant for mega-job consolidation. Caller passes a
 * sub-query that emits `(ad_id, date)` rows across all sub-jobs and
 * we apply the same HAVING.
 *
 * @param adIdDateSubquery  Any subquery returning columns `ad_id`, `date`
 *                          across the union of sub-jobs.
 */
export function qualifiedAdIdsCTEFromSubquery(adIdDateSubquery: string): string {
  return `qualified_ad_ids AS (
    SELECT ad_id FROM (
      ${adIdDateSubquery}
    ) u
    WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
    GROUP BY ad_id
    HAVING COUNT(DISTINCT date) >= ${MIN_DISTINCT_DAYS_FOR_HUMAN_MAID}
  )`;
}

/**
 * Variant for tempTables that already hold pre-filtered at-POI pings
 * with a `date` column. The tempTable schema is the at_poi_pings CTAS
 * (ad_id, date, utc_timestamp, lat, lng[, dwell_minutes]).
 */
export function qualifiedAdIdsCTEFromTempTable(tempTable: string): string {
  return `qualified_ad_ids AS (
    SELECT ad_id FROM ${tempTable}
    WHERE ad_id IS NOT NULL AND TRIM(ad_id) != ''
    GROUP BY ad_id
    HAVING COUNT(DISTINCT date) >= ${MIN_DISTINCT_DAYS_FOR_HUMAN_MAID}
  )`;
}
