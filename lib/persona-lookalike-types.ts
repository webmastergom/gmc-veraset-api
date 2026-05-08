/**
 * Look-alike segment enrichment for personas.
 *
 * Three complementary modes — each takes a persona cluster and produces a
 * NEW Master MAIDs contribution (attributeType='persona_lookalike') with
 * additional devices that "look like" the cluster's audience.
 *
 *   A. zip   — devices that share the cluster's top home ZIPs (geographic
 *              lookalike). Fast: a few SQL filters over the persona feature
 *              table + master maids consolidated.
 *
 *   B. traits — devices whose mobility traits (weekend tilt, hour profile,
 *               dwell, gyration, recency) fall within tolerance of the
 *               cluster's medians. Behavioral lookalike. Reuses the persona
 *               feature table — no new CTAS over the source data.
 *
 *   C. brands — devices that visited the same brand POIs as the cluster's
 *               top brands (per UNNEST of poi_ids in the source datasets).
 *               Brand-affinity lookalike. Needs a new CTAS over the source
 *               datasets joined to the brand_map.
 *
 * Output: an Athena CTAS table with one column `ad_id`, registered in the
 * Master MAIDs index as `attributeType='persona_lookalike'`. The
 * `attributeValue` carries the source name + persona name + mode, e.g.
 * "BK History · Urban Evening · ZIP".
 */

export type LookalikeMode = 'zip' | 'traits' | 'brands';

export type LookalikePhase =
  | 'starting'
  | 'ctas_launch'
  | 'ctas_polling'
  | 'register'
  | 'done'
  | 'error';

export interface LookalikeRequest {
  /** Persona run ID — used to look up state, feature tables, etc. */
  runId: string;
  /** Persona cluster id within that run. */
  personaId: number;
  mode: LookalikeMode;
}

export interface LookalikeState {
  phase: LookalikePhase;
  /** Stable hash key: `${runId}-${personaId}-${mode}`. */
  key: string;
  runId: string;
  personaId: number;
  personaName: string;
  mode: LookalikeMode;
  /** Country resolved from the run's source megajob/job — needed for Master MAIDs registration. */
  country?: string | null;
  /** Display name for sourceDataset on the master MAIDs row. */
  sourceDisplayName?: string;
  /** Date range derived from the run's megajobs. */
  dateRange?: { from: string; to: string };
  /** Athena query for the lookalike CTAS. */
  ctasQueryId?: string;
  /** Athena table created for the lookalike. */
  ctasTable?: string;
  /** S3 prefix of the CTAS output. */
  ctasS3Prefix?: string;
  /** MAID count after CTAS completes. */
  maidCount?: number;
  /** Master MAIDs contribution id after registration. */
  contributionId?: string;
  /** Attribute value used in the master MAIDs index. */
  attributeValue?: string;
  /** ISO timestamp. */
  updatedAt: string;
  /** Human-readable error if phase=error. */
  error?: string;
}

export interface LookalikeResult {
  phase: LookalikePhase;
  mode: LookalikeMode;
  personaId: number;
  personaName: string;
  maidCount?: number;
  contributionId?: string;
  attributeValue?: string;
  athenaTable?: string;
  error?: string;
}
