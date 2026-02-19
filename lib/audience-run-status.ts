/**
 * S3-backed run status for Roamy batch audience runs.
 *
 * Stores a single status file per dataset+country at:
 *   audiences/{datasetId}/{country}/_run/status.json
 *
 * This allows the dashboard to:
 *   - Track progress of a running batch (polling)
 *   - Resume UI state after navigation (reconnect on page load)
 *   - Request cancellation from a separate HTTP request
 */

import { PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { s3Client, BUCKET } from './s3-config';

// ── Types ────────────────────────────────────────────────────────────────

export interface AudienceRunStatus {
  runId: string;
  datasetId: string;
  country: string;
  status: 'running' | 'completed' | 'failed' | 'cancelled';
  phase: 'spatial_join' | 'origins' | 'geocoding' | 'processing';
  audienceIds: string[];
  current: number;              // current audience index (0-based in processing phase)
  total: number;                // total audience count
  currentAudienceName: string;
  percent: number;              // 0-100
  message: string;
  startedAt: string;            // ISO timestamp
  completedAt: string | null;
  error: string | null;
  completedAudiences: string[]; // audience IDs that finished successfully
  cancelRequested: boolean;     // cooperative cancellation flag
}

// ── S3 Key ───────────────────────────────────────────────────────────────

function statusKey(datasetId: string, country: string): string {
  return `audiences/${datasetId}/${country.toLowerCase()}/_run/status.json`;
}

// ── Write ────────────────────────────────────────────────────────────────

export async function saveRunStatus(status: AudienceRunStatus): Promise<void> {
  await s3Client.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: statusKey(status.datasetId, status.country),
    Body: JSON.stringify(status, null, 2),
    ContentType: 'application/json',
  }));
}

// ── Read ─────────────────────────────────────────────────────────────────

export async function getRunStatus(
  datasetId: string,
  country: string,
): Promise<AudienceRunStatus | null> {
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: BUCKET,
      Key: statusKey(datasetId, country),
    }));
    const body = await response.Body?.transformToString();
    return body ? JSON.parse(body) : null;
  } catch (err: any) {
    if (err.name === 'NoSuchKey' || err.$metadata?.httpStatusCode === 404) {
      return null;
    }
    throw err;
  }
}

// ── Cancellation ─────────────────────────────────────────────────────────

/**
 * Set the cancelRequested flag on the active run.
 * Returns true if a running status was found and updated.
 */
export async function requestCancellation(
  datasetId: string,
  country: string,
): Promise<boolean> {
  const status = await getRunStatus(datasetId, country);
  if (!status || status.status !== 'running') return false;
  status.cancelRequested = true;
  await saveRunStatus(status);
  return true;
}

/**
 * Quick check: is cancellation requested for this dataset+country?
 * Called from the runner between audience iterations.
 */
export async function isCancellationRequested(
  datasetId: string,
  country: string,
): Promise<boolean> {
  const status = await getRunStatus(datasetId, country);
  return status?.cancelRequested === true;
}

// ── Cleanup ──────────────────────────────────────────────────────────────

export async function clearRunStatus(
  datasetId: string,
  country: string,
): Promise<void> {
  try {
    await s3Client.send(new DeleteObjectCommand({
      Bucket: BUCKET,
      Key: statusKey(datasetId, country),
    }));
  } catch (err: any) {
    if (err.name !== 'NoSuchKey' && err.$metadata?.httpStatusCode !== 404) {
      throw err;
    }
  }
}
