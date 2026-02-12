/**
 * Sync system types - single source of truth for sync state and API responses.
 */

export type DayProgressStatus = 'pending' | 'copying' | 'completed' | 'failed';

export interface DayProgress {
  date: string;
  totalFiles: number;
  copiedFiles: number;
  failedFiles: number;
  totalBytes: number;
  copiedBytes: number;
  status: DayProgressStatus;
  errors?: Array<{ file: string; error: string }>;
}

export interface SyncProgress {
  currentDay?: string;
  currentFile?: number;
  totalFilesInCurrentDay?: number;
  dayProgress?: Record<string, DayProgress>;
  lastUpdated?: string;
}

export type SyncStatusKind =
  | 'not_started'
  | 'syncing'
  | 'completed'
  | 'cancelled'
  | 'error';

export interface SyncStatusResponse {
  status: SyncStatusKind;
  message: string;
  progress: number;
  total: number;
  totalBytes: number;
  copied: number;
  copiedBytes: number;
  syncProgress?: SyncProgress | null;
}

export interface S3PathParsed {
  bucket: string;
  key: string;
}

export interface CopyItem {
  sourceBucket: string;
  sourceKey: string;
  destBucket: string;
  destKey: string;
  size?: number;
}

export interface CopyBatchResult {
  copied: number;
  failed: number;
  totalBytes: number;
  errors: string[];
  failedKeys: string[];
}

export interface SyncVerificationResult {
  destCount: number;
  countMatch: boolean;
  sampleSize: number;
  etagMismatches: number;
  etagErrors: string[];
  multipartSkipped: number;
  integrityPassed: boolean;
}
