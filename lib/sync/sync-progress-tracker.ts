/**
 * Day-by-day sync progress tracking. Uses keyToSize Map (O(1)) instead of array find.
 */

import { extractUniqueDatesFromKeys } from '@/lib/s3';
import type { CopyItem } from '@/lib/s3';
import type { DayProgress, SyncProgress } from '@/lib/sync-types';

const DATE_PARTITION_REGEX = /date=(\d{4}-\d{2}-\d{2})/;

export interface SyncProgressTrackerInput {
  sourceKeys: string[];
  copies: CopyItem[];
}

/**
 * Build a Map from sourceKey to size for O(1) lookup in progress callback.
 */
export function buildKeyToSizeMap(copies: CopyItem[]): Map<string, number> {
  const map = new Map<string, number>();
  for (const c of copies) {
    map.set(c.sourceKey, c.size ?? 0);
  }
  return map;
}

/**
 * Build key -> date partition for progress by day.
 */
export function buildKeyToDateMap(sourceKeys: string[]): Map<string, string> {
  const map = new Map<string, string>();
  for (const key of sourceKeys) {
    const m = key.match(DATE_PARTITION_REGEX);
    const date = m ? m[1] : 'unknown';
    map.set(key, date);
  }
  return map;
}

/**
 * Initialize dayProgress from source keys and copies (by date).
 */
export function buildInitialDayProgress(
  sourceKeys: string[],
  copies: CopyItem[]
): Record<string, DayProgress> {
  const allDates = extractUniqueDatesFromKeys(sourceKeys);
  const keyToSize = buildKeyToSizeMap(copies);
  const objectsByDate = new Map<string, CopyItem[]>();
  for (const c of copies) {
    const m = c.sourceKey.match(DATE_PARTITION_REGEX);
    const date = m ? m[1] : 'unknown';
    if (!objectsByDate.has(date)) objectsByDate.set(date, []);
    objectsByDate.get(date)!.push(c);
  }
  const dayProgress: Record<string, DayProgress> = {};
  for (const date of allDates.sort()) {
    const dateItems = objectsByDate.get(date) ?? [];
    const totalBytes = dateItems.reduce((s, i) => s + (i.size ?? 0), 0);
    dayProgress[date] = {
      date,
      totalFiles: dateItems.length,
      copiedFiles: 0,
      failedFiles: 0,
      totalBytes,
      copiedBytes: 0,
      status: 'pending',
      errors: [],
    };
  }
  return dayProgress;
}

export interface ProgressUpdate {
  copied: number;
  total: number;
  bytes: number;
  currentKey?: string;
}

/**
 * Update day progress in place when a file is copied (uses keyToSize for O(1) size lookup).
 */
export function applyProgressUpdate(
  dayProgress: Record<string, DayProgress>,
  keyToDate: Map<string, string>,
  keyToSize: Map<string, number>,
  update: ProgressUpdate
): { currentDayIndex: number; currentFileInDay: number; sortedDates: string[] } {
  const sortedDates = Object.keys(dayProgress).sort();
  let currentDayIndex = 0;
  let currentFileInDay = 0;
  const currentKey = update.currentKey;
  if (currentKey && keyToDate.has(currentKey)) {
    const date = keyToDate.get(currentKey)!;
    const dayProg = dayProgress[date];
    if (dayProg) {
      const size = keyToSize.get(currentKey) ?? 0;
      dayProg.copiedFiles++;
      dayProg.copiedBytes += size;
      dayProg.status = 'copying';
      const dayIdx = sortedDates.indexOf(date);
      if (dayIdx >= 0) {
        currentDayIndex = dayIdx;
        currentFileInDay = dayProg.copiedFiles;
      }
    }
  }
  return { currentDayIndex, currentFileInDay, sortedDates };
}

/**
 * Build the syncProgress payload for updateJob.
 */
export function buildSyncProgressPayload(
  dayProgress: Record<string, DayProgress>,
  currentDay: string | null,
  currentFileInDay: number,
  totalFilesInCurrentDay?: number
): SyncProgress {
  return {
    currentDay: currentDay ?? undefined,
    currentFile: currentFileInDay,
    totalFilesInCurrentDay,
    dayProgress: { ...dayProgress },
    lastUpdated: new Date().toISOString(),
  };
}
