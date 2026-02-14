import { getConfig, putConfig, initConfigIfNeeded } from './s3-config';

export interface MonthUsage {
  used: number;
  limit: number;
  lastJobId?: string;
  lastJobAt?: string;
}

export type UsageData = Record<string, MonthUsage>;

const DEFAULT_USAGE: UsageData = {};

/**
 * Get current month in YYYY-MM format
 */
export function getCurrentMonth(): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  return `${year}-${month}`;
}

/**
 * Get usage for current month with initialization.
 * All jobs (internal and external) count toward the monthly quota.
 */
export async function getUsage(): Promise<MonthUsage & { month: string; remaining: number }> {
  const data = await initConfigIfNeeded<UsageData>('usage', DEFAULT_USAGE);

  const month = getCurrentMonth();
  const monthData = data[month] || { used: 0, limit: 200 };

  return {
    month,
    used: monthData.used,
    limit: monthData.limit,
    remaining: monthData.limit - monthData.used,
    lastJobId: monthData.lastJobId,
    lastJobAt: monthData.lastJobAt,
  };
}

/**
 * Get usage for a specific month
 */
export async function getUsageForMonth(month: string): Promise<MonthUsage> {
  const data = await getConfig<UsageData>('usage') || {};
  return data[month] || { used: 0, limit: 200 };
}

/**
 * Increment usage counter for current month (atomic operation)
 */
export async function incrementUsage(jobId: string): Promise<MonthUsage> {
  const data = await initConfigIfNeeded<UsageData>('usage', DEFAULT_USAGE);
  const month = getCurrentMonth();
  
  const current = data[month] || { used: 0, limit: 200 };
  
  // Check limit BEFORE incrementing
  if (current.used >= current.limit) {
    throw new Error(`Monthly limit reached: ${current.used}/${current.limit}`);
  }
  
  const updated: MonthUsage = {
    used: current.used + 1,
    limit: current.limit,
    lastJobId: jobId,
    lastJobAt: new Date().toISOString(),
  };
  
  data[month] = updated;
  
  try {
    await putConfig('usage', data);
    console.log(`✅ Usage incremented: ${updated.used}/${updated.limit} for ${month}`);
  } catch (error) {
    console.error(`❌ Failed to save usage increment:`, error);
    throw error;
  }
  
  return updated;
}

/**
 * Check if usage limit has been reached
 */
export async function isLimitReached(): Promise<boolean> {
  const usage = await getUsage();
  return usage.remaining <= 0;
}

/**
 * Get remaining API calls
 */
export async function getRemaining(): Promise<number> {
  const usage = await getUsage();
  return Math.max(0, usage.remaining);
}

/**
 * Check if job creation is allowed
 */
export async function canCreateJob(): Promise<{ allowed: boolean; reason?: string; remaining: number }> {
  const usage = await getUsage();
  
  if (usage.remaining <= 0) {
    return {
      allowed: false,
      reason: `Monthly limit reached (${usage.limit} calls). Resets next month.`,
      remaining: 0,
    };
  }
  
  return { allowed: true, remaining: usage.remaining };
}
