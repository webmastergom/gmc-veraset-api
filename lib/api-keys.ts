/**
 * API Key Management Module
 * Handles CRUD operations for API keys stored in S3
 */

import { getConfig, putConfig, initConfigIfNeeded } from './s3-config';
import * as crypto from 'crypto';

export interface ApiKey {
  id: string;
  keyHash: string; // SHA-256 hash of the API key (never store plain keys)
  name: string; // Client/organization name
  description?: string; // Optional description
  active: boolean; // For revoking without deletion
  createdAt: string; // ISO timestamp
  lastUsedAt?: string; // ISO timestamp (updated on each use)
  usageCount: number; // Number of API calls made
}

type ApiKeysData = Record<string, ApiKey>;

const DEFAULT_API_KEYS: ApiKeysData = {};

/**
 * Generate a new random API key (64-character hex string)
 */
export function generateApiKey(): string {
  return crypto.randomBytes(32).toString('hex');
}

/**
 * Hash an API key using SHA-256
 */
export function hashApiKey(key: string): string {
  return crypto.createHash('sha256').update(key).digest('hex');
}

/**
 * Get all API keys from S3
 */
export async function getAllApiKeys(): Promise<ApiKey[]> {
  const data = await initConfigIfNeeded<ApiKeysData>('api-keys', DEFAULT_API_KEYS);
  return Object.values(data).sort((a, b) => 
    new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
  );
}

/**
 * Get a specific API key by ID
 */
export async function getApiKeyById(id: string): Promise<ApiKey | null> {
  const data = await initConfigIfNeeded<ApiKeysData>('api-keys', DEFAULT_API_KEYS);
  return data[id] || null;
}

/**
 * Create a new API key
 * Returns the plain key once (for display to user), then only stores the hash
 */
export async function createApiKey(
  name: string,
  description?: string
): Promise<{ apiKey: ApiKey; plainKey: string }> {
  const data = await initConfigIfNeeded<ApiKeysData>('api-keys', DEFAULT_API_KEYS);
  
  // Generate new key
  const plainKey = generateApiKey();
  const keyHash = hashApiKey(plainKey);
  const id = crypto.randomUUID();
  
  const newApiKey: ApiKey = {
    id,
    keyHash,
    name,
    description,
    active: true,
    createdAt: new Date().toISOString(),
    usageCount: 0,
  };
  
  data[id] = newApiKey;
  
  try {
    await putConfig('api-keys', data);
    console.log(`✅ API key created: ${id} - ${name}`);
  } catch (error) {
    console.error(`❌ Failed to save API key ${id}:`, error);
    throw error;
  }
  
  return { apiKey: newApiKey, plainKey };
}

/**
 * Update an existing API key metadata
 */
export async function updateApiKey(
  id: string,
  updates: Partial<Pick<ApiKey, 'name' | 'description' | 'active'>>
): Promise<ApiKey | null> {
  const data = await initConfigIfNeeded<ApiKeysData>('api-keys', DEFAULT_API_KEYS);
  
  if (!data[id]) {
    console.error(`❌ API key not found: ${id}`);
    return null;
  }
  
  data[id] = {
    ...data[id],
    ...updates,
  };
  
  try {
    await putConfig('api-keys', data);
    console.log(`✅ API key updated: ${id}`);
  } catch (error) {
    console.error(`❌ Failed to update API key ${id}:`, error);
    throw error;
  }
  
  return data[id];
}

/**
 * Revoke an API key (set active to false)
 */
export async function revokeApiKey(id: string): Promise<ApiKey | null> {
  return updateApiKey(id, { active: false });
}

/**
 * Activate an API key (set active to true)
 */
export async function activateApiKey(id: string): Promise<ApiKey | null> {
  return updateApiKey(id, { active: true });
}

/**
 * Delete an API key
 */
export async function deleteApiKey(id: string): Promise<boolean> {
  const data = await initConfigIfNeeded<ApiKeysData>('api-keys', DEFAULT_API_KEYS);
  
  if (!data[id]) {
    console.error(`❌ API key not found: ${id}`);
    return false;
  }
  
  delete data[id];
  
  try {
    await putConfig('api-keys', data);
    console.log(`✅ API key deleted: ${id}`);
    return true;
  } catch (error) {
    console.error(`❌ Failed to delete API key ${id}:`, error);
    throw error;
  }
}

/**
 * Validate an API key and return the key ID if valid
 * Also updates lastUsedAt and usageCount
 */
export async function validateApiKey(key: string): Promise<{ valid: boolean; keyId?: string }> {
  const data = await initConfigIfNeeded<ApiKeysData>('api-keys', DEFAULT_API_KEYS);
  const keyHash = hashApiKey(key);
  
  // Find matching key
  // Convert hex strings to buffers for constant-time comparison
  const keyHashBuffer = Buffer.from(keyHash, 'hex');
  
  for (const [id, apiKey] of Object.entries(data)) {
    // Constant-time comparison to prevent timing attacks
    // Both hashes should be the same length (64 hex chars = 32 bytes)
    if (apiKey.keyHash.length === keyHash.length) {
      try {
        const storedHashBuffer = Buffer.from(apiKey.keyHash, 'hex');
        if (crypto.timingSafeEqual(storedHashBuffer, keyHashBuffer)) {
          // Check if key is active
          if (!apiKey.active) {
            return { valid: false };
          }
          
          // Update usage stats
          await recordApiKeyUsage(id);
          
          return { valid: true, keyId: id };
        }
      } catch (error) {
        // If comparison fails (e.g., invalid hex), continue to next key
        continue;
      }
    }
  }
  
  return { valid: false };
}

/**
 * Record API key usage (update lastUsedAt and increment usageCount)
 */
export async function recordApiKeyUsage(id: string): Promise<void> {
  const data = await initConfigIfNeeded<ApiKeysData>('api-keys', DEFAULT_API_KEYS);
  
  if (!data[id]) {
    return;
  }
  
  data[id] = {
    ...data[id],
    lastUsedAt: new Date().toISOString(),
    usageCount: (data[id].usageCount || 0) + 1,
  };
  
  try {
    await putConfig('api-keys', data);
  } catch (error) {
    // Don't throw - usage tracking is not critical
    console.warn(`Failed to record usage for API key ${id}:`, error);
  }
}
