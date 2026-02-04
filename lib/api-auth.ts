/**
 * API Authentication Middleware
 * Validates API keys from external requests
 */

import { NextRequest } from 'next/server';
import { validateApiKey } from './api-keys';

/**
 * Validate API key from request
 * Checks X-API-Key header or api_key query parameter
 * Returns validation result with key ID if valid
 */
export async function validateApiKeyFromRequest(
  request: NextRequest
): Promise<{ valid: boolean; keyId?: string; error?: string }> {
  // Try to get API key from header first
  const headerKey = request.headers.get('X-API-Key');
  
  // If not in header, try query parameter
  const { searchParams } = new URL(request.url);
  const queryKey = searchParams.get('api_key');
  
  const apiKey = headerKey || queryKey;
  
  if (!apiKey) {
    return {
      valid: false,
      error: 'API key is required. Provide it via X-API-Key header or api_key query parameter.',
    };
  }
  
  // Validate the key
  const validation = await validateApiKey(apiKey);
  
  if (!validation.valid) {
    return {
      valid: false,
      error: 'Invalid API key. Please check your API key and try again.',
    };
  }
  
  return {
    valid: true,
    keyId: validation.keyId,
  };
}
