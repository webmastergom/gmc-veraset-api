/**
 * Security utilities for the application
 */

/**
 * Get allowed origins from environment variable
 * Format: comma-separated list of origins, e.g., "https://example.com,https://app.example.com"
 */
export function getAllowedOrigins(): string[] {
  const allowedOrigins = process.env.ALLOWED_ORIGINS;
  
  if (!allowedOrigins) {
    // Default to same origin in production, allow all in development
    return process.env.NODE_ENV === 'production' 
      ? [] 
      : ['http://localhost:3000', 'http://127.0.0.1:3000'];
  }
  
  return allowedOrigins.split(',').map(origin => origin.trim()).filter(Boolean);
}

/**
 * Check if an origin is allowed
 */
export function isOriginAllowed(origin: string | null): boolean {
  if (!origin) return false;
  
  const allowedOrigins = getAllowedOrigins();
  
  // In development, allow localhost
  if (process.env.NODE_ENV !== 'production') {
    if (origin.startsWith('http://localhost') || origin.startsWith('http://127.0.0.1')) {
      return true;
    }
  }
  
  return allowedOrigins.includes(origin);
}

/**
 * Get CORS headers for a given request origin
 */
export function getCorsHeaders(origin: string | null): Record<string, string> {
  const headers: Record<string, string> = {
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
    'Access-Control-Allow-Credentials': 'true',
  };
  
  if (origin && isOriginAllowed(origin)) {
    headers['Access-Control-Allow-Origin'] = origin;
  } else if (process.env.NODE_ENV !== 'production') {
    // In development, allow the origin if it's localhost
    headers['Access-Control-Allow-Origin'] = origin || '*';
  } else {
    // In production, don't set the header if origin is not allowed
    // This effectively blocks CORS requests from unauthorized origins
  }
  
  return headers;
}

/**
 * Sanitize error messages for production
 * Prevents leaking sensitive information
 */
export function sanitizeError(error: unknown, isProduction: boolean = process.env.NODE_ENV === 'production'): string {
  if (!isProduction) {
    return error instanceof Error ? error.message : String(error);
  }
  
  // In production, return generic error messages
  if (error instanceof Error) {
    // Only expose specific error types that are safe
    if (error.message.includes('not configured') || error.message.includes('missing')) {
      return 'Configuration error. Please contact support.';
    }
    return 'An error occurred. Please try again later.';
  }
  
  return 'An error occurred. Please try again later.';
}

/**
 * Rate limiting helper (in-memory, simple implementation)
 * For production, consider using Redis or a dedicated service
 */
const rateLimitStore = new Map<string, { count: number; resetTime: number }>();

export function checkRateLimit(
  identifier: string,
  maxRequests: number = 10,
  windowMs: number = 60000 // 1 minute
): { allowed: boolean; remaining: number; resetAt: number } {
  const now = Date.now();
  const record = rateLimitStore.get(identifier);
  
  if (!record || now > record.resetTime) {
    // Create new record
    const resetAt = now + windowMs;
    rateLimitStore.set(identifier, { count: 1, resetTime: resetAt });
    
    // Cleanup old entries periodically
    if (rateLimitStore.size > 10000) {
      for (const [key, value] of rateLimitStore.entries()) {
        if (now > value.resetTime) {
          rateLimitStore.delete(key);
        }
      }
    }
    
    return { allowed: true, remaining: maxRequests - 1, resetAt };
  }
  
  if (record.count >= maxRequests) {
    return { allowed: false, remaining: 0, resetAt: record.resetTime };
  }
  
  record.count++;
  return { allowed: true, remaining: maxRequests - record.count, resetAt: record.resetTime };
}

/**
 * Get client identifier for rate limiting
 */
export function getClientIdentifier(request: Request | { headers: { get: (name: string) => string | null } }): string {
  // Try to get IP from various headers (for Vercel/proxy environments)
  const forwardedFor = request.headers.get('x-forwarded-for');
  const realIp = request.headers.get('x-real-ip');
  const ip = forwardedFor?.split(',')[0] || realIp || 'unknown';
  
  return ip;
}
