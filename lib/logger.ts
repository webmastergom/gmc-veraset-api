/**
 * Secure logging utilities
 * Prevents sensitive information from being logged in production
 */

const isProduction = process.env.NODE_ENV === 'production'

/**
 * Sanitize data before logging
 */
function sanitizeForLogging(data: unknown): unknown {
  if (typeof data !== 'object' || data === null) {
    return data
  }
  
  if (Array.isArray(data)) {
    return data.map(sanitizeForLogging)
  }
  
  const sensitiveKeys = [
    'password',
    'secret',
    'token',
    'key',
    'auth',
    'credential',
    'api_key',
    'access_key',
    'secret_key',
  ]
  
  const sanitized: Record<string, unknown> = {}
  
  for (const [key, value] of Object.entries(data)) {
    const lowerKey = key.toLowerCase()
    const isSensitive = sensitiveKeys.some(sk => lowerKey.includes(sk))
    
    if (isSensitive && isProduction) {
      sanitized[key] = '[REDACTED]'
    } else if (typeof value === 'object' && value !== null) {
      sanitized[key] = sanitizeForLogging(value)
    } else {
      sanitized[key] = value
    }
  }
  
  return sanitized
}

/**
 * Safe logger that redacts sensitive information in production
 */
export const logger = {
  log: (...args: unknown[]) => {
    if (!isProduction) {
      console.log(...args)
    } else {
      // In production, sanitize logs
      const sanitized = args.map(sanitizeForLogging)
      console.log(...sanitized)
    }
  },
  
  error: (message: string, error?: unknown) => {
    if (!isProduction) {
      console.error(message, error)
    } else {
      // In production, only log error messages, not full error objects
      const sanitizedError = error instanceof Error 
        ? { message: error.message, name: error.name }
        : sanitizeForLogging(error)
      console.error(message, sanitizedError)
    }
  },
  
  warn: (...args: unknown[]) => {
    if (!isProduction) {
      console.warn(...args)
    } else {
      const sanitized = args.map(sanitizeForLogging)
      console.warn(...sanitized)
    }
  },
  
  info: (...args: unknown[]) => {
    if (!isProduction) {
      console.info(...args)
    } else {
      const sanitized = args.map(sanitizeForLogging)
      console.info(...sanitized)
    }
  },
}
