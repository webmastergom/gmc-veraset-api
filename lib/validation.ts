/**
 * Validation schemas using Zod
 */

import { z } from 'zod'

/**
 * Date range validation schema
 */
export const dateRangeSchema = z.object({
  from: z.string().optional(),
  to: z.string().optional(),
  from_date: z.string().optional(),
  to_date: z.string().optional(),
}).refine(
  (data) => (data.from || data.from_date) && (data.to || data.to_date),
  { message: 'Both from and to dates are required' }
)

/**
 * Job creation schema
 */
export const createJobSchema = z.object({
  name: z.string().min(1).max(200),
  type: z.enum(['pings', 'devices', 'aggregate']),
  poiCount: z.number().int().min(0).optional(),
  poiCollectionId: z.string().optional(),
  dateRange: dateRangeSchema,
  radius: z.number().min(1).max(1000).optional().default(10),
  schema: z.enum(['BASIC', 'ENHANCED', 'FULL']).optional().default('BASIC'),
  verasetConfig: z.object({
    geo_radius: z.array(z.any()).optional(),
    place_key: z.array(z.string()).optional(), // For Veraset POIs (uses place_key instead of coordinates)
  }).optional(),
  pois: z.array(z.any()).optional(),
  poiMapping: z.record(z.string()).optional(),
  poiNames: z.record(z.string()).optional(),
})

/**
 * Login schema
 */
export const loginSchema = z.object({
  username: z.string().min(1).max(100),
  password: z.string().min(1).max(500),
})

/**
 * Dataset analysis filters schema
 */
export const analysisFiltersSchema = z.object({
  startDate: z.string().optional(),
  endDate: z.string().optional(),
  poiIds: z.array(z.string()).optional(),
  minDwellMinutes: z.number().min(0).optional(),
  maxDwellMinutes: z.number().min(0).optional(),
}).refine(
  (data) => !data.minDwellMinutes || !data.maxDwellMinutes || data.minDwellMinutes <= data.maxDwellMinutes,
  { message: 'minDwellMinutes must be less than or equal to maxDwellMinutes', path: ['minDwellMinutes'] }
)

/**
 * Validate and parse request body
 */
export async function validateRequestBody<T>(
  request: Request,
  schema: z.ZodSchema<T>
): Promise<{ success: true; data: T } | { success: false; error: string; status: number }> {
  try {
    const body = await request.json()
    const validatedData = schema.parse(body)
    return { success: true, data: validatedData }
  } catch (error) {
    if (error instanceof z.ZodError) {
      const errorMessages = error.errors.map(e => `${e.path.join('.')}: ${e.message}`).join(', ')
      return {
        success: false,
        error: `Validation error: ${errorMessages}`,
        status: 400,
      }
    }
    
    return {
      success: false,
      error: 'Invalid request body',
      status: 400,
    }
  }
}

/**
 * Sanitize string input (basic XSS prevention)
 */
export function sanitizeString(input: string, maxLength: number = 1000): string {
  return input
    .trim()
    .slice(0, maxLength)
    .replace(/[<>]/g, '') // Remove potential HTML tags
}

/**
 * Validate and sanitize array of strings
 */
export function sanitizeStringArray(input: unknown, maxLength: number = 100): string[] {
  if (!Array.isArray(input)) {
    return []
  }
  
  return input
    .filter((item): item is string => typeof item === 'string')
    .map(item => sanitizeString(item, maxLength))
    .filter(Boolean)
}
