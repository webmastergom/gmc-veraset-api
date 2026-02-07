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
 * External API: POI item schema
 */
const externalPoiSchema = z.object({
  id: z.string().min(1).max(200),
  name: z.string().max(500).optional(),
  latitude: z.number().min(-90).max(90),
  longitude: z.number().min(-180).max(180),
})

/**
 * External API: Job creation schema
 */
export const externalCreateJobSchema = z.object({
  name: z.string().min(1).max(200),
  country: z.string().length(2).toUpperCase(),
  type: z.enum(['pings', 'devices', 'aggregate']).optional().default('pings'),
  date_range: z.object({
    from: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
    to: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  }),
  radius: z.number().min(1).max(1000).optional().default(10),
  schema: z.enum(['BASIC', 'ENHANCED', 'FULL']).optional().default('BASIC'),
  pois: z.array(externalPoiSchema).min(1).max(25000),
  webhook_url: z.string().url().startsWith('https://').optional(),
}).refine(
  (data) => {
    const from = new Date(data.date_range.from)
    const to = new Date(data.date_range.to)
    const days = Math.ceil((to.getTime() - from.getTime()) / (1000 * 60 * 60 * 24))
    return days >= 1 && days <= 31
  },
  { message: 'Date range must be between 1 and 31 days', path: ['date_range'] }
)

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
