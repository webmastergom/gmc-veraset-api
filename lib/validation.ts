/**
 * Validation schemas using Zod
 */

import { z } from 'zod'

/**
 * Date range validation schema with strict validation
 */
export const dateRangeSchema = z.object({
  from: z.string().min(1).optional(),
  to: z.string().min(1).optional(),
  from_date: z.string().min(1).optional(),
  to_date: z.string().min(1).optional(),
}).refine(
  (data) => {
    const hasFrom = (data.from && data.from.length > 0) || (data.from_date && data.from_date.length > 0);
    const hasTo = (data.to && data.to.length > 0) || (data.to_date && data.to_date.length > 0);
    return hasFrom && hasTo;
  },
  { message: 'Both from and to dates are required' }
).refine(
  (data) => {
    const fromDate = data.from_date || data.from;
    const toDate = data.to_date || data.to;
    
    if (!fromDate || !toDate) return true; // Already validated above
    
    // Validate format YYYY-MM-DD
    const dateFormatRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateFormatRegex.test(fromDate) || !dateFormatRegex.test(toDate)) {
      return false;
    }
    
    // Validate dates are valid
    const from = new Date(fromDate);
    const to = new Date(toDate);
    if (isNaN(from.getTime()) || isNaN(to.getTime())) {
      return false;
    }
    
    // Validate order
    if (to < from) {
      return false;
    }
    
    // Validate range (max 31 days) - use consistent calculation (inclusive)
    // Same logic as calculateDaysInclusive but synchronous for Zod validation
    const diffMs = to.getTime() - from.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    const days = diffDays + 1; // +1 because both dates are inclusive
    if (days > 31) {
      return false;
    }
    
    return true;
  },
  { 
    message: 'Invalid date range. Dates must be in YYYY-MM-DD format, to_date must be after from_date, and range cannot exceed 31 days',
    path: ['dateRange']
  }
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
    type: z.enum(['pings', 'devices', 'aggregate']).optional(),
    date_range: z.object({
      from_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'from_date must be in YYYY-MM-DD format').optional(),
      to_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'to_date must be in YYYY-MM-DD format').optional(),
      from: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'from must be in YYYY-MM-DD format').optional(),
      to: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'to must be in YYYY-MM-DD format').optional(),
    }).refine(
      (data) => {
        const hasFrom = !!(data.from_date || data.from);
        const hasTo = !!(data.to_date || data.to);
        return hasFrom && hasTo;
      },
      { message: 'Both from_date and to_date are required in verasetConfig.date_range' }
    ).optional(),
    schema: z.enum(['BASIC', 'ENHANCED', 'FULL']).optional(),
    geo_radius: z.array(z.object({
      poi_id: z.string().min(1, 'poi_id is required'),
      latitude: z.number().min(-90).max(90),
      longitude: z.number().min(-180).max(180),
      distance_in_meters: z.number().min(1).max(1000).optional(),
      distance_in_miles: z.number().min(0).optional(),
    })).optional(),
    place_key: z.array(z.object({
      poi_id: z.string().min(1, 'poi_id is required'),
      placekey: z.string().min(1, 'placekey is required'),
    })).optional(),
  }).passthrough().optional(), // passthrough allows additional fields, validation done in code
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
      // Use consistent inclusive calculation
      const diffMs = to.getTime() - from.getTime()
      const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24))
      const days = diffDays + 1 // +1 because both dates are inclusive
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
  let parsedBody: unknown = undefined;
  try {
    parsedBody = await request.json();
    const validatedData = schema.parse(parsedBody);
    return { success: true, data: validatedData };
  } catch (error) {
    if (error instanceof z.ZodError) {
      const body = parsedBody as Record<string, unknown> | undefined;
      const errorDetails = error.errors.map(e => ({
        path: e.path.join('.') || 'root',
        message: e.message,
        code: e.code,
        received: e.path.length > 0 && body ? (body as Record<string, unknown>)[e.path[0] as string] : undefined,
      }))
      
      const errorMessages = errorDetails.map(e => 
        `${e.path}: ${e.message}${e.received !== undefined ? ` (received: ${JSON.stringify(e.received).substring(0, 100)})` : ''}`
      ).join(', ')
      
      console.error('[VALIDATION] Zod errors:', JSON.stringify(errorDetails, null, 2))
      
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
