import { NextResponse } from 'next/server'
import { cookies } from 'next/headers'
import { verifyCredentials } from '@/lib/auth'
import { checkRateLimit, getClientIdentifier, sanitizeError } from '@/lib/security'
import { validateRequestBody, loginSchema } from '@/lib/validation'
import { logger } from '@/lib/logger'

export async function POST(req: Request) {
  try {
    // Rate limiting for login endpoint (stricter)
    const clientId = getClientIdentifier(req)
    const rateLimit = checkRateLimit(`login:${clientId}`, 5, 900000) // 5 attempts per 15 minutes
    
    if (!rateLimit.allowed) {
      return NextResponse.json(
        { error: 'Too many login attempts. Please try again later.' },
        {
          status: 429,
          headers: {
            'Retry-After': String(Math.ceil((rateLimit.resetAt - Date.now()) / 1000)),
            'X-RateLimit-Limit': '5',
            'X-RateLimit-Remaining': '0',
            'X-RateLimit-Reset': String(Math.floor(rateLimit.resetAt / 1000)),
          },
        }
      )
    }
    
    // Validate request body
    const validation = await validateRequestBody(req, loginSchema);
    if (!validation.success) {
      return NextResponse.json(
        { error: validation.error },
        { status: validation.status }
      );
    }
    
    const { username, password } = validation.data;
    
    // Check if environment variables are configured
    const authSecret = process.env.AUTH_SECRET
    
    if (!authSecret) {
      const isProduction = process.env.NODE_ENV === 'production'
      const errorMessage = isProduction
        ? 'Server configuration error'
        : 'AUTH_SECRET not configured'
      
      logger.error('Auth configuration missing: AUTH_SECRET')
      
      return NextResponse.json(
        { error: errorMessage },
        { status: 500 }
      )
    }
    
    // Verify credentials
    if (verifyCredentials(username, password)) {
      // Set auth token cookie
      cookies().set('auth-token', authSecret, {
        httpOnly: true,
        secure: process.env.NODE_ENV === 'production',
        sameSite: process.env.NODE_ENV === 'production' ? 'strict' : 'lax', // 'lax' in dev for fetch compatibility, 'strict' in production
        maxAge: 60 * 60 * 24 * 7, // 7 days
        path: '/',
      })
      
      logger.info('Successful login', { username: username.substring(0, 3) + '***' })
      
      return NextResponse.json({ success: true })
    }
    
    // Don't reveal whether username or password was wrong (security best practice)
    return NextResponse.json(
      { error: 'Invalid credentials' },
      {
        status: 401,
        headers: {
          'X-RateLimit-Limit': '5',
          'X-RateLimit-Remaining': String(rateLimit.remaining - 1),
          'X-RateLimit-Reset': String(Math.floor(rateLimit.resetAt / 1000)),
        },
      }
    )
  } catch (error) {
    logger.error('Login error:', error)
    const isProduction = process.env.NODE_ENV === 'production'
    const errorMessage = sanitizeError(error, isProduction)
    
    return NextResponse.json(
      { error: errorMessage },
      { status: 400 }
    )
  }
}
