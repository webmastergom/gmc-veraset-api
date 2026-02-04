import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'
import { checkRateLimit, getClientIdentifier, getCorsHeaders } from './lib/security'

// Public API routes that don't require authentication
const PUBLIC_API_ROUTES = [
  '/api/auth/login',
  '/api/auth/logout',
  '/api/health',
]

// External API routes that use API key authentication (not cookie auth)
// These routes handle their own authentication via API keys
const EXTERNAL_API_ROUTES = [
  '/api/external',
]

// Veraset API routes that should be protected but may be called externally
// These should validate API keys or tokens separately
const VERASET_API_ROUTES = [
  '/api/veraset',
]

export function middleware(request: NextRequest) {
  const authCookie = request.cookies.get('auth-token')
  const isLoginPage = request.nextUrl.pathname === '/login'
  const pathname = request.nextUrl.pathname
  const isApiRoute = pathname.startsWith('/api/')
  
  // Handle CORS for API routes
  if (isApiRoute) {
    const origin = request.headers.get('origin')
    
    // Check route types first
    const isPublicRoute = PUBLIC_API_ROUTES.some(route => pathname.startsWith(route))
    const isExternalRoute = EXTERNAL_API_ROUTES.some(route => pathname.startsWith(route))
    const isVerasetRoute = pathname.startsWith('/api/veraset')
    
    // External API routes allow CORS from any origin (they use API key auth)
    const corsHeaders = isExternalRoute 
      ? {
          'Access-Control-Allow-Origin': origin || '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-API-Key, X-Requested-With',
          'Access-Control-Allow-Credentials': 'false',
        }
      : getCorsHeaders(origin)
    
    // Handle preflight requests
    if (request.method === 'OPTIONS') {
      return new NextResponse(null, {
        status: 204,
        headers: corsHeaders,
      })
    }
    
    // Apply rate limiting to all API routes
    const clientId = getClientIdentifier(request)
    // External routes get higher limits (they use API key auth)
    const rateLimitMax = isPublicRoute ? 5 : (isExternalRoute ? 100 : 20)
    const rateLimit = checkRateLimit(
      `${clientId}:${pathname}`,
      rateLimitMax,
      60000 // 1 minute window
    )
    
    if (!rateLimit.allowed) {
      return NextResponse.json(
        { error: 'Too many requests. Please try again later.' },
        {
          status: 429,
          headers: {
            ...corsHeaders,
            'Retry-After': String(Math.ceil((rateLimit.resetAt - Date.now()) / 1000)),
            'X-RateLimit-Limit': String(rateLimitMax),
            'X-RateLimit-Remaining': '0',
            'X-RateLimit-Reset': String(Math.floor(rateLimit.resetAt / 1000)),
          },
        }
      )
    }
    
    // Protect non-public API routes (except External and Veraset routes which handle their own auth)
    if (!isPublicRoute && !isVerasetRoute && !isExternalRoute) {
      if (!authCookie || authCookie.value !== process.env.AUTH_SECRET) {
        return NextResponse.json(
          { error: 'Unauthorized' },
          {
            status: 401,
            headers: corsHeaders,
          }
        )
      }
    }
    
    // Add rate limit headers to response
    const response = NextResponse.next()
    Object.entries(corsHeaders).forEach(([key, value]) => {
      response.headers.set(key, value)
    })
    response.headers.set('X-RateLimit-Limit', String(rateLimitMax))
    response.headers.set('X-RateLimit-Remaining', String(rateLimit.remaining))
    response.headers.set('X-RateLimit-Reset', String(Math.floor(rateLimit.resetAt / 1000)))
    
    return response
  }
  
  // Allow login page
  if (isLoginPage) {
    return NextResponse.next()
  }
  
  // Check authentication for all other routes (frontend pages)
  if (!authCookie || authCookie.value !== process.env.AUTH_SECRET) {
    return NextResponse.redirect(new URL('/login', request.url))
  }
  
  return NextResponse.next()
}

export const config = {
  matcher: ['/((?!_next/static|_next/image|favicon.ico|logo|images|assets).*)']
}
