/**
 * Authentication utilities
 */

import { cookies } from 'next/headers'
import { NextRequest } from 'next/server'
import crypto from 'crypto'

/**
 * Verify if a request is authenticated
 */
export function isAuthenticated(request?: NextRequest): boolean {
  if (request) {
    const authCookie = request.cookies.get('auth-token')
    return authCookie?.value === process.env.AUTH_SECRET
  }
  
  // Server-side check using cookies()
  try {
    const cookieStore = cookies()
    const authCookie = cookieStore.get('auth-token')
    return authCookie?.value === process.env.AUTH_SECRET
  } catch {
    return false
  }
}

/**
 * Hash a password using SHA-256 (for basic security)
 * Note: For production, consider using bcrypt or argon2
 */
export function hashPassword(password: string, salt?: string): { hash: string; salt: string } {
  const actualSalt = salt || crypto.randomBytes(16).toString('hex')
  const hash = crypto
    .createHash('sha256')
    .update(password + actualSalt)
    .digest('hex')
  
  return { hash, salt: actualSalt }
}

/**
 * Verify a password against a hash
 */
export function verifyPassword(password: string, hash: string, salt: string): boolean {
  const { hash: computedHash } = hashPassword(password, salt)
  return computedHash === hash
}

/**
 * Generate a secure random token
 */
export function generateSecureToken(length: number = 32): string {
  return crypto.randomBytes(length).toString('base64url')
}

/**
 * Verify credentials (with optional password hashing)
 */
export function verifyCredentials(
  username: string,
  password: string
): boolean {
  const authUsername = process.env.AUTH_USERNAME
  const authPassword = process.env.AUTH_PASSWORD
  
  if (!authUsername || !authPassword) {
    return false
  }
  
  // Check if password is hashed (has salt stored)
  const passwordHash = process.env.AUTH_PASSWORD_HASH
  const passwordSalt = process.env.AUTH_PASSWORD_SALT
  
  if (passwordHash && passwordSalt) {
    // Use hashed password verification
    return username === authUsername && verifyPassword(password, passwordHash, passwordSalt)
  }
  
  // Fallback to plain text comparison (less secure, but backward compatible)
  return username === authUsername && password === authPassword
}
