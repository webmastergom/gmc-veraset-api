import { NextRequest, NextResponse } from 'next/server';
import { getAllApiKeys, createApiKey } from '@/lib/api-keys';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/api-keys
 * List all API keys (without hashes, for display)
 */
export async function GET(request: NextRequest) {
  try {
    const keys = await getAllApiKeys();
    
    // Return keys without the hash for security
    const keysForDisplay = keys.map(key => ({
      id: key.id,
      name: key.name,
      description: key.description,
      active: key.active,
      createdAt: key.createdAt,
      lastUsedAt: key.lastUsedAt,
      usageCount: key.usageCount,
    }));
    
    return NextResponse.json({ keys: keysForDisplay });
  } catch (error: any) {
    console.error('GET /api/api-keys error:', error);
    return NextResponse.json(
      { error: 'Failed to fetch API keys', details: error.message },
      { status: 500 }
    );
  }
}

/**
 * POST /api/api-keys
 * Create a new API key
 * Returns the plain key once (for display to user)
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json().catch(() => ({}));
    const { name, description } = body;
    
    if (!name || typeof name !== 'string' || name.trim().length === 0) {
      return NextResponse.json(
        { error: 'Name is required and must be a non-empty string' },
        { status: 400 }
      );
    }
    
    const { apiKey, plainKey } = await createApiKey(name.trim(), description?.trim());
    
    // Return the plain key once (this is the only time it will be available)
    return NextResponse.json({
      id: apiKey.id,
      name: apiKey.name,
      description: apiKey.description,
      active: apiKey.active,
      createdAt: apiKey.createdAt,
      apiKey: plainKey, // Plain key - only returned once!
      warning: 'Save this API key now. It will not be shown again.',
    });
  } catch (error: any) {
    console.error('POST /api/api-keys error:', error);
    return NextResponse.json(
      { error: 'Failed to create API key', details: error.message },
      { status: 500 }
    );
  }
}
