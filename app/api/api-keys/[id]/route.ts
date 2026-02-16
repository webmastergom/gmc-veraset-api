import { NextRequest, NextResponse } from 'next/server';
import {
  getApiKeyById,
  updateApiKey,
  deleteApiKey,
  activateApiKey,
  revokeApiKey,
} from '@/lib/api-keys';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/api-keys/[id]
 * Get specific API key metadata
 */
export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const key = await getApiKeyById(params.id);

    if (!key) {
      return NextResponse.json(
        { error: 'API key not found' },
        { status: 404 }
      );
    }

    // Return key without the hash for security
    return NextResponse.json({
      id: key.id,
      name: key.name,
      description: key.description,
      active: key.active,
      createdAt: key.createdAt,
      lastUsedAt: key.lastUsedAt,
      usageCount: key.usageCount,
    });
  } catch (error: any) {
    console.error(`GET /api/api-keys/[id] error:`, error);
    return NextResponse.json(
      { error: 'Failed to fetch API key', details: error.message },
      { status: 500 }
    );
  }
}

/**
 * PATCH /api/api-keys/[id]
 * Update API key (name, description, active status)
 */
export async function PATCH(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const body = await request.json().catch(() => ({}));
    const { name, description, active } = body;

    const updates: any = {};
    if (name !== undefined) {
      if (typeof name !== 'string' || name.trim().length === 0) {
        return NextResponse.json(
          { error: 'Name must be a non-empty string' },
          { status: 400 }
        );
      }
      updates.name = name.trim();
    }

    if (description !== undefined) {
      updates.description = typeof description === 'string' ? description.trim() : null;
    }

    if (active !== undefined) {
      if (typeof active !== 'boolean') {
        return NextResponse.json(
          { error: 'Active must be a boolean' },
          { status: 400 }
        );
      }
      updates.active = active;
    }

    if (Object.keys(updates).length === 0) {
      return NextResponse.json(
        { error: 'No valid updates provided' },
        { status: 400 }
      );
    }

    const updated = await updateApiKey(params.id, updates);

    if (!updated) {
      return NextResponse.json(
        { error: 'API key not found' },
        { status: 404 }
      );
    }

    // Return updated key without the hash
    return NextResponse.json({
      id: updated.id,
      name: updated.name,
      description: updated.description,
      active: updated.active,
      createdAt: updated.createdAt,
      lastUsedAt: updated.lastUsedAt,
      usageCount: updated.usageCount,
    });
  } catch (error: any) {
    console.error(`PATCH /api/api-keys/[id] error:`, error);
    return NextResponse.json(
      { error: 'Failed to update API key', details: error.message },
      { status: 500 }
    );
  }
}

/**
 * DELETE /api/api-keys/[id]
 * Delete an API key
 */
export async function DELETE(
  request: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const params = await context.params;
    const deleted = await deleteApiKey(params.id);

    if (!deleted) {
      return NextResponse.json(
        { error: 'API key not found' },
        { status: 404 }
      );
    }

    return NextResponse.json({ success: true, message: 'API key deleted' });
  } catch (error: any) {
    console.error(`DELETE /api/api-keys/[id] error:`, error);
    return NextResponse.json(
      { error: 'Failed to delete API key', details: error.message },
      { status: 500 }
    );
  }
}
