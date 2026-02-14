import { NextResponse } from 'next/server';
import { POI_CATEGORIES, CATEGORY_LABELS, CATEGORY_GROUPS } from '@/lib/laboratory-types';

export const dynamic = 'force-dynamic';

/**
 * GET /api/laboratory/countries
 * Returns POI categories and groups for the laboratory UI.
 */
export async function GET(): Promise<NextResponse> {
  return NextResponse.json({
    categories: POI_CATEGORIES.map(cat => ({
      id: cat,
      label: CATEGORY_LABELS[cat],
    })),
    categoryGroups: Object.entries(CATEGORY_GROUPS).map(([key, g]) => ({
      id: key,
      label: g.label,
      icon: g.icon,
      color: g.color,
      categories: g.categories,
    })),
  });
}
