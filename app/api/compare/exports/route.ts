import { NextRequest, NextResponse } from 'next/server';
import { isAuthenticated } from '@/lib/auth';
import { listS3Objects } from '@/lib/s3';
import { BUCKET } from '@/lib/s3-config';

export const dynamic = 'force-dynamic';

/**
 * GET /api/compare/exports?dataset={name}
 *
 * Lists available export CSVs for a dataset from S3 exports/ prefix.
 * Returns category exports, NSE exports, and MAID exports.
 */
export async function GET(request: NextRequest) {
  if (!isAuthenticated(request)) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  const dataset = request.nextUrl.searchParams.get('dataset');
  if (!dataset) {
    return NextResponse.json({ error: 'dataset parameter required' }, { status: 400 });
  }

  try {
    const objects = await listS3Objects(BUCKET, `exports/${dataset}-`);

    const exports = objects
      .filter(obj => obj.Key.endsWith('.csv') && obj.Size > 0)
      .map(obj => {
        const fileName = obj.Key.replace('exports/', '');
        let type = 'other';
        let label = fileName;

        // Parse export type from filename patterns
        if (fileName.includes('-category-')) {
          type = 'category';
          // e.g. ds-category-education-1234567890.csv
          const match = fileName.match(/-category-(.+?)-\d+\.csv$/);
          label = match ? `Category: ${match[1].replace(/_/g, ' ')}` : fileName;
        } else if (fileName.includes('-maids-nse-')) {
          type = 'nse';
          // e.g. ds-maids-nse-0-19-1234567890.csv
          const match = fileName.match(/-maids-nse-(\d+)-(\d+)-\d+\.csv$/);
          label = match ? `NSE ${match[1]}-${match[2]}` : fileName;
        } else if (fileName.includes('-maids-')) {
          type = 'maids';
          label = 'MAIDs export';
        }

        // Extract timestamp for date display
        const tsMatch = fileName.match(/-(\d{13})\.csv$/);
        const date = tsMatch ? new Date(parseInt(tsMatch[1])).toISOString().split('T')[0] : '';

        return { file: fileName, type, label, date };
      })
      .sort((a, b) => b.date.localeCompare(a.date)); // newest first

    return NextResponse.json({ exports });
  } catch (error: any) {
    console.error('[COMPARE-EXPORTS] Error:', error.message);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
