/**
 * Client-side exports for Compare results.
 * - CSV: built in-browser, no deps.
 * - XLSX: uses the `xlsx` lib (dynamically imported to keep initial bundle smaller).
 */

export interface ComparePoiExportRow {
  side: 'A' | 'B';
  poiId: string;
  name?: string;
  lat?: number;
  lng?: number;
  overlapDevices: number;
}

export interface CompareExportContext {
  datasetALabel: string;
  datasetBLabel: string;
  totalA: number;
  totalB: number;
  overlap: number;
  overlapPctA: number;
  overlapPctB: number;
  zipCodes?: string[];
  countryA?: string;
  countryB?: string;
}

function buildRows(pois: ComparePoiExportRow[], side: 'A' | 'B', totalSide: number, overlap: number) {
  return pois
    .filter(p => p.side === side)
    .map(p => ({
      side,
      name: p.name || '',
      poi_id: p.poiId,
      lat: Number.isFinite(p.lat as number) ? p.lat : '',
      lng: Number.isFinite(p.lng as number) ? p.lng : '',
      overlap_devices: p.overlapDevices,
      pct_of_sample: totalSide > 0 ? +((p.overlapDevices / totalSide) * 100).toFixed(4) : 0,
      pct_of_match: overlap > 0 ? +((p.overlapDevices / overlap) * 100).toFixed(4) : 0,
    }));
}

function escapeCsv(value: unknown): string {
  const s = value == null ? '' : String(value);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function rowsToCsv(rows: Array<Record<string, unknown>>, headers: string[]): string {
  const lines = [headers.join(',')];
  for (const r of rows) {
    lines.push(headers.map(h => escapeCsv(r[h])).join(','));
  }
  return lines.join('\n');
}

function triggerDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/**
 * Export the POI-penetration table for a single side as a CSV file.
 */
export function downloadComparePoisCsv(
  pois: ComparePoiExportRow[],
  side: 'A' | 'B',
  ctx: CompareExportContext,
): void {
  const totalSide = side === 'A' ? ctx.totalA : ctx.totalB;
  const rows = buildRows(pois, side, totalSide, ctx.overlap);
  const headers = ['side', 'name', 'poi_id', 'lat', 'lng', 'overlap_devices', 'pct_of_sample', 'pct_of_match'];
  const csv = rowsToCsv(rows, headers);
  const label = side === 'A' ? ctx.datasetALabel : ctx.datasetBLabel;
  const safe = label.replace(/[^\w.-]+/g, '_').slice(0, 60);
  triggerDownload(new Blob([csv], { type: 'text/csv;charset=utf-8' }), `compare-pois-${side}-${safe}.csv`);
}

/**
 * Export the full comparison as an XLSX workbook with multiple sheets:
 *   - Summary: overlap stats + ZIP filter info
 *   - POIs A: penetration table for side A
 *   - POIs B: penetration table for side B
 */
export async function downloadCompareXlsx(
  pois: ComparePoiExportRow[],
  ctx: CompareExportContext,
): Promise<void> {
  const XLSX = await import('xlsx');

  const summary: Array<[string, string | number]> = [
    ['Dataset A', ctx.datasetALabel],
    ['Dataset B', ctx.datasetBLabel],
    ['Total A (devices touching ≥1 POI)', ctx.totalA],
    ['Total B (devices touching ≥1 POI)', ctx.totalB],
    ['Overlap (A ∩ B)', ctx.overlap],
    ['% of A in overlap', `${ctx.overlapPctA}%`],
    ['% of B in overlap', `${ctx.overlapPctB}%`],
  ];
  if (ctx.zipCodes && ctx.zipCodes.length) {
    summary.push(['ZIP filter', ctx.zipCodes.join(', ')]);
    if (ctx.countryA) summary.push(['Country A', ctx.countryA]);
    if (ctx.countryB) summary.push(['Country B', ctx.countryB]);
  }

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.aoa_to_sheet([['Metric', 'Value'], ...summary]),
    'Summary',
  );

  const rowsA = buildRows(pois, 'A', ctx.totalA, ctx.overlap);
  const rowsB = buildRows(pois, 'B', ctx.totalB, ctx.overlap);

  if (rowsA.length) {
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rowsA), `POIs A`);
  }
  if (rowsB.length) {
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(rowsB), `POIs B`);
  }

  const buf = XLSX.write(wb, { type: 'array', bookType: 'xlsx' }) as ArrayBuffer;
  const labelA = ctx.datasetALabel.replace(/[^\w.-]+/g, '_').slice(0, 30);
  const labelB = ctx.datasetBLabel.replace(/[^\w.-]+/g, '_').slice(0, 30);
  triggerDownload(
    new Blob([buf], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }),
    `compare-${labelA}-vs-${labelB}.xlsx`,
  );
}
