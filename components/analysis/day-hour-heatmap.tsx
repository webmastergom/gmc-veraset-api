'use client';

import { Fragment, useMemo, useState } from 'react';

export interface DayHourCell {
  /** ISO 8601 day-of-week: 1=Monday..7=Sunday */
  dow: number;
  /** Hour of day: 0..23 */
  hour: number;
  pings: number;
  devices: number;
}

interface Props {
  cells: DayHourCell[];
  /** Default 'devices' — what to colour cells by */
  metric?: 'devices' | 'pings';
}

const DAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

/**
 * Viridis-inspired sequential palette — wide perceptual contrast across the
 * full range so even cells with similar values are visually distinct.
 */
const PALETTE: Array<[number, [number, number, number]]> = [
  [0.00, [38, 6, 80]],       // dark purple
  [0.25, [50, 75, 140]],     // indigo
  [0.50, [27, 158, 158]],    // teal
  [0.75, [120, 200, 90]],    // green
  [1.00, [253, 231, 37]],    // yellow
];

/**
 * Cell colour using min-max normalization across the visible grid. With
 * real-world POI data the absolute range can be narrow (e.g. min=1064,
 * max=1957 — only 1.8× spread); a 0-anchored or log scale flattens the
 * variation, so we anchor the palette at the grid's actual (min, max).
 *
 * @param value cell value
 * @param min   smallest non-zero value across the grid
 * @param max   largest value across the grid
 * @param hasData whether any cell has value > 0
 */
function cellColor(value: number, min: number, max: number, hasData: boolean): string {
  if (!hasData) return 'rgba(255,255,255,0.04)';
  if (value <= 0) return 'rgba(255,255,255,0.04)';
  const span = max - min;
  // All cells equal — use the mid colour rather than a flat dark/bright
  const t = span > 0 ? (value - min) / span : 0.5;
  const clamped = Math.max(0, Math.min(1, t));
  let lo = PALETTE[0], hi = PALETTE[PALETTE.length - 1];
  for (let i = 0; i < PALETTE.length - 1; i++) {
    if (clamped >= PALETTE[i][0] && clamped <= PALETTE[i + 1][0]) { lo = PALETTE[i]; hi = PALETTE[i + 1]; break; }
  }
  const segSpan = hi[0] - lo[0] || 1;
  const f = (clamped - lo[0]) / segSpan;
  const r = Math.round(lo[1][0] + (hi[1][0] - lo[1][0]) * f);
  const g = Math.round(lo[1][1] + (hi[1][1] - lo[1][1]) * f);
  const b = Math.round(lo[1][2] + (hi[1][2] - lo[1][2]) * f);
  return `rgb(${r}, ${g}, ${b})`;
}

export function DayHourHeatmap({ cells, metric = 'devices' }: Props) {
  const [selectedMetric, setSelectedMetric] = useState<'devices' | 'pings'>(metric);
  const [hover, setHover] = useState<DayHourCell | null>(null);

  // Build a 7×24 grid (7 days × 24 hours), pre-filled with zeros for missing combos.
  const grid = useMemo(() => {
    const g: DayHourCell[][] = Array.from({ length: 7 }, (_, dIdx) =>
      Array.from({ length: 24 }, (_, hIdx) => ({ dow: dIdx + 1, hour: hIdx, pings: 0, devices: 0 }))
    );
    for (const c of cells || []) {
      const dIdx = c.dow - 1;
      if (dIdx < 0 || dIdx > 6 || c.hour < 0 || c.hour > 23) continue;
      g[dIdx][c.hour] = { ...c };
    }
    return g;
  }, [cells]);

  // Compute (min, max) over non-zero cells so the palette uses the grid's
  // actual range. With narrow spreads this is what makes the contrast pop.
  const { min, max, hasData } = useMemo(() => {
    let mn = Infinity, mx = 0, any = false;
    for (const row of grid) for (const c of row) {
      const v = selectedMetric === 'devices' ? c.devices : c.pings;
      if (v <= 0) continue;
      any = true;
      if (v < mn) mn = v;
      if (v > mx) mx = v;
    }
    return { min: any ? mn : 0, max: mx, hasData: any };
  }, [grid, selectedMetric]);

  return (
    <div>
      <div className="flex items-center gap-2 mb-3 text-xs">
        <span className="text-muted-foreground uppercase tracking-wider">Metric:</span>
        <button
          onClick={() => setSelectedMetric('devices')}
          className={`px-2 py-1 rounded ${selectedMetric === 'devices' ? 'bg-primary text-primary-foreground' : 'bg-muted hover:bg-muted/70'}`}
        >
          Unique devices
        </button>
        <button
          onClick={() => setSelectedMetric('pings')}
          className={`px-2 py-1 rounded ${selectedMetric === 'pings' ? 'bg-primary text-primary-foreground' : 'bg-muted hover:bg-muted/70'}`}
        >
          Pings
        </button>
        {hover && (
          <span className="ml-auto text-muted-foreground tabular-nums">
            {DAY_LABELS[hover.dow - 1]} {String(hover.hour).padStart(2, '0')}h
            {' · '}
            <span className="text-foreground font-semibold">
              {(selectedMetric === 'devices' ? hover.devices : hover.pings).toLocaleString()}
            </span>
            {' '}{selectedMetric}
            {' · '}
            <span className="text-muted-foreground">
              {(selectedMetric === 'devices' ? hover.pings : hover.devices).toLocaleString()} {selectedMetric === 'devices' ? 'pings' : 'devices'}
            </span>
          </span>
        )}
      </div>
      <div className="overflow-x-auto">
        <div className="inline-grid gap-px" style={{ gridTemplateColumns: '40px repeat(24, minmax(22px, 1fr))' }}>
          {/* Header row: hours */}
          <div />
          {Array.from({ length: 24 }, (_, h) => (
            <div key={`h-${h}`} className="text-[10px] text-muted-foreground text-center tabular-nums leading-none py-1">
              {String(h).padStart(2, '0')}
            </div>
          ))}
          {/* Body rows: one per day */}
          {grid.map((row, dIdx) => (
            <Fragment key={`row-${dIdx}`}>
              <div className="text-[11px] text-muted-foreground pr-2 self-center tabular-nums">
                {DAY_LABELS[dIdx]}
              </div>
              {row.map((cell) => {
                const value = selectedMetric === 'devices' ? cell.devices : cell.pings;
                return (
                  <div
                    key={`c-${dIdx}-${cell.hour}`}
                    onMouseEnter={() => setHover(cell)}
                    onMouseLeave={() => setHover(null)}
                    title={`${DAY_LABELS[dIdx]} ${String(cell.hour).padStart(2, '0')}h — ${cell.devices.toLocaleString()} devices, ${cell.pings.toLocaleString()} pings`}
                    className="h-7 rounded-[2px] cursor-default"
                    style={{ backgroundColor: cellColor(value, min, max, hasData) }}
                  />
                );
              })}
            </Fragment>
          ))}
        </div>
      </div>
      <div className="flex items-center gap-2 mt-3 text-[10px] text-muted-foreground">
        <span className="tabular-nums">{min.toLocaleString()}</span>
        <div
          className="h-2 flex-1 rounded"
          style={{
            // Render the gradient from the actual palette stops so the legend
            // matches what the cells use.
            background: `linear-gradient(to right, ${PALETTE.map(([_, [r, g, b]]) => `rgb(${r},${g},${b})`).join(', ')})`,
          }}
        />
        <span className="tabular-nums">{max.toLocaleString()} {selectedMetric}</span>
      </div>
    </div>
  );
}
