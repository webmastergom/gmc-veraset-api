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
 * Cell colour scale: dark slate when 0, ramping through teal → bright cyan
 * as the value approaches the grid max. Returns a CSS background colour.
 */
function cellColor(value: number, max: number): string {
  if (max <= 0 || value <= 0) return 'rgba(255,255,255,0.04)';
  // Log-scale so a few high cells don't crush the rest
  const t = Math.log(value + 1) / Math.log(max + 1);
  const clamped = Math.max(0, Math.min(1, t));
  // Mix from #0f172a (slate-900) through #0e7490 (cyan-700) to #67e8f9 (cyan-300)
  const stops: Array<[number, [number, number, number]]> = [
    [0.00, [15, 23, 42]],     // slate-900
    [0.35, [14, 116, 144]],   // cyan-700
    [0.70, [34, 211, 238]],   // cyan-400
    [1.00, [165, 243, 252]],  // cyan-200
  ];
  let lo = stops[0], hi = stops[stops.length - 1];
  for (let i = 0; i < stops.length - 1; i++) {
    if (clamped >= stops[i][0] && clamped <= stops[i + 1][0]) { lo = stops[i]; hi = stops[i + 1]; break; }
  }
  const span = hi[0] - lo[0] || 1;
  const f = (clamped - lo[0]) / span;
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

  const max = useMemo(() => {
    let m = 0;
    for (const row of grid) for (const c of row) m = Math.max(m, selectedMetric === 'devices' ? c.devices : c.pings);
    return m;
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
                    style={{ backgroundColor: cellColor(value, max) }}
                  />
                );
              })}
            </Fragment>
          ))}
        </div>
      </div>
      <div className="flex items-center gap-2 mt-3 text-[10px] text-muted-foreground">
        <span>0</span>
        <div
          className="h-2 flex-1 rounded"
          style={{
            background: `linear-gradient(to right, ${cellColor(0, max)}, ${cellColor(max * 0.35, max)}, ${cellColor(max * 0.7, max)}, ${cellColor(max, max)})`,
          }}
        />
        <span className="tabular-nums">{max.toLocaleString()} {selectedMetric}</span>
      </div>
    </div>
  );
}
