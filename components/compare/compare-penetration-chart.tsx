'use client';

import { useMemo, useState } from 'react';
import {
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts';

export interface ComparePoi {
  side: 'A' | 'B';
  poiId: string;
  name?: string;
  lat?: number;
  lng?: number;
  overlapDevices: number;
}

interface Props {
  pois: ComparePoi[];           // already filtered to ONE side
  side: 'A' | 'B';
  totalForSide: number;         // totalA or totalB — denominator for "% of sample"
  overlap: number;              // overlap count — denominator for "% of match"
  maxBars?: number;             // top-N bars shown (default 20)
  /**
   * Selection keys of the form `${side}-${poiId}` shared with the map. When
   * present, clicking a bar toggles its key. Selected bars stand out and
   * unselected bars dim (only when there's at least one selection).
   */
  selectedPoiKeys?: Set<string>;
  onTogglePoi?: (key: string) => void;
  onClearSelection?: () => void;
}

type Metric = 'sample' | 'match';

const COLOR_BY_SIDE: Record<'A' | 'B', string> = {
  A: '#3b82f6',
  B: '#f97316',
};
// Selected bars/markers use the same accent color across sides so the eye
// can pick them out at a glance regardless of which tab they came from.
const SELECTED_COLOR = '#facc15'; // yellow-400

export default function ComparePenetrationChart({ pois, side, totalForSide, overlap, maxBars = 20, selectedPoiKeys, onTogglePoi, onClearSelection }: Props) {
  const [metric, setMetric] = useState<Metric>('sample');
  const hasSelection = !!selectedPoiKeys && selectedPoiKeys.size > 0;

  const data = useMemo(() => {
    const denom = metric === 'sample' ? totalForSide : overlap;
    if (!denom || pois.length === 0) return [];
    const rows = pois
      .map(p => {
        const pctSample = totalForSide > 0 ? (p.overlapDevices / totalForSide) * 100 : 0;
        const pctMatch = overlap > 0 ? (p.overlapDevices / overlap) * 100 : 0;
        const label = p.name || p.poiId;
        return {
          label: label.length > 40 ? label.slice(0, 37) + '…' : label,
          fullLabel: label,
          poiId: p.poiId,
          overlapDevices: p.overlapDevices,
          pctSample: +pctSample.toFixed(2),
          pctMatch: +pctMatch.toFixed(2),
          value: +((p.overlapDevices / denom) * 100).toFixed(2),
        };
      })
      .sort((a, b) => b.value - a.value)
      .slice(0, maxBars);
    return rows;
  }, [pois, totalForSide, overlap, metric, maxBars]);

  if (pois.length === 0) {
    return <p className="p-4 text-xs italic text-muted-foreground">No POIs with overlap matches for this side.</p>;
  }

  const color = COLOR_BY_SIDE[side];
  // Dynamic height based on bar count
  const height = Math.max(240, data.length * 28 + 60);

  return (
    <div>
      <div className="flex items-center gap-2 px-3 py-2 border-b">
        <span className="text-xs uppercase tracking-wider text-muted-foreground">Metric:</span>
        <button
          onClick={() => setMetric('sample')}
          className={`px-2 py-1 text-xs rounded ${metric === 'sample' ? 'bg-primary text-primary-foreground' : 'bg-muted hover:bg-muted/70'}`}
        >
          % of sample (of {totalForSide.toLocaleString()})
        </button>
        <button
          onClick={() => setMetric('match')}
          className={`px-2 py-1 text-xs rounded ${metric === 'match' ? 'bg-primary text-primary-foreground' : 'bg-muted hover:bg-muted/70'}`}
        >
          % of match (of {overlap.toLocaleString()})
        </button>
        {data.length >= maxBars && pois.length > maxBars && (
          <span className="text-xs text-muted-foreground ml-auto">Top {maxBars} of {pois.length}</span>
        )}
        {hasSelection && onClearSelection && (
          <button
            onClick={onClearSelection}
            className={`text-xs px-2 py-1 rounded bg-yellow-400/20 text-yellow-300 hover:bg-yellow-400/30 ${data.length >= maxBars && pois.length > maxBars ? 'ml-2' : 'ml-auto'}`}
            title="Clear selection (works across both tabs)"
          >
            Clear selection ({selectedPoiKeys!.size})
          </button>
        )}
      </div>
      <div style={{ height }} className="p-2">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={data}
            layout="vertical"
            margin={{ top: 8, right: 40, left: 8, bottom: 8 }}
            onClick={(e: any) => {
              const payload = e?.activePayload?.[0]?.payload;
              if (payload?.poiId && onTogglePoi) {
                onTogglePoi(`${side}-${payload.poiId}`);
              }
            }}
          >
            <CartesianGrid strokeDasharray="3 3" horizontal={false} opacity={0.3} />
            <XAxis
              type="number"
              tick={{ fontSize: 11 }}
              tickFormatter={(v) => `${v}%`}
            />
            <YAxis
              type="category"
              dataKey="label"
              tick={{ fontSize: 11 }}
              width={220}
              interval={0}
            />
            <Tooltip
              cursor={{ fill: 'rgba(128,128,128,0.1)' }}
              contentStyle={{ fontSize: 12, background: 'hsl(var(--popover))', border: '1px solid hsl(var(--border))', borderRadius: 6 }}
              formatter={(_v: any, _name: any, props: any) => {
                const p = props.payload;
                return [
                  <div key="details" className="text-xs">
                    <div>Devices: <b>{p.overlapDevices.toLocaleString()}</b></div>
                    <div>% of sample: <b>{p.pctSample}%</b></div>
                    <div>% of match: <b>{p.pctMatch}%</b></div>
                  </div>,
                  '',
                ];
              }}
              labelFormatter={(_label: any, payload: any) => {
                const p = payload?.[0]?.payload;
                return p ? `${p.fullLabel} (${p.poiId})` : '';
              }}
            />
            <Bar dataKey="value" radius={[0, 4, 4, 0]} cursor="pointer">
              {data.map((entry) => {
                const key = `${side}-${entry.poiId}`;
                const isSel = selectedPoiKeys?.has(key) ?? false;
                const fill = isSel ? SELECTED_COLOR : color;
                // Dim other bars only when there is at least one selection
                const opacity = hasSelection && !isSel ? 0.35 : 1;
                return <Cell key={key} fill={fill} fillOpacity={opacity} />;
              })}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
