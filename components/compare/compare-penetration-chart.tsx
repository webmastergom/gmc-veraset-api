'use client';

import { useMemo, useState } from 'react';
import {
  BarChart,
  Bar,
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
}

type Metric = 'sample' | 'match';

const COLOR_BY_SIDE: Record<'A' | 'B', string> = {
  A: '#3b82f6',
  B: '#f97316',
};

export default function ComparePenetrationChart({ pois, side, totalForSide, overlap, maxBars = 20 }: Props) {
  const [metric, setMetric] = useState<Metric>('sample');

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
      </div>
      <div style={{ height }} className="p-2">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{ top: 8, right: 40, left: 8, bottom: 8 }}>
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
            <Bar dataKey="value" fill={color} radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
