'use client';

import { useState } from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';

interface DailyDataPoint {
  date: string;
  pings: number;
  devices: number;
}

interface MegaDailyChartProps {
  data: DailyDataPoint[];
}

export function MegaDailyChart({ data }: MegaDailyChartProps) {
  const [showPings, setShowPings] = useState(true);
  const [showDevices, setShowDevices] = useState(true);

  if (!data?.length) {
    return (
      <div className="h-80 flex items-center justify-center text-muted-foreground">
        No daily data available.
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center gap-6 mb-4">
        <div className="flex items-center gap-2">
          <Switch checked={showPings} onCheckedChange={setShowPings} id="pings-toggle" />
          <Label htmlFor="pings-toggle" className="text-sm cursor-pointer">
            <span className="inline-block w-3 h-3 rounded bg-blue-500 mr-1" />
            Pings
          </Label>
        </div>
        <div className="flex items-center gap-2">
          <Switch checked={showDevices} onCheckedChange={setShowDevices} id="devices-toggle" />
          <Label htmlFor="devices-toggle" className="text-sm cursor-pointer">
            <span className="inline-block w-3 h-3 rounded bg-cyan-500 mr-1" />
            Devices
          </Label>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={350}>
        <AreaChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 11, fill: 'hsl(var(--foreground))' }}
            angle={-45}
            textAnchor="end"
            height={60}
          />
          <YAxis tick={{ fontSize: 12, fill: 'hsl(var(--foreground))' }} tickFormatter={(v) => v.toLocaleString()} />
          <Tooltip
            contentStyle={{ backgroundColor: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', color: 'hsl(var(--foreground))' }}
            labelStyle={{ color: 'hsl(var(--foreground))' }}
            itemStyle={{ color: 'hsl(var(--foreground))' }}
            formatter={(value: number) => [value.toLocaleString(), '']}
          />
          <Legend wrapperStyle={{ color: 'hsl(var(--foreground))' }} />
          {showPings && (
            <Area
              type="monotone"
              dataKey="pings"
              name="Pings"
              stroke="#3b82f6"
              fill="#3b82f6"
              fillOpacity={0.15}
              strokeWidth={2}
            />
          )}
          {showDevices && (
            <Area
              type="monotone"
              dataKey="devices"
              name="Devices"
              stroke="#06b6d4"
              fill="#06b6d4"
              fillOpacity={0.15}
              strokeWidth={2}
            />
          )}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
