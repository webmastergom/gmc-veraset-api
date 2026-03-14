'use client';

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';

interface HourlyEntry {
  hour: number;
  pings?: number;
  devices?: number;
  deviceDays?: number;
}

interface HourlyChartProps {
  data: HourlyEntry[];
  /** Which data key to chart */
  dataKey?: 'pings' | 'devices' | 'deviceDays';
  label?: string;
  color?: string;
}

export function HourlyChart({
  data,
  dataKey = 'devices',
  label = 'Devices',
  color = '#3b82f6',
}: HourlyChartProps) {
  if (!data?.length) {
    return (
      <div className="h-60 flex items-center justify-center text-muted-foreground">
        No hourly data available.
      </div>
    );
  }

  // Ensure all 24 hours are present
  const fullData = Array.from({ length: 24 }, (_, h) => {
    const existing = data.find((d) => d.hour === h);
    return {
      hour: `${h.toString().padStart(2, '0')}:00`,
      [dataKey]: existing ? (existing as any)[dataKey] || 0 : 0,
    };
  });

  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={fullData} margin={{ top: 8, right: 12, left: 0, bottom: 4 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
        <XAxis dataKey="hour" tick={{ fontSize: 11, fill: 'hsl(var(--foreground))' }} />
        <YAxis tick={{ fontSize: 12, fill: 'hsl(var(--foreground))' }} tickFormatter={(v) => v.toLocaleString()} />
        <Tooltip
          contentStyle={{ backgroundColor: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', color: 'hsl(var(--foreground))' }}
          labelStyle={{ color: 'hsl(var(--foreground))' }}
          itemStyle={{ color: 'hsl(var(--foreground))' }}
          formatter={(value: number) => [value.toLocaleString(), label]}
        />
        <Bar dataKey={dataKey} name={label} fill={color} radius={[2, 2, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
