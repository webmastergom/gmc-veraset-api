'use client';

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

export interface DailyDataPoint {
  date: string;
  pings: number;
  devices: number;
}

interface DailyChartProps {
  data: DailyDataPoint[];
}

export function DailyChart({ data }: DailyChartProps) {
  if (!data?.length) {
    return (
      <div className="h-80 flex items-center justify-center text-muted-foreground">
        No daily data.
      </div>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart data={data} margin={{ top: 12, right: 12, left: 0, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 11 }}
          angle={-45}
          textAnchor="end"
          height={60}
        />
        <YAxis tick={{ fontSize: 12 }} tickFormatter={(v) => v.toLocaleString()} />
        <Tooltip
          contentStyle={{ backgroundColor: 'hsl(var(--card))', border: '1px solid hsl(var(--border))' }}
          labelStyle={{ color: 'hsl(var(--foreground))' }}
          formatter={(value: number) => [value.toLocaleString(), '']}
          labelFormatter={(label) => `Day: ${label}`}
        />
        <Legend />
        <Bar
          dataKey="pings"
          name="Pings"
          fill="#3b82f6"
          radius={[2, 2, 0, 0]}
        />
        <Bar
          dataKey="devices"
          name="Unique devices"
          fill="#06b6d4"
          radius={[2, 2, 0, 0]}
        />
      </BarChart>
    </ResponsiveContainer>
  );
}
