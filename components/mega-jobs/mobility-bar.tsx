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

interface MobilityEntry {
  category: string;
  deviceDays: number;
  hits: number;
}

interface MobilityBarProps {
  data: MobilityEntry[];
  maxBars?: number;
}

export function MobilityBar({ data, maxBars = 20 }: MobilityBarProps) {
  if (!data?.length) {
    return (
      <div className="h-60 flex items-center justify-center text-muted-foreground">
        No mobility data available.
      </div>
    );
  }

  const chartData = data.slice(0, maxBars).map((d) => ({
    category: d.category.length > 25 ? d.category.slice(0, 22) + '...' : d.category,
    fullCategory: d.category,
    deviceDays: d.deviceDays,
    hits: d.hits,
  }));

  return (
    <ResponsiveContainer width="100%" height={Math.max(300, chartData.length * 28)}>
      <BarChart
        data={chartData}
        layout="vertical"
        margin={{ top: 8, right: 24, left: 120, bottom: 4 }}
      >
        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" horizontal={false} />
        <XAxis type="number" tick={{ fontSize: 12, fill: 'hsl(var(--foreground))' }} tickFormatter={(v) => v.toLocaleString()} />
        <YAxis
          type="category"
          dataKey="category"
          tick={{ fontSize: 11, fill: 'hsl(var(--foreground))' }}
          width={110}
        />
        <Tooltip
          contentStyle={{ backgroundColor: 'hsl(var(--card))', border: '1px solid hsl(var(--border))', color: 'hsl(var(--foreground))' }}
          labelStyle={{ color: 'hsl(var(--foreground))' }}
          itemStyle={{ color: 'hsl(var(--foreground))' }}
          formatter={(value: number) => [value.toLocaleString(), 'Device-Days']}
          labelFormatter={(_: any, payload: any) => payload?.[0]?.payload?.fullCategory || ''}
        />
        <Bar dataKey="deviceDays" name="Device-Days" fill="#8b5cf6" radius={[0, 4, 4, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}
