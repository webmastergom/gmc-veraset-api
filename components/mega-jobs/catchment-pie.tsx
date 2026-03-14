'use client';

import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

interface ZipEntry {
  zipCode: string;
  city: string;
  deviceDays: number;
}

interface CatchmentPieProps {
  data: ZipEntry[];
  maxSlices?: number;
}

const COLORS = [
  '#3b82f6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444',
  '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#6366f1',
  '#84cc16', '#d946ef', '#0ea5e9', '#22c55e', '#e11d48',
];

export function CatchmentPie({ data, maxSlices = 12 }: CatchmentPieProps) {
  if (!data?.length) {
    return (
      <div className="h-80 flex items-center justify-center text-muted-foreground">
        No catchment data available.
      </div>
    );
  }

  // Take top N, group rest as "Other"
  const sorted = [...data].sort((a, b) => b.deviceDays - a.deviceDays);
  const top = sorted.slice(0, maxSlices);
  const rest = sorted.slice(maxSlices);
  const otherTotal = rest.reduce((sum, z) => sum + z.deviceDays, 0);

  const chartData = top.map((z) => ({
    name: z.zipCode === 'UNKNOWN' ? 'Unknown' : `${z.zipCode} (${z.city})`,
    value: z.deviceDays,
  }));

  if (otherTotal > 0) {
    chartData.push({ name: `Other (${rest.length})`, value: otherTotal });
  }

  const total = chartData.reduce((s, d) => s + d.value, 0);

  return (
    <ResponsiveContainer width="100%" height={400}>
      <PieChart>
        <Pie
          data={chartData}
          cx="50%"
          cy="50%"
          innerRadius={80}
          outerRadius={150}
          dataKey="value"
          label={({ name, percent }) =>
            percent > 0.03 ? `${name.split(' (')[0]} ${(percent * 100).toFixed(1)}%` : ''
          }
          labelLine={false}
        >
          {chartData.map((_, i) => (
            <Cell key={i} fill={COLORS[i % COLORS.length]} />
          ))}
        </Pie>
        <Tooltip
          contentStyle={{ backgroundColor: 'hsl(var(--card))', border: '1px solid hsl(var(--border))' }}
          formatter={(value: number) => [
            `${value.toLocaleString()} device-days (${((value / total) * 100).toFixed(1)}%)`,
            '',
          ]}
        />
        <Legend />
      </PieChart>
    </ResponsiveContainer>
  );
}
