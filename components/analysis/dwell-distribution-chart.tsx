'use client';

import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function DwellDistributionChart({ data }: { data: Record<string, number> }) {
  if (!data || Object.keys(data).length === 0) {
    return (
      <div className="h-96 flex items-center justify-center text-muted-foreground">
        No data available
      </div>
    );
  }

  const chartData = Object.entries(data).map(([range, count]) => ({
    range,
    count,
  }));

  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart data={chartData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="range" />
        <YAxis />
        <Tooltip />
        <Bar dataKey="count" fill="#8884d8" name="Device-POI pairs" />
      </BarChart>
    </ResponsiveContainer>
  );
}
