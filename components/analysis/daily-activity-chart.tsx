'use client';

import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

interface DailyData {
  date: string;
  pings: number;
  devices: number;
}

// Format date to "dd.Mes" format (e.g., "01.Ene", "15.Feb")
function formatDate(dateStr: string): string {
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) {
      return dateStr; // Return original if invalid
    }
    
    const day = String(date.getDate()).padStart(2, '0');
    const monthNames = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];
    const month = monthNames[date.getMonth()];
    
    return `${day}.${month}`;
  } catch {
    return dateStr;
  }
}

// Custom tick formatter for X-axis
function formatTick(dateStr: string): string {
  return formatDate(dateStr);
}

// Custom tooltip formatter
function formatTooltipLabel(label: string): string {
  try {
    const date = new Date(label);
    if (isNaN(date.getTime())) {
      return label;
    }
    const day = String(date.getDate()).padStart(2, '0');
    const monthNames = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 
                        'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];
    const month = monthNames[date.getMonth()];
    const year = date.getFullYear();
    return `${day} ${month} ${year}`;
  } catch {
    return label;
  }
}

export default function DailyActivityChart({ data }: { data: DailyData[] }) {
  if (!data || data.length === 0) {
    return (
      <div className="h-96 flex items-center justify-center text-muted-foreground">
        No data available
      </div>
    );
  }

  // Format data for display and ensure dates are unique
  const formattedData = data
    .map(item => ({
      ...item,
      dateFormatted: formatDate(item.date),
    }))
    .sort((a, b) => a.date.localeCompare(b.date)); // Ensure sorted

  // Calculate interval for x-axis labels based on data length
  const labelInterval = formattedData.length > 30 ? Math.ceil(formattedData.length / 30) : 0;

  return (
    <ResponsiveContainer width="100%" height={400}>
      <LineChart data={formattedData}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis 
          dataKey="dateFormatted" 
          tick={{ fontSize: 11 }}
          angle={-45}
          textAnchor="end"
          height={80}
          interval={labelInterval}
        />
        <YAxis yAxisId="left" />
        <YAxis yAxisId="right" orientation="right" />
        <Tooltip 
          labelFormatter={formatTooltipLabel}
          formatter={(value: number, name: string) => [
            value.toLocaleString(),
            name === 'pings' ? 'Pings' : 'Devices'
          ]}
        />
        <Legend />
        <Line 
          yAxisId="left" 
          type="monotone" 
          dataKey="pings" 
          stroke="#8884d8" 
          name="Pings"
          dot={{ r: 3 }}
          strokeWidth={2}
        />
        <Line 
          yAxisId="right" 
          type="monotone" 
          dataKey="devices" 
          stroke="#82ca9d" 
          name="Devices"
          dot={{ r: 3 }}
          strokeWidth={2}
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
