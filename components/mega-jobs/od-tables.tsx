'use client';

import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';

interface ODEntry {
  zipCode: string;
  city: string;
  country: string;
  deviceDays: number;
}

interface ODTablesProps {
  origins: ODEntry[];
  destinations: ODEntry[];
  maxRows?: number;
}

function ODTable({ data, maxRows = 30 }: { data: ODEntry[]; maxRows?: number }) {
  const total = data.reduce((s, d) => s + d.deviceDays, 0);

  return (
    <div className="max-h-96 overflow-y-auto">
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-background">
          <tr className="border-b">
            <th className="text-left py-2">Zip Code</th>
            <th className="text-left py-2">City</th>
            <th className="text-left py-2">Country</th>
            <th className="text-right py-2">Device-Days</th>
            <th className="text-right py-2">Share</th>
          </tr>
        </thead>
        <tbody>
          {data.slice(0, maxRows).map((row, i) => (
            <tr key={i} className="border-b border-border/50">
              <td className="py-1.5 font-medium">{row.zipCode}</td>
              <td className="py-1.5">{row.city}</td>
              <td className="py-1.5">{row.country}</td>
              <td className="py-1.5 text-right">{row.deviceDays.toLocaleString()}</td>
              <td className="py-1.5 text-right text-muted-foreground">
                {total > 0 ? `${((row.deviceDays / total) * 100).toFixed(1)}%` : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > maxRows && (
        <p className="text-sm text-muted-foreground text-center py-2">
          Showing top {maxRows} of {data.length}. Download CSV for full data.
        </p>
      )}
    </div>
  );
}

export function ODTables({ origins, destinations, maxRows = 30 }: ODTablesProps) {
  if (!origins?.length && !destinations?.length) {
    return (
      <div className="h-40 flex items-center justify-center text-muted-foreground">
        No origin/destination data available.
      </div>
    );
  }

  return (
    <Tabs defaultValue="origins">
      <TabsList className="grid w-full grid-cols-2 max-w-xs">
        <TabsTrigger value="origins">
          Origins ({origins.length})
        </TabsTrigger>
        <TabsTrigger value="destinations">
          Destinations ({destinations.length})
        </TabsTrigger>
      </TabsList>
      <TabsContent value="origins" className="mt-3">
        <ODTable data={origins} maxRows={maxRows} />
      </TabsContent>
      <TabsContent value="destinations" className="mt-3">
        <ODTable data={destinations} maxRows={maxRows} />
      </TabsContent>
    </Tabs>
  );
}
