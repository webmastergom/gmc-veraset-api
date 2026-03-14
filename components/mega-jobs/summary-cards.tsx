'use client';

import { Card, CardContent } from '@/components/ui/card';
import { Activity, Users, Calendar, MapPin, Layers } from 'lucide-react';

interface SummaryCardsProps {
  totalPings: number;
  uniqueDevices: number;
  dateRange: { from: string; to: string };
  totalPois: number;
  subJobCount: number;
}

export function SummaryCards({
  totalPings,
  uniqueDevices,
  dateRange,
  totalPois,
  subJobCount,
}: SummaryCardsProps) {
  const stats = [
    { label: 'Total Pings', value: totalPings.toLocaleString(), icon: <Activity className="h-4 w-4 text-blue-400" /> },
    { label: 'Unique Devices', value: uniqueDevices.toLocaleString(), icon: <Users className="h-4 w-4 text-cyan-400" /> },
    { label: 'Date Range', value: `${dateRange.from} — ${dateRange.to}`, icon: <Calendar className="h-4 w-4 text-green-400" /> },
    { label: 'POIs', value: totalPois.toLocaleString(), icon: <MapPin className="h-4 w-4 text-orange-400" /> },
    { label: 'Sub-jobs', value: subJobCount.toString(), icon: <Layers className="h-4 w-4 text-purple-400" /> },
  ];

  return (
    <div className="grid grid-cols-5 gap-4">
      {stats.map((s) => (
        <Card key={s.label}>
          <CardContent className="py-4 text-center">
            <div className="flex items-center justify-center gap-1.5 mb-1">
              {s.icon}
              <p className="text-xs text-muted-foreground uppercase tracking-wide">{s.label}</p>
            </div>
            <p className="text-xl font-bold">{s.value}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
