'use client';

import { useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import { ChevronLeft, ChevronRight, Shuffle } from 'lucide-react';

interface SampleStop {
  ad_id: string;
  date: string;
  ts: string;
  direction: 'before' | 'during' | 'after';
  group_key: string;
  group_label: string;
  dwell_minutes: number;
}

interface RouteTimelineProps {
  data: SampleStop[];
  onRefresh?: () => void;
  refreshing?: boolean;
}

// Same palette as SankeyChart
const GROUP_COLORS: Record<string, string> = {
  retail: '#f472b6',
  food_and_beverage: '#fb923c',
  automotive: '#94a3b8',
  beauty: '#e879f9',
  healthcare: '#f87171',
  finance: '#34d399',
  sports: '#a3e635',
  entertainment: '#a78bfa',
  accommodation: '#60a5fa',
  education: '#fbbf24',
  luxury: '#c084fc',
  home: '#a78bfa',
  electronics: '#22d3ee',
  pets: '#fb7185',
  pharma: '#2dd4bf',
  transport: '#38bdf8',
  logistics: '#818cf8',
  government: '#a1a1aa',
  energy: '#facc15',
  gaming: '#4ade80',
  moviegoers: '#c084fc',
  corporate: '#64748b',
  attractions: '#f59e0b',
  other: '#71717a',
};

const PAGE_SIZE = 25;

export function RouteTimeline({ data, onRefresh, refreshing }: RouteTimelineProps) {
  const [page, setPage] = useState(0);
  const [expandedDevice, setExpandedDevice] = useState<string | null>(null);

  // Group stops by device
  const deviceRoutes = useMemo(() => {
    const map = new Map<string, SampleStop[]>();
    for (const stop of data) {
      const key = `${stop.ad_id}::${stop.date}`;
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(stop);
    }
    // Sort each device's stops by timestamp
    for (const stops of map.values()) {
      stops.sort((a, b) => a.ts.localeCompare(b.ts));
    }
    // Sort devices by number of stops (most interesting first)
    return Array.from(map.entries())
      .sort((a, b) => b[1].length - a[1].length);
  }, [data]);

  // Collect legend entries from visible data
  const legendGroups = useMemo(() => {
    const seen = new Map<string, string>();
    for (const stop of (data || [])) {
      if (!seen.has(stop.group_key)) {
        seen.set(stop.group_key, stop.group_label);
      }
    }
    return Array.from(seen.entries()).sort((a, b) => a[1].localeCompare(b[1]));
  }, [data]);

  if (!data?.length) {
    return (
      <div className="h-40 flex items-center justify-center text-muted-foreground">
        No sample routes available.
      </div>
    );
  }

  const totalPages = Math.ceil(deviceRoutes.length / PAGE_SIZE);
  const pageDevices = deviceRoutes.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <div className="space-y-4">
      {/* Header + controls */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          {deviceRoutes.length.toLocaleString()} device-days sampled
        </p>
        <div className="flex items-center gap-2">
          {onRefresh && (
            <Button variant="outline" size="sm" onClick={onRefresh} disabled={refreshing}>
              <Shuffle className="mr-1.5 h-3.5 w-3.5" />
              New Sample
            </Button>
          )}
          {totalPages > 1 && (
            <div className="flex items-center gap-1">
              <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}>
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="text-xs text-muted-foreground px-1">{page + 1}/{totalPages}</span>
              <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1}>
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          )}
        </div>
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-x-3 gap-y-1">
        {legendGroups.map(([key, label]) => (
          <div key={key} className="flex items-center gap-1">
            <span
              className="inline-block w-3 h-3 rounded-sm"
              style={{ backgroundColor: GROUP_COLORS[key] || GROUP_COLORS.other }}
            />
            <span className="text-[10px] text-muted-foreground">{label}</span>
          </div>
        ))}
        <div className="flex items-center gap-1">
          <span className="inline-block w-3 h-3 rounded-sm bg-primary" />
          <span className="text-[10px] text-muted-foreground">Target POI</span>
        </div>
      </div>

      {/* Timeline strips */}
      <div className="space-y-1">
        {pageDevices.map(([deviceKey, stops]) => {
          const adId = stops[0].ad_id;
          const shortId = adId.slice(0, 8);
          const isExpanded = expandedDevice === deviceKey;
          const beforeStops = stops.filter(s => s.direction === 'before');
          const duringStops = stops.filter(s => s.direction === 'during');
          const afterStops = stops.filter(s => s.direction === 'after');
          const totalStops = stops.length;

          return (
            <div key={deviceKey}>
              {/* Compact strip */}
              <div
                className="flex items-center gap-1 cursor-pointer hover:bg-muted/30 rounded px-1 py-0.5 group"
                onClick={() => setExpandedDevice(isExpanded ? null : deviceKey)}
              >
                {/* Device ID */}
                <span className="text-[10px] font-mono text-muted-foreground w-16 shrink-0 truncate" title={adId}>
                  {shortId}
                </span>

                {/* Timeline bar */}
                <div className="flex items-center gap-[2px] flex-1 h-6">
                  {/* Before stops */}
                  {beforeStops.map((stop, i) => (
                    <div
                      key={`b-${i}`}
                      className="h-5 rounded-sm transition-all"
                      style={{
                        backgroundColor: GROUP_COLORS[stop.group_key] || GROUP_COLORS.other,
                        width: `${Math.max(100 / totalStops, 3)}%`,
                        opacity: 0.8,
                      }}
                      title={`${stop.group_label} (${stop.dwell_minutes}min) — Before`}
                    />
                  ))}

                  {/* Target visit marker */}
                  {duringStops.length > 0 && (
                    <div
                      className="h-6 rounded-sm bg-primary border border-primary shadow-sm shadow-primary/30"
                      style={{ width: `${Math.max(100 / totalStops * duringStops.length, 4)}%` }}
                      title={`Target POI (${duringStops.map(s => `${s.group_label} ${s.dwell_minutes}min`).join(', ')})`}
                    />
                  )}

                  {/* After stops */}
                  {afterStops.map((stop, i) => (
                    <div
                      key={`a-${i}`}
                      className="h-5 rounded-sm transition-all"
                      style={{
                        backgroundColor: GROUP_COLORS[stop.group_key] || GROUP_COLORS.other,
                        width: `${Math.max(100 / totalStops, 3)}%`,
                        opacity: 0.8,
                      }}
                      title={`${stop.group_label} (${stop.dwell_minutes}min) — After`}
                    />
                  ))}
                </div>

                {/* Stop count */}
                <span className="text-[10px] text-muted-foreground w-8 text-right shrink-0">
                  {totalStops}
                </span>
              </div>

              {/* Expanded detail */}
              {isExpanded && (
                <div className="ml-[68px] mr-8 mb-2 rounded-md border border-border/50 bg-muted/20 p-2">
                  <p className="text-[10px] font-mono text-muted-foreground mb-2">{adId} · {stops[0].date}</p>
                  <div className="space-y-1">
                    {stops.map((stop, i) => {
                      const time = stop.ts ? new Date(stop.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '??:??';
                      const dirIcon = stop.direction === 'before' ? '←' : stop.direction === 'after' ? '→' : '●';
                      const dirLabel = stop.direction === 'before' ? 'Before' : stop.direction === 'after' ? 'After' : 'At Target';
                      return (
                        <div key={i} className="flex items-center gap-2 text-xs">
                          <span className="font-mono text-muted-foreground w-12">{time}</span>
                          <span className="w-4 text-center">{dirIcon}</span>
                          <span
                            className="w-2.5 h-2.5 rounded-sm shrink-0"
                            style={{ backgroundColor: stop.direction === 'during' ? 'hsl(var(--primary))' : (GROUP_COLORS[stop.group_key] || GROUP_COLORS.other) }}
                          />
                          <span className={stop.direction === 'during' ? 'font-semibold text-primary' : ''}>
                            {stop.direction === 'during' ? 'Target POI' : stop.group_label}
                          </span>
                          {stop.dwell_minutes > 0 && (
                            <span className="text-muted-foreground">
                              ({stop.dwell_minutes >= 60 ? `${(stop.dwell_minutes / 60).toFixed(1)}h` : `${Math.round(stop.dwell_minutes)}min`})
                            </span>
                          )}
                          <span className="text-muted-foreground/50 ml-auto">{dirLabel}</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
