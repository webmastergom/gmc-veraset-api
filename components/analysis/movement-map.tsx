'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { MapPin, Loader2, RefreshCw, Filter } from 'lucide-react';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';
import dynamic from 'next/dynamic';

export interface MovementPoint {
  lat: number;
  lng: number;
  utc: string;
}

export interface DeviceMovement {
  adId: string;
  points: MovementPoint[];
}

const COLORS = [
  '#3b82f6', '#ef4444', '#22c55e', '#eab308', '#8b5cf6',
  '#ec4899', '#06b6d4', '#f97316', '#14b8a6', '#6366f1',
  '#84cc16', '#f43f5e', '#0ea5e9', '#a855f7', '#d946ef',
];

function getDeviceColor(index: number): string {
  return COLORS[index % COLORS.length];
}

const MapContent = dynamic(
  () => import('./movement-map-inner').then((m) => m.MovementMapInner),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-96 items-center justify-center bg-muted text-muted-foreground">
        <Loader2 className="h-8 w-8 animate-spin" />
      </div>
    ),
  }
);

interface MovementMapProps {
  datasetName: string;
  dateFrom: string;
  dateTo: string;
}

export function MovementMap({ datasetName, dateFrom, dateTo }: MovementMapProps) {
  const [data, setData] = useState<{ devices: DeviceMovement[]; dateRange: { from: string; to: string } } | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [visibleDevices, setVisibleDevices] = useState<Set<string>>(new Set());
  const [filterOpen, setFilterOpen] = useState(true);
  const [showPois, setShowPois] = useState(true);
  const [poiPositions, setPoiPositions] = useState<Array<{ poiId: string; lat: number; lng: number; name?: string }>>([]);

  const fetchMovements = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(
        `/api/datasets/${encodeURIComponent(datasetName)}/movements?dateFrom=${dateFrom}&dateTo=${dateTo}&sample=50`,
        { credentials: 'include' }
      );
      const json = await res.json();
      if (!res.ok) throw new Error(json.details || json.error || 'Failed to fetch');
      setData(json);
      setVisibleDevices(new Set(json.devices.map((d: DeviceMovement) => d.adId)));
    } catch (e: any) {
      setError(e.message || 'Error loading movements');
    } finally {
      setLoading(false);
    }
  }, [datasetName, dateFrom, dateTo]);

  useEffect(() => {
    fetchMovements();
  }, [fetchMovements]);

  const fetchPoiPositions = useCallback(async () => {
    try {
      const res = await fetch(`/api/datasets/${encodeURIComponent(datasetName)}/pois/positions`, {
        credentials: 'include',
      });
      const json = await res.json();
      if (res.ok && json.positions?.length) {
        setPoiPositions(json.positions);
      }
    } catch {
      // ignore
    }
  }, [datasetName]);

  useEffect(() => {
    fetchPoiPositions();
  }, [fetchPoiPositions]);

  useEffect(() => {
    if (poiPositions.length > 0) setShowPois(true);
  }, [poiPositions.length]);

  const toggleDevice = (adId: string) => {
    setVisibleDevices((prev) => {
      const next = new Set(prev);
      if (next.has(adId)) next.delete(adId);
      else next.add(adId);
      return next;
    });
  };

  const showAll = () => {
    if (data) setVisibleDevices(new Set(data.devices.map((d) => d.adId)));
  };

  const hideAll = () => setVisibleDevices(new Set());

  const filteredDevices = useMemo(() => {
    if (!data) return [];
    return data.devices.filter((d) => visibleDevices.has(d.adId));
  }, [data, visibleDevices]);

  const deviceIndices = useMemo(() => {
    if (!data) return [];
    const idxMap = new Map(data.devices.map((d, i) => [d.adId, i]));
    return filteredDevices.map((d) => idxMap.get(d.adId) ?? 0);
  }, [data, filteredDevices]);

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <div>
          <CardTitle className="flex items-center gap-2">
            <MapPin className="h-5 w-5" />
            Movement map
          </CardTitle>
          <CardDescription>
            50 random devices: movement trajectories during the analysis period. Filter by device.
          </CardDescription>
        </div>
        <div className="flex items-center gap-4">
          {poiPositions.length > 0 ? (
            <div className="flex items-center gap-2">
              <Switch
                id="show-pois"
                checked={showPois}
                onCheckedChange={setShowPois}
              />
              <Label htmlFor="show-pois" className="text-sm cursor-pointer whitespace-nowrap">
                POIs on map ({poiPositions.length})
              </Label>
            </div>
          ) : (
            <span className="text-xs text-muted-foreground" title="POI positions come from job config. External or place_key jobs may not have coordinates.">
              No POI positions
            </span>
          )}
          <Button variant="outline" size="sm" onClick={fetchMovements} disabled={loading}>
            {loading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            <span className="ml-2">Refresh</span>
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        {error && (
          <div className="mb-4 rounded-md bg-destructive/10 p-3 text-sm text-destructive">
            {error}
          </div>
        )}

        {loading && !data ? (
          <div className="flex h-96 items-center justify-center bg-muted text-muted-foreground">
            <Loader2 className="h-8 w-8 animate-spin" />
          </div>
        ) : data && data.devices.length ? (
          <div className="flex gap-4">
            <div className="w-64 shrink-0">
              <Button
                variant="ghost"
                size="sm"
                className="mb-2 w-full justify-start"
                onClick={() => setFilterOpen(!filterOpen)}
              >
                <Filter className="mr-2 h-4 w-4" />
                {filterOpen ? 'Hide' : 'Show'} device filter
              </Button>
              {filterOpen && (
                <div className="space-y-1 max-h-64 overflow-y-auto rounded border border-border p-2">
                  <div className="flex flex-wrap gap-2 pb-2">
                    <Button variant="outline" size="sm" onClick={showAll} className="text-xs">
                      All
                    </Button>
                    <Button variant="outline" size="sm" onClick={hideAll} className="text-xs">
                      None
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={fetchMovements}
                      disabled={loading}
                      className="text-xs"
                      title="Obtener otros 50 dispositivos aleatorios"
                    >
                      {loading ? (
                        <Loader2 className="h-3 w-3 animate-spin" />
                      ) : (
                        <RefreshCw className="h-3 w-3" />
                      )}
                      <span className="ml-1">Refresh</span>
                    </Button>
                  </div>
                  {data.devices.map((d, i) => (
                    <label
                      key={d.adId}
                      className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 hover:bg-muted/50"
                    >
                      <input
                        type="checkbox"
                        checked={visibleDevices.has(d.adId)}
                        onChange={() => toggleDevice(d.adId)}
                        className="rounded"
                      />
                      <span
                        className="h-3 w-3 shrink-0 rounded-full"
                        style={{ backgroundColor: getDeviceColor(i) }}
                      />
                      <span className="truncate font-mono text-xs" title={d.adId}>
                        {d.adId.slice(0, 8)}…
                      </span>
                      <span className="text-xs text-muted-foreground">({d.points.length})</span>
                    </label>
                  ))}
                </div>
              )}
            </div>
            <div className="flex-1">
              <div className="min-h-96 rounded-md overflow-hidden border border-border">
                <MapContent
                  devices={filteredDevices}
                  deviceIndices={deviceIndices}
                  getColor={getDeviceColor}
                  pois={showPois ? poiPositions : []}
                />
              </div>
            </div>
          </div>
        ) : (
          <div className="flex h-96 items-center justify-center rounded-md bg-muted text-muted-foreground">
            {data && data.devices.length === 0
              ? 'No devices with movement data in this period.'
              : 'Load movements to see the map.'}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
