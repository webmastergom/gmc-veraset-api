'use client';

import { useState } from 'react';
import dynamic from 'next/dynamic';
import { Button } from '@/components/ui/button';
import { Loader2, Play, Download, Target, MapPin } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

// Reuse the existing compare map for spatial display
const CompareMap = dynamic(() => import('@/components/compare/compare-map-inner'), { ssr: false });

interface ReachByPoi {
  poiId: string;
  poiName: string;
  lat: number;
  lng: number;
  potentialVisitors: number;
  avgPings: number;
  avgDwellMinutes: number;
}

interface ReachDirectionResult {
  source: { datasetName: string; visitorCount: number };
  target: { datasetName: string; poiCount: number };
  totalPotentialVisitors: number;
  byPoi: ReachByPoi[];
  downloadKey: string;
}

interface ReachResult {
  config: { maxDistanceMeters: number; minPings: number; minDwellMinutes: number };
  aToB?: ReachDirectionResult;
  bToA?: ReachDirectionResult;
}

interface SourceFilters {
  minDwell?: number;
  maxDwell?: number;
  hourFrom?: number;
  hourTo?: number;
  minVisits?: number;
}

interface Props {
  datasetA: string;
  datasetB: string;
  dsALabel: string;
  dsBLabel: string;
  /** Filters from the A side selector (Dwell/Hour/MinVisits). Applied to A→B source visitors. */
  filtersA?: SourceFilters;
  /** Filters from the B side selector. Applied to B→A source visitors. */
  filtersB?: SourceFilters;
}

const DIRECTION_OPTIONS: { value: 'both' | 'aToB' | 'bToA'; label: string }[] = [
  { value: 'both',  label: 'Both directions (A↔B)' },
  { value: 'aToB',  label: 'Only A → B (A-visitors near B-POIs)' },
  { value: 'bToA',  label: 'Only B → A (B-visitors near A-POIs)' },
];

function hasFilters(f?: SourceFilters): boolean {
  if (!f) return false;
  return (
    (f.minDwell ?? 0) > 0 ||
    (f.maxDwell ?? 0) > 0 ||
    (f.hourFrom ?? 0) > 0 ||
    (f.hourTo ?? 23) < 23 ||
    (f.minVisits ?? 1) > 1
  );
}

function summarizeFilters(f?: SourceFilters): string {
  if (!f) return 'none';
  const parts: string[] = [];
  if ((f.minDwell ?? 0) > 0 || (f.maxDwell ?? 0) > 0) {
    parts.push(`dwell ${f.minDwell || 0}-${f.maxDwell || '∞'}m`);
  }
  if ((f.hourFrom ?? 0) > 0 || (f.hourTo ?? 23) < 23) {
    parts.push(`hours ${String(f.hourFrom ?? 0).padStart(2, '0')}-${String(f.hourTo ?? 23).padStart(2, '0')}`);
  }
  if ((f.minVisits ?? 1) > 1) parts.push(`min ${f.minVisits}+ days`);
  return parts.length ? parts.join(' · ') : 'none';
}

export default function CompareReach({ datasetA, datasetB, dsALabel, dsBLabel, filtersA, filtersB }: Props) {
  const [maxDistance, setMaxDistance] = useState<number>(200);
  const [minPings, setMinPings] = useState<number>(3);
  const [minDwell, setMinDwell] = useState<number>(5);
  const [directions, setDirections] = useState<'both' | 'aToB' | 'bToA'>('both');
  const [running, setRunning] = useState(false);
  const [progressMsg, setProgressMsg] = useState<string>('');
  const [result, setResult] = useState<ReachResult | null>(null);
  // Reach state ID — needed to drive the catchment download per direction.
  const [reachStateId, setReachStateId] = useState<string>('');
  // Per-direction catchment status. Keyed by direction.
  const [catchmentStatus, setCatchmentStatus] = useState<Record<'aToB' | 'bToA', {
    loading?: boolean;
    progress?: string;
    downloadKey?: string;
    rowCount?: number;
    nativeZipCount?: number;
    geocodedCount?: number;
    error?: string;
  }>>({ aToB: {}, bToA: {} });
  const { toast } = useToast();

  const safePoll = async (url: string, init?: RequestInit) => {
    const r = await fetch(url, init);
    if (r.status === 504) return { phase: 'polling', progress: { message: 'Server processing (retrying…)' } };
    let data: any;
    try { data = await r.json(); } catch { return { phase: 'polling', progress: { message: 'Server processing (retrying…)' } }; }
    if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
    return data;
  };

  const handleRun = async () => {
    if (!datasetA || !datasetB) {
      toast({ title: 'Pick both datasets first', variant: 'destructive' });
      return;
    }
    setRunning(true);
    setResult(null);
    setProgressMsg('Starting…');
    try {
      const dirs = directions === 'both' ? ['aToB', 'bToA'] : [directions];
      let data = await safePoll('/api/compare/reach-poll', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          datasetA, datasetB,
          maxDistanceMeters: maxDistance,
          minPings,
          minDwellMinutes: minDwell,
          directions: dirs,
          filtersA: filtersA || {},
          filtersB: filtersB || {},
        }),
      });
      const stateId = data.stateId;

      while (data.phase !== 'done' && data.phase !== 'error') {
        setProgressMsg(data.progress?.message || data.phase);
        await new Promise((r) => setTimeout(r, 4000));
        data = await safePoll('/api/compare/reach-poll', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ stateId }),
        });
      }
      if (data.phase === 'error') throw new Error(data.error || 'Reach analysis failed');
      setResult(data.result);
      setReachStateId(stateId);
      setCatchmentStatus({ aToB: {}, bToA: {} });
      toast({ title: 'Reach analysis complete' });
    } catch (e: any) {
      toast({ title: 'Reach failed', description: e.message, variant: 'destructive' });
    } finally {
      setRunning(false);
      setProgressMsg('');
    }
  };

  const downloadMaids = (downloadKey: string, filename: string) => {
    if (!downloadKey) return;
    // downloadKey looks like `athena-results/<queryId>.csv` — extract queryId
    const match = downloadKey.match(/athena-results\/([a-f0-9-]+)\.csv/);
    if (!match) return;
    const link = document.createElement('a');
    link.href = `/api/compare/download?queryId=${match[1]}`;
    link.download = filename;
    link.click();
  };

  /**
   * Trigger the catchment-ZIPs build for a direction. Multi-phase polling
   * mirrors the main reach analysis. When done, download the CSV via the
   * `?key=` form of the compare/download endpoint.
   */
  const buildCatchmentZips = async (direction: 'aToB' | 'bToA') => {
    if (!reachStateId) return;
    setCatchmentStatus((s) => ({ ...s, [direction]: { loading: true, progress: 'Starting…' } }));
    try {
      let data = await safePoll('/api/compare/reach-catchment', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stateId: reachStateId, direction }),
      });
      const catchmentId = data.catchmentId;
      while (data.phase !== 'done' && data.phase !== 'error') {
        setCatchmentStatus((s) => ({ ...s, [direction]: { loading: true, progress: data.progress?.message || data.phase } }));
        await new Promise((r) => setTimeout(r, 4000));
        data = await safePoll('/api/compare/reach-catchment', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ catchmentId }),
        });
      }
      if (data.phase === 'error') throw new Error(data.error || 'Catchment build failed');
      setCatchmentStatus((s) => ({
        ...s,
        [direction]: {
          loading: false,
          downloadKey: data.downloadKey,
          rowCount: data.rowCount,
          nativeZipCount: data.nativeZipCount,
          geocodedCount: data.geocodedCount,
        },
      }));
      // Auto-trigger the download
      const link = document.createElement('a');
      link.href = `/api/compare/download?key=${encodeURIComponent(data.downloadKey)}`;
      link.click();
      toast({ title: 'Catchment ZIPs ready', description: `${(data.rowCount || 0).toLocaleString()} devices` });
    } catch (e: any) {
      setCatchmentStatus((s) => ({ ...s, [direction]: { loading: false, error: e.message } }));
      toast({ title: 'Catchment download failed', description: e.message, variant: 'destructive' });
    }
  };

  const renderDirection = (
    label: string,
    description: string,
    dir: ReachDirectionResult,
    directionKey: 'aToB' | 'bToA',
  ) => {
    const catchmentInfo = catchmentStatus[directionKey];
    const sourceVisits = dir.source.visitorCount;
    const reachPct = sourceVisits > 0 ? (dir.totalPotentialVisitors / sourceVisits) * 100 : 0;
    // Map data: highlight target POIs by potentialVisitors as intensity
    const max = dir.byPoi.reduce((m, p) => Math.max(m, p.potentialVisitors), 0);
    const mapPois = dir.byPoi
      .filter((p) => Number.isFinite(p.lat) && Number.isFinite(p.lng))
      .map((p) => ({
        side: 'B' as const, // colour as orange (target side)
        poiId: p.poiId,
        name: p.poiName,
        lat: p.lat,
        lng: p.lng,
        overlapDevices: p.potentialVisitors, // map sizes circles by this
      }));

    return (
      <div className="border rounded-lg overflow-hidden">
        <div className="px-4 py-2 text-sm font-semibold bg-muted/30 border-b flex items-center gap-2">
          <Target className="h-4 w-4" />
          {label}
        </div>
        <div className="p-4 space-y-4">
          <p className="text-xs text-muted-foreground">{description}</p>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
            <div className="border rounded p-3">
              <div className="text-2xl font-bold tabular-nums">{dir.totalPotentialVisitors.toLocaleString()}</div>
              <div className="text-xs text-muted-foreground">Potential reach (devices)</div>
            </div>
            <div className="border rounded p-3">
              <div className="text-2xl font-bold tabular-nums">{sourceVisits.toLocaleString()}</div>
              <div className="text-xs text-muted-foreground">{dir.source.datasetName} POI visitors</div>
            </div>
            <div className="border rounded p-3">
              <div className="text-2xl font-bold tabular-nums">{reachPct.toFixed(2)}%</div>
              <div className="text-xs text-muted-foreground">Reach as % of source visitors</div>
            </div>
          </div>

          {mapPois.length > 0 && (
            <div className="border rounded overflow-hidden" style={{ height: 320 }}>
              <CompareMap pois={mapPois} dsALabel={dir.source.datasetName} dsBLabel={dir.target.datasetName} />
            </div>
          )}

          <div className="flex items-center justify-between gap-2 flex-wrap">
            <span className="text-sm font-medium">By target POI ({dir.byPoi.length})</span>
            <div className="flex items-center gap-2 flex-wrap">
              <Button
                variant="outline"
                size="sm"
                onClick={() => downloadMaids(dir.downloadKey, `reach-${label.replace(/\s+/g, '-').toLowerCase()}-maids.csv`)}
                disabled={!dir.downloadKey}
              >
                <Download className="h-3 w-3 mr-1" /> MAIDs CSV
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => buildCatchmentZips(directionKey)}
                disabled={catchmentInfo.loading || !reachStateId}
                title="Build a CSV of the home location (lat/lng + ZIP + city) for every qualified device. Uses geo_fields['zipcode'] when present (FULL schema) and reverse-geocodes the rest."
              >
                {catchmentInfo.loading ? (
                  <><Loader2 className="h-3 w-3 mr-1 animate-spin" />{catchmentInfo.progress || 'Working…'}</>
                ) : catchmentInfo.downloadKey ? (
                  <><MapPin className="h-3 w-3 mr-1" /> Catchment ZIPs CSV (re-download)</>
                ) : (
                  <><MapPin className="h-3 w-3 mr-1" /> Catchment ZIPs CSV</>
                )}
              </Button>
            </div>
          </div>
          {catchmentInfo.downloadKey && (
            <div className="text-xs text-muted-foreground -mt-2">
              {catchmentInfo.rowCount?.toLocaleString()} devices ready
              {(catchmentInfo.nativeZipCount ?? 0) > 0 && ` · ${catchmentInfo.nativeZipCount} from FULL geo_fields`}
              {(catchmentInfo.geocodedCount ?? 0) > 0 && ` · ${catchmentInfo.geocodedCount} reverse-geocoded`}
            </div>
          )}
          {catchmentInfo.error && (
            <div className="text-xs text-red-500 -mt-2">Catchment failed: {catchmentInfo.error}</div>
          )}

          <div className="max-h-80 overflow-y-auto border rounded">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-muted/50 text-xs uppercase tracking-wider text-muted-foreground">
                <tr>
                  <th className="text-left px-3 py-2">POI</th>
                  <th className="text-left px-3 py-2">ID</th>
                  <th className="text-right px-3 py-2">Potential</th>
                  <th className="text-right px-3 py-2">% of {dir.source.datasetName}</th>
                  <th className="text-right px-3 py-2">Avg pings</th>
                  <th className="text-right px-3 py-2">Avg dwell (min)</th>
                </tr>
              </thead>
              <tbody>
                {dir.byPoi.slice(0, 200).map((p, i) => {
                  const pct = sourceVisits > 0 ? (p.potentialVisitors / sourceVisits) * 100 : 0;
                  const intensity = max > 0 ? p.potentialVisitors / max : 0;
                  return (
                    <tr key={`${p.poiId}-${i}`} className="border-b border-border/50 hover:bg-muted/30">
                      <td className="px-3 py-2">{p.poiName}</td>
                      <td className="px-3 py-2 text-xs text-muted-foreground font-mono">{p.poiId}</td>
                      <td className="px-3 py-2 text-right tabular-nums">
                        <div className="flex items-center justify-end gap-2">
                          <div className="w-16 h-2 bg-muted rounded-full overflow-hidden">
                            <div
                              className="h-full rounded-full bg-orange-400"
                              style={{ width: `${intensity * 100}%` }}
                            />
                          </div>
                          <span className="tabular-nums w-16 text-right font-semibold">{p.potentialVisitors.toLocaleString()}</span>
                        </div>
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums">{pct.toFixed(2)}%</td>
                      <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">{p.avgPings.toFixed(1)}</td>
                      <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">{p.avgDwellMinutes.toFixed(1)}</td>
                    </tr>
                  );
                })}
                {dir.byPoi.length > 200 && (
                  <tr><td colSpan={6} className="px-3 py-2 text-xs italic text-muted-foreground">Showing top 200 of {dir.byPoi.length}. Download MAIDs CSV for full data.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-4">
      {/* Inputs */}
      <div className="border rounded-lg p-4 space-y-4">
        <div>
          <p className="text-sm font-medium mb-1">Potential Reach analysis</p>
          <p className="text-xs text-muted-foreground">
            Identifies devices that visited POIs of one dataset and have a meaningful cluster of pings near POIs of the other —
            &ldquo;potential visitors&rdquo; by mobility proximity. Capped at 2000 POIs per side.
          </p>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
          <div>
            <label className="text-xs text-muted-foreground block mb-1">Max distance (m)</label>
            <input
              type="number"
              min={10}
              max={5000}
              step={10}
              value={maxDistance}
              onChange={(e) => setMaxDistance(parseInt(e.target.value, 10) || 200)}
              className="h-9 w-full rounded-md border border-input bg-background px-2 text-sm"
            />
          </div>
          <div>
            <label className="text-xs text-muted-foreground block mb-1">Min pings (threshold)</label>
            <input
              type="number"
              min={1}
              max={1000}
              value={minPings}
              onChange={(e) => setMinPings(parseInt(e.target.value, 10) || 3)}
              className="h-9 w-full rounded-md border border-input bg-background px-2 text-sm"
            />
          </div>
          <div>
            <label className="text-xs text-muted-foreground block mb-1">OR min dwell (min)</label>
            <input
              type="number"
              min={0}
              max={1440}
              value={minDwell}
              onChange={(e) => setMinDwell(parseInt(e.target.value, 10) || 0)}
              className="h-9 w-full rounded-md border border-input bg-background px-2 text-sm"
            />
          </div>
          <div>
            <label className="text-xs text-muted-foreground block mb-1">Direction</label>
            <select
              value={directions}
              onChange={(e) => setDirections(e.target.value as any)}
              className="h-9 w-full rounded-md border border-input bg-background px-2 text-sm"
            >
              {DIRECTION_OPTIONS.map((o) => (<option key={o.value} value={o.value}>{o.label}</option>))}
            </select>
          </div>
        </div>
        <div className="text-xs text-muted-foreground">
          A device counts if it has <strong>≥ {minPings} pings</strong> OR <strong>≥ {minDwell} min</strong> within{' '}
          <strong>{maxDistance} m</strong> of a target POI.
        </div>
        {(hasFilters(filtersA) || hasFilters(filtersB)) && (
          <div className="text-xs rounded border bg-muted/30 px-3 py-2 space-y-0.5">
            <div className="font-medium text-foreground">Source-side filters from the panel above:</div>
            {hasFilters(filtersA) && (
              <div className="text-muted-foreground">
                <span className="text-blue-500 font-semibold">{dsALabel}:</span> {summarizeFilters(filtersA)}
              </div>
            )}
            {hasFilters(filtersB) && (
              <div className="text-muted-foreground">
                <span className="text-orange-500 font-semibold">{dsBLabel}:</span> {summarizeFilters(filtersB)}
              </div>
            )}
            <div className="text-muted-foreground italic pt-1">
              Applied to the &ldquo;visitors of source POIs&rdquo; set per direction. Change them above and re-run to see the effect.
            </div>
          </div>
        )}
        <Button onClick={handleRun} disabled={running || !datasetA || !datasetB}>
          {running ? (
            <><Loader2 className="h-4 w-4 mr-2 animate-spin" />{progressMsg || 'Running…'}</>
          ) : (
            <><Play className="h-4 w-4 mr-2" />Run reach analysis</>
          )}
        </Button>
      </div>

      {/* Results */}
      {result && (
        <>
          {result.aToB && renderDirection(
            `${dsALabel} → ${dsBLabel}`,
            `Of devices that visited ${dsALabel}'s POIs, how many had a meaningful cluster of pings near ${dsBLabel}'s POIs.`,
            result.aToB,
            'aToB',
          )}
          {result.bToA && renderDirection(
            `${dsBLabel} → ${dsALabel}`,
            `Of devices that visited ${dsBLabel}'s POIs, how many had a meaningful cluster of pings near ${dsALabel}'s POIs.`,
            result.bToA,
            'bToA',
          )}
        </>
      )}
    </div>
  );
}
