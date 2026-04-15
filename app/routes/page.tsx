'use client';

import { useState, useEffect } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Card, CardContent } from '@/components/ui/card';
import { Loader2, Play, Route, Users, Activity, Navigation } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { SankeyChart } from '@/components/analysis/sankey-chart';
import { RouteTimeline } from '@/components/analysis/route-timeline';

interface Dataset {
  id: string;
  name: string;
  country?: string | null;
}

const DWELL_OPTIONS = [
  { value: 0, label: 'No limit' },
  { value: 2, label: '2 min' },
  { value: 5, label: '5 min' },
  { value: 10, label: '10 min' },
  { value: 15, label: '15 min' },
  { value: 30, label: '30 min' },
  { value: 45, label: '45 min' },
  { value: 60, label: '1 hr' },
  { value: 90, label: '1.5 hr' },
  { value: 120, label: '2 hr' },
  { value: 180, label: '3 hr' },
  { value: 240, label: '4 hr' },
  { value: 360, label: '6 hr' },
  { value: 480, label: '8 hr' },
];

const HOUR_OPTIONS = Array.from({ length: 24 }, (_, i) => ({
  value: i,
  label: `${i.toString().padStart(2, '0')}:00`,
}));

export default function RoutesPage() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);
  const { toast } = useToast();

  // Filters
  const [selectedDataset, setSelectedDataset] = useState('');
  const [dwellMin, setDwellMin] = useState(0);
  const [dwellMax, setDwellMax] = useState(0);
  const [hourFrom, setHourFrom] = useState(0);
  const [hourTo, setHourTo] = useState(23);
  const [minVisits, setMinVisits] = useState(1);

  // Analysis state
  const [analyzing, setAnalyzing] = useState(false);
  const [progress, setProgress] = useState<string | null>(null);
  const [result, setResult] = useState<any>(null);
  const [stateKey, setStateKey] = useState<string | null>(null);

  // Get selected dataset info
  const selectedDs = datasets.find(d => d.id === selectedDataset);
  const country = selectedDs?.country || '';

  // Load datasets
  useEffect(() => {
    fetch('/api/datasets', { credentials: 'include' })
      .then(r => r.json())
      .then(data => setDatasets(data.datasets || []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const canRun = selectedDataset && country;

  const safePollFetch = async (url: string, options?: RequestInit) => {
    const res = await fetch(url, options);
    if (res.status === 504) {
      return { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
    }
    let data;
    try { data = await res.json(); } catch {
      return { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
    }
    if (!res.ok) throw new Error(data.error || 'Failed');
    return data;
  };

  const runAnalysis = async () => {
    if (!canRun) return;

    setAnalyzing(true);
    setProgress('Starting route analysis...');
    setResult(null);

    try {
      let data = await safePollFetch(`/api/datasets/${selectedDataset}/routes/poll`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          country,
          minDwell: dwellMin,
          maxDwell: dwellMax,
          hourFrom,
          hourTo,
          minVisits,
        }),
      });

      const sk = data.stateKey || '';
      setStateKey(sk);

      while (data.phase !== 'done' && data.phase !== 'error') {
        setProgress(data.progress?.message || 'Processing...');
        await new Promise(r => setTimeout(r, 4000));

        data = await safePollFetch(`/api/datasets/${selectedDataset}/routes/poll`, {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ stateKey: sk }),
        });
      }

      if (data.phase === 'error') {
        throw new Error(data.error || 'Route analysis failed');
      }

      setResult(data.result);
      toast({ title: 'Route analysis complete', description: `${data.result?.sankey?.length || 0} category flows found` });
    } catch (e: any) {
      toast({ title: 'Route analysis failed', description: e.message, variant: 'destructive' });
    } finally {
      setAnalyzing(false);
      setProgress(null);
    }
  };

  const dsLabel = selectedDs?.name || selectedDataset;

  return (
    <MainLayout>
      <div className="max-w-6xl mx-auto space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Route className="h-6 w-6" />
            Device Routes & Category Flows
          </h1>
          <p className="text-muted-foreground mt-1">
            Analyze what POI categories devices visit before and after the target POI
          </p>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <>
            {/* Controls */}
            <Card>
              <CardContent className="py-4">
                <div className="flex flex-wrap items-end gap-4">
                  {/* Dataset */}
                  <div className="flex-1 min-w-[200px]">
                    <label className="text-xs text-muted-foreground block mb-1">Dataset</label>
                    <Select value={selectedDataset} onValueChange={v => { setSelectedDataset(v); setResult(null); }}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select dataset..." />
                      </SelectTrigger>
                      <SelectContent className="max-h-[300px]">
                        {datasets.map(ds => (
                          <SelectItem key={ds.id} value={ds.id}>
                            {ds.name || ds.id} {ds.country ? `(${ds.country})` : ''}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  {/* Dwell range */}
                  <div>
                    <label className="text-xs text-muted-foreground block mb-1">Dwell Time</label>
                    <div className="flex items-center gap-1">
                      <Select value={String(dwellMin)} onValueChange={v => { setDwellMin(Number(v)); setResult(null); }}>
                        <SelectTrigger className="w-24">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {DWELL_OPTIONS.map(opt => (
                            <SelectItem key={opt.value} value={String(opt.value)}>{opt.value === 0 ? 'Min' : opt.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <span className="text-xs text-muted-foreground">to</span>
                      <Select value={String(dwellMax)} onValueChange={v => { setDwellMax(Number(v)); setResult(null); }}>
                        <SelectTrigger className="w-24">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {DWELL_OPTIONS.map(opt => (
                            <SelectItem key={opt.value} value={String(opt.value)}>{opt.value === 0 ? 'Max' : opt.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  {/* Hour range */}
                  <div>
                    <label className="text-xs text-muted-foreground block mb-1">Hour of Visit</label>
                    <div className="flex items-center gap-1">
                      <Select value={String(hourFrom)} onValueChange={v => { setHourFrom(Number(v)); setResult(null); }}>
                        <SelectTrigger className="w-24">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {HOUR_OPTIONS.map(opt => (
                            <SelectItem key={opt.value} value={String(opt.value)}>{opt.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <span className="text-xs text-muted-foreground">to</span>
                      <Select value={String(hourTo)} onValueChange={v => { setHourTo(Number(v)); setResult(null); }}>
                        <SelectTrigger className="w-24">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {HOUR_OPTIONS.map(opt => (
                            <SelectItem key={opt.value} value={String(opt.value)}>{opt.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  {/* Min visits */}
                  <div>
                    <label className="text-xs text-muted-foreground block mb-1">Min Visits</label>
                    <Select value={String(minVisits)} onValueChange={v => { setMinVisits(Number(v)); setResult(null); }}>
                      <SelectTrigger className="w-20">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {[1, 2, 3, 5, 10, 15, 20].map(n => (
                          <SelectItem key={n} value={String(n)}>{n}+</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  {/* Run button */}
                  <Button onClick={runAnalysis} disabled={analyzing || !canRun} size="lg">
                    {analyzing ? (
                      <><Loader2 className="mr-2 h-4 w-4 animate-spin" />{progress || 'Analyzing...'}</>
                    ) : (
                      <><Play className="mr-2 h-4 w-4" />Analyze Routes</>
                    )}
                  </Button>
                </div>

                {/* Country warning */}
                {selectedDataset && !country && (
                  <p className="text-xs text-amber-400 mt-2">
                    This dataset has no country set. Set the country on the job first.
                  </p>
                )}
              </CardContent>
            </Card>

            {/* Results */}
            {result && (
              <div className="space-y-6">
                {/* Stats cards */}
                <div className="grid grid-cols-3 gap-4">
                  <Card>
                    <CardContent className="py-4 text-center">
                      <div className="flex items-center justify-center gap-1.5 mb-1">
                        <Users className="h-4 w-4 text-cyan-400" />
                        <p className="text-xs text-muted-foreground uppercase tracking-wide">Visitors</p>
                      </div>
                      <p className="text-xl font-bold">{result.totalVisitors?.toLocaleString() || 0}</p>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="py-4 text-center">
                      <div className="flex items-center justify-center gap-1.5 mb-1">
                        <Activity className="h-4 w-4 text-purple-400" />
                        <p className="text-xs text-muted-foreground uppercase tracking-wide">Category Flows</p>
                      </div>
                      <p className="text-xl font-bold">{result.sankey?.length || 0}</p>
                    </CardContent>
                  </Card>
                  <Card>
                    <CardContent className="py-4 text-center">
                      <div className="flex items-center justify-center gap-1.5 mb-1">
                        <Navigation className="h-4 w-4 text-orange-400" />
                        <p className="text-xs text-muted-foreground uppercase tracking-wide">Devices Sampled</p>
                      </div>
                      <p className="text-xl font-bold">
                        {new Set(result.sampleRoutes?.map((s: any) => s.ad_id) || []).size}
                      </p>
                    </CardContent>
                  </Card>
                </div>

                {/* Sankey diagram */}
                {result.sankey?.length > 0 && (
                  <Card>
                    <CardContent className="py-6">
                      <h3 className="text-lg font-semibold mb-1">Category Flows</h3>
                      <p className="text-sm text-muted-foreground mb-4">
                        POI categories visited before arriving at and after leaving <span className="font-semibold text-foreground">{dsLabel}</span>.
                        Width proportional to device count. Hover for details.
                      </p>
                      <SankeyChart
                        data={result.sankey}
                        targetLabel={dsLabel}
                      />
                    </CardContent>
                  </Card>
                )}

                {/* Sample routes timeline */}
                {result.sampleRoutes?.length > 0 && (
                  <Card>
                    <CardContent className="py-6">
                      <h3 className="text-lg font-semibold mb-1">Sample Device Routes</h3>
                      <p className="text-sm text-muted-foreground mb-4">
                        Daily routes for a random sample of devices. Each colored bar is a POI visit by category.
                        The highlighted segment is the target POI visit. Click a device to expand.
                      </p>
                      <RouteTimeline
                        data={result.sampleRoutes}
                        onRefresh={() => runAnalysis()}
                        refreshing={analyzing}
                      />
                    </CardContent>
                  </Card>
                )}
              </div>
            )}

            {/* Empty state (no result, not analyzing) */}
            {!result && !analyzing && (
              <Card>
                <CardContent className="py-12 text-center">
                  <Route className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
                  <p className="text-muted-foreground">
                    Select a dataset and click &quot;Analyze Routes&quot; to see category flows
                    and device route patterns.
                  </p>
                </CardContent>
              </Card>
            )}
          </>
        )}
      </div>
    </MainLayout>
  );
}
