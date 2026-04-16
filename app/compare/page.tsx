'use client';

import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';
import { MainLayout } from '@/components/layout/main-layout';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Loader2, Play, Download, GitCompareArrows } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

const CompareMap = dynamic(() => import('@/components/compare/compare-map-inner'), { ssr: false });
const ComparePenetrationChart = dynamic(() => import('@/components/compare/compare-penetration-chart'), { ssr: false });

interface Dataset {
  id: string;
  name: string;
  country?: string | null;
}

interface ExportOption {
  file: string;
  type: string;
  label: string;
  date: string;
}

interface PoiMatchRow {
  side: 'A' | 'B';
  poiId: string;
  name?: string;
  lat?: number;
  lng?: number;
  overlapDevices: number;
}

interface CompareResult {
  totalA: number;
  totalB: number;
  overlap: number;
  overlapPctA: number;
  overlapPctB: number;
  downloadKey: string;
  pois: PoiMatchRow[];
  zipFilter?: {
    zipCodes: string[];
    countryA?: string;
    countryB?: string;
    keptA?: number;
    keptB?: number;
  };
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

const VISIT_OPTIONS = [1, 2, 3, 5, 10, 15, 20];

type SourceType = 'all' | 'category-export' | 'nse-export';

function DatasetSideSelector({
  label,
  datasets,
  selectedDataset,
  onDatasetChange,
  source,
  onSourceChange,
  minDwell,
  onMinDwellChange,
  maxDwell,
  onMaxDwellChange,
  hourFrom,
  onHourFromChange,
  hourTo,
  onHourToChange,
  minVisits,
  onMinVisitsChange,
  exportFile,
  onExportFileChange,
  exports,
  loadingExports,
}: {
  label: string;
  datasets: Dataset[];
  selectedDataset: string;
  onDatasetChange: (v: string) => void;
  source: SourceType;
  onSourceChange: (v: SourceType) => void;
  minDwell: number;
  onMinDwellChange: (v: number) => void;
  maxDwell: number;
  onMaxDwellChange: (v: number) => void;
  hourFrom: number;
  onHourFromChange: (v: number) => void;
  hourTo: number;
  onHourToChange: (v: number) => void;
  minVisits: number;
  onMinVisitsChange: (v: number) => void;
  exportFile: string;
  onExportFileChange: (v: string) => void;
  exports: ExportOption[];
  loadingExports: boolean;
}) {
  const filteredExports = exports.filter(e =>
    source === 'category-export' ? e.type === 'category' :
    source === 'nse-export' ? e.type === 'nse' :
    true
  );

  return (
    <div className="space-y-3 p-4 border rounded-lg bg-card">
      <h3 className="font-semibold text-sm text-muted-foreground uppercase tracking-wider">{label}</h3>

      <div>
        <label className="text-xs text-muted-foreground block mb-1">Dataset</label>
        <Select value={selectedDataset} onValueChange={onDatasetChange}>
          <SelectTrigger className="w-full">
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

      {selectedDataset && (
        <div>
          <label className="text-xs text-muted-foreground block mb-1">MAID Source</label>
          <Select value={source} onValueChange={v => onSourceChange(v as SourceType)}>
            <SelectTrigger className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All MAIDs</SelectItem>
              <SelectItem value="category-export">Category Export</SelectItem>
              <SelectItem value="nse-export">NSE Export</SelectItem>
            </SelectContent>
          </Select>
        </div>
      )}

      {selectedDataset && source === 'all' && (
        <>
          <div>
            <label className="text-xs text-muted-foreground block mb-1">Dwell Time at POI</label>
            <div className="grid grid-cols-2 gap-2">
              <Select value={String(minDwell)} onValueChange={v => onMinDwellChange(Number(v))}>
                <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {DWELL_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>From: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={String(maxDwell)} onValueChange={v => onMaxDwellChange(Number(v))}>
                <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {DWELL_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>To: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div>
            <label className="text-xs text-muted-foreground block mb-1">Hour of Visit</label>
            <div className="grid grid-cols-2 gap-2">
              <Select value={String(hourFrom)} onValueChange={v => onHourFromChange(Number(v))}>
                <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {HOUR_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>From: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={String(hourTo)} onValueChange={v => onHourToChange(Number(v))}>
                <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {HOUR_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>To: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div>
            <label className="text-xs text-muted-foreground block mb-1">Min Visits (distinct days visiting POI)</label>
            <Select value={String(minVisits)} onValueChange={v => onMinVisitsChange(Number(v))}>
              <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
              <SelectContent>
                {VISIT_OPTIONS.map(n => (
                  <SelectItem key={n} value={String(n)}>{n}+ day{n === 1 ? '' : 's'}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </>
      )}

      {selectedDataset && (source === 'category-export' || source === 'nse-export') && (
        <div>
          <label className="text-xs text-muted-foreground block mb-1">
            Select Export {loadingExports && <Loader2 className="inline h-3 w-3 animate-spin ml-1" />}
          </label>
          {filteredExports.length === 0 && !loadingExports ? (
            <p className="text-xs text-muted-foreground italic">No {source === 'category-export' ? 'category' : 'NSE'} exports found. Run an analysis first.</p>
          ) : (
            <Select value={exportFile} onValueChange={onExportFileChange}>
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select export..." />
              </SelectTrigger>
              <SelectContent>
                {filteredExports.map(exp => (
                  <SelectItem key={exp.file} value={exp.file}>
                    {exp.label} {exp.date ? `(${exp.date})` : ''}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}
        </div>
      )}
    </div>
  );
}

export default function ComparePage() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);

  // Side A
  const [datasetA, setDatasetA] = useState('');
  const [sourceA, setSourceA] = useState<SourceType>('all');
  const [minDwellA, setMinDwellA] = useState(0);
  const [maxDwellA, setMaxDwellA] = useState(0);
  const [hourFromA, setHourFromA] = useState(0);
  const [hourToA, setHourToA] = useState(23);
  const [minVisitsA, setMinVisitsA] = useState(1);
  const [exportFileA, setExportFileA] = useState('');
  const [exportsA, setExportsA] = useState<ExportOption[]>([]);
  const [loadingExportsA, setLoadingExportsA] = useState(false);

  // Side B
  const [datasetB, setDatasetB] = useState('');
  const [sourceB, setSourceB] = useState<SourceType>('all');
  const [minDwellB, setMinDwellB] = useState(0);
  const [maxDwellB, setMaxDwellB] = useState(0);
  const [hourFromB, setHourFromB] = useState(0);
  const [hourToB, setHourToB] = useState(23);
  const [minVisitsB, setMinVisitsB] = useState(1);
  const [exportFileB, setExportFileB] = useState('');
  const [exportsB, setExportsB] = useState<ExportOption[]>([]);
  const [loadingExportsB, setLoadingExportsB] = useState(false);

  // Global ZIP filter
  const [zipCodesInput, setZipCodesInput] = useState('');

  // Compare state
  const [comparing, setComparing] = useState(false);
  const [compareProgress, setCompareProgress] = useState<string | null>(null);
  const [result, setResult] = useState<CompareResult | null>(null);
  const [poiTab, setPoiTab] = useState<'A' | 'B'>('A');
  const { toast } = useToast();

  useEffect(() => {
    fetch('/api/datasets', { credentials: 'include' })
      .then(r => r.json())
      .then(data => setDatasets(data.datasets || []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const loadExports = async (dataset: string, setter: (v: ExportOption[]) => void, setLoading: (v: boolean) => void) => {
    if (!dataset) { setter([]); return; }
    setLoading(true);
    try {
      const res = await fetch(`/api/compare/exports?dataset=${encodeURIComponent(dataset)}`, { credentials: 'include' });
      if (res.ok) {
        const data = await res.json();
        setter(data.exports || []);
      }
    } catch {} finally { setLoading(false); }
  };

  useEffect(() => { loadExports(datasetA, setExportsA, setLoadingExportsA); }, [datasetA]);
  useEffect(() => { loadExports(datasetB, setExportsB, setLoadingExportsB); }, [datasetB]);

  useEffect(() => { setExportFileA(''); }, [sourceA]);
  useEffect(() => { setExportFileB(''); }, [sourceB]);

  const parseZipCodes = (): string[] => {
    return Array.from(new Set(
      zipCodesInput
        .split(/[\s,;\n]+/)
        .map(z => z.trim().toUpperCase())
        .filter(Boolean)
    ));
  };

  const canCompare = () => {
    if (!datasetA || !datasetB) return false;
    if (sourceA !== 'all' && !exportFileA) return false;
    if (sourceB !== 'all' && !exportFileB) return false;
    return true;
  };

  const safePollFetch = async (url: string, options?: RequestInit) => {
    const res = await fetch(url, options);
    // 504 = Vercel gateway timeout → retry is legit
    if (res.status === 504) {
      return { phase: 'main_polling', progress: { message: 'Server processing (retrying...)' } };
    }
    let data: any;
    try { data = await res.json(); } catch {
      // Any non-504 that is also not JSON → surface as error, don't silently retry
      throw new Error(`Server error (status ${res.status})`);
    }
    if (!res.ok) throw new Error(data.error || `Server error (status ${res.status})`);
    return data;
  };

  const handleCompare = async () => {
    if (!canCompare()) return;
    const zipCodes = parseZipCodes();

    setComparing(true);
    setCompareProgress(zipCodes.length > 0 ? 'Starting comparison with ZIP filter...' : 'Starting comparison...');
    setResult(null);

    try {
      let data = await safePollFetch('/api/compare', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          zipCodes,
          datasetA: {
            name: datasetA,
            source: sourceA,
            minDwell: sourceA === 'all' ? minDwellA : undefined,
            maxDwell: sourceA === 'all' ? maxDwellA : undefined,
            hourFrom: sourceA === 'all' ? hourFromA : undefined,
            hourTo: sourceA === 'all' ? hourToA : undefined,
            minVisits: sourceA === 'all' ? minVisitsA : undefined,
            exportFile: sourceA !== 'all' ? exportFileA : undefined,
          },
          datasetB: {
            name: datasetB,
            source: sourceB,
            minDwell: sourceB === 'all' ? minDwellB : undefined,
            maxDwell: sourceB === 'all' ? maxDwellB : undefined,
            hourFrom: sourceB === 'all' ? hourFromB : undefined,
            hourTo: sourceB === 'all' ? hourToB : undefined,
            minVisits: sourceB === 'all' ? minVisitsB : undefined,
            exportFile: sourceB !== 'all' ? exportFileB : undefined,
          },
        }),
      });

      const stateId = data.stateId;

      while (data.phase !== 'done' && data.phase !== 'error') {
        setCompareProgress(data.progress?.message || 'Processing...');
        await new Promise(r => setTimeout(r, 4000));

        data = await safePollFetch('/api/compare', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ stateId }),
        });
      }

      if (data.phase === 'error') throw new Error(data.error || 'Comparison failed');

      setResult(data.result);
      toast({ title: 'Comparison complete', description: `${data.result.overlap.toLocaleString()} MAIDs in common` });
    } catch (e: any) {
      toast({ title: 'Comparison failed', description: e.message, variant: 'destructive' });
    } finally {
      setComparing(false);
      setCompareProgress(null);
    }
  };

  const handleDownload = () => {
    if (!result?.downloadKey) return;
    const link = document.createElement('a');
    link.href = result.downloadKey;
    link.download = `compare-${datasetA}-${datasetB}.csv`;
    link.click();
  };

  const dsALabel = datasets.find(d => d.id === datasetA)?.name || datasetA;
  const dsBLabel = datasets.find(d => d.id === datasetB)?.name || datasetB;

  const poisForSide = result?.pois.filter(p => p.side === poiTab) || [];
  const placedPois = result?.pois.filter(p => Number.isFinite(p.lat) && Number.isFinite(p.lng)) || [];

  return (
    <MainLayout>
      <div className="max-w-5xl mx-auto space-y-6">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <GitCompareArrows className="h-6 w-6" />
            Compare MAIDs
          </h1>
          <p className="text-muted-foreground mt-1">
            Compare MAID overlap between two datasets or exports
          </p>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <DatasetSideSelector
                label="Dataset A"
                datasets={datasets}
                selectedDataset={datasetA}
                onDatasetChange={v => { setDatasetA(v); setResult(null); }}
                source={sourceA}
                onSourceChange={v => { setSourceA(v); setResult(null); }}
                minDwell={minDwellA}
                onMinDwellChange={v => { setMinDwellA(v); setResult(null); }}
                maxDwell={maxDwellA}
                onMaxDwellChange={v => { setMaxDwellA(v); setResult(null); }}
                hourFrom={hourFromA}
                onHourFromChange={v => { setHourFromA(v); setResult(null); }}
                hourTo={hourToA}
                onHourToChange={v => { setHourToA(v); setResult(null); }}
                minVisits={minVisitsA}
                onMinVisitsChange={v => { setMinVisitsA(v); setResult(null); }}
                exportFile={exportFileA}
                onExportFileChange={v => { setExportFileA(v); setResult(null); }}
                exports={exportsA}
                loadingExports={loadingExportsA}
              />

              <DatasetSideSelector
                label="Dataset B"
                datasets={datasets}
                selectedDataset={datasetB}
                onDatasetChange={v => { setDatasetB(v); setResult(null); }}
                source={sourceB}
                onSourceChange={v => { setSourceB(v); setResult(null); }}
                minDwell={minDwellB}
                onMinDwellChange={v => { setMinDwellB(v); setResult(null); }}
                maxDwell={maxDwellB}
                onMaxDwellChange={v => { setMaxDwellB(v); setResult(null); }}
                hourFrom={hourFromB}
                onHourFromChange={v => { setHourFromB(v); setResult(null); }}
                hourTo={hourToB}
                onHourToChange={v => { setHourToB(v); setResult(null); }}
                minVisits={minVisitsB}
                onMinVisitsChange={v => { setMinVisitsB(v); setResult(null); }}
                exportFile={exportFileB}
                onExportFileChange={v => { setExportFileB(v); setResult(null); }}
                exports={exportsB}
                loadingExports={loadingExportsB}
              />
            </div>

            {/* Global ZIP-code filter */}
            <div className="p-4 border rounded-lg bg-card">
              <label className="text-sm font-semibold block mb-1">ZIP Codes filter (applies to both datasets)</label>
              <p className="text-xs text-muted-foreground mb-2">
                Comma or newline-separated. Only devices whose home ZIP (first ping of day, repeated 2+ days) is in this list will be considered. Requires Country set on each dataset&apos;s job. Leave empty to skip.
              </p>
              <textarea
                value={zipCodesInput}
                onChange={e => { setZipCodesInput(e.target.value); setResult(null); }}
                placeholder="e.g. 11560, 01210, 06700"
                rows={2}
                className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
              />
            </div>

            <Button
              onClick={handleCompare}
              disabled={comparing || !canCompare()}
              className="w-full"
              size="lg"
            >
              {comparing ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  {compareProgress || 'Comparing...'}
                </>
              ) : (
                <>
                  <Play className="mr-2 h-4 w-4" />
                  Compare MAIDs
                </>
              )}
            </Button>

            {result && (
              <div className="border rounded-lg p-6 space-y-6">
                <div className="flex items-center justify-center gap-0">
                  <div className="relative">
                    <div className="w-40 h-40 rounded-full border-2 border-blue-500 bg-blue-500/10 flex items-center justify-center -mr-8 relative z-10">
                      <div className="text-center -ml-6">
                        <p className="text-lg font-bold">{result.totalA.toLocaleString()}</p>
                        <p className="text-xs text-muted-foreground truncate max-w-[100px]">{dsALabel}</p>
                      </div>
                    </div>
                  </div>
                  <div className="relative z-20 bg-background border-2 border-primary rounded-full w-24 h-24 flex items-center justify-center -mx-6 shadow-lg">
                    <div className="text-center">
                      <p className="text-lg font-bold text-primary">{result.overlap.toLocaleString()}</p>
                      <p className="text-[10px] text-muted-foreground">overlap</p>
                    </div>
                  </div>
                  <div className="relative">
                    <div className="w-40 h-40 rounded-full border-2 border-orange-500 bg-orange-500/10 flex items-center justify-center -ml-8 relative z-10">
                      <div className="text-center ml-6">
                        <p className="text-lg font-bold">{result.totalB.toLocaleString()}</p>
                        <p className="text-xs text-muted-foreground truncate max-w-[100px]">{dsBLabel}</p>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-center">
                  <div className="p-3 rounded-lg bg-blue-500/5 border border-blue-500/20">
                    <p className="text-2xl font-bold">{result.totalA.toLocaleString()}</p>
                    <p className="text-xs text-muted-foreground">Total A</p>
                  </div>
                  <div className="p-3 rounded-lg bg-orange-500/5 border border-orange-500/20">
                    <p className="text-2xl font-bold">{result.totalB.toLocaleString()}</p>
                    <p className="text-xs text-muted-foreground">Total B</p>
                  </div>
                  <div className="p-3 rounded-lg bg-primary/5 border border-primary/20">
                    <p className="text-2xl font-bold">{result.overlapPctA}%</p>
                    <p className="text-xs text-muted-foreground">of A in B</p>
                  </div>
                  <div className="p-3 rounded-lg bg-primary/5 border border-primary/20">
                    <p className="text-2xl font-bold">{result.overlapPctB}%</p>
                    <p className="text-xs text-muted-foreground">of B in A</p>
                  </div>
                </div>

                {result.zipFilter && (
                  <div className="text-xs text-muted-foreground p-3 rounded bg-muted/30 border">
                    ZIP filter active: <b>{result.zipFilter.zipCodes.length}</b> zip(s)
                    {result.zipFilter.countryA ? ` · A=${result.zipFilter.countryA}` : ''}
                    {result.zipFilter.countryB ? ` · B=${result.zipFilter.countryB}` : ''}
                    {' · '}kept A={(result.zipFilter.keptA ?? 0).toLocaleString()} · kept B={(result.zipFilter.keptB ?? 0).toLocaleString()}
                  </div>
                )}

                {result.downloadKey && result.overlap > 0 && (
                  <Button variant="outline" onClick={handleDownload} className="w-full">
                    <Download className="mr-2 h-4 w-4" />
                    Download {result.overlap.toLocaleString()} Overlapping MAIDs (CSV)
                  </Button>
                )}

                {/* POI map */}
                {placedPois.length > 0 && (
                  <div className="border rounded-lg overflow-hidden">
                    <div className="px-4 py-2 text-sm font-semibold bg-muted/30 border-b flex items-center gap-3">
                      <span>POIs with overlap matches ({placedPois.length})</span>
                      <span className="text-xs text-muted-foreground">
                        <span className="inline-block w-3 h-3 rounded-full bg-blue-500 mr-1 align-middle" />A ·{' '}
                        <span className="inline-block w-3 h-3 rounded-full bg-orange-500 mr-1 align-middle" />B
                      </span>
                    </div>
                    <div style={{ height: 420 }}>
                      <CompareMap pois={placedPois} dsALabel={dsALabel} dsBLabel={dsBLabel} />
                    </div>
                  </div>
                )}

                {/* POI list + penetration chart (shared A/B tabs) */}
                {result.pois.length > 0 && (
                  <div className="border rounded-lg overflow-hidden">
                    <div className="flex border-b">
                      <button
                        onClick={() => setPoiTab('A')}
                        className={`px-4 py-2 text-sm font-semibold ${poiTab === 'A' ? 'bg-blue-500/10 border-b-2 border-blue-500' : 'text-muted-foreground hover:bg-muted/30'}`}
                      >
                        {dsALabel} POIs ({result.pois.filter(p => p.side === 'A').length})
                      </button>
                      <button
                        onClick={() => setPoiTab('B')}
                        className={`px-4 py-2 text-sm font-semibold ${poiTab === 'B' ? 'bg-orange-500/10 border-b-2 border-orange-500' : 'text-muted-foreground hover:bg-muted/30'}`}
                      >
                        {dsBLabel} POIs ({result.pois.filter(p => p.side === 'B').length})
                      </button>
                    </div>

                    {/* Penetration chart */}
                    <ComparePenetrationChart
                      pois={poisForSide}
                      side={poiTab}
                      totalForSide={poiTab === 'A' ? result.totalA : result.totalB}
                      overlap={result.overlap}
                    />

                    {/* POI list table */}
                    <div className="max-h-[400px] overflow-y-auto border-t">
                      {poisForSide.length === 0 ? (
                        <p className="p-4 text-xs italic text-muted-foreground">No POIs with overlap matches for this side.</p>
                      ) : (
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-muted/50 text-xs uppercase tracking-wider text-muted-foreground">
                            <tr>
                              <th className="text-left px-3 py-2">POI</th>
                              <th className="text-left px-3 py-2">ID</th>
                              <th className="text-right px-3 py-2">Overlap devices</th>
                              <th className="text-right px-3 py-2">% of sample</th>
                              <th className="text-right px-3 py-2">% of match</th>
                            </tr>
                          </thead>
                          <tbody>
                            {poisForSide.map((p, i) => {
                              const denomSample = poiTab === 'A' ? result.totalA : result.totalB;
                              const pctSample = denomSample > 0 ? (p.overlapDevices / denomSample) * 100 : 0;
                              const pctMatch = result.overlap > 0 ? (p.overlapDevices / result.overlap) * 100 : 0;
                              return (
                                <tr key={`${p.side}-${p.poiId}-${i}`} className="border-t hover:bg-muted/30">
                                  <td className="px-3 py-2">{p.name || <span className="text-muted-foreground italic">(unnamed)</span>}</td>
                                  <td className="px-3 py-2 font-mono text-xs text-muted-foreground">{p.poiId}</td>
                                  <td className="px-3 py-2 text-right font-semibold">{p.overlapDevices.toLocaleString()}</td>
                                  <td className="px-3 py-2 text-right">{pctSample.toFixed(2)}%</td>
                                  <td className="px-3 py-2 text-right">{pctMatch.toFixed(2)}%</td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </MainLayout>
  );
}
