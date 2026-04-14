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
import { Loader2, Play, Download, GitCompareArrows } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

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

interface CompareResult {
  totalA: number;
  totalB: number;
  overlap: number;
  overlapPctA: number;
  overlapPctB: number;
  downloadKey: string;
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

      {/* Dataset selector */}
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

      {/* Source selector */}
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

      {/* Filters (only for "all" source) */}
      {selectedDataset && source === 'all' && (
        <>
          {/* Dwell time interval */}
          <div>
            <label className="text-xs text-muted-foreground block mb-1">Dwell Time at POI</label>
            <div className="grid grid-cols-2 gap-2">
              <Select value={String(minDwell)} onValueChange={v => onMinDwellChange(Number(v))}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DWELL_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>From: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={String(maxDwell)} onValueChange={v => onMaxDwellChange(Number(v))}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DWELL_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>To: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Hour of visit interval */}
          <div>
            <label className="text-xs text-muted-foreground block mb-1">Hour of Visit</label>
            <div className="grid grid-cols-2 gap-2">
              <Select value={String(hourFrom)} onValueChange={v => onHourFromChange(Number(v))}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {HOUR_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>From: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Select value={String(hourTo)} onValueChange={v => onHourToChange(Number(v))}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {HOUR_OPTIONS.map(opt => (
                    <SelectItem key={opt.value} value={String(opt.value)}>To: {opt.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </>
      )}

      {/* Export file selector */}
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

  // Side A state
  const [datasetA, setDatasetA] = useState('');
  const [sourceA, setSourceA] = useState<SourceType>('all');
  const [minDwellA, setMinDwellA] = useState(0);
  const [maxDwellA, setMaxDwellA] = useState(0);
  const [hourFromA, setHourFromA] = useState(0);
  const [hourToA, setHourToA] = useState(23);
  const [exportFileA, setExportFileA] = useState('');
  const [exportsA, setExportsA] = useState<ExportOption[]>([]);
  const [loadingExportsA, setLoadingExportsA] = useState(false);

  // Side B state
  const [datasetB, setDatasetB] = useState('');
  const [sourceB, setSourceB] = useState<SourceType>('all');
  const [minDwellB, setMinDwellB] = useState(0);
  const [maxDwellB, setMaxDwellB] = useState(0);
  const [hourFromB, setHourFromB] = useState(0);
  const [hourToB, setHourToB] = useState(23);
  const [exportFileB, setExportFileB] = useState('');
  const [exportsB, setExportsB] = useState<ExportOption[]>([]);
  const [loadingExportsB, setLoadingExportsB] = useState(false);

  // Compare state
  const [comparing, setComparing] = useState(false);
  const [compareProgress, setCompareProgress] = useState<string | null>(null);
  const [result, setResult] = useState<CompareResult | null>(null);
  const { toast } = useToast();

  // Load datasets
  useEffect(() => {
    fetch('/api/datasets', { credentials: 'include' })
      .then(r => r.json())
      .then(data => setDatasets(data.datasets || []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  // Load exports when dataset changes
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

  // Reset export file when source changes
  useEffect(() => { setExportFileA(''); }, [sourceA]);
  useEffect(() => { setExportFileB(''); }, [sourceB]);

  const canCompare = () => {
    if (!datasetA || !datasetB) return false;
    if (sourceA !== 'all' && !exportFileA) return false;
    if (sourceB !== 'all' && !exportFileB) return false;
    return true;
  };

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

  const handleCompare = async () => {
    if (!canCompare()) return;

    setComparing(true);
    setCompareProgress('Starting comparison...');
    setResult(null);

    try {
      let data = await safePollFetch('/api/compare', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          datasetA: {
            name: datasetA,
            source: sourceA,
            minDwell: sourceA === 'all' ? minDwellA : undefined,
            maxDwell: sourceA === 'all' ? maxDwellA : undefined,
            hourFrom: sourceA === 'all' ? hourFromA : undefined,
            hourTo: sourceA === 'all' ? hourToA : undefined,
            exportFile: sourceA !== 'all' ? exportFileA : undefined,
          },
          datasetB: {
            name: datasetB,
            source: sourceB,
            minDwell: sourceB === 'all' ? minDwellB : undefined,
            maxDwell: sourceB === 'all' ? maxDwellB : undefined,
            hourFrom: sourceB === 'all' ? hourFromB : undefined,
            hourTo: sourceB === 'all' ? hourToB : undefined,
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

      if (data.phase === 'error') {
        throw new Error(data.error || 'Comparison failed');
      }

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

  return (
    <MainLayout>
      <div className="max-w-4xl mx-auto space-y-6">
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
            {/* Two-column selector */}
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
                exportFile={exportFileB}
                onExportFileChange={v => { setExportFileB(v); setResult(null); }}
                exports={exportsB}
                loadingExports={loadingExportsB}
              />
            </div>

            {/* Compare button */}
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

            {/* Results */}
            {result && (
              <div className="border rounded-lg p-6 space-y-6">
                {/* Venn diagram visual */}
                <div className="flex items-center justify-center gap-0">
                  <div className="relative">
                    {/* Circle A */}
                    <div className="w-40 h-40 rounded-full border-2 border-blue-500 bg-blue-500/10 flex items-center justify-center -mr-8 relative z-10">
                      <div className="text-center -ml-6">
                        <p className="text-lg font-bold">{result.totalA.toLocaleString()}</p>
                        <p className="text-xs text-muted-foreground truncate max-w-[100px]">{dsALabel}</p>
                      </div>
                    </div>
                  </div>
                  {/* Overlap indicator */}
                  <div className="relative z-20 bg-background border-2 border-primary rounded-full w-24 h-24 flex items-center justify-center -mx-6 shadow-lg">
                    <div className="text-center">
                      <p className="text-lg font-bold text-primary">{result.overlap.toLocaleString()}</p>
                      <p className="text-[10px] text-muted-foreground">overlap</p>
                    </div>
                  </div>
                  <div className="relative">
                    {/* Circle B */}
                    <div className="w-40 h-40 rounded-full border-2 border-orange-500 bg-orange-500/10 flex items-center justify-center -ml-8 relative z-10">
                      <div className="text-center ml-6">
                        <p className="text-lg font-bold">{result.totalB.toLocaleString()}</p>
                        <p className="text-xs text-muted-foreground truncate max-w-[100px]">{dsBLabel}</p>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Stats table */}
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

                {/* Download */}
                {result.downloadKey && result.overlap > 0 && (
                  <Button variant="outline" onClick={handleDownload} className="w-full">
                    <Download className="mr-2 h-4 w-4" />
                    Download {result.overlap.toLocaleString()} Overlapping MAIDs (CSV)
                  </Button>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </MainLayout>
  );
}
