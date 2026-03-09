'use client';

import { useState, useEffect, useRef, useMemo } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Label } from '@/components/ui/label';
import { Separator } from '@/components/ui/separator';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from '@/components/ui/tabs';
import {
  MapPinned,
  Globe,
  Database,
  Loader2,
  Search,
  Download,
  Play,
  X,
  Plus,
  Hash,
  BarChart3,
  Users,
  CheckCircle,
  AlertTriangle,
  MailPlus,
  Copy,
  FileDown,
  RotateCcw,
} from 'lucide-react';
import type { PostalMaidResult, PostalMaidDevice } from '@/lib/postal-maid-types';

// ── Types ─────────────────────────────────────────────────────────────
interface DatasetInfo {
  id: string;
  name: string;
  jobId: string | null;
  objectCount: number;
  totalBytes: number;
  dateRange: { from: string; to: string } | null;
  syncedAt: string | null;
}

type Phase = 'setup' | 'running' | 'results';

// ── Helpers ───────────────────────────────────────────────────────────
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(0)} KB`;
  if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`;
  return `${(bytes / 1073741824).toFixed(2)} GB`;
}

function formatNumber(n: number): string {
  return n.toLocaleString('en-US');
}

const COUNTRY_FLAGS: Record<string, string> = {
  MX: '\u{1F1F2}\u{1F1FD}', GT: '\u{1F1EC}\u{1F1F9}', DO: '\u{1F1E9}\u{1F1F4}',
  SV: '\u{1F1F8}\u{1F1FB}', NI: '\u{1F1F3}\u{1F1EE}', CR: '\u{1F1E8}\u{1F1F7}',
  PA: '\u{1F1F5}\u{1F1E6}', CO: '\u{1F1E8}\u{1F1F4}', CL: '\u{1F1E8}\u{1F1F1}',
  EC: '\u{1F1EA}\u{1F1E8}', HN: '\u{1F1ED}\u{1F1F3}', AR: '\u{1F1E6}\u{1F1F7}',
  FR: '\u{1F1EB}\u{1F1F7}', GB: '\u{1F1EC}\u{1F1E7}', ES: '\u{1F1EA}\u{1F1F8}',
  SE: '\u{1F1F8}\u{1F1EA}', NL: '\u{1F1F3}\u{1F1F1}', BE: '\u{1F1E7}\u{1F1EA}',
  DE: '\u{1F1E9}\u{1F1EA}', PT: '\u{1F1F5}\u{1F1F9}', IE: '\u{1F1EE}\u{1F1EA}',
  IT: '\u{1F1EE}\u{1F1F9}',
};

const COUNTRIES = Object.keys(COUNTRY_FLAGS);

// ── Progress steps ────────────────────────────────────────────────────
const PROGRESS_STEPS = [
  { key: 'initializing', label: 'Init' },
  { key: 'preparing_table', label: 'Table' },
  { key: 'running_queries', label: 'Query' },
  { key: 'geocoding', label: 'Geocode' },
  { key: 'matching', label: 'Match' },
];

// ── Main page ─────────────────────────────────────────────────────────
export default function ZipCodeSignalsPage() {
  // Phase
  const [phase, setPhase] = useState<Phase>('setup');

  // Datasets
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [loadingDatasets, setLoadingDatasets] = useState(true);

  // Config
  const [selectedDatasetId, setSelectedDatasetId] = useState<string>('');
  const [selectedCountry, setSelectedCountry] = useState<string>('');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');

  // Postal codes input
  const [postalInput, setPostalInput] = useState<string>('');
  const [postalCodes, setPostalCodes] = useState<string[]>([]);

  // Running state
  const [progress, setProgress] = useState<{
    step: string; percent: number; message: string; detail?: string;
  } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  // Results
  const [result, setResult] = useState<PostalMaidResult | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [copiedAll, setCopiedAll] = useState(false);

  // ── Fetch datasets ────────────────────────────────────────────────
  useEffect(() => {
    fetch('/api/datasets', { credentials: 'include' })
      .then(r => r.json())
      .then(data => {
        const ds = (data.datasets || []).filter((d: any) =>
          d.jobId && d.objectCount > 0 && d.totalBytes > 0
        );
        setDatasets(ds);
        setLoadingDatasets(false);
      })
      .catch(() => setLoadingDatasets(false));
  }, []);

  // ── Derived ───────────────────────────────────────────────────────
  const selectedDataset = datasets.find(d => d.id === selectedDatasetId);

  const canRun = !!(
    selectedDatasetId &&
    selectedCountry &&
    postalCodes.length > 0
  );

  // Filtered devices for search
  const filteredDevices = useMemo(() => {
    if (!result?.devices) return [];
    if (!searchQuery.trim()) return result.devices;
    const q = searchQuery.trim().toLowerCase();
    return result.devices.filter(d =>
      d.adId.toLowerCase().includes(q) ||
      d.postalCodes.some(pc => pc.toLowerCase().includes(q))
    );
  }, [result, searchQuery]);

  // ── Postal code chip management ───────────────────────────────────
  const addPostalCodes = (input: string) => {
    // Support comma, space, newline, semicolon as delimiters
    const codes = input
      .split(/[,;\n\r\t]+/)
      .map(c => c.trim().toUpperCase())
      .filter(c => c.length > 0);
    if (codes.length === 0) return;

    setPostalCodes(prev => {
      const existing = new Set(prev);
      for (const c of codes) existing.add(c);
      return Array.from(existing);
    });
    setPostalInput('');
  };

  const removePostalCode = (code: string) => {
    setPostalCodes(prev => prev.filter(c => c !== code));
  };

  const handlePostalKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      if (postalInput.trim()) addPostalCodes(postalInput);
    }
    if (e.key === 'Backspace' && postalInput === '' && postalCodes.length > 0) {
      setPostalCodes(prev => prev.slice(0, -1));
    }
  };

  const handlePaste = (e: React.ClipboardEvent<HTMLInputElement>) => {
    e.preventDefault();
    const pasted = e.clipboardData.getData('text');
    addPostalCodes(pasted);
  };

  // ── Run analysis ──────────────────────────────────────────────────
  const handleRun = async () => {
    if (!canRun || !selectedDataset) return;

    setPhase('running');
    setError(null);
    setResult(null);
    setProgress({ step: 'initializing', percent: 0, message: 'Starting...' });

    const abort = new AbortController();
    abortRef.current = abort;

    try {
      const response = await fetch('/api/zip-code-signals/analyze/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          datasetName: selectedDataset.id,
          postalCodes,
          country: selectedCountry,
          dateFrom: dateFrom || undefined,
          dateTo: dateTo || undefined,
        }),
        credentials: 'include',
        signal: abort.signal,
      });

      if (!response.ok || !response.body) {
        throw new Error(`Server error: ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let gotResult = false;
      let backendError = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const messages = buffer.split('\n\n');
        buffer = messages.pop() || '';

        for (const msg of messages) {
          const lines = msg.split('\n');
          let eventType = '';
          let dataStr = '';
          for (const line of lines) {
            if (line.startsWith('event: ')) eventType = line.slice(7).trim();
            else if (line.startsWith('data: ')) dataStr += line.slice(6);
          }
          if (!eventType || !dataStr) continue;
          try {
            const data = JSON.parse(dataStr);
            if (eventType === 'progress') {
              if (data.step === 'error') {
                backendError = data.message || 'Analysis failed';
              } else {
                setProgress(data);
              }
            } else if (eventType === 'result') {
              gotResult = true;
              setResult(data);
              setPhase('results');
              setProgress(null);
            }
          } catch { /* ignore parse errors */ }
        }
      }

      // Process remaining buffer
      if (buffer.trim()) {
        const lines = buffer.trim().split('\n');
        let eventType = '';
        let dataStr = '';
        for (const line of lines) {
          if (line.startsWith('event: ')) eventType = line.slice(7).trim();
          else if (line.startsWith('data: ')) dataStr += line.slice(6);
        }
        if (eventType && dataStr) {
          try {
            const data = JSON.parse(dataStr);
            if (eventType === 'progress' && data.step === 'error') {
              backendError = data.message || 'Analysis failed';
            } else if (eventType === 'result') {
              gotResult = true;
              setResult(data);
              setPhase('results');
              setProgress(null);
            }
          } catch { /* ignore */ }
        }
      }

      if (!gotResult && backendError) {
        throw new Error(backendError);
      }
      if (!gotResult) {
        throw new Error('Analysis completed without returning results');
      }
    } catch (err: any) {
      if (err.name === 'AbortError') return;
      setError(err.message || 'An unexpected error occurred');
      setPhase('setup');
      setProgress(null);
    }
  };

  const handleCancel = () => {
    abortRef.current?.abort();
    setPhase('setup');
    setProgress(null);
  };

  const handleReset = () => {
    setPhase('setup');
    setResult(null);
    setProgress(null);
    setError(null);
    setSearchQuery('');
  };

  // ── Export CSV ────────────────────────────────────────────────────
  const exportDevicesCsv = () => {
    if (!result?.devices?.length) return;
    const rows = [['ad_id', 'device_days', 'postal_codes'].join(',')];
    for (const d of result.devices) {
      rows.push([d.adId, String(d.deviceDays), `"${d.postalCodes.join(';')}"`].join(','));
    }
    const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `zip-signals-${result.dataset}-${result.filters.country}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const copyAllMaids = () => {
    if (!result?.devices?.length) return;
    const ids = result.devices.map(d => d.adId).join('\n');
    navigator.clipboard.writeText(ids).then(() => {
      setCopiedAll(true);
      setTimeout(() => setCopiedAll(false), 2000);
    });
  };

  // ── Render ────────────────────────────────────────────────────────
  return (
    <MainLayout>
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center gap-3">
          <div className="bg-theme-accent/10 rounded-xl p-2.5">
            <MapPinned className="w-6 h-6 text-theme-accent" />
          </div>
          <div>
            <h1 className="text-2xl font-bold">Zip Code Signals</h1>
            <p className="text-sm text-muted-foreground">
              Find MAIDs whose residential origin matches your target postal codes
            </p>
          </div>
        </div>

        {/* ── SETUP PHASE ──────────────────────────────────────────── */}
        {phase === 'setup' && (
          <div className="grid gap-6 lg:grid-cols-3">
            {/* Left column: config */}
            <div className="lg:col-span-2 space-y-6">
              {/* Dataset + Country */}
              <Card>
                <CardHeader className="pb-4">
                  <CardTitle className="text-base flex items-center gap-2">
                    <Database className="w-4 h-4 text-theme-accent" />
                    Dataset & Country
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid gap-4 sm:grid-cols-2">
                    {/* Dataset selector */}
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wider">Dataset</Label>
                      {loadingDatasets ? (
                        <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                          <Loader2 className="w-4 h-4 animate-spin" /> Loading datasets...
                        </div>
                      ) : (
                        <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
                          <SelectTrigger>
                            <SelectValue placeholder="Select a dataset" />
                          </SelectTrigger>
                          <SelectContent>
                            {datasets.map(d => (
                              <SelectItem key={d.id} value={d.id}>
                                <span className="font-medium">{d.name}</span>
                                <span className="text-muted-foreground ml-2 text-xs">
                                  {formatBytes(d.totalBytes)}
                                </span>
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      )}
                    </div>

                    {/* Country selector */}
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wider">Country</Label>
                      <Select value={selectedCountry} onValueChange={setSelectedCountry}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select country" />
                        </SelectTrigger>
                        <SelectContent>
                          {COUNTRIES.map(code => (
                            <SelectItem key={code} value={code}>
                              {COUNTRY_FLAGS[code]} {code}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  {/* Date range (optional) */}
                  <div className="grid gap-4 sm:grid-cols-2">
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wider">Date from (optional)</Label>
                      <Input
                        type="date"
                        value={dateFrom}
                        onChange={e => setDateFrom(e.target.value)}
                        className="text-sm"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wider">Date to (optional)</Label>
                      <Input
                        type="date"
                        value={dateTo}
                        onChange={e => setDateTo(e.target.value)}
                        className="text-sm"
                      />
                    </div>
                  </div>

                  {/* Dataset info */}
                  {selectedDataset && (
                    <div className="bg-secondary/50 rounded-xl p-3 flex items-center gap-4 text-xs text-muted-foreground">
                      <span>{selectedDataset.objectCount} parquets</span>
                      <span>{formatBytes(selectedDataset.totalBytes)}</span>
                      {selectedDataset.dateRange && (
                        <span>
                          {selectedDataset.dateRange.from} &rarr; {selectedDataset.dateRange.to}
                        </span>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Postal codes input */}
              <Card>
                <CardHeader className="pb-4">
                  <CardTitle className="text-base flex items-center gap-2">
                    <MailPlus className="w-4 h-4 text-theme-accent" />
                    Postal Codes
                  </CardTitle>
                  <CardDescription>
                    Enter postal/zip codes. Paste a comma-separated list or type one at a time.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Chips area */}
                  <div className="min-h-[60px] border border-border rounded-xl p-2 flex flex-wrap gap-2 bg-background focus-within:ring-2 focus-within:ring-theme-accent/30 transition-all">
                    {postalCodes.map(code => (
                      <Badge
                        key={code}
                        variant="secondary"
                        className="h-7 px-2.5 gap-1.5 text-sm font-mono"
                      >
                        {code}
                        <button
                          onClick={() => removePostalCode(code)}
                          className="text-muted-foreground hover:text-foreground ml-0.5"
                        >
                          <X className="w-3 h-3" />
                        </button>
                      </Badge>
                    ))}
                    <input
                      type="text"
                      value={postalInput}
                      onChange={e => setPostalInput(e.target.value)}
                      onKeyDown={handlePostalKeyDown}
                      onPaste={handlePaste}
                      onBlur={() => { if (postalInput.trim()) addPostalCodes(postalInput); }}
                      placeholder={postalCodes.length ? 'Add more...' : 'Type or paste postal codes...'}
                      className="flex-1 min-w-[120px] bg-transparent border-none outline-none text-sm font-mono placeholder:text-muted-foreground/50 py-1"
                    />
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-xs text-muted-foreground">
                      {postalCodes.length} postal code{postalCodes.length !== 1 ? 's' : ''} added
                    </span>
                    {postalCodes.length > 0 && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setPostalCodes([])}
                        className="text-xs text-muted-foreground hover:text-foreground"
                      >
                        Clear all
                      </Button>
                    )}
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Right column: summary + run */}
            <div className="space-y-6">
              <Card className="border-theme-accent/30">
                <CardHeader className="pb-4">
                  <CardTitle className="text-base">Analysis Summary</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-3">
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">Dataset</span>
                      <span className="font-medium truncate max-w-[140px]">
                        {selectedDataset?.name || '—'}
                      </span>
                    </div>
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">Country</span>
                      <span className="font-medium">
                        {selectedCountry ? `${COUNTRY_FLAGS[selectedCountry] || ''} ${selectedCountry}` : '—'}
                      </span>
                    </div>
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-muted-foreground">Postal Codes</span>
                      <Badge variant="secondary" className="font-mono">
                        {postalCodes.length}
                      </Badge>
                    </div>
                    {(dateFrom || dateTo) && (
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-muted-foreground">Date Range</span>
                        <span className="text-xs font-mono">
                          {dateFrom || '...'} &rarr; {dateTo || '...'}
                        </span>
                      </div>
                    )}
                  </div>

                  <Separator />

                  <p className="text-xs text-muted-foreground leading-relaxed">
                    The analysis will find all mobile devices (MAIDs) whose daily residential
                    origin falls within your target postal codes, using first-ping-of-day
                    reverse geocoding.
                  </p>

                  <Button
                    onClick={handleRun}
                    disabled={!canRun}
                    className="w-full gap-2"
                    size="lg"
                  >
                    <Play className="w-4 h-4" />
                    Run Analysis
                  </Button>

                  {error && (
                    <div className="bg-red-500/10 border border-red-500/20 rounded-xl p-3 text-sm text-red-400 flex items-start gap-2">
                      <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
                      <span>{error}</span>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          </div>
        )}

        {/* ── RUNNING PHASE ────────────────────────────────────────── */}
        {phase === 'running' && progress && (
          <Card className="max-w-2xl mx-auto">
            <CardContent className="pt-8 pb-8 space-y-6">
              <div className="text-center space-y-2">
                <Loader2 className="w-10 h-10 text-theme-accent animate-spin mx-auto" />
                <h2 className="text-lg font-semibold">{progress.message}</h2>
                {progress.detail && (
                  <p className="text-sm text-muted-foreground">{progress.detail}</p>
                )}
              </div>

              {/* Progress bar */}
              <div className="space-y-2">
                <div className="w-full bg-muted rounded-full h-2.5">
                  <div
                    className="bg-theme-accent h-2.5 rounded-full transition-all duration-500"
                    style={{ width: `${progress.percent}%` }}
                  />
                </div>
                <div className="flex items-center justify-between text-xs text-muted-foreground">
                  <span>{progress.percent}%</span>
                  <span className="capitalize">{progress.step.replace(/_/g, ' ')}</span>
                </div>
              </div>

              {/* Step indicators */}
              <div className="flex items-center justify-center gap-2">
                {PROGRESS_STEPS.map(s => {
                  const stepIndex = PROGRESS_STEPS.findIndex(p => p.key === progress.step);
                  const thisIndex = PROGRESS_STEPS.findIndex(p => p.key === s.key);
                  const isDone = thisIndex < stepIndex;
                  const isActive = s.key === progress.step;
                  return (
                    <div
                      key={s.key}
                      className={`px-2.5 py-1 rounded-lg text-xs font-medium transition-all ${
                        isDone
                          ? 'bg-emerald-400/10 text-emerald-400'
                          : isActive
                          ? 'bg-theme-accent/10 text-theme-accent'
                          : 'bg-muted text-muted-foreground'
                      }`}
                    >
                      {isDone ? <CheckCircle className="w-3 h-3 inline mr-1" /> : null}
                      {s.label}
                    </div>
                  );
                })}
              </div>

              <div className="text-center">
                <Button variant="outline" size="sm" onClick={handleCancel} className="gap-2">
                  <X className="w-3 h-3" /> Cancel
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        {/* ── RESULTS PHASE ────────────────────────────────────────── */}
        {phase === 'results' && result && (
          <div className="space-y-6">
            {/* Summary cards */}
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              <Card>
                <CardContent className="pt-5 pb-4">
                  <div className="flex items-center gap-3">
                    <div className="bg-theme-accent/10 rounded-lg p-2">
                      <Users className="w-5 h-5 text-theme-accent" />
                    </div>
                    <div>
                      <p className="text-2xl font-bold">{formatNumber(result.summary.totalMaids)}</p>
                      <p className="text-xs text-muted-foreground">MAIDs Found</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="pt-5 pb-4">
                  <div className="flex items-center gap-3">
                    <div className="bg-emerald-400/10 rounded-lg p-2">
                      <MapPinned className="w-5 h-5 text-emerald-400" />
                    </div>
                    <div>
                      <p className="text-2xl font-bold">
                        {result.coverage.postalCodesWithDevices}/{result.coverage.postalCodesRequested}
                      </p>
                      <p className="text-xs text-muted-foreground">Postal Codes w/ Data</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="pt-5 pb-4">
                  <div className="flex items-center gap-3">
                    <div className="bg-blue-400/10 rounded-lg p-2">
                      <Database className="w-5 h-5 text-blue-400" />
                    </div>
                    <div>
                      <p className="text-2xl font-bold">{formatNumber(result.coverage.totalDevicesInDataset)}</p>
                      <p className="text-xs text-muted-foreground">Total Devices in Dataset</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardContent className="pt-5 pb-4">
                  <div className="flex items-center gap-3">
                    <div className="bg-amber-400/10 rounded-lg p-2">
                      <Hash className="w-5 h-5 text-amber-400" />
                    </div>
                    <div>
                      <p className="text-2xl font-bold">{formatNumber(result.coverage.matchedDeviceDays)}</p>
                      <p className="text-xs text-muted-foreground">Matched Device-Days</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Tabs: Devices / Postal Breakdown */}
            <Card>
              <Tabs defaultValue="devices">
                <CardHeader className="pb-0">
                  <div className="flex items-center justify-between flex-wrap gap-3">
                    <TabsList>
                      <TabsTrigger value="devices" className="gap-1.5">
                        <Users className="w-3.5 h-3.5" /> Devices
                      </TabsTrigger>
                      <TabsTrigger value="postal" className="gap-1.5">
                        <BarChart3 className="w-3.5 h-3.5" /> Postal Breakdown
                      </TabsTrigger>
                    </TabsList>

                    <div className="flex items-center gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={copyAllMaids}
                        className="gap-1.5 text-xs"
                      >
                        <Copy className="w-3.5 h-3.5" />
                        {copiedAll ? 'Copied!' : 'Copy MAIDs'}
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={exportDevicesCsv}
                        className="gap-1.5 text-xs"
                      >
                        <FileDown className="w-3.5 h-3.5" /> Export CSV
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={handleReset}
                        className="gap-1.5 text-xs"
                      >
                        <RotateCcw className="w-3.5 h-3.5" /> New Search
                      </Button>
                    </div>
                  </div>
                </CardHeader>

                {/* Devices tab */}
                <TabsContent value="devices" className="p-0">
                  <CardContent className="pt-4">
                    {/* Search */}
                    <div className="relative mb-4">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                      <Input
                        placeholder="Search by MAID or postal code..."
                        value={searchQuery}
                        onChange={e => setSearchQuery(e.target.value)}
                        className="pl-9 text-sm"
                      />
                    </div>

                    <div className="text-xs text-muted-foreground mb-2">
                      Showing {formatNumber(filteredDevices.length)} of {formatNumber(result.devices.length)} devices
                    </div>

                    <div className="rounded-xl border overflow-hidden">
                      <div className="max-h-[500px] overflow-y-auto">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead className="w-[50px] text-xs">#</TableHead>
                              <TableHead className="text-xs">Ad ID (MAID)</TableHead>
                              <TableHead className="text-xs text-right">Device Days</TableHead>
                              <TableHead className="text-xs">Postal Codes</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {filteredDevices.length === 0 ? (
                              <TableRow>
                                <TableCell colSpan={4} className="text-center text-muted-foreground py-8">
                                  {searchQuery ? 'No matches found' : 'No devices found'}
                                </TableCell>
                              </TableRow>
                            ) : (
                              filteredDevices.slice(0, 500).map((d, i) => (
                                <TableRow key={d.adId}>
                                  <TableCell className="text-xs text-muted-foreground">{i + 1}</TableCell>
                                  <TableCell className="font-mono text-xs">{d.adId}</TableCell>
                                  <TableCell className="text-right text-sm font-medium">{d.deviceDays}</TableCell>
                                  <TableCell>
                                    <div className="flex flex-wrap gap-1">
                                      {d.postalCodes.map(pc => (
                                        <Badge key={pc} variant="secondary" className="text-xs font-mono px-1.5 py-0">
                                          {pc}
                                        </Badge>
                                      ))}
                                    </div>
                                  </TableCell>
                                </TableRow>
                              ))
                            )}
                          </TableBody>
                        </Table>
                      </div>
                      {filteredDevices.length > 500 && (
                        <div className="text-center text-xs text-muted-foreground py-3 border-t">
                          Showing first 500 of {formatNumber(filteredDevices.length)} devices. Export CSV for full list.
                        </div>
                      )}
                    </div>
                  </CardContent>
                </TabsContent>

                {/* Postal breakdown tab */}
                <TabsContent value="postal" className="p-0">
                  <CardContent className="pt-4">
                    <div className="rounded-xl border overflow-hidden">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead className="text-xs">Postal Code</TableHead>
                            <TableHead className="text-xs text-right">Devices</TableHead>
                            <TableHead className="text-xs text-right">Device Days</TableHead>
                            <TableHead className="text-xs">Distribution</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {result.postalCodeBreakdown.length === 0 ? (
                            <TableRow>
                              <TableCell colSpan={4} className="text-center text-muted-foreground py-8">
                                No postal code data
                              </TableCell>
                            </TableRow>
                          ) : (
                            result.postalCodeBreakdown.map(pc => {
                              const maxDevices = result.postalCodeBreakdown[0]?.devices || 1;
                              const barWidth = (pc.devices / maxDevices) * 100;
                              return (
                                <TableRow key={pc.postalCode}>
                                  <TableCell className="font-mono text-sm font-medium">{pc.postalCode}</TableCell>
                                  <TableCell className="text-right text-sm">{formatNumber(pc.devices)}</TableCell>
                                  <TableCell className="text-right text-sm text-muted-foreground">{formatNumber(pc.deviceDays)}</TableCell>
                                  <TableCell className="w-[200px]">
                                    <div className="flex items-center gap-2">
                                      <div className="flex-1 bg-muted rounded-full h-2">
                                        <div
                                          className="bg-theme-accent h-2 rounded-full transition-all"
                                          style={{ width: `${barWidth}%` }}
                                        />
                                      </div>
                                      <span className="text-xs text-muted-foreground w-10 text-right">
                                        {pc.devices > 0 ? `${((pc.devices / result.summary.totalMaids) * 100).toFixed(1)}%` : '0%'}
                                      </span>
                                    </div>
                                  </TableCell>
                                </TableRow>
                              );
                            })
                          )}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </TabsContent>
              </Tabs>
            </Card>

            {/* Methodology note */}
            <Card>
              <CardContent className="py-4">
                <div className="flex items-start gap-3 text-xs text-muted-foreground">
                  <Globe className="w-4 h-4 mt-0.5 shrink-0 text-theme-accent" />
                  <div>
                    <p className="font-medium text-foreground mb-1">Methodology</p>
                    <p>{result.methodology.description}</p>
                    <p className="mt-1">
                      Accuracy threshold: {result.methodology.accuracyThresholdMeters}m &middot;
                      Coordinate precision: {result.methodology.coordinatePrecision} decimals (~11m)
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </MainLayout>
  );
}
