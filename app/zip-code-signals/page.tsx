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
  ScrollText,
  Zap,
  MapPin,
  Clock,
  Sparkles,
  Award,
  Activity,
  Building2,
} from 'lucide-react';
import type { PostalMaidResult, PostalMaidDevice, ZipSignature } from '@/lib/postal-maid-types';

// ── Types ─────────────────────────────────────────────────────────────
interface DatasetInfo {
  id: string;
  name: string;
  jobId: string | null;
  objectCount: number;
  totalBytes: number;
  dateRange: { from: string; to: string } | null;
  syncedAt: string | null;
  country: string | null;
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

interface ProgressLogEntry {
  elapsedMs: number;
  step: string;
  percent: number;
  message: string;
  detail?: string;
}

function formatElapsed(ms: number): string {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const r = s % 60;
  if (m <= 0) return `${r}s`;
  return `${m}m ${r.toString().padStart(2, '0')}s`;
}

// ── Main page ─────────────────────────────────────────────────────────
export default function ZipCodeSignalsPage() {
  // Phase
  const [phase, setPhase] = useState<Phase>('setup');

  // Datasets
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [loadingDatasets, setLoadingDatasets] = useState(true);
  // Megajobs (consolidated only — needed to reuse the MAIDs CSV for the
  // β fast path that skips the per-table POI scan).
  const [megaJobs, setMegaJobs] = useState<Array<{
    id: string;
    name: string;
    country?: string;
    syncedSubJobs: number;
    hasMaids: boolean;
  }>>([]);

  // Config — selected source ID is prefixed: "ds:<dataset>" or "mj:<megaJobId>".
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
  const [progressLog, setProgressLog] = useState<ProgressLogEntry[]>([]);
  const [logCopied, setLogCopied] = useState(false);
  const runStartedAtRef = useRef<number>(0);
  const lastProgressRef = useRef<{ percent: number; message: string } | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  // Results
  const [result, setResult] = useState<PostalMaidResult | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [copiedAll, setCopiedAll] = useState(false);

  // ── Fetch datasets + megajobs (only when country is selected) ──────
  useEffect(() => {
    if (!selectedCountry) {
      setDatasets([]);
      setMegaJobs([]);
      setSelectedDatasetId('');
      setLoadingDatasets(false);
      return;
    }
    setLoadingDatasets(true);
    setSelectedDatasetId('');
    Promise.all([
      fetch('/api/datasets', { credentials: 'include' }).then(r => r.json()).catch(() => ({})),
      fetch('/api/mega-jobs', { credentials: 'include' }).then(r => r.json()).catch(() => ([])),
    ]).then(([dsData, mjData]) => {
      const allMega = Array.isArray(mjData) ? mjData : (mjData?.megaJobs || []);
      // Sub-jobs that live INSIDE any megajob — consolidated or not.
      // We hide them from the "single datasets" list because:
      //   - consolidated megajobs cover them (use the megajob entry instead)
      //   - non-consolidated megajobs are unfinished work and orphan
      //     sub-jobs are misleading to surface separately.
      const subJobIdsInAnyMegajob = new Set<string>();
      for (const m of allMega) {
        for (const sub of (m.subJobIds || [])) subJobIdsInAnyMegajob.add(sub);
      }
      const ds = (dsData?.datasets || []).filter((d: any) =>
        d.jobId && d.objectCount > 0 && d.totalBytes > 0 &&
        d.country === selectedCountry &&
        !subJobIdsInAnyMegajob.has(d.jobId)
      );
      setDatasets(ds);
      // Megajobs: filter by lightweight status='completed' (proxy of
      // "consolidation done") because /api/mega-jobs returns the index
      // which intentionally STRIPS the heavy consolidatedReports field.
      // The strict consolidatedReports.maids check still runs server-side
      // when the analyzer kicks off — catches the rare "completed but
      // maids materialization failed" case with a clear error.
      const mjList = allMega
        .filter((m: any) =>
          m.country === selectedCountry &&
          m.status === 'completed',
        )
        .map((m: any) => ({
          id: m.megaJobId,
          name: m.name,
          country: m.country,
          syncedSubJobs: m.progress?.synced ?? 0,
          hasMaids: true, // assumed when status=completed; analyzer verifies
        }));
      setMegaJobs(mjList);
      setLoadingDatasets(false);
    }).catch(() => setLoadingDatasets(false));
  }, [selectedCountry]);

  // ── Derived ───────────────────────────────────────────────────────
  // selectedDatasetId is prefixed: "ds:<id>" for datasets, "mj:<id>" for megajobs.
  const isMegaJob = selectedDatasetId.startsWith('mj:');
  const isDataset = selectedDatasetId.startsWith('ds:');
  const rawSourceId = selectedDatasetId.slice(3);
  const selectedDataset = isDataset ? datasets.find(d => d.id === rawSourceId) : undefined;
  const selectedMegaJob = isMegaJob ? megaJobs.find(m => m.id === rawSourceId) : undefined;
  const sourceLabel = selectedDataset?.name || selectedMegaJob?.name || '';

  const canRun = !!(
    (isDataset || isMegaJob) &&
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

  useEffect(() => {
    if (phase === 'running' && progressLog.length > 0) {
      logEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
    }
  }, [phase, progressLog]);

  const appendProgressLog = (data: { step: string; percent: number; message: string; detail?: string }) => {
    const t0 = runStartedAtRef.current || Date.now();
    const elapsedMs = Date.now() - t0;
    setProgressLog(prev => [
      ...prev,
      {
        elapsedMs,
        step: data.step,
        percent: data.percent,
        message: data.message,
        detail: data.detail,
      },
    ]);
  };

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
    if (!canRun) return;

    /** Empty date fields default to dataset catalog range when known so Athena
     *  doesn't scan 100+ GB unpartitioned. Megajobs don't expose a dataset-level
     *  range here, so we let the backend default unless the user picks dates. */
    const effDateFrom = (dateFrom || selectedDataset?.dateRange?.from || '').trim();
    const effDateTo = (dateTo || selectedDataset?.dateRange?.to || '').trim();

    setPhase('running');
    setError(null);
    setResult(null);
    setProgressLog([]);
    setLogCopied(false);
    runStartedAtRef.current = Date.now();
    lastProgressRef.current = { percent: 0, message: 'Starting...' };
    const runMeta = {
      source: isMegaJob ? `megajob:${rawSourceId}` : `dataset:${rawSourceId}`,
      country: selectedCountry,
      postalCount: postalCodes.length,
      dateFrom: effDateFrom || null,
      dateTo: effDateTo || null,
    };
    setProgress({ step: 'initializing', percent: 0, message: 'Starting...' });
    appendProgressLog({
      step: 'initializing',
      percent: 0,
      message: 'Run started (client)',
      detail: `${runMeta.source} · ${runMeta.country} · ${runMeta.postalCount} postal code(s)${
        runMeta.dateFrom || runMeta.dateTo
          ? ` · Athena dates ${runMeta.dateFrom ?? '…'} → ${runMeta.dateTo ?? '…'}`
          : ' · Athena dates: full table (all partitions)'
      }`,
    });

    const abort = new AbortController();
    abortRef.current = abort;

    const resolveSpillIfNeeded = async (data: PostalMaidResult): Promise<PostalMaidResult> => {
      if (!data.devicesSpillKey) return data;
      appendProgressLog({
        step: 'running_queries',
        percent: 99,
        message: 'Fetching full device list from storage…',
        detail: `${data.devicesSpillTotal?.toLocaleString?.() ?? '?'} MAIDs (spilled payload)`,
      });
      const r = await fetch(
        `/api/zip-code-signals/spill?key=${encodeURIComponent(data.devicesSpillKey)}`,
        { credentials: 'include' },
      );
      if (!r.ok) {
        let msg = `Spill download HTTP ${r.status}`;
        try {
          const j = await r.json() as { error?: string };
          if (j?.error) msg = j.error;
        } catch { /* ignore */ }
        throw new Error(msg);
      }
      return r.json() as Promise<PostalMaidResult>;
    };

    try {
      const response = await fetch('/api/zip-code-signals/analyze/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          // Branch on selectedDatasetId prefix — backend dispatches to
          // analyzePostalMaid (single dataset) or analyzePostalMaidMegaJob
          // (megajob, reuses consolidated MAIDs CSV).
          ...(isMegaJob ? { megaJobId: rawSourceId } : { datasetName: rawSourceId }),
          postalCodes,
          country: selectedCountry,
          dateFrom: effDateFrom || undefined,
          dateTo: effDateTo || undefined,
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
                lastProgressRef.current = { percent: 0, message: data.message || 'error' };
                appendProgressLog({
                  step: 'error',
                  percent: 0,
                  message: data.message || 'Analysis failed',
                  detail: data.detail,
                });
              } else {
                lastProgressRef.current = {
                  percent: data.percent,
                  message: data.message,
                };
                setProgress(data);
                appendProgressLog(data);
              }
            } else if (eventType === 'result') {
              gotResult = true;
              appendProgressLog({
                step: 'completed',
                percent: 100,
                message: 'Result packet received',
                detail: `${Array.isArray(data?.devices) ? data.devices.length : 0} MAID row(s) in SSE${data?.devicesSpillKey ? ' (preview — loading spill…)' : ''}`,
              });
              const merged = await resolveSpillIfNeeded(data as PostalMaidResult);
              appendProgressLog({
                step: 'completed',
                percent: 100,
                message: 'Result ready',
                detail: `${merged.devices?.length?.toLocaleString?.() ?? 0} MAID row(s)`,
              });
              setResult(merged);
              setPhase('results');
              setProgress(null);
            }
          } catch (parseErr: unknown) {
            const pe = parseErr instanceof Error ? parseErr.message : String(parseErr);
            if (eventType === 'result') {
              const hint = `Could not parse result JSON (${dataStr.length} chars). The response may be truncated, or the browser ran out of memory.`;
              backendError = hint;
              appendProgressLog({
                step: 'error',
                percent: 0,
                message: hint,
                detail: pe.slice(0, 400),
              });
            } else {
              appendProgressLog({
                step: 'error',
                percent: 0,
                message: `Could not parse ${eventType} event`,
                detail: pe.slice(0, 400),
              });
            }
          }
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
              appendProgressLog({
                step: 'error',
                percent: 0,
                message: data.message || 'Analysis failed',
                detail: data.detail,
              });
            } else if (eventType === 'result') {
              gotResult = true;
              appendProgressLog({
                step: 'completed',
                percent: 100,
                message: 'Result packet received (tail buffer)',
                detail: `${Array.isArray(data?.devices) ? data.devices.length : 0} MAID row(s)${data?.devicesSpillKey ? ' — loading spill…' : ''}`,
              });
              const merged = await resolveSpillIfNeeded(data as PostalMaidResult);
              setResult(merged);
              setPhase('results');
              setProgress(null);
            }
          } catch (parseErr: unknown) {
            const pe = parseErr instanceof Error ? parseErr.message : String(parseErr);
            if (eventType === 'result') {
              backendError = `Tail buffer: could not parse result (${dataStr.length} chars): ${pe.slice(0, 200)}`;
              appendProgressLog({ step: 'error', percent: 0, message: backendError, detail: pe.slice(0, 400) });
            }
          }
        }
      }

      if (!gotResult && backendError) {
        throw new Error(backendError);
      }
      if (!gotResult) {
        throw new Error(
          'The server closed the stream without a result. Typical causes: (1) Vercel/server time limit — use the date range (defaults now match the dataset catalog); (2) network timeout; (3) server error — check the activity log for the last step. This is not the same as "zero postal matches."',
        );
      }
    } catch (err: any) {
      const last = lastProgressRef.current;
      if (err.name === 'AbortError') {
        appendProgressLog({
          step: 'cancelled',
          percent: last?.percent ?? 0,
          message: 'Request aborted (cancel button, timeout, or closed connection)',
          detail: last?.message,
        });
        return;
      }
      appendProgressLog({
        step: 'error',
        percent: last?.percent ?? 0,
        message: err.message || 'An unexpected error occurred',
        detail: 'Client-side failure after last server update',
      });
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
    setProgressLog([]);
    setLogCopied(false);
    setError(null);
    setSearchQuery('');
  };

  const copyProgressLog = async () => {
    const lines = progressLog.map(
      e =>
        `[+${formatElapsed(e.elapsedMs)}] ${e.percent}% · ${e.step} · ${e.message}${
          e.detail ? ` — ${e.detail}` : ''
        }`,
    );
    try {
      await navigator.clipboard.writeText(lines.join('\n'));
      setLogCopied(true);
      setTimeout(() => setLogCopied(false), 2000);
    } catch {
      setLogCopied(false);
    }
  };

  // ── Export CSV ────────────────────────────────────────────────────
  const exportDevicesCsv = () => {
    if (!result?.devices?.length) return;
    // Enriched columns when FULL schema fast-path produced extras
    const enriched = !!result.methodology?.fastPath;
    const headers = enriched
      ? ['ad_id', 'device_days', 'postal_codes', 'region', 'city', 'quality_tier', 'overnight_presence']
      : ['ad_id', 'device_days', 'postal_codes'];
    const rows = [headers.join(',')];
    const csvCell = (s: string) => /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    for (const d of result.devices) {
      const base = [d.adId, String(d.deviceDays), csvCell(d.postalCodes.join(';'))];
      if (enriched) {
        base.push(csvCell(d.region || ''));
        base.push(csvCell(d.city || ''));
        base.push(d.qualityTier || '');
        base.push(d.overnightPresence ? 'true' : 'false');
      }
      rows.push(base.join(','));
    }
    const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    const suffix = enriched ? '-enriched' : '';
    a.href = url;
    a.download = `zip-signals${suffix}-${result.dataset}-${result.filters.country}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const exportZipSignaturesCsv = () => {
    const sigs = result?.fullSchema?.zipSignatures;
    if (!sigs?.length) return;
    const headers = [
      'postal_code', 'region', 'top_cities', 'devices', 'device_days',
      'peak_hour_bucket', 'weekend_share', 'overnight_share',
      'quality_tier', 'gps_share', 'avg_circle_score',
      'persistence_once', 'persistence_casual', 'persistence_regular', 'persistence_resident',
      'centroid_lat', 'centroid_lng', 'top_h3_lat', 'top_h3_lng', 'top_h3_devices',
    ];
    const csvCell = (s: string) => /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    const rows = [headers.join(',')];
    for (const s of sigs) {
      const top = s.topH3Cells[0];
      rows.push([
        s.postalCode, csvCell(s.region || ''),
        csvCell(s.topCities.map((c) => `${c.city}:${c.devices}`).join(';')),
        String(s.devices), String(s.deviceDays),
        s.peakHourBucket,
        s.weekendShare.toFixed(3),
        s.overnightShare.toFixed(3),
        s.qualityTier,
        s.gpsShare.toFixed(3),
        s.avgCircleScore.toFixed(3),
        String(s.persistence.onceOnly), String(s.persistence.casual),
        String(s.persistence.regular), String(s.persistence.resident),
        s.centroid.lat.toFixed(6), s.centroid.lng.toFixed(6),
        top ? top.lat.toFixed(6) : '', top ? top.lng.toFixed(6) : '', top ? String(top.devices) : '',
      ].join(','));
    }
    const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `zip-signatures-${result!.dataset}-${result!.filters.country}-${new Date().toISOString().slice(0, 10)}.csv`;
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
                    {/* Country selector (step 1) */}
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wider">1. Country</Label>
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

                    {/* Dataset selector (step 2, filtered by country) */}
                    <div className="space-y-2">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wider">2. Dataset</Label>
                      {!selectedCountry ? (
                        <div className="text-sm text-muted-foreground py-2">
                          Select a country first
                        </div>
                      ) : loadingDatasets ? (
                        <div className="flex items-center gap-2 text-sm text-muted-foreground py-2">
                          <Loader2 className="w-4 h-4 animate-spin" /> Loading datasets...
                        </div>
                      ) : datasets.length === 0 && megaJobs.length === 0 ? (
                        <div className="text-sm text-muted-foreground py-2">
                          No datasets or consolidated megajobs for {COUNTRY_FLAGS[selectedCountry]} {selectedCountry}
                        </div>
                      ) : (
                        <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
                          <SelectTrigger>
                            <SelectValue placeholder="Select a dataset or megajob" />
                          </SelectTrigger>
                          <SelectContent>
                            {/* Megajobs first — visually distinguished as a group */}
                            {megaJobs.length > 0 && (
                              <>
                                <div className="px-2 py-1.5 text-[10px] uppercase tracking-wider text-muted-foreground/70 font-medium">
                                  ⚡ Megajobs (consolidated · β fast path)
                                </div>
                                {megaJobs.map(m => (
                                  <SelectItem key={`mj:${m.id}`} value={`mj:${m.id}`}>
                                    <span className="font-medium">{m.name}</span>
                                    <span className="text-muted-foreground ml-2 text-xs">
                                      {m.syncedSubJobs} sub-jobs
                                    </span>
                                  </SelectItem>
                                ))}
                                <div className="border-t my-1" />
                              </>
                            )}
                            {datasets.length > 0 && (
                              <>
                                <div className="px-2 py-1.5 text-[10px] uppercase tracking-wider text-muted-foreground/70 font-medium">
                                  Single datasets
                                </div>
                                {datasets.map(d => (
                                  <SelectItem key={`ds:${d.id}`} value={`ds:${d.id}`}>
                                    <span className="font-medium">{d.name}</span>
                                    <span className="text-muted-foreground ml-2 text-xs">
                                      {formatBytes(d.totalBytes)}
                                    </span>
                                  </SelectItem>
                                ))}
                              </>
                            )}
                          </SelectContent>
                        </Select>
                      )}
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
                  <p className="text-xs text-muted-foreground leading-relaxed">
                    If you leave dates blank, the run uses this dataset&apos;s catalog range above (recommended).
                    Omitting that filter forces Athena over all partitions and often hits server time limits on large jobs.
                  </p>

                  {/* Source info */}
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
                  {selectedMegaJob && (
                    <div className="bg-amber-500/10 border border-amber-500/30 rounded-xl p-3 flex items-center gap-3 text-xs">
                      <span className="px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-500 border border-amber-500/40 font-medium text-[10px]">⚡ FAST PATH</span>
                      <span className="text-muted-foreground">
                        Reusing the consolidated MAIDs CSV — skips the per-table POI scan.
                      </span>
                      <span className="text-muted-foreground ml-auto">{selectedMegaJob.syncedSubJobs} sub-jobs</span>
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

                  {selectedDataset && selectedDataset.totalBytes >= 40 * 1024 ** 3 && (
                    <div className="rounded-lg border border-amber-500/25 bg-amber-500/10 px-3 py-2.5 text-xs text-amber-100/90 leading-relaxed space-y-1.5">
                      <p className="font-medium text-amber-100">Very large dataset (~{formatBytes(selectedDataset.totalBytes)})</p>
                      <p>
                        Zip Code Signals runs in a <strong>single serverless request</strong> (on Vercel Pro often capped around <strong>5 minutes</strong>).
                        Athena + geocoding can exceed that even with the default catalog date window — you may see a dropped connection, not “zero matches.”
                      </p>
                      <p className="text-amber-100/80">
                        Mitigations: shorten the date range in the form; set env <code className="text-[10px] px-1 rounded bg-black/30">POSTAL_MAID_SQL_ROW_LIMIT</code> for a capped (biased) sample;
                        run <code className="text-[10px] px-1 rounded bg-black/30">analyzePostalMaid</code> from a long-lived worker; or add an <strong>async job + poll</strong> flow (same idea as dataset reports).
                      </p>
                    </div>
                  )}

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

              {/* Chronological activity log (every SSE progress + client milestones) */}
              <div className="rounded-xl border border-border/60 bg-muted/20 overflow-hidden text-left">
                <div className="flex items-center justify-between gap-2 px-3 py-2 border-b border-border/50 bg-muted/40">
                  <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
                    <ScrollText className="w-3.5 h-3.5 shrink-0 opacity-80" />
                    <span>Activity log</span>
                    <span className="font-normal opacity-70 hidden sm:inline">
                      — elapsed time · % · step · message
                    </span>
                  </div>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-7 text-xs shrink-0"
                    disabled={progressLog.length === 0}
                    onClick={copyProgressLog}
                  >
                    {logCopied ? 'Copied' : 'Copy log'}
                  </Button>
                </div>
                <div className="max-h-56 overflow-y-auto px-3 py-2 font-mono text-[11px] leading-snug space-y-2.5">
                  {progressLog.length === 0 ? (
                    <p className="text-muted-foreground italic">Waiting for first event…</p>
                  ) : (
                    progressLog.map((e, i) => (
                      <div
                        key={`${e.elapsedMs}-${i}-${e.step}`}
                        className="border-l-2 border-border/70 pl-2.5 -ml-px"
                      >
                        <div className="flex flex-wrap items-baseline gap-x-1.5 gap-y-0.5">
                          <span className="text-theme-accent tabular-nums shrink-0">
                            +{formatElapsed(e.elapsedMs)}
                          </span>
                          <span className="text-muted-foreground/80">{e.percent}%</span>
                          <span className="text-muted-foreground">·</span>
                          <span className="font-medium text-foreground/95">{e.step}</span>
                        </div>
                        <p className="text-foreground/90 mt-0.5">{e.message}</p>
                        {e.detail ? (
                          <p className="text-muted-foreground text-[10px] mt-0.5 break-words">
                            {e.detail}
                          </p>
                        ) : null}
                      </div>
                    ))
                  )}
                  <div ref={logEndRef} className="h-px" aria-hidden />
                </div>
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

            {/* ── FULL schema fast-path enrichment ──────────────────── */}
            {result.methodology?.fastPath && result.fullSchema && (
              <FullSchemaEnrichmentSection
                fullSchema={result.fullSchema}
                onExportZipCsv={exportZipSignaturesCsv}
              />
            )}

            {progressLog.length > 0 && (
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between gap-2">
                    <CardTitle className="text-base flex items-center gap-2">
                      <ScrollText className="w-4 h-4 text-muted-foreground" />
                      Run activity log
                    </CardTitle>
                    <Button type="button" variant="outline" size="sm" className="h-8 text-xs" onClick={copyProgressLog}>
                      {logCopied ? 'Copied' : 'Copy log'}
                    </Button>
                  </div>
                  <CardDescription>
                    Timeline of server progress events for this run (same as during loading).
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="max-h-48 overflow-y-auto rounded-lg border border-border/60 bg-muted/20 px-3 py-2 font-mono text-[11px] leading-snug space-y-2">
                    {progressLog.map((e, i) => (
                      <div key={`${e.elapsedMs}-${i}-res-${e.step}`} className="border-l-2 border-border/70 pl-2.5">
                        <div className="flex flex-wrap items-baseline gap-x-1.5">
                          <span className="text-theme-accent tabular-nums">+{formatElapsed(e.elapsedMs)}</span>
                          <span className="text-muted-foreground/80">{e.percent}%</span>
                          <span className="text-muted-foreground">·</span>
                          <span className="font-medium">{e.step}</span>
                        </div>
                        <p className="text-foreground/90 mt-0.5">{e.message}</p>
                        {e.detail ? (
                          <p className="text-muted-foreground text-[10px] mt-0.5 break-words">{e.detail}</p>
                        ) : null}
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

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
                    <div className="flex items-center gap-2 mb-1">
                      <p className="font-medium text-foreground">Methodology</p>
                      {result.methodology.fastPath && (
                        <span className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-500/15 text-amber-500 border border-amber-500/30">
                          <Zap className="w-3 h-3" /> FULL fast path
                        </span>
                      )}
                    </div>
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

// ── FULL-schema fast-path enrichment ──────────────────────────────────

const HOUR_BUCKET_LABELS: Record<string, string> = {
  morning: '5am–10am',
  midday: '11am–1pm',
  afternoon: '2pm–5pm',
  evening: '6pm–9pm',
  night: '10pm–4am',
};

const QUALITY_TIER_COLORS: Record<string, string> = {
  high: 'bg-emerald-500/15 text-emerald-500 border-emerald-500/30',
  mixed: 'bg-amber-500/15 text-amber-500 border-amber-500/30',
  low: 'bg-rose-500/15 text-rose-500 border-rose-500/30',
};

function fmtPct(n: number): string {
  return `${(Math.max(0, Math.min(1, n)) * 100).toFixed(0)}%`;
}

function fmtNumCompact(n: number): string {
  if (!Number.isFinite(n)) return '0';
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return Math.round(n).toLocaleString();
}

function FullSchemaEnrichmentSection({
  fullSchema,
  onExportZipCsv,
}: {
  fullSchema: NonNullable<PostalMaidResult['fullSchema']>;
  onExportZipCsv: () => void;
}) {
  const { zipSignatures, regionSummary, qualityHistogram } = fullSchema;
  const totalQuality = qualityHistogram.high + qualityHistogram.medium + qualityHistogram.low || 1;

  return (
    <Card className="border-amber-500/30 bg-gradient-to-br from-amber-500/5 to-transparent">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div>
            <CardTitle className="text-base flex items-center gap-2">
              <Zap className="w-4 h-4 text-amber-500" />
              FULL schema enrichment
              <span className="text-[10px] font-normal px-1.5 py-0.5 rounded bg-amber-500/15 text-amber-500 border border-amber-500/30">
                fast path
              </span>
            </CardTitle>
            <CardDescription className="text-xs mt-1">
              Per-ZIP signatures derived from <code>geo_fields</code> directly — no Node geocoding step.
              Includes region, top cities, peak hour, persistence, quality tier, and sub-ZIP H3 hotspots.
            </CardDescription>
          </div>
          <Button variant="outline" size="sm" onClick={onExportZipCsv} className="gap-1.5 text-xs">
            <FileDown className="w-3.5 h-3.5" /> Export ZIP signatures
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-5">
        {/* Top-level histograms */}
        <div className="grid gap-3 md:grid-cols-2">
          {/* Quality histogram */}
          <div className="rounded-lg border bg-card/40 p-3">
            <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground mb-2">
              <Award className="w-3.5 h-3.5" /> Device quality distribution
            </div>
            <div className="space-y-1.5">
              {(['high', 'medium', 'low'] as const).map((tier) => {
                const n = qualityHistogram[tier];
                const pct = (n / totalQuality) * 100;
                const cls = QUALITY_TIER_COLORS[tier === 'medium' ? 'mixed' : tier];
                return (
                  <div key={tier} className="flex items-center gap-2 text-xs">
                    <span className={`px-1.5 py-0.5 rounded text-[10px] border w-16 text-center ${cls}`}>{tier}</span>
                    <div className="flex-1 h-2 bg-muted rounded overflow-hidden">
                      <div
                        className={`h-full ${tier === 'high' ? 'bg-emerald-500' : tier === 'medium' ? 'bg-amber-500' : 'bg-rose-500'}`}
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                    <span className="tabular-nums w-12 text-right">{fmtNumCompact(n)}</span>
                    <span className="text-muted-foreground tabular-nums w-10 text-right">{pct.toFixed(0)}%</span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Region rollup */}
          <div className="rounded-lg border bg-card/40 p-3">
            <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground mb-2">
              <Building2 className="w-3.5 h-3.5" /> Region rollup
              <span className="text-[10px] font-normal opacity-70">
                ({regionSummary.length} regions)
              </span>
            </div>
            <div className="space-y-1 max-h-40 overflow-y-auto">
              {regionSummary.length === 0 ? (
                <p className="text-xs text-muted-foreground italic">No region data.</p>
              ) : regionSummary.slice(0, 10).map((r) => (
                <div key={r.region} className="flex items-center gap-2 text-xs">
                  <span className="flex-1 truncate">{r.region}</span>
                  <span className="text-muted-foreground tabular-nums w-10 text-right">{r.zips} ZIPs</span>
                  <div className="w-16 h-1.5 bg-muted rounded overflow-hidden">
                    <div className="h-full bg-blue-500" style={{ width: `${r.shareOfTotal * 100}%` }} />
                  </div>
                  <span className="tabular-nums w-12 text-right font-medium">{fmtNumCompact(r.devices)}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Per-ZIP signature cards */}
        <div>
          <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground mb-2">
            <Sparkles className="w-3.5 h-3.5" /> Per-ZIP signatures
            <span className="text-[10px] font-normal opacity-70">
              ({zipSignatures.length} ZIPs with data)
            </span>
          </div>
          <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
            {zipSignatures.slice(0, 24).map((s) => (
              <ZipSignatureCard key={s.postalCode} sig={s} />
            ))}
          </div>
          {zipSignatures.length > 24 && (
            <p className="text-xs text-muted-foreground mt-3 text-center">
              Showing top 24 of {zipSignatures.length} ZIPs · download the CSV for the full list.
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function ZipSignatureCard({ sig }: { sig: ZipSignature }) {
  const totalDD = sig.deviceDays || 1;
  const buckets = sig.hourBuckets;
  const maxBucket = Math.max(buckets.morning, buckets.midday, buckets.afternoon, buckets.evening, buckets.night) || 1;
  const persistTotal =
    sig.persistence.onceOnly + sig.persistence.casual + sig.persistence.regular + sig.persistence.resident || 1;
  const tierClass = QUALITY_TIER_COLORS[sig.qualityTier] || '';
  const top = sig.topH3Cells[0];

  return (
    <div className="rounded-lg border bg-card/60 p-3 space-y-2.5 text-xs">
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="font-mono font-bold text-sm">{sig.postalCode}</div>
          <div className="text-muted-foreground text-[11px] truncate">
            {sig.region || '—'}
            {sig.topCities[0] ? ` · ${sig.topCities[0].city}` : ''}
          </div>
        </div>
        <span className={`px-1.5 py-0.5 rounded text-[10px] border ${tierClass}`}>{sig.qualityTier}</span>
      </div>

      {/* Headline numbers */}
      <div className="flex items-baseline gap-2">
        <div className="text-lg font-bold tabular-nums">{fmtNumCompact(sig.devices)}</div>
        <div className="text-[11px] text-muted-foreground">devices</div>
        <div className="ml-auto text-[11px] text-muted-foreground tabular-nums">
          {fmtNumCompact(sig.deviceDays)} dev-days
        </div>
      </div>

      {/* Hour distribution mini-bars */}
      <div>
        <div className="flex items-center gap-1 text-[10px] text-muted-foreground mb-1">
          <Clock className="w-3 h-3" /> Origin hour ·
          <span className="text-foreground font-medium capitalize">
            peak {sig.peakHourBucket}
          </span>
          <span className="opacity-70">{HOUR_BUCKET_LABELS[sig.peakHourBucket]}</span>
        </div>
        <div className="flex items-end gap-1 h-7">
          {(['morning','midday','afternoon','evening','night'] as const).map((b) => {
            const v = buckets[b];
            const h = (v / maxBucket) * 100;
            const isPeak = b === sig.peakHourBucket;
            return (
              <div key={b} className="flex-1 flex flex-col items-center justify-end" title={`${b}: ${v.toLocaleString()} dev-days`}>
                <div
                  className={`w-full rounded-t ${isPeak ? 'bg-amber-500' : 'bg-blue-500/60'}`}
                  style={{ height: `${Math.max(4, h)}%` }}
                />
              </div>
            );
          })}
        </div>
      </div>

      {/* Weekend + overnight */}
      <div className="grid grid-cols-2 gap-1.5 text-[11px]">
        <div className="rounded bg-muted/30 px-2 py-1">
          <div className="text-muted-foreground text-[10px]">Weekend</div>
          <div className="tabular-nums font-medium">{fmtPct(sig.weekendShare)}</div>
        </div>
        <div className="rounded bg-muted/30 px-2 py-1">
          <div className="text-muted-foreground text-[10px]">Overnight</div>
          <div className="tabular-nums font-medium">{fmtPct(sig.overnightShare)}</div>
        </div>
      </div>

      {/* Persistence histogram */}
      <div>
        <div className="text-[10px] text-muted-foreground mb-1 flex items-center gap-1">
          <Activity className="w-3 h-3" /> Persistence
        </div>
        <div className="flex h-1.5 rounded overflow-hidden bg-muted/40">
          <div className="bg-rose-400" style={{ width: `${(sig.persistence.onceOnly / persistTotal) * 100}%` }} title={`Once only: ${sig.persistence.onceOnly}`} />
          <div className="bg-amber-400" style={{ width: `${(sig.persistence.casual / persistTotal) * 100}%` }} title={`Casual: ${sig.persistence.casual}`} />
          <div className="bg-blue-400" style={{ width: `${(sig.persistence.regular / persistTotal) * 100}%` }} title={`Regular: ${sig.persistence.regular}`} />
          <div className="bg-emerald-500" style={{ width: `${(sig.persistence.resident / persistTotal) * 100}%` }} title={`Resident: ${sig.persistence.resident}`} />
        </div>
        <div className="flex justify-between text-[9px] text-muted-foreground mt-0.5">
          <span>1 day</span><span>2-7</span><span>8-30</span><span>30+</span>
        </div>
      </div>

      {/* Top H3 hotspot — sub-ZIP precision */}
      {top && (
        <div className="text-[10px] text-muted-foreground border-t pt-1.5 flex items-center gap-1">
          <MapPin className="w-3 h-3" />
          <span>Top hotspot</span>
          <span className="font-mono">{top.lat.toFixed(4)}, {top.lng.toFixed(4)}</span>
          <span className="ml-auto tabular-nums">{top.devices} dev</span>
        </div>
      )}
    </div>
  );
}
