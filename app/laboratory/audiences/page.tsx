'use client';

import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Separator } from '@/components/ui/separator';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  Users,
  Play,
  Search,
  Download,
  Loader2,
  CheckCircle,
  XCircle,
  Clock,
  BarChart3,
  MapPinned,
  TrendingUp,
  Activity,
  RefreshCw,
  Eye,
  Filter,
  Zap,
  // Audience icons
  Film,
  Trophy,
  Wine,
  Music,
  Dumbbell,
  Flag,
  Waves,
  GraduationCap,
  Utensils,
  Coffee,
  Train,
  Bus,
  TrainFront,
  Plane,
  Car,
  Crown,
  Home,
  ShoppingCart,
  Sparkles,
  PawPrint,
  Tent,
  Gamepad2,
  Landmark,
  Briefcase,
  type LucideIcon,
} from 'lucide-react';
import type {
  AudienceDefinition,
  AudienceRunResult,
  AudienceGroup,
} from '@/lib/audience-catalog';
import { AUDIENCE_GROUP_LABELS } from '@/lib/audience-catalog';

// ── Icon map ──────────────────────────────────────────────────────────────

const ICON_MAP: Record<string, LucideIcon> = {
  Film,
  Trophy,
  Wine,
  Music,
  Dumbbell,
  Flag,
  Waves,
  GraduationCap,
  Utensils,
  Coffee,
  Train,
  Bus,
  TrainFront,
  Plane,
  Car,
  Zap,
  Crown,
  Home,
  ShoppingCart,
  Sparkles,
  PawPrint,
  Tent,
  Gamepad2,
  Landmark,
  Briefcase,
};

function getAudienceIcon(iconName: string): LucideIcon {
  return ICON_MAP[iconName] || Users;
}

// ── Helpers ───────────────────────────────────────────────────────────────

function getAffinityColor(index: number): string {
  if (index >= 80) return 'text-emerald-400';
  if (index >= 60) return 'text-green-400';
  if (index >= 40) return 'text-yellow-400';
  if (index >= 20) return 'text-orange-400';
  return 'text-red-400';
}

function getAffinityBg(index: number): string {
  if (index >= 80) return 'bg-emerald-400/10 border-emerald-400/20';
  if (index >= 60) return 'bg-green-400/10 border-green-400/20';
  if (index >= 40) return 'bg-yellow-400/10 border-yellow-400/20';
  if (index >= 20) return 'bg-orange-400/10 border-orange-400/20';
  return 'bg-red-400/10 border-red-400/20';
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

// ── Types ─────────────────────────────────────────────────────────────────

interface EnabledDataset {
  jobId: string;
  name: string;
  datasetId: string;
  country: string;
  dateRange: { from: string; to: string };
  actualDateRange?: { from: string; to: string; days: number };
  poiCount: number;
}

interface CatalogResponse {
  catalog: AudienceDefinition[];
  groupLabels: Record<string, string>;
  results: Record<string, AudienceRunResult>;
  enabledDatasets: EnabledDataset[];
}

interface BatchProgress {
  phase: string;
  audienceId?: string;
  audienceName?: string;
  current: number;
  total: number;
  percent: number;
  message: string;
}

// ── Page ──────────────────────────────────────────────────────────────────

export default function AudiencesPage() {
  // Data
  const [catalog, setCatalog] = useState<AudienceDefinition[]>([]);
  const [groupLabels, setGroupLabels] = useState<Record<string, string>>({});
  const [results, setResults] = useState<Record<string, AudienceRunResult>>({});
  const [enabledDatasets, setEnabledDatasets] = useState<EnabledDataset[]>([]);
  const [loading, setLoading] = useState(true);

  // Selection
  const [selectedDatasetId, setSelectedDatasetId] = useState<string>('');
  const [searchQuery, setSearchQuery] = useState('');
  const [groupFilter, setGroupFilter] = useState<string>('all');

  // Running state
  const [runningAudienceId, setRunningAudienceId] = useState<string | null>(null);
  const [runningProgress, setRunningProgress] = useState<{ percent: number; message: string } | null>(null);
  const [batchRunning, setBatchRunning] = useState(false);
  const [batchProgress, setBatchProgress] = useState<BatchProgress | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const pollIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);

  // Detail dialog
  const [detailAudience, setDetailAudience] = useState<AudienceDefinition | null>(null);

  // ── Derived ───────────────────────────────────────────────────────────

  const selectedDataset = useMemo(
    () => enabledDatasets.find(d => d.datasetId === selectedDatasetId),
    [enabledDatasets, selectedDatasetId],
  );

  const groups = useMemo(() => {
    const gs = new Set<string>();
    for (const a of catalog) gs.add(a.group);
    return Array.from(gs);
  }, [catalog]);

  const filteredAudiences = useMemo(() => {
    let list = catalog;
    if (groupFilter !== 'all') {
      list = list.filter(a => a.group === groupFilter);
    }
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      list = list.filter(a =>
        a.name.toLowerCase().includes(q) ||
        a.description.toLowerCase().includes(q) ||
        a.group.toLowerCase().includes(q),
      );
    }
    return list;
  }, [catalog, groupFilter, searchQuery]);

  const completedCount = useMemo(
    () => Object.values(results).filter(r => r.status === 'completed').length,
    [results],
  );

  const totalSegmentDevices = useMemo(
    () => Object.values(results)
      .filter(r => r.status === 'completed')
      .reduce((sum, r) => sum + (r.segmentSize || 0), 0),
    [results],
  );

  // ── Load catalog ──────────────────────────────────────────────────────

  // Keep a ref for enabledDatasets so loadCatalog always sees the latest value
  const enabledDatasetsRef = useRef(enabledDatasets);
  enabledDatasetsRef.current = enabledDatasets;

  const loadCatalog = useCallback(async (datasetId?: string, forceRefreshDatasets = false) => {
    try {
      const params = new URLSearchParams();
      const dsId = datasetId || selectedDatasetId;

      // Use ref to avoid stale closure
      const currentDatasets = enabledDatasetsRef.current;
      const ds = currentDatasets.find(d => d.datasetId === dsId);

      if (dsId) params.set('datasetId', dsId);
      if (ds?.country) params.set('country', ds.country);

      const res = await fetch(`/api/laboratory/audiences/catalog?${params}`, {
        credentials: 'include',
      });
      const data: CatalogResponse = await res.json();

      setCatalog(data.catalog);
      setGroupLabels(data.groupLabels);
      setResults(data.results);

      // ALWAYS update enabledDatasets from the API so country changes are reflected
      if (data.enabledDatasets.length > 0) {
        // Update both the React state AND the ref immediately
        // (the ref ensures the next loadCatalog call sees fresh data
        //  even before the React re-render completes)
        setEnabledDatasets(data.enabledDatasets);
        enabledDatasetsRef.current = data.enabledDatasets;

        // Auto-select first dataset if none selected
        if (!selectedDatasetId && data.enabledDatasets.length > 0) {
          const firstDs = data.enabledDatasets[0];
          setSelectedDatasetId(firstDs.datasetId);

          // If the first load had no country (because no dataset was selected yet),
          // re-fetch with the country now that we know it
          if (!ds?.country && firstDs.country) {
            const params2 = new URLSearchParams();
            params2.set('datasetId', firstDs.datasetId);
            params2.set('country', firstDs.country);
            const res2 = await fetch(`/api/laboratory/audiences/catalog?${params2}`, {
              credentials: 'include',
            });
            const data2: CatalogResponse = await res2.json();
            setResults(data2.results);
          }
        }
      }
    } catch (err) {
      console.error('Error loading catalog:', err);
    } finally {
      setLoading(false);
    }
  }, [selectedDatasetId]);

  useEffect(() => {
    loadCatalog();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Re-load results when dataset changes + check for active run
  useEffect(() => {
    if (!selectedDatasetId) return;

    setLoading(true);
    loadCatalog(selectedDatasetId);

    // Check for an active run (e.g., user navigated away and came back)
    const ds = enabledDatasetsRef.current.find(d => d.datasetId === selectedDatasetId);
    if (!ds?.country) return;

    const checkActiveRun = async () => {
      try {
        const res = await fetch(
          `/api/laboratory/audiences/status?datasetId=${encodeURIComponent(ds.datasetId)}&country=${encodeURIComponent(ds.country)}`,
          { credentials: 'include' },
        );
        const data = await res.json();

        if (data.active) {
          // There's a run in progress — resume the UI
          setBatchRunning(true);
          setBatchProgress({
            phase: data.phase,
            audienceId: undefined,
            audienceName: data.currentAudienceName,
            current: data.current,
            total: data.total,
            percent: data.percent,
            message: data.message,
          });
          setActiveRunId(data.runId);

          // Start polling
          if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = setInterval(() => {
            // Inline poll to avoid stale closure
            fetch(
              `/api/laboratory/audiences/status?datasetId=${encodeURIComponent(ds.datasetId)}&country=${encodeURIComponent(ds.country)}`,
              { credentials: 'include' },
            )
              .then(r => r.json())
              .then(status => {
                if (!status.active) {
                  if (pollIntervalRef.current) {
                    clearInterval(pollIntervalRef.current);
                    pollIntervalRef.current = null;
                  }
                  setBatchRunning(false);
                  setBatchProgress(null);
                  setActiveRunId(null);
                  // Reload results
                  loadCatalog(ds.datasetId);
                  return;
                }
                setBatchProgress({
                  phase: status.phase,
                  audienceId: undefined,
                  audienceName: status.currentAudienceName,
                  current: status.current,
                  total: status.total,
                  percent: status.percent,
                  message: status.message,
                });
              })
              .catch(() => {});
          }, 4000);
        }
      } catch {
        // No active run — ignore
      }
    };
    checkActiveRun();
  }, [selectedDatasetId]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Poll run status (fallback for when client disconnects) ──────────

  const pollRunStatus = useCallback(async () => {
    if (!selectedDataset) return;

    try {
      const res = await fetch(
        `/api/laboratory/audiences/status?datasetId=${encodeURIComponent(selectedDataset.datasetId)}&country=${encodeURIComponent(selectedDataset.country)}`,
        { credentials: 'include' },
      );
      const data = await res.json();

      if (!data.active) {
        // Run is done — stop polling
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }

        if (data.status === 'completed' || data.status === 'cancelled' || data.status === 'failed') {
          // Reload results from S3
          await loadCatalog(selectedDataset.datasetId);
        }

        setBatchRunning(false);
        setBatchProgress(null);
        setActiveRunId(null);
        return;
      }

      // Update progress from polled status
      setBatchProgress({
        phase: data.phase,
        audienceId: undefined,
        audienceName: data.currentAudienceName,
        current: data.current,
        total: data.total,
        percent: data.percent,
        message: data.message,
      });
    } catch (err) {
      console.error('Poll error:', err);
    }
  }, [selectedDataset, loadCatalog]);

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
      }
    };
  }, []);

  // ── Run single audience ───────────────────────────────────────────────

  const runSingle = useCallback(async (audienceId: string) => {
    if (!selectedDataset) return;
    if (runningAudienceId || batchRunning) return;

    setRunningAudienceId(audienceId);
    setRunningProgress({ percent: 0, message: 'Starting...' });

    // Mark as running in local state
    setResults(prev => ({
      ...prev,
      [audienceId]: {
        ...prev[audienceId],
        audienceId,
        datasetId: selectedDataset.datasetId,
        country: selectedDataset.country,
        status: 'running',
      },
    }));

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      const res = await fetch('/api/laboratory/audiences/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        signal: controller.signal,
        body: JSON.stringify({
          audienceId,
          datasetId: selectedDataset.datasetId,
          datasetName: selectedDataset.name,
          jobId: selectedDataset.jobId,
          country: selectedDataset.country,
          dateFrom: selectedDataset.actualDateRange?.from || selectedDataset.dateRange.from,
          dateTo: selectedDataset.actualDateRange?.to || selectedDataset.dateRange.to,
        }),
      });

      const reader = res.body?.getReader();
      if (!reader) throw new Error('No stream');

      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            const eventType = line.slice(7).trim();
            const nextLine = lines[lines.indexOf(line) + 1];
            if (nextLine?.startsWith('data: ')) {
              try {
                const data = JSON.parse(nextLine.slice(6));
                if (eventType === 'progress') {
                  setRunningProgress({ percent: data.percent || 0, message: data.message || '' });
                } else if (eventType === 'result') {
                  const result = data as AudienceRunResult;
                  setResults(prev => ({ ...prev, [audienceId]: result }));
                } else if (eventType === 'error') {
                  setResults(prev => ({
                    ...prev,
                    [audienceId]: {
                      ...prev[audienceId],
                      audienceId,
                      datasetId: selectedDataset.datasetId,
                      country: selectedDataset.country,
                      status: 'failed',
                      error: data.message,
                    },
                  }));
                }
              } catch { /* parse error */ }
            }
          }
        }
      }
    } catch (err: any) {
      if (err.name !== 'AbortError') {
        setResults(prev => ({
          ...prev,
          [audienceId]: {
            ...prev[audienceId],
            audienceId,
            datasetId: selectedDataset.datasetId,
            country: selectedDataset.country,
            status: 'failed',
            error: err.message,
          },
        }));
      }
    } finally {
      setRunningAudienceId(null);
      setRunningProgress(null);
      abortRef.current = null;
    }
  }, [selectedDataset, runningAudienceId, batchRunning]);

  // ── Run batch ─────────────────────────────────────────────────────────

  const runBatch = useCallback(async () => {
    if (!selectedDataset) return;
    if (runningAudienceId || batchRunning) return;

    // Run only audiences not yet completed
    const audienceIds = catalog
      .filter(a => results[a.id]?.status !== 'completed')
      .map(a => a.id);

    if (audienceIds.length === 0) return;

    setBatchRunning(true);
    setBatchProgress({ phase: 'spatial_join', current: 0, total: audienceIds.length, percent: 0, message: 'Starting batch...' });

    // Mark all as running
    setResults(prev => {
      const next = { ...prev };
      for (const id of audienceIds) {
        next[id] = {
          ...next[id],
          audienceId: id,
          datasetId: selectedDataset.datasetId,
          country: selectedDataset.country,
          status: 'running',
        };
      }
      return next;
    });

    const controller = new AbortController();
    abortRef.current = controller;

    // Start polling as backup — if SSE disconnects, polling takes over
    if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    pollIntervalRef.current = setInterval(pollRunStatus, 4000);

    try {
      const res = await fetch('/api/laboratory/audiences/run-batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        signal: controller.signal,
        body: JSON.stringify({
          audienceIds,
          datasetId: selectedDataset.datasetId,
          datasetName: selectedDataset.name,
          jobId: selectedDataset.jobId,
          country: selectedDataset.country,
          dateFrom: selectedDataset.actualDateRange?.from || selectedDataset.dateRange.from,
          dateTo: selectedDataset.actualDateRange?.to || selectedDataset.dateRange.to,
        }),
      });

      // Get runId from header
      const runId = res.headers.get('X-Run-Id');
      if (runId) setActiveRunId(runId);

      const reader = res.body?.getReader();
      if (!reader) throw new Error('No stream');

      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            const eventType = line.slice(7).trim();
            const nextLine = lines[lines.indexOf(line) + 1];
            if (nextLine?.startsWith('data: ')) {
              try {
                const data = JSON.parse(nextLine.slice(6));
                if (eventType === 'progress') {
                  setBatchProgress(data as BatchProgress);
                } else if (eventType === 'result') {
                  // Batch complete — data.results is Record<audienceId, AudienceRunResult>
                  const batchResults = data.results as Record<string, AudienceRunResult>;
                  setResults(prev => ({ ...prev, ...batchResults }));
                } else if (eventType === 'error') {
                  console.error('Batch error:', data.message);
                }
              } catch { /* parse error */ }
            }
          }
        }
      }

      // SSE stream completed cleanly — stop polling
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
      setBatchRunning(false);
      setBatchProgress(null);
      setActiveRunId(null);
    } catch (err: any) {
      if (err.name === 'AbortError') {
        // User clicked Stop — polling will detect cancellation
        return;
      }
      // SSE disconnected (user navigated away) but run continues on server.
      // Polling is already running and will track progress.
      console.log('SSE disconnected, polling will continue tracking progress');
    }
  }, [selectedDataset, catalog, results, runningAudienceId, batchRunning, pollRunStatus]);

  // ── Cancel ────────────────────────────────────────────────────────────

  const cancel = useCallback(async () => {
    // Abort the SSE connection (cleanup)
    abortRef.current?.abort();

    // Send cancel request to the server
    if (selectedDataset) {
      try {
        await fetch('/api/laboratory/audiences/stop', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify({
            datasetId: selectedDataset.datasetId,
            country: selectedDataset.country,
          }),
        });
      } catch (err) {
        console.error('Cancel request failed:', err);
      }
    }
  }, [selectedDataset]);

  // ── Download CSV ──────────────────────────────────────────────────────

  const downloadCsv = useCallback(async (audienceId: string) => {
    const result = results[audienceId];
    if (!result?.s3SegmentCsvPath) return;

    // Extract S3 key from path like s3://bucket/audiences/...
    const key = result.s3SegmentCsvPath.replace(/^s3:\/\/[^/]+\//, '');

    try {
      const res = await fetch(`/api/laboratory/audiences/download?key=${encodeURIComponent(key)}`, {
        credentials: 'include',
      });
      const data = await res.json();
      if (data.downloadUrl) {
        window.open(data.downloadUrl, '_blank');
      }
    } catch (err) {
      console.error('Download error:', err);
    }
  }, [results]);

  // ── Render ────────────────────────────────────────────────────────────

  if (loading && !catalog.length) {
    return (
      <MainLayout>
        <div className="flex items-center justify-center h-[60vh]">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </MainLayout>
    );
  }

  const isRunning = !!runningAudienceId || batchRunning;
  const pendingCount = catalog.length - completedCount;

  return (
    <MainLayout>
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <div className="flex items-center gap-3">
            <div className="p-2 bg-theme-accent/10 rounded-lg">
              <Users className="h-6 w-6 text-theme-accent" />
            </div>
            <div>
              <h1 className="text-3xl font-bold tracking-tight">Roamy</h1>
              <p className="text-muted-foreground mt-1">
                Roaming Observation Agent for Mobility Yield
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Dataset selector + controls */}
      <Card className="mb-6">
        <CardContent className="pt-6">
          <div className="flex flex-col md:flex-row gap-4 items-start md:items-end">
            {/* Dataset */}
            <div className="flex-1 min-w-[220px]">
              <label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">
                Dataset
              </label>
              {enabledDatasets.length === 0 ? (
                <p className="text-sm text-muted-foreground">
                  No datasets enabled. Enable &quot;Roamy&quot; on a job to get started.
                </p>
              ) : (
                <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select a dataset" />
                  </SelectTrigger>
                  <SelectContent>
                    {enabledDatasets.map(ds => (
                      <SelectItem key={ds.datasetId} value={ds.datasetId}>
                        <span className="font-medium">{ds.name}</span>
                        <span className="text-muted-foreground ml-2 text-xs">
                          {ds.country} &middot; {ds.poiCount} POIs
                        </span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            </div>

            {/* Country */}
            {selectedDataset && (
              <div className="min-w-[100px]">
                <label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">
                  Country
                </label>
                <div className="h-10 flex items-center px-3 border border-border rounded-md bg-muted/50">
                  <span className="text-sm font-medium">{selectedDataset.country || 'N/A'}</span>
                </div>
              </div>
            )}

            {/* Date range */}
            {selectedDataset && (
              <div className="min-w-[180px]">
                <label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">
                  Date Range
                </label>
                <div className="h-10 flex items-center px-3 border border-border rounded-md bg-muted/50">
                  <span className="text-xs text-muted-foreground">
                    {selectedDataset.actualDateRange?.from || selectedDataset.dateRange.from}
                    {' '}&rarr;{' '}
                    {selectedDataset.actualDateRange?.to || selectedDataset.dateRange.to}
                  </span>
                </div>
              </div>
            )}

            {/* Actions */}
            <div className="flex gap-2">
              <Button
                onClick={runBatch}
                disabled={!selectedDataset || isRunning || pendingCount === 0}
              >
                {batchRunning ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                {batchRunning ? 'Running...' : `Run All (${pendingCount})`}
              </Button>

              {isRunning && (
                <Button variant="outline" onClick={cancel}>
                  Stop
                </Button>
              )}

              <Button variant="ghost" onClick={() => loadCatalog()} disabled={isRunning}>
                <RefreshCw className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Batch progress */}
      {batchRunning && batchProgress && (
        <Card className="mb-6 border-theme-accent/30">
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center gap-3 mb-2">
              <Loader2 className="h-4 w-4 animate-spin text-theme-accent" />
              <span className="text-sm font-medium">
                {batchProgress.audienceName
                  ? `Processing ${batchProgress.audienceName} (${batchProgress.current}/${batchProgress.total})`
                  : batchProgress.message}
              </span>
            </div>
            <Progress value={batchProgress.percent} className="h-2" />
            <p className="text-xs text-muted-foreground mt-1">{batchProgress.message}</p>
          </CardContent>
        </Card>
      )}

      {/* Single run progress */}
      {runningAudienceId && runningProgress && !batchRunning && (
        <Card className="mb-6 border-theme-accent/30">
          <CardContent className="pt-4 pb-4">
            <div className="flex items-center gap-3 mb-2">
              <Loader2 className="h-4 w-4 animate-spin text-theme-accent" />
              <span className="text-sm font-medium">
                Running: {catalog.find(a => a.id === runningAudienceId)?.name}
              </span>
            </div>
            <Progress value={runningProgress.percent} className="h-2" />
            <p className="text-xs text-muted-foreground mt-1">{runningProgress.message}</p>
          </CardContent>
        </Card>
      )}

      {/* Summary stats */}
      {selectedDataset && completedCount > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <Card>
            <CardContent className="pt-4 pb-4">
              <div className="text-xs text-muted-foreground uppercase tracking-wider">Completed</div>
              <div className="text-2xl font-bold mt-1">{completedCount} / {catalog.length}</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4 pb-4">
              <div className="text-xs text-muted-foreground uppercase tracking-wider">Total Segment</div>
              <div className="text-2xl font-bold mt-1">{formatNumber(totalSegmentDevices)}</div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4 pb-4">
              <div className="text-xs text-muted-foreground uppercase tracking-wider">Avg. Affinity</div>
              <div className="text-2xl font-bold mt-1">
                {(Object.values(results)
                  .filter(r => r.status === 'completed' && r.avgAffinityIndex)
                  .reduce((s, r) => s + (r.avgAffinityIndex || 0), 0) /
                  Math.max(1, Object.values(results).filter(r => r.status === 'completed' && r.avgAffinityIndex).length)
                ).toFixed(0)}
              </div>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-4 pb-4">
              <div className="text-xs text-muted-foreground uppercase tracking-wider">Dataset Devices</div>
              <div className="text-2xl font-bold mt-1">
                {formatNumber(Object.values(results).find(r => r.totalDevicesInDataset)?.totalDevicesInDataset || 0)}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Filter bar */}
      {selectedDataset && (
        <div className="flex flex-col sm:flex-row gap-3 mb-6">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search audiences..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
          <Select value={groupFilter} onValueChange={setGroupFilter}>
            <SelectTrigger className="w-[200px]">
              <Filter className="h-4 w-4 mr-2" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Groups</SelectItem>
              {groups.map(g => (
                <SelectItem key={g} value={g}>
                  {AUDIENCE_GROUP_LABELS[g as AudienceGroup] || g}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      )}

      {/* No dataset selected */}
      {!selectedDataset && enabledDatasets.length > 0 && (
        <Card>
          <CardContent className="pt-12 pb-12 text-center">
            <Activity className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-lg font-medium mb-2">Select a dataset</h3>
            <p className="text-muted-foreground text-sm">Choose an enabled dataset to view and run audience segments.</p>
          </CardContent>
        </Card>
      )}

      {/* Empty state */}
      {enabledDatasets.length === 0 && !loading && (
        <Card>
          <CardContent className="pt-12 pb-12 text-center">
            <Users className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            <h3 className="text-lg font-medium mb-2">No datasets enabled</h3>
            <p className="text-muted-foreground text-sm max-w-md mx-auto">
              Go to a job&apos;s detail page and enable &quot;Roamy&quot; to start building audience segments from that dataset.
            </p>
          </CardContent>
        </Card>
      )}

      {/* Audience grid */}
      {selectedDataset && (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {filteredAudiences.map(audience => {
            const result = results[audience.id];
            const status = result?.status || 'pending';
            const Icon = getAudienceIcon(audience.icon);
            const isThisRunning = runningAudienceId === audience.id || (batchRunning && status === 'running');

            return (
              <Card
                key={audience.id}
                className={`transition-all ${
                  status === 'completed'
                    ? 'border-green-500/20'
                    : status === 'running'
                    ? 'border-theme-accent/40'
                    : status === 'failed'
                    ? 'border-red-500/20'
                    : ''
                }`}
              >
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-2">
                      <div className={`p-1.5 rounded-md bg-muted ${audience.color}`}>
                        <Icon className="h-4 w-4" />
                      </div>
                      <div>
                        <CardTitle className="text-sm font-semibold leading-tight">{audience.name}</CardTitle>
                        <p className="text-xs text-muted-foreground mt-0.5">
                          {AUDIENCE_GROUP_LABELS[audience.group] || audience.group}
                        </p>
                      </div>
                    </div>
                    {/* Status badge */}
                    {status === 'completed' && (
                      <Badge variant="success" className="text-[10px] px-1.5 py-0">
                        <CheckCircle className="h-3 w-3 mr-1" />
                        Done
                      </Badge>
                    )}
                    {status === 'running' && (
                      <Badge variant="warning" className="text-[10px] px-1.5 py-0">
                        <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                        Running
                      </Badge>
                    )}
                    {status === 'failed' && (
                      <Badge variant="destructive" className="text-[10px] px-1.5 py-0">
                        <XCircle className="h-3 w-3 mr-1" />
                        Failed
                      </Badge>
                    )}
                    {status === 'pending' && (
                      <Badge variant="outline" className="text-[10px] px-1.5 py-0 text-muted-foreground">
                        <Clock className="h-3 w-3 mr-1" />
                        Pending
                      </Badge>
                    )}
                  </div>
                </CardHeader>
                <CardContent className="pt-0">
                  <p className="text-xs text-muted-foreground mb-3 line-clamp-2">{audience.description}</p>

                  {/* Completed stats */}
                  {status === 'completed' && result && (
                    <div className="space-y-2 mb-3">
                      <div className="flex items-center justify-between text-xs">
                        <span className="text-muted-foreground flex items-center gap-1">
                          <Users className="h-3 w-3" /> Devices
                        </span>
                        <span className="font-semibold">{formatNumber(result.segmentSize || 0)}</span>
                      </div>
                      <div className="flex items-center justify-between text-xs">
                        <span className="text-muted-foreground flex items-center gap-1">
                          <TrendingUp className="h-3 w-3" /> Affinity
                        </span>
                        <span className={`font-semibold ${getAffinityColor(result.avgAffinityIndex || 0)}`}>
                          {(result.avgAffinityIndex || 0).toFixed(0)}
                        </span>
                      </div>
                      <div className="flex items-center justify-between text-xs">
                        <span className="text-muted-foreground flex items-center gap-1">
                          <MapPinned className="h-3 w-3" /> Zip Codes
                        </span>
                        <span className="font-semibold">{result.totalPostalCodes || 0}</span>
                      </div>
                    </div>
                  )}

                  {/* Failed error */}
                  {status === 'failed' && result?.error && (
                    <p className="text-xs text-red-400 mb-3 line-clamp-2">{result.error}</p>
                  )}

                  {/* Filters info */}
                  <div className="flex flex-wrap gap-1 mb-3">
                    {audience.minDwellMinutes && (
                      <span className="text-[10px] bg-muted px-1.5 py-0.5 rounded">
                        Dwell &ge; {audience.minDwellMinutes}m
                      </span>
                    )}
                    {audience.minFrequency && (
                      <span className="text-[10px] bg-muted px-1.5 py-0.5 rounded">
                        Freq &ge; {audience.minFrequency}
                      </span>
                    )}
                    {audience.timeWindow && (
                      <span className="text-[10px] bg-muted px-1.5 py-0.5 rounded">
                        {audience.timeWindow.hourFrom}h-{audience.timeWindow.hourTo}h
                      </span>
                    )}
                    <span className="text-[10px] bg-muted px-1.5 py-0.5 rounded">
                      {audience.categories.length} cat{audience.categories.length > 1 ? 's' : ''}
                    </span>
                  </div>

                  <Separator className="mb-3" />

                  {/* Actions */}
                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      className="flex-1 h-7 text-xs"
                      onClick={() => runSingle(audience.id)}
                      disabled={isRunning}
                    >
                      {isThisRunning ? (
                        <Loader2 className="h-3 w-3 mr-1 animate-spin" />
                      ) : (
                        <Play className="h-3 w-3 mr-1" />
                      )}
                      {status === 'completed' ? 'Re-run' : 'Run'}
                    </Button>
                    {status === 'completed' && (
                      <>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 text-xs px-2"
                          onClick={() => setDetailAudience(audience)}
                        >
                          <Eye className="h-3 w-3" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 text-xs px-2"
                          onClick={() => downloadCsv(audience.id)}
                        >
                          <Download className="h-3 w-3" />
                        </Button>
                      </>
                    )}
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}

      {/* Detail dialog */}
      <Dialog open={!!detailAudience} onOpenChange={open => !open && setDetailAudience(null)}>
        {detailAudience && (
          <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
            <DialogHeader>
              <div className="flex items-center gap-3">
                {(() => {
                  const Icon = getAudienceIcon(detailAudience.icon);
                  return (
                    <div className={`p-2 rounded-lg bg-muted ${detailAudience.color}`}>
                      <Icon className="h-5 w-5" />
                    </div>
                  );
                })()}
                <div>
                  <DialogTitle>{detailAudience.name}</DialogTitle>
                  <DialogDescription>{detailAudience.description}</DialogDescription>
                </div>
              </div>
            </DialogHeader>

            {(() => {
              const result = results[detailAudience.id];
              if (!result || result.status !== 'completed') {
                return (
                  <p className="text-muted-foreground text-sm py-6 text-center">
                    No results yet. Run this audience to see details.
                  </p>
                );
              }

              return (
                <div className="space-y-6 mt-4">
                  {/* Stats grid */}
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                    <div className="p-3 rounded-lg border">
                      <div className="text-xs text-muted-foreground">Segment Size</div>
                      <div className="text-xl font-bold">{formatNumber(result.segmentSize || 0)}</div>
                      <div className="text-xs text-muted-foreground">
                        {(result.segmentPercent || 0).toFixed(2)}% of dataset
                      </div>
                    </div>
                    <div className={`p-3 rounded-lg border ${getAffinityBg(result.avgAffinityIndex || 0)}`}>
                      <div className="text-xs text-muted-foreground">Avg. Affinity Index</div>
                      <div className={`text-xl font-bold ${getAffinityColor(result.avgAffinityIndex || 0)}`}>
                        {(result.avgAffinityIndex || 0).toFixed(1)}
                      </div>
                    </div>
                    <div className="p-3 rounded-lg border">
                      <div className="text-xs text-muted-foreground">Postal Codes</div>
                      <div className="text-xl font-bold">{result.totalPostalCodes || 0}</div>
                    </div>
                    <div className="p-3 rounded-lg border">
                      <div className="text-xs text-muted-foreground">Avg. Dwell</div>
                      <div className="text-xl font-bold">{(result.avgDwellMinutes || 0).toFixed(0)}m</div>
                    </div>
                    <div className="p-3 rounded-lg border">
                      <div className="text-xs text-muted-foreground">Total Devices</div>
                      <div className="text-xl font-bold">{formatNumber(result.totalDevicesInDataset || 0)}</div>
                    </div>
                    <div className="p-3 rounded-lg border">
                      <div className="text-xs text-muted-foreground">Completed</div>
                      <div className="text-sm font-medium">
                        {result.completedAt ? new Date(result.completedAt).toLocaleString() : '-'}
                      </div>
                    </div>
                  </div>

                  {/* Top hotspots */}
                  {result.topHotspots && result.topHotspots.length > 0 && (
                    <div>
                      <h4 className="text-sm font-semibold mb-2 flex items-center gap-2">
                        <BarChart3 className="h-4 w-4" /> Top Hotspots
                      </h4>
                      <div className="rounded-lg border overflow-hidden">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead className="text-xs">Zip Code</TableHead>
                              <TableHead className="text-xs">City</TableHead>
                              <TableHead className="text-xs">Category</TableHead>
                              <TableHead className="text-xs text-right">Affinity</TableHead>
                              <TableHead className="text-xs text-right">Visits</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {result.topHotspots.map((h, i) => (
                              <TableRow key={i}>
                                <TableCell className="text-xs font-mono">{h.zipcode}</TableCell>
                                <TableCell className="text-xs">{h.city}</TableCell>
                                <TableCell className="text-xs">{h.category}</TableCell>
                                <TableCell className={`text-xs text-right font-semibold ${getAffinityColor(h.affinityIndex)}`}>
                                  {h.affinityIndex.toFixed(0)}
                                </TableCell>
                                <TableCell className="text-xs text-right">{h.visits}</TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      </div>
                    </div>
                  )}

                  {/* Categories */}
                  <div>
                    <h4 className="text-sm font-semibold mb-2">POI Categories</h4>
                    <div className="flex flex-wrap gap-1">
                      {detailAudience.categories.map(c => (
                        <Badge key={c} variant="outline" className="text-xs">
                          {c.replace(/_/g, ' ')}
                        </Badge>
                      ))}
                    </div>
                  </div>

                  {/* Download buttons */}
                  <div className="flex gap-3">
                    <Button
                      variant="default"
                      size="sm"
                      onClick={() => downloadCsv(detailAudience.id)}
                    >
                      <Download className="h-4 w-4 mr-2" />
                      Download Segment CSV
                    </Button>
                  </div>
                </div>
              );
            })()}
          </DialogContent>
        )}
      </Dialog>
    </MainLayout>
  );
}
