'use client';

import { useState, useEffect, useCallback } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
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
  MapPin,
  Activity,
  Calendar,
  Download,
  Loader2,
  ArrowLeft,
  FileText,
  ShieldCheck,
  AlertTriangle,
  CheckCircle,
  Database,
  Search,
  MapPinned,
  BarChart3,
  Zap,
  TrendingUp,
  Navigation,
  Compass,
  Timer,
  Play,
  Target,
} from 'lucide-react';
import Link from 'next/link';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import type { DailyData, VisitByPoi } from '@/lib/dataset-analysis';
import { MovementMap } from '@/components/analysis/movement-map';
import { Progress } from '@/components/ui/progress';
import { useToast } from '@/hooks/use-toast';

// Mega-jobs dashboard components (reused)
import { CollapsibleCard } from '@/components/mega-jobs/collapsible-card';
import { MegaDailyChart } from '@/components/mega-jobs/daily-chart';
import { CatchmentPie } from '@/components/mega-jobs/catchment-pie';
import { CatchmentMap } from '@/components/mega-jobs/catchment-map';
import { NseModal } from './nse-modal';
import { CategoryMaidModal } from './category-maid-modal';
import { ODTables } from '@/components/mega-jobs/od-tables';
import { MobilityBar } from '@/components/mega-jobs/mobility-bar';
import { HourlyChart } from '@/components/mega-jobs/hourly-chart';
import { PoiFilter } from '@/components/mega-jobs/poi-filter';
import { SankeyChart } from '@/components/analysis/sankey-chart';
import { RouteTimeline } from '@/components/analysis/route-timeline';

interface DatasetInfo {
  id: string;
  name: string;
  jobId?: string | null;
  dateRange?: { from: string; to: string } | null;
  external?: boolean;
  country?: string | null;
}

interface AnalysisResult {
  dataset: string;
  analyzedAt: string;
  summary: {
    totalPings: number;
    uniqueDevices: number;
    uniquePois: number;
    dateRange: { from: string; to: string };
    daysAnalyzed: number;
  };
  dailyData: DailyData[];
  visitsByPoi: VisitByPoi[];
}

/** Dwell time options (in minutes) for the min/max interval selectors */
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
const DWELL_MIN_OPTIONS = DWELL_OPTIONS; // "No limit" (0) = no minimum
const DWELL_MAX_OPTIONS = DWELL_OPTIONS; // "No limit" (0) = no maximum

function downloadCsv(filename: string, headers: string[], rows: (string | number)[][]) {
  const line = (arr: (string | number)[]) =>
    arr.map((c) => (typeof c === 'string' && (c.includes(',') || c.includes('"')) ? `"${c.replace(/"/g, '""')}"` : c)).join(',');
  const content = [line(headers), ...rows.map((r) => line(r))].join('\n');
  const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

export default function DatasetAnalysisPage() {
  const params = useParams();
  const router = useRouter();
  const { toast } = useToast();
  const datasetName = params.name as string;

  const [datasetInfo, setDatasetInfo] = useState<DatasetInfo | null>(null);
  const [analysis, setAnalysis] = useState<AnalysisResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [downloadingFull, setDownloadingFull] = useState(false);
  const [downloadingMaids, setDownloadingMaids] = useState(false);
  const [activating, setActivating] = useState(false);
  const [activateModal, setActivateModal] = useState(false);
  const [activateStep, setActivateStep] = useState('');
  const [activatePercent, setActivatePercent] = useState(0);
  const [activateMessage, setActivateMessage] = useState('');
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditResult, setAuditResult] = useState<any>(null);

  // Reports (from /api/datasets/[name]/reports/poll)
  const [generatingReports, setGeneratingReports] = useState(false);
  const [reportProgress, setReportProgress] = useState<{ step: string; percent: number; message: string } | null>(null);

  // Dwell filter (min/max interval in minutes, 0 = no limit)
  const [dwellMin, setDwellMin] = useState<number>(0);
  const [dwellMax, setDwellMax] = useState<number>(0);
  // Hour filter
  const [hourFrom, setHourFrom] = useState<number>(0);
  const [hourTo, setHourTo] = useState<number>(23);
  const [odReport, setODReport] = useState<any>(null);
  const [hourlyReport, setHourlyReport] = useState<any>(null);
  const [catchmentReport, setCatchmentReport] = useState<any>(null);
  const [mobilityReport, setMobilityReport] = useState<any>(null);
  const [temporalReport, setTemporalReport] = useState<any>(null);
  const [affinityReport, setAffinityReport] = useState<any>(null);
  const [reportVersion, setReportVersion] = useState(0);
  const [selectedPoiIds, setSelectedPoiIds] = useState<string[]>([]);
  const [nseModalOpen, setNseModalOpen] = useState(false);
  const [categoryMaidModalOpen, setCategoryMaidModalOpen] = useState(false);

  // Route analysis state
  const [routesLoading, setRoutesLoading] = useState(false);
  const [routesProgress, setRoutesProgress] = useState<string | null>(null);
  const [routesResult, setRoutesResult] = useState<any>(null);
  const [routesStateKey, setRoutesStateKey] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/datasets', { credentials: 'include' })
      .then((r) => r.json())
      .then((data) => {
        const ds = data.datasets?.find((d: any) => d.id === datasetName);
        if (ds) {
          setDatasetInfo({
            id: ds.id,
            name: ds.name,
            jobId: ds.jobId,
            dateRange: ds.dateRange,
            external: ds.external,
            country: ds.country,
          });
        }
      })
      .catch(console.error);
  }, [datasetName]);

  // Load reports for specific dwell interval + hour range
  const loadReportsForFilters = (dMin = dwellMin, dMax = dwellMax, hFrom = hourFrom, hTo = hourTo) => {
    const types = ['od', 'hourly', 'catchment', 'mobility', 'temporal', 'affinity'];
    const setters: Record<string, (d: any) => void> = {
      od: setODReport,
      hourly: setHourlyReport,
      catchment: setCatchmentReport,
      mobility: setMobilityReport,
      temporal: setTemporalReport,
      affinity: setAffinityReport,
    };
    const dwellParams = (dMin > 0 ? `&dwellMin=${dMin}` : '') + (dMax > 0 ? `&dwellMax=${dMax}` : '');
    const hourParams = (hFrom > 0 || hTo < 23) ? `&hourFrom=${hFrom}&hourTo=${hTo}` : '';
    for (const type of types) {
      fetch(`/api/datasets/${datasetName}/reports?type=${type}${dwellParams}${hourParams}`, { credentials: 'include' })
        .then((r) => r.ok ? r.json() : null)
        .then((data) => { if (data) setters[type](data); })
        .catch(() => {});
    }
  };

  // Load saved reports on mount
  useEffect(() => {
    loadReportsForFilters(dwellMin, dwellMax);
  }, [datasetName, reportVersion]);

  // ── Run analysis (basic stats) ──────────────────────────────────
  const runAnalysisOnly = async () => {
    try {
      const poll = async (retries = 0): Promise<AnalysisResult> => {
        const res = await fetch(`/api/datasets/${datasetName}/analyze/poll`, {
          method: 'POST',
          credentials: 'include',
        });
        if (res.status === 429 && retries < 10) {
          await new Promise(r => setTimeout(r, 3000 + retries * 2000));
          return poll(retries + 1);
        }
        if (!res.ok) {
          const err = await res.json().catch(() => ({}));
          throw new Error(err.error || err.details || `HTTP ${res.status}`);
        }
        const state = await res.json();
        if (state.status === 'error') throw new Error(state.error || state.progress?.message || 'Analysis failed');
        if (state.status === 'completed' && state.result) return state.result as AnalysisResult;
        await new Promise(r => setTimeout(r, 3000));
        return poll();
      };
      const data = await poll();
      setAnalysis(data);
    } catch (e: any) {
      toast({ title: 'Analysis failed', description: e.message, variant: 'destructive' });
    }
  };

  // ── Unified "Analyze" button ────────────────────────────────────
  // Single flow: only generateReportsOnly (with dwell filter).
  // Summary stats come from temporal report (totalPings, totalUniqueDevices).
  // No parallel basic analysis — avoids 429s and inconsistent stats.
  const runFullAnalysis = async () => {
    setLoading(true);
    setGeneratingReports(true);
    setReportProgress({ step: 'starting', percent: 0, message: 'Starting analysis...' });

    try {
      // Run reports + basic analysis (with MAIDs extraction) in parallel
      await Promise.all([
        generateReportsOnly(),
        runAnalysisOnly().catch(e => console.warn('[ANALYZE] Basic analysis (MAIDs) failed:', e.message)),
      ]);
    } finally {
      setLoading(false);
    }
  };

  // ── Generate full reports (OD, hourly, catchment, mobility) ─────
  const generateReportsOnly = async () => {
    if (!generatingReports) setGeneratingReports(true);
    if (!reportProgress) setReportProgress({ step: 'starting', percent: 0, message: 'Starting report generation...' });

    try {
      let done = false;
      let consecutiveErrors = 0;
      while (!done) {
        let res: Response;
        try {
          res = await fetch(`/api/datasets/${datasetName}/reports/poll`, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              ...(selectedPoiIds.length > 0 ? { poiIds: selectedPoiIds } : {}),
              ...(dwellMin > 0 ? { minDwell: dwellMin } : {}),
              ...(dwellMax > 0 ? { maxDwell: dwellMax } : {}),
              ...(hourFrom > 0 || hourTo < 23 ? { hourFrom, hourTo } : {}),
            }),
          });
          consecutiveErrors = 0;
        } catch (fetchErr: any) {
          consecutiveErrors++;
          console.warn(`[REPORT-POLL] Fetch failed (${consecutiveErrors}):`, fetchErr.message);
          if (consecutiveErrors >= 10) {
            setReportProgress({ step: 'error', percent: 0, message: 'Lost connection to server. Click Analyze to resume.' });
            break;
          }
          await new Promise((r) => setTimeout(r, 3000 + consecutiveErrors * 1000));
          continue;
        }

        if (res.status === 429) {
          consecutiveErrors++;
          console.warn(`[REPORT-POLL] Rate limited, backing off...`);
          await new Promise((r) => setTimeout(r, 5000 + consecutiveErrors * 2000));
          continue;
        }

        if (!res.ok) {
          consecutiveErrors++;
          const text = await res.text();
          let errMsg: string;
          try { errMsg = JSON.parse(text).error || `Server error ${res.status}`; }
          catch { errMsg = `Server error ${res.status}`; }
          console.error(`[REPORT-POLL] ${res.status}:`, errMsg);
          if (consecutiveErrors >= 5) {
            setReportProgress({ step: 'error', percent: 0, message: `Error: ${errMsg}` });
            toast({ title: 'Report generation failed', description: errMsg, variant: 'destructive' });
            break;
          }
          await new Promise((r) => setTimeout(r, 5000));
          continue;
        }

        const data = await res.json();

        if (data.phase === 'error' || data.error) {
          setReportProgress({ step: 'error', percent: 0, message: `Error: ${data.error || 'Unknown error'}` });
          toast({ title: 'Report generation failed', description: data.error, variant: 'destructive' });
          break;
        }

        setReportProgress(data.progress || { step: data.phase, percent: 50, message: data.phase });

        if (data.phase === 'done') {
          done = true;
          setReportVersion((v) => v + 1);
          toast({ title: 'Reports generated', description: 'All reports are ready.' });
        } else {
          await new Promise((r) => setTimeout(r, 4000));
        }
      }
    } catch (err: any) {
      setReportProgress({ step: 'error', percent: 0, message: `Error: ${err.message}` });
      toast({ title: 'Report generation failed', description: err.message, variant: 'destructive' });
    } finally {
      setGeneratingReports(false);
      setReportProgress(null);
    }
  };

  // ── Route analysis (Sankey + Sample Routes) ─────────────────────
  const runRouteAnalysis = async (isRefresh = false) => {
    if (!datasetInfo?.country) {
      toast({ title: 'Country required', description: 'Set the country on the job before running route analysis.', variant: 'destructive' });
      return;
    }
    setRoutesLoading(true);
    setRoutesProgress('Starting route analysis...');
    if (!isRefresh) setRoutesResult(null);

    try {
      let data = await fetch(`/api/datasets/${datasetName}/routes/poll`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          country: datasetInfo.country,
          minDwell: dwellMin,
          maxDwell: dwellMax,
          hourFrom,
          hourTo,
        }),
      }).then(r => r.json());

      const sk = data.stateKey || '';
      setRoutesStateKey(sk);

      while (data.phase !== 'done' && data.phase !== 'error') {
        setRoutesProgress(data.progress?.message || 'Processing...');
        await new Promise(r => setTimeout(r, 4000));

        const res = await fetch(`/api/datasets/${datasetName}/routes/poll`, {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ stateKey: sk }),
        });

        if (res.status === 504) {
          data = { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
          continue;
        }
        data = await res.json();
      }

      if (data.phase === 'error') {
        throw new Error(data.error || 'Route analysis failed');
      }

      setRoutesResult(data.result);
      toast({ title: 'Route analysis complete', description: `${data.result?.sankey?.length || 0} category flows found` });
    } catch (e: any) {
      toast({ title: 'Route analysis failed', description: e.message, variant: 'destructive' });
    } finally {
      setRoutesLoading(false);
      setRoutesProgress(null);
    }
  };

  // ── Download handlers ───────────────────────────────────────────
  const handleDownloadFull = async () => {
    setDownloadingFull(true);
    try {
      const res = await fetch(`/api/datasets/${datasetName}/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filters: {}, format: 'full' }),
        credentials: 'include',
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || data.details || 'Export failed');
      if (data.downloadUrl) {
        const url = data.downloadUrl.startsWith('http') ? data.downloadUrl : `${window.location.origin}${data.downloadUrl}`;
        const fileName = new URL(url, window.location.origin).searchParams.get('file') || `${datasetName}-full.csv`;
        const fileRes = await fetch(url, { credentials: 'include' });
        if (!fileRes.ok) throw new Error('Download failed');
        const blob = await fileRes.blob();
        const blobUrl = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = blobUrl;
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(blobUrl);
        toast({ title: 'Export complete', description: `Exported ${data.deviceCount?.toLocaleString() || ''} rows` });
      }
    } catch (e: any) {
      toast({ title: 'Export failed', description: e.message, variant: 'destructive' });
    } finally {
      setDownloadingFull(false);
    }
  };

  const handleDownloadMaids = async () => {
    setDownloadingMaids(true);
    try {
      const res = await fetch(`/api/datasets/${datasetName}/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filters: {}, format: 'maids' }),
        credentials: 'include',
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || data.details || 'Export failed');
      if (data.downloadUrl) {
        const url = data.downloadUrl.startsWith('http') ? data.downloadUrl : `${window.location.origin}${data.downloadUrl}`;
        const fileName = new URL(url, window.location.origin).searchParams.get('file') || `${datasetName}-maids.csv`;
        const fileRes = await fetch(url, { credentials: 'include' });
        if (!fileRes.ok) throw new Error('Download failed');
        const blob = await fileRes.blob();
        const blobUrl = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = blobUrl;
        a.download = fileName;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(blobUrl);
        toast({ title: 'Export complete', description: `Exported ${data.deviceCount?.toLocaleString() || ''} MAIDs` });
      }
    } catch (e: any) {
      toast({ title: 'Export failed', description: e.message, variant: 'destructive' });
    } finally {
      setDownloadingMaids(false);
    }
  };

  const handleActivate = async () => {
    setActivating(true);
    setActivateModal(true);
    setActivateStep('');
    setActivatePercent(0);
    setActivateMessage('Starting activation...');

    try {
      const poll = async (): Promise<void> => {
        const res = await fetch(`/api/datasets/${datasetName}/activate/poll`, { method: 'POST' });
        if (!res.ok) {
          const err = await res.json().catch(() => ({ error: 'Activation failed' }));
          throw new Error(err.error || err.progress?.message || `HTTP ${res.status}`);
        }
        const state = await res.json();
        setActivateStep(state.progress.step);
        setActivatePercent(state.progress.percent);
        setActivateMessage(state.progress.message);

        if (state.status === 'error') throw new Error(state.error || state.progress.message);
        if (state.status === 'completed') {
          toast({
            title: 'Activation complete',
            description: `${state.result.deviceCount.toLocaleString()} MAIDs uploaded to ${state.result.folderName}`,
          });
          return;
        }
        await new Promise(r => setTimeout(r, 2000));
        return poll();
      };
      await poll();
    } catch (e: any) {
      setActivateMessage(`Error: ${e.message}`);
      toast({ title: 'Activation failed', description: e.message, variant: 'destructive' });
    } finally {
      setActivating(false);
    }
  };

  const runAudit = async () => {
    setAuditLoading(true);
    setAuditResult(null);
    try {
      const res = await fetch(`/api/datasets/${encodeURIComponent(datasetName)}/audit`, {
        method: 'POST',
        credentials: 'include',
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || data.details || 'Audit failed');
      setAuditResult(data);
    } catch (e: any) {
      toast({ title: 'Audit failed', description: e.message, variant: 'destructive' });
    } finally {
      setAuditLoading(false);
    }
  };

  const handleReportVisitsByPoi = () => {
    if (!analysis?.visitsByPoi?.length) return;
    downloadCsv(
      `${datasetName}-visits-by-poi-${new Date().toISOString().slice(0, 10)}.csv`,
      ['poi_name', 'poi_id', 'visits', 'devices'],
      analysis.visitsByPoi.map((r) => [r.name || r.poiId, r.poiId, r.visits, r.devices])
    );
  };

  const displayName = datasetInfo?.name || datasetName;

  // Transform analysis dailyData to mega-jobs format (same shape)
  const dailyChartData = analysis?.dailyData?.map((d) => ({
    date: d.date,
    pings: d.pings,
    devices: d.devices,
  })) || [];

  const hasReports = odReport || hourlyReport || catchmentReport || mobilityReport || temporalReport || affinityReport;

  return (
    <MainLayout>
      {/* Header */}
      <div className="mb-6">
        <Button variant="ghost" className="mb-4" onClick={() => router.push('/datasets')}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Back to Datasets
        </Button>
        <div className="flex items-center gap-3">
          <h1 className="text-3xl font-bold text-white">{displayName}</h1>
          {datasetInfo?.external && (
            <span className="rounded-full border border-blue-500/20 bg-blue-500/10 px-2 py-0.5 text-xs text-blue-400">
              External
            </span>
          )}
        </div>
        {datasetInfo && datasetInfo.id !== datasetInfo.name && (
          <p className="mt-2 font-mono text-sm text-gray-500">{datasetInfo.id}</p>
        )}
      </div>

      {/* Action buttons */}
      <div className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-xl font-semibold">Data analysis</h2>
        <div className="flex flex-wrap gap-2 items-center">
          <div className="flex items-center gap-1 mr-2">
            <label className="text-xs text-muted-foreground whitespace-nowrap">Dwell:</label>
            <select
              value={dwellMin}
              onChange={(e) => {
                const v = parseInt(e.target.value, 10);
                setDwellMin(v);
                loadReportsForFilters(v, dwellMax);
              }}
              className="h-8 w-20 rounded-md border border-input bg-background px-1 text-sm text-center"
            >
              {DWELL_MIN_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>{opt.value === 0 ? 'Min' : opt.label}</option>
              ))}
            </select>
            <span className="text-xs text-muted-foreground">to</span>
            <select
              value={dwellMax}
              onChange={(e) => {
                const v = parseInt(e.target.value, 10);
                setDwellMax(v);
                loadReportsForFilters(dwellMin, v);
              }}
              className="h-8 w-20 rounded-md border border-input bg-background px-1 text-sm text-center"
            >
              {DWELL_MAX_OPTIONS.map((opt) => (
                <option key={opt.value} value={opt.value}>{opt.value === 0 ? 'Max' : opt.label}</option>
              ))}
            </select>
          </div>
          <div className="flex items-center gap-1 mr-2">
            <label className="text-xs text-muted-foreground whitespace-nowrap">Hours:</label>
            <select
              value={hourFrom}
              onChange={(e) => {
                const v = parseInt(e.target.value, 10);
                setHourFrom(v);
                loadReportsForFilters(dwellMin, dwellMax, v, hourTo);
              }}
              className="h-8 w-16 rounded-md border border-input bg-background px-1 text-sm text-center"
            >
              {Array.from({ length: 24 }, (_, i) => (
                <option key={i} value={i}>{String(i).padStart(2, '0')}h</option>
              ))}
            </select>
            <span className="text-xs text-muted-foreground">to</span>
            <select
              value={hourTo}
              onChange={(e) => {
                const v = parseInt(e.target.value, 10);
                setHourTo(v);
                loadReportsForFilters(dwellMin, dwellMax, hourFrom, v);
              }}
              className="h-8 w-16 rounded-md border border-input bg-background px-1 text-sm text-center"
            >
              {Array.from({ length: 24 }, (_, i) => (
                <option key={i} value={i}>{String(i).padStart(2, '0')}h</option>
              ))}
            </select>
          </div>
          <Button onClick={runFullAnalysis} disabled={loading || generatingReports}>
            {loading || generatingReports ? (
              <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Analyzing...</>
            ) : (
              <><Play className="mr-2 h-4 w-4" />Analyze</>
            )}
          </Button>
          <Button variant="outline" onClick={handleDownloadFull} disabled={downloadingFull}>
            {downloadingFull ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Download className="mr-2 h-4 w-4" />}
            Download full dataset
          </Button>
          <Button variant="outline" onClick={handleDownloadMaids} disabled={downloadingMaids}>
            {downloadingMaids ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Download className="mr-2 h-4 w-4" />}
            Download MAIDs
          </Button>
          <Button variant="outline" onClick={() => setNseModalOpen(true)}>
            <Users className="mr-2 h-4 w-4" />
            MAIDs by NSE
          </Button>
          <Button variant="outline" onClick={() => setCategoryMaidModalOpen(true)}>
            <Target className="mr-2 h-4 w-4" />
            MAIDs by Category
          </Button>
          <Button variant="outline" onClick={handleActivate} disabled={activating} title="Upload MAIDs to S3 activations folder">
            {activating ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Zap className="mr-2 h-4 w-4" />}
            Activate
          </Button>
          <Button variant="outline" onClick={runAudit} disabled={auditLoading} title="Compare with Veraset source">
            {auditLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <ShieldCheck className="mr-2 h-4 w-4" />}
            Audit
          </Button>
        </div>
      </div>

      {/* Report generation progress */}
      {generatingReports && reportProgress && (
        <Card className="mb-6">
          <CardContent className="py-4">
            <div className="space-y-3">
              <Progress value={reportProgress.percent} className="h-2" />
              <div className="flex items-center gap-2 text-sm">
                <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                <span className="font-medium">{reportProgress.message}</span>
                <span className="ml-auto font-mono text-muted-foreground">{reportProgress.percent}%</span>
              </div>
              {(reportProgress as any).detail && (
                <div className="mt-2 rounded-md bg-muted/50 p-3 font-mono text-xs leading-relaxed text-muted-foreground">
                  {(reportProgress as any).detail.split('\n').map((line: string, i: number) => (
                    <div key={i} className={line.startsWith('✅') ? 'text-green-400' : line.startsWith('❌') ? 'text-red-400' : ''}>{line}</div>
                  ))}
                </div>
              )}
              {/* Dwell bucket status grid */}
              {((reportProgress as any).completedBuckets || (reportProgress as any).runningBuckets || (reportProgress as any).pendingBuckets) && (
                <div className="flex flex-wrap gap-1.5 pt-1">
                  {(reportProgress as any).completedBuckets?.map((b: number) => (
                    <span key={`c-${b}`} className="inline-flex items-center gap-1 rounded-full bg-green-500/10 px-2.5 py-0.5 text-xs text-green-400">
                      ✓ {DWELL_OPTIONS.find(o => o.value === b)?.label || `${b}min`}
                    </span>
                  ))}
                  {(reportProgress as any).runningBuckets?.map((b: number) => (
                    <span key={`r-${b}`} className="inline-flex items-center gap-1 rounded-full bg-yellow-500/10 px-2.5 py-0.5 text-xs text-yellow-400 animate-pulse">
                      ⟳ {DWELL_OPTIONS.find(o => o.value === b)?.label || `${b}min`}
                    </span>
                  ))}
                  {(reportProgress as any).pendingBuckets?.map((b: number) => (
                    <span key={`p-${b}`} className="inline-flex items-center gap-1 rounded-full bg-zinc-500/10 px-2.5 py-0.5 text-xs text-zinc-500">
                      ○ {DWELL_OPTIONS.find(o => o.value === b)?.label || `${b}min`}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* ── Movement Map (always visible) ──────────────────────── */}
      {datasetInfo && (
        <div className="mb-4">
          <MovementMap
            datasetName={datasetName}
            dateFrom={datasetInfo.dateRange?.from || new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10)}
            dateTo={datasetInfo.dateRange?.to || new Date().toISOString().slice(0, 10)}
          />
        </div>
      )}

      {/* ── ANALYSIS RESULTS ──────────────────────────────────────── */}
      {analysis && (
        <>
          {/* Summary Cards */}
          <div className="mb-6 grid gap-4 md:grid-cols-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Total pings</CardTitle>
                <Activity className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{analysis.summary.totalPings.toLocaleString()}</div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Unique devices</CardTitle>
                <Users className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{analysis.summary.uniqueDevices.toLocaleString()}</div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">POIs with visits</CardTitle>
                <MapPin className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{analysis.summary.uniquePois.toLocaleString()}</div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Days analyzed</CardTitle>
                <Calendar className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{analysis.summary.daysAnalyzed}</div>
                <p className="text-xs text-muted-foreground">
                  {analysis.summary.dateRange.from} → {analysis.summary.dateRange.to}
                </p>
              </CardContent>
            </Card>
          </div>

          {/* Daily Activity */}
          <CollapsibleCard title="Daily Activity" icon={<TrendingUp className="h-4 w-4" />}>
            <MegaDailyChart data={dailyChartData} />
          </CollapsibleCard>

          {/* Visits by POI */}
          <CollapsibleCard
            title="Visits by POI"
            icon={<BarChart3 className="h-4 w-4" />}
            defaultOpen={false}
          >
            <div className="flex items-center justify-between mb-4">
              <p className="text-sm text-muted-foreground">
                {analysis.visitsByPoi?.length || 0} POIs with visits
              </p>
              <Button variant="outline" size="sm" onClick={handleReportVisitsByPoi} disabled={!analysis.visitsByPoi?.length}>
                <FileText className="mr-2 h-4 w-4" />
                Download CSV
              </Button>
            </div>
            {analysis.visitsByPoi?.length ? (
              <div className="max-h-96 overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-background">
                    <tr className="border-b">
                      <th className="text-left py-2">POI</th>
                      <th className="text-right py-2">Visits</th>
                      <th className="text-right py-2">Devices</th>
                    </tr>
                  </thead>
                  <tbody>
                    {analysis.visitsByPoi.slice(0, 50).map((r) => (
                      <tr key={r.poiId} className="border-b border-border/50">
                        <td className="py-2">
                          {r.name ? (
                            <div>
                              <p className="font-medium">{r.name}</p>
                              <p className="text-xs text-muted-foreground font-mono">{r.poiId}</p>
                            </div>
                          ) : (
                            <span className="font-mono text-xs">{r.poiId}</span>
                          )}
                        </td>
                        <td className="text-right">{r.visits.toLocaleString()}</td>
                        <td className="text-right">{r.devices.toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {analysis.visitsByPoi.length > 50 && (
                  <p className="text-sm text-muted-foreground text-center py-2">
                    Showing top 50 of {analysis.visitsByPoi.length}. Download CSV for full data.
                  </p>
                )}
              </div>
            ) : (
              <p className="text-muted-foreground">No visits data.</p>
            )}
          </CollapsibleCard>
        </>
      )}

      {/* ── FULL REPORTS (from Generate Full Report) ───────────────── */}
      {hasReports && (
        <div className="space-y-4 mt-4">
          {/* POI Filter */}
          {(() => {
            const poiOptions = (analysis?.visitsByPoi || [])
              .filter((v: VisitByPoi) => v.poiId)
              .map((v: VisitByPoi) => ({
                id: v.poiId,
                name: v.name || v.poiId,
              }));
            return poiOptions.length > 1 ? (
              <PoiFilter
                pois={poiOptions}
                selectedIds={selectedPoiIds}
                onChange={setSelectedPoiIds}
              />
            ) : null;
          })()}

          {/* Summary Cards */}
          {temporalReport?.daily && (() => {
            const daily = temporalReport.daily as { date: string; pings: number; devices: number }[];
            const totalPings = daily.reduce((s: number, d: any) => s + d.pings, 0);
            // Use dedicated total unique devices query (not sum of daily counts which gives device-days)
            const totalDevices = temporalReport.totalUniqueDevices
              || analysis?.summary?.uniqueDevices
              || daily.reduce((s: number, d: any) => s + d.devices, 0);
            const totalDeviceDays = daily.reduce((s: number, d: any) => s + d.devices, 0);
            const dates = daily.map((d: any) => d.date).sort();
            const dateFrom = dates[0] || '—';
            const dateTo = dates[dates.length - 1] || '—';
            const stats = [
              { label: 'Total Pings', value: totalPings.toLocaleString(), icon: <Activity className="h-4 w-4 text-blue-400" /> },
              { label: 'Unique Devices', value: totalDevices.toLocaleString(), icon: <Users className="h-4 w-4 text-cyan-400" /> },
              { label: 'Date Range', value: `${dateFrom} — ${dateTo}`, icon: <Calendar className="h-4 w-4 text-green-400" /> },
              { label: 'Device-Days', value: totalDeviceDays.toLocaleString(), icon: <TrendingUp className="h-4 w-4 text-orange-400" /> },
            ];
            return (
              <div className="grid grid-cols-4 gap-4">
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
          })()}

          {/* Action buttons for filtered reports */}
          <div className="flex flex-wrap gap-2 items-center">
            <Button variant="outline" size="sm" onClick={handleDownloadMaids} disabled={downloadingMaids}>
              {downloadingMaids ? <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : <Download className="mr-1.5 h-3.5 w-3.5" />}
              Download MAIDs
            </Button>
            <Button variant="outline" size="sm" onClick={() => setNseModalOpen(true)}>
              <Users className="mr-1.5 h-3.5 w-3.5" />
              MAIDs by NSE
            </Button>
            <Button variant="outline" size="sm" onClick={() => setCategoryMaidModalOpen(true)}>
              <Target className="mr-1.5 h-3.5 w-3.5" />
              MAIDs by Category
            </Button>
            <Button variant="outline" size="sm" onClick={handleActivate} disabled={activating} title="Upload MAIDs to S3 activations folder">
              {activating ? <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : <Zap className="mr-1.5 h-3.5 w-3.5" />}
              Activate
            </Button>
          </div>

          {/* Temporal Daily Chart */}
          {temporalReport?.daily && (
            <CollapsibleCard title="Daily Activity (POI Visitors)" icon={<TrendingUp className="h-4 w-4" />}>
              <MegaDailyChart data={temporalReport.daily} />
            </CollapsibleCard>
          )}

          {/* Catchment Pie */}
          {catchmentReport?.byZipCode && (
            <CollapsibleCard
              title="Catchment by Zip Code"
              icon={<MapPin className="h-4 w-4" />}
            >
              <div className="flex items-center justify-between mb-4">
                <p className="text-sm text-muted-foreground">
                  {catchmentReport.totalDeviceDays?.toLocaleString()} total device-days across{' '}
                  {catchmentReport.byZipCode.length} zip codes
                </p>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    const total = catchmentReport.totalDeviceDays || 1;
                    const sorted = [...catchmentReport.byZipCode].sort((a, b) => b.deviceDays - a.deviceDays);
                    const cleanZip = (z: string) => z.replace(/^["']+|["']+$/g, '').replace(/^[A-Z]{2}[-\s]/, '');
                    downloadCsv(
                      `${datasetName}-catchment-zipcodes.csv`,
                      ['zip_code', 'city', 'country', 'device_days', 'share_pct', 'lat', 'lng'],
                      sorted.map((z) => [
                        cleanZip(z.zipCode),
                        z.city,
                        z.country,
                        z.deviceDays,
                        ((z.deviceDays / total) * 100).toFixed(2),
                        z.lat,
                        z.lng,
                      ])
                    );
                  }}
                >
                  <Download className="mr-2 h-4 w-4" />
                  Download Zipcodes
                </Button>
              </div>
              <CatchmentPie data={catchmentReport.byZipCode} />
            </CollapsibleCard>
          )}

          {/* Catchment Map */}
          {catchmentReport?.byZipCode && (
            <CollapsibleCard
              title="Catchment Map"
              icon={<Compass className="h-4 w-4" />}
              defaultOpen={false}
            >
              <CatchmentMap data={catchmentReport.byZipCode} />
            </CollapsibleCard>
          )}

          {/* Origin & Destination */}
          {odReport && (
            <CollapsibleCard
              title="Origin & Destination"
              icon={<Navigation className="h-4 w-4" />}
            >
              <p className="text-sm text-muted-foreground mb-4">
                {odReport.totalDeviceDays?.toLocaleString()} device-days analyzed
              </p>
              <ODTables
                origins={odReport.origins}
                destinations={odReport.destinations}
              />
            </CollapsibleCard>
          )}

          {/* Mobility Trends (before/after) */}
          {mobilityReport?.categories && (
            <CollapsibleCard
              title="Mobility Trends (±2h of visit)"
              icon={<Activity className="h-4 w-4" />}
            >
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div>
                  <p className="text-sm text-muted-foreground mb-3">
                    🕐 Places visited <span className="font-semibold text-foreground">before</span> arriving at target POIs
                  </p>
                  <MobilityBar data={mobilityReport.before || mobilityReport.categories} />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground mb-3">
                    🕐 Places visited <span className="font-semibold text-foreground">after</span> leaving target POIs
                  </p>
                  <MobilityBar data={mobilityReport.after || mobilityReport.categories} />
                </div>
              </div>
            </CollapsibleCard>
          )}

          {/* ── Device Routes & Category Flows ────────────────────── */}
          <CollapsibleCard
            title="Device Routes & Category Flows"
            icon={<Navigation className="h-4 w-4" />}
          >
            {!routesResult && !routesLoading && (
              <div className="text-center py-6">
                <p className="text-sm text-muted-foreground mb-3">
                  Analyze device routes to see which POI categories are visited before and after the target POI.
                  Uses spatial join with {'>'}200K real POIs from Overture Maps.
                </p>
                <Button onClick={() => runRouteAnalysis()} disabled={routesLoading || !datasetInfo?.country}>
                  <Play className="mr-2 h-4 w-4" />
                  Run Route Analysis
                </Button>
                {!datasetInfo?.country && (
                  <p className="text-xs text-amber-400 mt-2">Set the country on the job first</p>
                )}
              </div>
            )}

            {routesLoading && (
              <div className="flex items-center justify-center gap-2 py-8 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                {routesProgress || 'Processing...'}
              </div>
            )}

            {routesResult && (
              <div className="space-y-6">
                {/* Stats */}
                <div className="flex items-center gap-4 text-sm">
                  <span className="text-muted-foreground">
                    <span className="font-semibold text-foreground">{routesResult.totalVisitors?.toLocaleString()}</span> visitors analyzed
                  </span>
                  <span className="text-muted-foreground">
                    <span className="font-semibold text-foreground">{routesResult.sankey?.length || 0}</span> category flows
                  </span>
                  <span className="text-muted-foreground">
                    <span className="font-semibold text-foreground">
                      {new Set(routesResult.sampleRoutes?.map((s: any) => s.ad_id) || []).size}
                    </span> devices sampled
                  </span>
                  <Button variant="outline" size="sm" onClick={() => runRouteAnalysis()} disabled={routesLoading} className="ml-auto">
                    <Play className="mr-1.5 h-3.5 w-3.5" />
                    Re-run
                  </Button>
                </div>

                {/* Sankey diagram */}
                {routesResult.sankey?.length > 0 && (
                  <div>
                    <h4 className="text-sm font-semibold mb-2">Category Flows (Before → Target → After)</h4>
                    <p className="text-xs text-muted-foreground mb-3">
                      Categories of POIs visited before arriving at and after leaving the target POI.
                      Width proportional to device count. Hover for details.
                    </p>
                    <SankeyChart
                      data={routesResult.sankey}
                      targetLabel={datasetInfo?.name || datasetName}
                    />
                  </div>
                )}

                {/* Sample routes timeline */}
                {routesResult.sampleRoutes?.length > 0 && (
                  <div>
                    <h4 className="text-sm font-semibold mb-2">Sample Device Routes</h4>
                    <p className="text-xs text-muted-foreground mb-3">
                      Daily routes for a random sample of devices. Each bar is a POI visit —
                      colored by category. The highlighted segment is the target POI visit.
                      Click a device to expand its timeline.
                    </p>
                    <RouteTimeline
                      data={routesResult.sampleRoutes}
                      onRefresh={() => runRouteAnalysis(true)}
                      refreshing={routesLoading}
                    />
                  </div>
                )}
              </div>
            )}
          </CollapsibleCard>

          {/* Departure Hour */}
          {catchmentReport?.departureByHour && (
            <CollapsibleCard
              title="Departure Hour (Catchment)"
              icon={<Timer className="h-4 w-4" />}
              defaultOpen={false}
            >
              <p className="text-sm text-muted-foreground mb-4">
                Hour of first ping of the day (proxy for when visitors leave home)
              </p>
              <HourlyChart
                data={catchmentReport.departureByHour}
                dataKey="deviceDays"
                label="Device-Days"
                color="#f59e0b"
              />
            </CollapsibleCard>
          )}

          {/* Affinity Heatmap */}
          {affinityReport?.byZipCode?.length > 0 && (
            <CollapsibleCard
              title="Affinity Heatmap"
              icon={<Target className="h-4 w-4" />}
            >
              <CatchmentMap
                data={affinityReport.byZipCode.map((z: any) => ({
                  zipCode: z.zipCode,
                  city: z.city,
                  country: z.country,
                  lat: z.lat,
                  lng: z.lng,
                  deviceDays: z.affinityIndex,
                }))}
              />
            </CollapsibleCard>
          )}

          {/* Affinity Index by Postal Code */}
          {affinityReport?.byZipCode?.length > 0 && (
            <CollapsibleCard
              title="Affinity Index by Postal Code"
              icon={<Target className="h-4 w-4" />}
            >
              <p className="text-sm text-muted-foreground mb-4">
                Affinity = 50% dwell time + 50% visit frequency. Scale 0-100.
                {' '}{affinityReport.byZipCode.length} postal codes analyzed.
              </p>
              <div className="flex justify-end mb-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    const sorted = [...affinityReport.byZipCode].sort((a: any, b: any) => b.affinityIndex - a.affinityIndex);
                    const csv = 'zip_code,city,country,affinity_index,avg_dwell_min,avg_frequency,unique_devices,total_visit_days\n' +
                      sorted.map((z: any) => `${z.zipCode},${z.city},${z.country},${z.affinityIndex},${z.avgDwellMinutes},${z.avgFrequency},${z.uniqueDevices},${z.totalVisitDays}`).join('\n');
                    const blob = new Blob([csv], { type: 'text/csv' });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url; a.download = `${datasetName}-affinity-index.csv`; a.click();
                  }}
                >
                  <Download className="h-3 w-3 mr-1" /> Download CSV
                </Button>
              </div>
              <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-background border-b">
                    <tr>
                      <th className="text-left py-2 px-3">Postal Code</th>
                      <th className="text-left py-2 px-3">City</th>
                      <th className="text-right py-2 px-3">Affinity</th>
                      <th className="text-right py-2 px-3">Avg Dwell (min)</th>
                      <th className="text-right py-2 px-3">Avg Frequency</th>
                      <th className="text-right py-2 px-3">Devices</th>
                      <th className="text-right py-2 px-3">Visit-Days</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...affinityReport.byZipCode]
                      .sort((a: any, b: any) => b.affinityIndex - a.affinityIndex)
                      .slice(0, 100)
                      .map((z: any, i: number) => (
                        <tr key={z.zipCode} className="border-b border-border/50 hover:bg-muted/30">
                          <td className="py-2 px-3 font-mono">{z.zipCode}</td>
                          <td className="py-2 px-3 text-muted-foreground">{z.city}</td>
                          <td className="py-2 px-3 text-right">
                            <div className="flex items-center justify-end gap-2">
                              <div className="w-16 h-2 bg-muted rounded-full overflow-hidden">
                                <div
                                  className="h-full rounded-full"
                                  style={{
                                    width: `${z.affinityIndex}%`,
                                    backgroundColor: z.affinityIndex >= 70 ? '#22c55e' : z.affinityIndex >= 40 ? '#eab308' : '#ef4444',
                                  }}
                                />
                              </div>
                              <span className="font-semibold w-8 text-right">{z.affinityIndex}</span>
                            </div>
                          </td>
                          <td className="py-2 px-3 text-right text-muted-foreground">{z.avgDwellMinutes}</td>
                          <td className="py-2 px-3 text-right text-muted-foreground">{z.avgFrequency}</td>
                          <td className="py-2 px-3 text-right">{z.uniqueDevices?.toLocaleString()}</td>
                          <td className="py-2 px-3 text-right text-muted-foreground">{z.totalVisitDays?.toLocaleString()}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </CollapsibleCard>
          )}

          {/* POI Activity by Hour */}
          {hourlyReport?.hourly && (
            <CollapsibleCard
              title="POI Activity by Hour"
              icon={<BarChart3 className="h-4 w-4" />}
            >
              <p className="text-sm text-muted-foreground mb-4">
                When POIs are busiest throughout the day
              </p>
              <HourlyChart
                data={hourlyReport.hourly}
                dataKey="devices"
                label="Devices"
                color="#3b82f6"
              />
            </CollapsibleCard>
          )}
        </div>
      )}

      {/* Empty state */}
      {!analysis && !loading && !hasReports && (
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted-foreground">
              Click &quot;Analyze&quot; to run basic stats and generate the full report dashboard.
            </p>
          </CardContent>
        </Card>
      )}

      {/* Audit Dialog */}
      <Dialog open={!!auditResult} onOpenChange={(open) => !open && setAuditResult(null)}>
        <DialogContent className="max-w-md bg-[#111] border-[#222] text-white">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {auditResult?.symmetric ? (
                <CheckCircle className="h-5 w-5 text-green-500" />
              ) : (
                <AlertTriangle className="h-5 w-5 text-amber-500" />
              )}
              Audit: {auditResult?.jobName || auditResult?.datasetName}
            </DialogTitle>
            <DialogDescription className="text-gray-400">
              {auditResult?.message}
            </DialogDescription>
          </DialogHeader>
          {auditResult && (
            <div className="space-y-3 text-sm">
              <div className="grid grid-cols-2 gap-2">
                <span className="text-gray-500">Veraset source</span>
                <span className="font-mono">{auditResult.sourceCount} files</span>
                <span className="text-gray-500">Our S3</span>
                <span className="font-mono">{auditResult.destCount} files</span>
              </div>
              {!auditResult.symmetric && (
                <>
                  <div className="rounded border border-[#333] p-2 bg-[#0a0a0a]">
                    <span className="text-amber-500 font-medium">Missing in our S3:</span>{' '}
                    {auditResult.missingInDestCount}
                    {auditResult.missingInDest?.length > 0 && (
                      <ul className="mt-1 text-xs font-mono text-gray-400 max-h-24 overflow-y-auto">
                        {auditResult.missingInDest.slice(0, 10).map((k: string, i: number) => (
                          <li key={i} className="truncate">{k}</li>
                        ))}
                        {auditResult.missingInDestCount > 10 && (
                          <li>... and {auditResult.missingInDestCount - 10} more</li>
                        )}
                      </ul>
                    )}
                  </div>
                  <div className="rounded border border-[#333] p-2 bg-[#0a0a0a]">
                    <span className="text-gray-400 font-medium">Extra in our S3:</span>{' '}
                    {auditResult.extraInDestCount}
                  </div>
                  <Button asChild className="w-full mt-2">
                    <Link
                      href={`/sync?jobId=${encodeURIComponent(auditResult.jobId)}&destPath=${encodeURIComponent(auditResult.destPath)}`}
                    >
                      Re-sync to fix
                    </Link>
                  </Button>
                </>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>

      {/* Activate progress modal */}
      <Dialog open={activateModal} onOpenChange={(open) => { if (!activating) setActivateModal(open); }}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Activating for Karlsgate</DialogTitle>
            <DialogDescription>{datasetName}</DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-2">
            <Progress value={activatePercent} className="h-2" />
            <div className="flex items-center gap-2 text-sm">
              {activating ? (
                <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
              ) : activatePercent === 100 ? (
                <CheckCircle className="h-4 w-4 text-green-500" />
              ) : (
                <AlertTriangle className="h-4 w-4 text-red-500" />
              )}
              <span className="text-muted-foreground">{activateMessage}</span>
            </div>
            {activatePercent === 100 && !activating && (
              <Button variant="outline" className="w-full" onClick={() => setActivateModal(false)}>
                Close
              </Button>
            )}
          </div>
        </DialogContent>
      </Dialog>
      <NseModal
        open={nseModalOpen}
        onClose={() => setNseModalOpen(false)}
        datasetName={datasetName}
        catchmentData={catchmentReport?.byZipCode || null}
        dwellMin={dwellMin}
        dwellMax={dwellMax}
        hourFrom={hourFrom}
        hourTo={hourTo}
        jobCountry={datasetInfo?.country || null}
      />
      <CategoryMaidModal
        open={categoryMaidModalOpen}
        onClose={() => setCategoryMaidModalOpen(false)}
        datasetName={datasetName}
        jobCountry={datasetInfo?.country || null}
        dwellMin={dwellMin}
        dwellMax={dwellMax}
        hourFrom={hourFrom}
        hourTo={hourTo}
      />
    </MainLayout>
  );
}
