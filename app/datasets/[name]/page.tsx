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
import { ODTables } from '@/components/mega-jobs/od-tables';
import { MobilityBar } from '@/components/mega-jobs/mobility-bar';
import { HourlyChart } from '@/components/mega-jobs/hourly-chart';
import { PoiFilter } from '@/components/mega-jobs/poi-filter';

interface DatasetInfo {
  id: string;
  name: string;
  jobId?: string | null;
  dateRange?: { from: string; to: string } | null;
  external?: boolean;
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

  // Dwell filter
  const [dwellMin, setDwellMin] = useState<string>('');
  const [dwellMax, setDwellMax] = useState<string>('');
  const [odReport, setODReport] = useState<any>(null);
  const [hourlyReport, setHourlyReport] = useState<any>(null);
  const [catchmentReport, setCatchmentReport] = useState<any>(null);
  const [mobilityReport, setMobilityReport] = useState<any>(null);
  const [temporalReport, setTemporalReport] = useState<any>(null);
  const [reportVersion, setReportVersion] = useState(0);
  const [selectedPoiIds, setSelectedPoiIds] = useState<string[]>([]);

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
          });
        }
      })
      .catch(console.error);
  }, [datasetName]);

  // Load saved reports on mount
  useEffect(() => {
    const types = ['od', 'hourly', 'catchment', 'mobility', 'temporal'];
    const setters: Record<string, (d: any) => void> = {
      od: setODReport,
      hourly: setHourlyReport,
      catchment: setCatchmentReport,
      mobility: setMobilityReport,
      temporal: setTemporalReport,
    };

    for (const type of types) {
      fetch(`/api/datasets/${datasetName}/reports?type=${type}`, { credentials: 'include' })
        .then((r) => r.ok ? r.json() : null)
        .then((data) => { if (data) setters[type](data); })
        .catch(() => {});
    }
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
      await generateReportsOnly();
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
      let attempts = 0;
      while (!done && attempts < 200) {
        attempts++;
        const resetParam = attempts === 1 ? '?reset=true' : '';
        let res: Response;
        try {
          res = await fetch(`/api/datasets/${datasetName}/reports/poll${resetParam}`, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              ...(selectedPoiIds.length > 0 ? { poiIds: selectedPoiIds } : {}),
              ...(dwellMin || dwellMax ? {
                dwellFilter: {
                  ...(dwellMin ? { minMinutes: parseFloat(dwellMin) } : {}),
                  ...(dwellMax ? { maxMinutes: parseFloat(dwellMax) } : {}),
                }
              } : {}),
            }),
          });
        } catch (fetchErr: any) {
          // Network error or timeout — retry
          console.warn(`[REPORT-POLL] Fetch failed (attempt ${attempts}):`, fetchErr.message);
          await new Promise((r) => setTimeout(r, 5000));
          continue;
        }

        if (res.status === 429) {
          console.warn(`[REPORT-POLL] Rate limited (attempt ${attempts}), backing off...`);
          await new Promise((r) => setTimeout(r, 5000 + attempts * 1000));
          continue;
        }

        if (!res.ok && res.status >= 500) {
          const text = await res.text();
          let errMsg: string;
          try {
            const parsed = JSON.parse(text);
            errMsg = parsed.error || `Server error ${res.status}`;
          } catch {
            errMsg = `Server error ${res.status}: ${text.substring(0, 200)}`;
          }
          console.error(`[REPORT-POLL] ${res.status}:`, errMsg);
          // Retry on server errors (Vercel timeouts, transient failures)
          if (attempts < 5) {
            await new Promise((r) => setTimeout(r, 5000));
            continue;
          }
          setReportProgress({ step: 'error', percent: 0, message: `Error: ${errMsg}` });
          break;
        }

        const data = await res.json();

        if (data.error) {
          setReportProgress({ step: 'error', percent: 0, message: `Error: ${data.error}` });
          break;
        }

        setReportProgress(data.progress || { step: data.phase, percent: 50, message: data.phase });

        if (data.phase === 'done') {
          done = true;
          setReportVersion((v) => v + 1);
          toast({ title: 'Reports generated', description: 'All reports are ready.' });
        } else {
          await new Promise((r) => setTimeout(r, 3000));
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

  const hasReports = odReport || hourlyReport || catchmentReport || mobilityReport || temporalReport;

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
          <div className="flex items-center gap-2 mr-2">
            <label className="text-xs text-muted-foreground whitespace-nowrap">Dwell (min):</label>
            <input
              type="number"
              placeholder="Min"
              value={dwellMin}
              onChange={(e) => setDwellMin(e.target.value)}
              className="h-8 w-20 rounded-md border border-input bg-background px-2 text-sm"
            />
            <span className="text-xs text-muted-foreground">-</span>
            <input
              type="number"
              placeholder="Max"
              value={dwellMax}
              onChange={(e) => setDwellMax(e.target.value)}
              className="h-8 w-20 rounded-md border border-input bg-background px-2 text-sm"
            />
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
            <div className="space-y-2">
              <Progress value={reportProgress.percent} className="h-2" />
              <div className="flex items-center gap-2 text-sm">
                <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                <span className="text-muted-foreground">{reportProgress.message}</span>
                <span className="ml-auto text-muted-foreground">{reportProgress.percent}%</span>
              </div>
            </div>
          </CardContent>
        </Card>
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

          {/* Movement Map */}
          <div className="mb-4">
            <MovementMap
              datasetName={datasetName}
              dateFrom={analysis.summary.dateRange.from}
              dateTo={analysis.summary.dateRange.to}
            />
          </div>

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
    </MainLayout>
  );
}
