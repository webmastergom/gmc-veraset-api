'use client';

import { useState, useEffect } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { DailyChart } from '@/components/analysis/daily-chart';
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
  const [catchment, setCatchment] = useState<{
    zipcodes: Array<{
      zipcode: string;
      city: string;
      province: string;
      region: string;
      devices: number;
      percentOfTotal: number;
      percentage?: number;
      source?: string;
    }>;
    coverage?: {
      totalDevicesVisitedPois: number;
      devicesMatchedToZipcode: number;
      geocodingComplete: boolean;
      classificationRatePercent: number;
    };
    summary: { totalZipcodes: number; devicesMatchedToZipcode: number };
  } | null>(null);
  const [loadingCatchment, setLoadingCatchment] = useState(false);
  const [catchmentProgress, setCatchmentProgress] = useState<{
    step: string;
    percent: number;
    message: string;
    detail?: string;
  } | null>(null);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditResult, setAuditResult] = useState<{
    datasetName: string;
    jobId: string;
    jobName: string;
    sourcePath: string;
    destPath: string;
    sourceCount: number;
    destCount: number;
    missingInDestCount: number;
    extraInDestCount: number;
    missingInDest: string[];
    extraInDest: string[];
    symmetric: boolean;
    message: string;
  } | null>(null);

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

  const runAnalysis = async () => {
    setLoading(true);
    setAnalysis(null);
    try {
      const res = await fetch(`/api/datasets/${datasetName}/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
        credentials: 'include',
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.details || err.error || res.statusText);
      }
      const data: AnalysisResult = await res.json();
      setAnalysis(data);
    } catch (e: any) {
      alert(`Analysis failed: ${e.message || 'Unknown error'}`);
    } finally {
      setLoading(false);
    }
  };

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
        const msg = data.totalDevices != null && data.deviceCount < data.totalDevices
          ? `Exported ${data.deviceCount.toLocaleString()} rows (truncated from ${data.totalDevices.toLocaleString()} total)`
          : `Exported ${data.deviceCount.toLocaleString()} rows`;
        toast({ title: 'Export complete', description: msg });
      }
    } catch (e: any) {
      toast({ title: 'Export failed', description: e.message || 'Error downloading full dataset', variant: 'destructive' });
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
        const msg = data.totalDevices != null && data.deviceCount < data.totalDevices
          ? `Exported ${data.deviceCount.toLocaleString()} MAIDs (truncated from ${data.totalDevices.toLocaleString()} total)`
          : `Exported ${data.deviceCount.toLocaleString()} MAIDs`;
        toast({ title: 'Export complete', description: msg });
      }
    } catch (e: any) {
      toast({ title: 'Export failed', description: e.message || 'Error downloading MAIDs list', variant: 'destructive' });
    } finally {
      setDownloadingMaids(false);
    }
  };

  const handleReportVisitsByPoi = () => {
    if (!analysis?.visitsByPoi?.length) return;
    downloadCsv(
      `${datasetName}-visitas-por-poi-${new Date().toISOString().slice(0, 10)}.csv`,
      ['poi_name', 'poi_id', 'visits', 'devices'],
      analysis.visitsByPoi.map((r) => [
        r.name || r.poiId, // Use name if available, otherwise use ID
        r.poiId,
        r.visits,
        r.devices
      ])
    );
  };

  const loadCatchment = () => {
    setLoadingCatchment(true);
    setCatchment(null);
    setCatchmentProgress({ step: 'initializing', percent: 0, message: 'Starting analysis...' });

    const es = new EventSource(`/api/datasets/${datasetName}/catchment/stream`);

    es.addEventListener('progress', (event) => {
      try {
        const data = JSON.parse(event.data);
        setCatchmentProgress({
          step: data.step,
          percent: data.percent,
          message: data.message,
          detail: data.detail,
        });
        if (data.step === 'error') {
          es.close();
          setLoadingCatchment(false);
          alert(`Catchment failed: ${data.message}`);
        }
      } catch { /* ignore parse errors */ }
    });

    es.addEventListener('result', (event) => {
      try {
        const data = JSON.parse(event.data);
        setCatchment({
          zipcodes: data.zipcodes || [],
          coverage: data.coverage,
          summary: {
            totalZipcodes: data.summary?.totalZipcodes ?? 0,
            devicesMatchedToZipcode: data.summary?.devicesMatchedToZipcode ?? data.coverage?.devicesMatchedToZipcode ?? 0,
          },
        });
      } catch { /* ignore */ }
      es.close();
      setLoadingCatchment(false);
      setCatchmentProgress(null);
    });

    es.onerror = () => {
      es.close();
      // If we didn't get a result yet, try the regular endpoint as fallback
      if (!catchment) {
        setCatchmentProgress({ step: 'running_queries', percent: 50, message: 'Reconnecting...', detail: 'Falling back to standard request' });
        fetch(`/api/datasets/${datasetName}/catchment`, { credentials: 'include' })
          .then((res) => {
            if (!res.ok) throw new Error(res.statusText);
            return res.json();
          })
          .then((data) => {
            setCatchment({
              zipcodes: data.zipcodes || [],
              coverage: data.coverage,
              summary: {
                totalZipcodes: data.summary?.totalZipcodes ?? 0,
                devicesMatchedToZipcode: data.summary?.devicesMatchedToZipcode ?? data.coverage?.devicesMatchedToZipcode ?? 0,
              },
            });
          })
          .catch((e) => {
            alert(`Catchment failed: ${e.message || 'Unknown error'}`);
          })
          .finally(() => {
            setLoadingCatchment(false);
            setCatchmentProgress(null);
          });
      }
    };
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
      alert(e.message || 'Audit failed');
    } finally {
      setAuditLoading(false);
    }
  };

  const handleReportCatchment = () => {
    if (!catchment || !catchment.zipcodes?.length) return;
    const rows: (string | number)[][] = catchment.zipcodes.map((z) => [
      z.zipcode,
      z.city,
      z.province,
      z.region,
      z.devices,
      `${(z.percentOfTotal ?? z.percentage ?? 0).toFixed(2)}%`,
    ]);
    downloadCsv(
      `${datasetName}-origen-codigo-postal-${new Date().toISOString().slice(0, 10)}.csv`,
      ['postal_code', 'city', 'province', 'region', 'devices', 'percent_of_total'],
      rows
    );
  };

  const displayName = datasetInfo?.name || datasetName;

  return (
    <MainLayout>
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

      <div className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <h2 className="text-xl font-semibold">Data analysis</h2>
        <div className="flex flex-wrap gap-2">
          <Button onClick={runAnalysis} disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Analyzing...
              </>
            ) : (
              'Run analysis'
            )}
          </Button>
          <Button variant="outline" onClick={handleDownloadFull} disabled={downloadingFull}>
            {downloadingFull ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Download className="mr-2 h-4 w-4" />
            )}
            Download full dataset
          </Button>
          <Button variant="outline" onClick={handleDownloadMaids} disabled={downloadingMaids}>
            {downloadingMaids ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Download className="mr-2 h-4 w-4" />
            )}
            Download MAIDs list
          </Button>
          <Button variant="outline" onClick={runAudit} disabled={auditLoading} title="Compare with Veraset source; detect asymmetries">
            {auditLoading ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <ShieldCheck className="mr-2 h-4 w-4" />
            )}
            Audit
          </Button>
        </div>
      </div>

      {analysis && (
        <>
          <div className="mb-6 grid gap-4 md:grid-cols-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Total pings</CardTitle>
                <Activity className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {analysis.summary.totalPings.toLocaleString()}
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Unique devices</CardTitle>
                <Users className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {analysis.summary.uniqueDevices.toLocaleString()}
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">POIs with visits</CardTitle>
                <MapPin className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {analysis.summary.uniquePois.toLocaleString()}
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Days analyzed</CardTitle>
                <Calendar className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {analysis.summary.daysAnalyzed}
                </div>
                <p className="text-xs text-muted-foreground">
                  {analysis.summary.dateRange.from} → {analysis.summary.dateRange.to}
                </p>
              </CardContent>
            </Card>
          </div>

          <Card className="mb-6">
            <CardHeader>
              <CardTitle>Activity by day</CardTitle>
              <CardDescription>
                One chart per day. All job days are read without exception.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <DailyChart data={analysis.dailyData} />
            </CardContent>
          </Card>

          <div className="mb-6">
            <MovementMap
              datasetName={datasetName}
              dateFrom={analysis.summary.dateRange.from}
              dateTo={analysis.summary.dateRange.to}
            />
          </div>

          <div className="grid gap-6 md:grid-cols-2">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <div>
                  <CardTitle>Report: Visits by POI</CardTitle>
                  <CardDescription>Download CSV with visits and devices per POI.</CardDescription>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleReportVisitsByPoi}
                  disabled={!analysis.visitsByPoi?.length}
                >
                  <FileText className="mr-2 h-4 w-4" />
                  Download CSV
                </Button>
              </CardHeader>
              <CardContent>
                {analysis.visitsByPoi?.length ? (
                  <div className="max-h-64 overflow-auto">
                    <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>POI Name</TableHead>
                            <TableHead className="text-right">Visits</TableHead>
                            <TableHead className="text-right">Devices</TableHead>
                          </TableRow>
                        </TableHeader>
                      <TableBody>
                        {analysis.visitsByPoi.slice(0, 50).map((r) => (
                          <TableRow key={r.poiId}>
                            <TableCell>
                              {r.name ? (
                                <div>
                                  <div className="font-medium">{r.name}</div>
                                  <div className="font-mono text-xs text-muted-foreground">{r.poiId}</div>
                                </div>
                              ) : (
                                <span className="font-mono text-xs">{r.poiId}</span>
                              )}
                            </TableCell>
                            <TableCell className="text-right">{r.visits.toLocaleString()}</TableCell>
                            <TableCell className="text-right">{r.devices.toLocaleString()}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                    {analysis.visitsByPoi.length > 50 && (
                      <p className="mt-2 text-xs text-muted-foreground">
                        Showing 50 of {analysis.visitsByPoi.length}. Use &quot;Download CSV&quot; for the full list.
                      </p>
                    )}
                  </div>
                ) : (
                  <p className="text-muted-foreground">No visits-by-POI data.</p>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between">
                <div>
                  <CardTitle>Report: Origin by postal code</CardTitle>
                  <CardDescription>
                    Where do visitors come from? First GPS ping of each device-day, geocoded to postal code.
                  </CardDescription>
                </div>
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={loadCatchment}
                    disabled={loadingCatchment}
                  >
                    {loadingCatchment ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      'Generate report'
                    )}
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleReportCatchment}
                    disabled={!catchment || (catchment.zipcodes?.length ?? 0) === 0}
                  >
                    <FileText className="mr-2 h-4 w-4" />
                    Download CSV
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {loadingCatchment && catchmentProgress ? (
                  <div className="space-y-4 py-2">
                    {/* Step indicators */}
                    <div className="space-y-2">
                      {[
                        { key: 'initializing', label: 'Initializing', icon: Database },
                        { key: 'preparing_table', label: 'Preparing table', icon: Database },
                        { key: 'running_queries', label: 'Running Athena queries', icon: Search },
                        { key: 'geocoding', label: 'Geocoding coordinates', icon: MapPinned },
                        { key: 'aggregating', label: 'Aggregating results', icon: BarChart3 },
                      ].map((s, i) => {
                        const stepOrder = ['initializing', 'preparing_table', 'running_queries', 'geocoding', 'aggregating', 'completed'];
                        const currentIdx = stepOrder.indexOf(catchmentProgress.step);
                        const thisIdx = stepOrder.indexOf(s.key);
                        const isActive = catchmentProgress.step === s.key;
                        const isDone = currentIdx > thisIdx;
                        const Icon = s.icon;
                        return (
                          <div key={s.key} className={`flex items-center gap-3 text-sm transition-all ${isActive ? 'text-white' : isDone ? 'text-green-500' : 'text-muted-foreground/40'}`}>
                            {isDone ? (
                              <CheckCircle className="h-4 w-4 shrink-0 text-green-500" />
                            ) : isActive ? (
                              <Loader2 className="h-4 w-4 shrink-0 animate-spin" />
                            ) : (
                              <Icon className="h-4 w-4 shrink-0" />
                            )}
                            <span className={isActive ? 'font-medium' : ''}>{s.label}</span>
                            {isActive && catchmentProgress.detail && (
                              <span className="ml-auto text-xs text-muted-foreground truncate max-w-[200px]">
                                {catchmentProgress.detail}
                              </span>
                            )}
                          </div>
                        );
                      })}
                    </div>
                    {/* Progress bar */}
                    <div className="space-y-1">
                      <Progress value={catchmentProgress.percent} className="h-2" />
                      <div className="flex justify-between text-xs text-muted-foreground">
                        <span>{catchmentProgress.message}</span>
                        <span>{catchmentProgress.percent}%</span>
                      </div>
                    </div>
                  </div>
                ) : catchment ? (
                  <>
                    {catchment.coverage && (
                      <div className="mb-4 space-y-3">
                        {catchment.coverage.geocodingComplete === false && (
                          <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-300">
                            Geocoding was truncated due to Nominatim API limits. Results may be incomplete.
                          </div>
                        )}
                        <p className="text-sm text-muted-foreground">
                          <strong className="text-foreground">{catchment.coverage.totalDevicesVisitedPois.toLocaleString()}</strong> devices visited your POIs
                        </p>
                        <p className="text-sm">
                          <strong className="text-green-600 dark:text-green-500">{catchment.coverage.devicesMatchedToZipcode.toLocaleString()}</strong> matched to postal code
                          ({catchment.coverage.classificationRatePercent.toFixed(1)}%)
                        </p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          Methodology: first GPS ping of each device-day, reverse geocoded to postal code
                        </p>
                      </div>
                    )}
                    {catchment.zipcodes?.length ? (
                      <div className="max-h-64 overflow-auto">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Code</TableHead>
                              <TableHead>City</TableHead>
                              <TableHead className="text-right">Devices</TableHead>
                              <TableHead className="text-right">% total</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {catchment.zipcodes.slice(0, 30).map((z) => (
                              <TableRow key={z.zipcode}>
                                <TableCell>{z.zipcode}</TableCell>
                                <TableCell>{z.city}</TableCell>
                                <TableCell className="text-right">{z.devices.toLocaleString()}</TableCell>
                                <TableCell className="text-right">
                                  {(z.percentOfTotal ?? z.percentage ?? 0).toFixed(1)}%
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                        {catchment.zipcodes.length > 30 && (
                          <p className="mt-2 text-xs text-muted-foreground">
                            Showing 30 of {catchment.zipcodes.length}. Download CSV for full list.
                          </p>
                        )}
                      </div>
                    ) : (
                      <p className="text-muted-foreground">No origin results.</p>
                    )}
                  </>
                ) : (
                  <p className="text-muted-foreground">
                    Click &quot;Generate report&quot; to compute visitor origins by postal code.
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        </>
      )}

      {!analysis && !loading && (
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted-foreground">
              Click &quot;Run analysis&quot; to analyze this dataset. All job days will be read without exception.
            </p>
          </CardContent>
        </Card>
      )}

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
                    {auditResult.missingInDest.length > 0 && (
                      <ul className="mt-1 text-xs font-mono text-gray-400 max-h-24 overflow-y-auto">
                        {auditResult.missingInDest.slice(0, 10).map((k, i) => (
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
    </MainLayout>
  );
}
