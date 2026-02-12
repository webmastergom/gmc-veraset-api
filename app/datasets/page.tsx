'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
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
  Database,
  Calendar,
  FileBox,
  MapPin,
  Search,
  LayoutGrid,
  List,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Loader2,
  FolderOpen,
  ChevronDown,
  ChevronRight,
  RefreshCw,
  Download,
  ShieldCheck,
  AlertTriangle,
  CheckCircle
} from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { ExportDialog } from '@/components/export/export-dialog';

interface Dataset {
  id: string;
  name: string;
  jobId?: string;
  type?: string;
  poiCount?: number;
  external?: boolean;
  objectCount: number;
  totalBytes: number;
  dateRange: { from: string; to: string } | null;
  lastModified?: string;
  syncedAt?: string | null;
  dateRangeDiscrepancy?: {
    requestedDays: number;
    actualDays: number;
    missingDays: number;
  } | null;
  verasetPayload?: {
    date_range: { from_date: string; to_date: string };
  } | null;
  actualDateRange?: {
    from: string;
    to: string;
    days: number;
  } | null;
}

type SortField = 'name' | 'date' | 'size' | 'files';
type SortDirection = 'asc' | 'desc';
type ViewMode = 'modern' | 'classic';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatDate(dateStr?: string): string {
  if (!dateStr) return '-';
  try {
    return new Date(dateStr).toLocaleDateString('es-ES', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  } catch {
    return dateStr;
  }
}

export default function DatasetsPage() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [sortField, setSortField] = useState<SortField>('date');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [viewMode, setViewMode] = useState<ViewMode>('modern');
  const [bucketReviewOpen, setBucketReviewOpen] = useState(false);
  const [bucketList, setBucketList] = useState<{ bucket: string; listedAt: string; items: Array<{ prefix: string; objectCount: number; totalBytes: number; isSystem: boolean }> } | null>(null);
  const [bucketLoading, setBucketLoading] = useState(false);
  const [bucketError, setBucketError] = useState<string | null>(null);
  const [exportDatasetId, setExportDatasetId] = useState<string | null>(null);
  const [downloadingFullId, setDownloadingFullId] = useState<string | null>(null);
  const [auditLoadingId, setAuditLoadingId] = useState<string | null>(null);
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

  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    fetch('/api/datasets', { credentials: 'include' })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.error || data.details || r.statusText || 'Failed to load datasets');
        return data;
      })
      .then((data) => setDatasets(data.datasets ?? []))
      .catch((err) => {
        console.error('Error fetching datasets:', err);
        setFetchError(err.message || 'Unknown error');
        setDatasets([]);
      })
      .finally(() => setLoading(false));
  }, []);

  // Filter and sort datasets
  const filteredDatasets = useMemo(() => {
    let result = [...datasets];

    // Filter by search query
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      result = result.filter(ds =>
        ds.name.toLowerCase().includes(query) ||
        ds.id.toLowerCase().includes(query) ||
        ds.type?.toLowerCase().includes(query)
      );
    }

    // Sort
    result.sort((a, b) => {
      let comparison = 0;

      switch (sortField) {
        case 'name':
          comparison = a.name.localeCompare(b.name);
          break;
        case 'date':
          const dateA = a.dateRange?.to || a.lastModified || '0000';
          const dateB = b.dateRange?.to || b.lastModified || '0000';
          comparison = dateA.localeCompare(dateB);
          break;
        case 'size':
          comparison = a.totalBytes - b.totalBytes;
          break;
        case 'files':
          comparison = a.objectCount - b.objectCount;
          break;
      }

      return sortDirection === 'asc' ? comparison : -comparison;
    });

    return result;
  }, [datasets, searchQuery, sortField, sortDirection]);

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return <ArrowUpDown className="h-4 w-4 opacity-50" />;
    return sortDirection === 'asc'
      ? <ArrowUp className="h-4 w-4" />
      : <ArrowDown className="h-4 w-4" />;
  };

  const handleDownloadFullDataset = async (datasetId: string) => {
    setDownloadingFullId(datasetId);
    try {
      const res = await fetch(`/api/datasets/${datasetId}/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filters: { minDwellTime: null, minPings: null } }),
        credentials: 'include',
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || data.details || 'Export failed');
      if (data.downloadUrl) {
        window.open(data.downloadUrl, '_blank', 'noopener,noreferrer');
      }
    } catch (err: any) {
      console.error('Full dataset export failed:', err);
      alert(err.message || 'Error al descargar el dataset completo');
    } finally {
      setDownloadingFullId(null);
    }
  };

  const runAudit = async (datasetId: string) => {
    setAuditLoadingId(datasetId);
    setAuditResult(null);
    try {
      const res = await fetch(`/api/datasets/${encodeURIComponent(datasetId)}/audit`, {
        method: 'POST',
        credentials: 'include',
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || data.details || 'Audit failed');
      setAuditResult(data);
    } catch (err: any) {
      console.error('Audit failed:', err);
      alert(err.message || 'Audit failed');
    } finally {
      setAuditLoadingId(null);
    }
  };

  const fetchBucketList = () => {
    setBucketLoading(true);
    setBucketError(null);
    const apiUrl = typeof window !== 'undefined' ? `${window.location.origin}/api/s3/list` : '/api/s3/list';
    fetch(apiUrl, { credentials: 'include' })
      .then(r => r.json())
      .then(data => {
        if (data.error) throw new Error(data.details || data.error);
        setBucketList(data);
      })
      .catch(err => {
        console.error('Error fetching S3 list:', err);
        setBucketError(err.message || 'Error al listar el bucket');
        setBucketList(null);
      })
      .finally(() => setBucketLoading(false));
  };

  if (loading) {
    return (
      <MainLayout>
        <div className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-gray-500" />
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout>
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-3xl font-bold text-white">Datasets</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Same name = different job runs. File count is what was synced from Veraset at sync time; use Re-sync on the job to refresh.
          </p>
        </div>
        <div className="flex items-center gap-2">
          {/* View Mode Toggle */}
          <div className="flex items-center border border-[#222] rounded-lg p-1 bg-[#0a0a0a]">
            <Button
              variant="ghost"
              size="sm"
              className={`px-3 ${viewMode === 'modern' ? 'bg-[#1a1a1a] text-white' : 'text-gray-500 hover:text-white'}`}
              onClick={() => setViewMode('modern')}
            >
              <LayoutGrid className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className={`px-3 ${viewMode === 'classic' ? 'bg-[#1a1a1a] text-white' : 'text-gray-500 hover:text-white'}`}
              onClick={() => setViewMode('classic')}
            >
              <List className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </div>

      {/* Search and Sort Controls */}
      <div className="flex flex-col sm:flex-row gap-3 mb-6">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-500" />
          <Input
            placeholder="Search datasets..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 bg-[#0a0a0a] border-[#222] focus:border-[#333]"
          />
        </div>

        <Select value={sortField} onValueChange={(v) => setSortField(v as SortField)}>
          <SelectTrigger className="w-[160px] bg-[#0a0a0a] border-[#222]">
            <SelectValue placeholder="Sort by" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="name">Name</SelectItem>
            <SelectItem value="date">Date</SelectItem>
            <SelectItem value="size">Size</SelectItem>
            <SelectItem value="files">Files</SelectItem>
          </SelectContent>
        </Select>

        <Button
          variant="outline"
          size="icon"
          onClick={() => setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc')}
          className="bg-[#0a0a0a] border-[#222] hover:bg-[#1a1a1a]"
        >
          {sortDirection === 'asc' ? <ArrowUp className="h-4 w-4" /> : <ArrowDown className="h-4 w-4" />}
        </Button>
      </div>

      {/* Results count */}
      {searchQuery && (
        <p className="text-sm text-gray-500 mb-4">
          {filteredDatasets.length} {filteredDatasets.length === 1 ? 'result' : 'results'} found
        </p>
      )}

      {datasets.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center">
            <Database className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            {fetchError ? (
              <p className="text-red-400">
                Error loading datasets: {fetchError}
              </p>
            ) : (
              <p className="text-muted-foreground">
                No datasets available. Sync a completed job to create a dataset.
              </p>
            )}
          </CardContent>
        </Card>
      ) : filteredDatasets.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center">
            <Search className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
            <p className="text-muted-foreground">
              No datasets match your search.
            </p>
            <Button
              variant="link"
              onClick={() => setSearchQuery('')}
              className="mt-2"
            >
              Clear search
            </Button>
          </CardContent>
        </Card>
      ) : viewMode === 'modern' ? (
        /* Modern View - Cards */
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {filteredDatasets.map((ds) => (
            <Card key={ds.id} className="bg-[#111] border-[#1a1a1a] hover:border-[#333] transition-colors">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg text-white flex items-center gap-2 truncate">
                    <Database className="h-5 w-5 flex-shrink-0" />
                    <span className="truncate">{ds.name}</span>
                  </CardTitle>
                  {ds.external && (
                    <span className="text-xs px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20 flex-shrink-0">
                      External
                    </span>
                  )}
                </div>
                {ds.id !== ds.name && (
                  <p className="text-xs text-gray-500 font-mono truncate mt-1" title="Job/dataset ID. Same name = different runs; file count depends on what Veraset delivered at sync time.">
                    {ds.id}
                  </p>
                )}
                {ds.syncedAt && (
                  <p className="text-xs text-gray-500 mt-0.5">
                    Synced: {formatDate(ds.syncedAt)}
                  </p>
                )}
              </CardHeader>
              <CardContent className="space-y-2">
                <div className="flex items-center gap-2 text-sm text-gray-400">
                  <FileBox className="h-4 w-4 flex-shrink-0" />
                  {ds.objectCount.toLocaleString()} files • {formatBytes(ds.totalBytes)}
                </div>
                {ds.dateRange && (
                  <div className="flex items-center gap-2 text-sm text-gray-400">
                    <Calendar className="h-4 w-4 flex-shrink-0" />
                    {ds.dateRange.from} → {ds.dateRange.to}
                  </div>
                )}
                {ds.dateRangeDiscrepancy && ds.dateRangeDiscrepancy.missingDays > 0 && (
                  <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-md p-2 text-xs">
                    <div className="flex items-center gap-1 text-yellow-400 font-semibold mb-1">
                      <AlertTriangle className="h-3 w-3" />
                      Missing {ds.dateRangeDiscrepancy.missingDays} day{ds.dateRangeDiscrepancy.missingDays !== 1 ? 's' : ''}
                    </div>
                    <div className="text-yellow-300/80">
                      Requested: {ds.dateRangeDiscrepancy.requestedDays} days
                      {ds.verasetPayload?.date_range && (
                        <> ({ds.verasetPayload.date_range.from_date} to {ds.verasetPayload.date_range.to_date})</>
                      )}
                    </div>
                    <div className="text-yellow-300/80">
                      Received: {ds.dateRangeDiscrepancy.actualDays} days
                      {ds.actualDateRange && (
                        <> ({ds.actualDateRange.from} to {ds.actualDateRange.to})</>
                      )}
                    </div>
                  </div>
                )}
                {ds.poiCount && (
                  <div className="flex items-center gap-2 text-sm text-gray-400">
                    <MapPin className="h-4 w-4 flex-shrink-0" />
                    {ds.poiCount.toLocaleString()} POIs
                  </div>
                )}
                <div className="flex flex-col gap-2 mt-4">
                  <div className="flex gap-2 flex-wrap">
                    <Button asChild className="flex-1 min-w-0">
                      <Link href={`/datasets/${ds.id}`}>
                        Analyze
                      </Link>
                    </Button>
                    <Button
                      variant="outline"
                      size="icon"
                      className="border-[#333] shrink-0"
                      onClick={() => setExportDatasetId(ds.id)}
                      title="Export IDs with filters"
                    >
                      <Download className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="outline"
                      size="icon"
                      className="border-[#333] shrink-0"
                      onClick={() => runAudit(ds.id)}
                      disabled={auditLoadingId === ds.id}
                      title="Compare with Veraset source; detect asymmetries"
                    >
                      {auditLoadingId === ds.id ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <ShieldCheck className="h-4 w-4" />
                      )}
                    </Button>
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="w-full text-gray-400 hover:text-white border border-[#222] hover:border-[#333]"
                    onClick={() => handleDownloadFullDataset(ds.id)}
                    disabled={downloadingFullId === ds.id}
                    title="Descargar todos los device IDs (sin filtros)"
                  >
                    {downloadingFullId === ds.id ? (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin shrink-0" />
                    ) : (
                      <Download className="h-4 w-4 mr-2 shrink-0" />
                    )}
                    Descargar dataset completo
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : (
        /* Classic View - Table */
        <Card className="bg-[#111] border-[#1a1a1a]">
          <Table>
            <TableHeader>
              <TableRow className="border-[#222] hover:bg-transparent">
                <TableHead
                  className="text-gray-400 cursor-pointer hover:text-white"
                  onClick={() => toggleSort('name')}
                >
                  <div className="flex items-center gap-2">
                    Name
                    <SortIcon field="name" />
                  </div>
                </TableHead>
                <TableHead
                  className="text-gray-400 cursor-pointer hover:text-white"
                  onClick={() => toggleSort('date')}
                >
                  <div className="flex items-center gap-2">
                    Date Range
                    <SortIcon field="date" />
                  </div>
                </TableHead>
                <TableHead
                  className="text-gray-400 cursor-pointer hover:text-white text-right"
                  onClick={() => toggleSort('files')}
                >
                  <div className="flex items-center gap-2 justify-end">
                    Files
                    <SortIcon field="files" />
                  </div>
                </TableHead>
                <TableHead
                  className="text-gray-400 cursor-pointer hover:text-white text-right"
                  onClick={() => toggleSort('size')}
                >
                  <div className="flex items-center gap-2 justify-end">
                    Size
                    <SortIcon field="size" />
                  </div>
                </TableHead>
                <TableHead className="text-gray-400 text-right">POIs</TableHead>
                <TableHead className="text-gray-400 w-[240px]"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredDatasets.map((ds) => (
                <TableRow key={ds.id} className="border-[#222] hover:bg-[#1a1a1a]">
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Database className="h-4 w-4 text-gray-500" />
                      <div>
                        <div className="font-medium text-white flex items-center gap-2">
                          {ds.name}
                          {ds.external && (
                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-400 border border-blue-500/20">
                              External
                            </span>
                          )}
                        </div>
                        {ds.id !== ds.name && (
                          <div className="text-xs text-gray-500 font-mono truncate max-w-[200px]">
                            {ds.id}
                          </div>
                        )}
                      </div>
                    </div>
                  </TableCell>
                  <TableCell className="text-gray-400">
                    {ds.dateRange ? (
                      <div className="flex items-center gap-2">
                        <span>{ds.dateRange.from} → {ds.dateRange.to}</span>
                        {ds.dateRangeDiscrepancy && ds.dateRangeDiscrepancy.missingDays > 0 && (
                          <span className="text-yellow-400" title={`Missing ${ds.dateRangeDiscrepancy.missingDays} days: Requested ${ds.dateRangeDiscrepancy.requestedDays}, received ${ds.dateRangeDiscrepancy.actualDays}`}>
                            <AlertTriangle className="h-4 w-4" />
                          </span>
                        )}
                      </div>
                    ) : (
                      <span className="text-gray-600">-</span>
                    )}
                  </TableCell>
                  <TableCell className="text-right text-gray-400">
                    {ds.objectCount.toLocaleString()}
                  </TableCell>
                  <TableCell className="text-right text-gray-400">
                    {formatBytes(ds.totalBytes)}
                  </TableCell>
                  <TableCell className="text-right text-gray-400">
                    {ds.poiCount ? ds.poiCount.toLocaleString() : '-'}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex items-center justify-end gap-2 flex-wrap">
                      <Button
                        variant="outline"
                        size="sm"
                        className="border-[#333]"
                        onClick={() => setExportDatasetId(ds.id)}
                        title="Exportar IDs con filtros"
                      >
                        <Download className="h-4 w-4 mr-1" />
                        Exportar IDs
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="border-[#333]"
                        onClick={() => handleDownloadFullDataset(ds.id)}
                        disabled={downloadingFullId === ds.id}
                        title="Descargar todos los device IDs (sin filtros)"
                      >
                        {downloadingFullId === ds.id ? (
                          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                        ) : (
                          <Download className="h-4 w-4 mr-1" />
                        )}
                        Dataset completo
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="border-[#333]"
                        onClick={() => runAudit(ds.id)}
                        disabled={auditLoadingId === ds.id}
                        title="Compare with Veraset source"
                      >
                        {auditLoadingId === ds.id ? (
                          <Loader2 className="h-4 w-4 mr-1 animate-spin" />
                        ) : (
                          <ShieldCheck className="h-4 w-4 mr-1" />
                        )}
                        Audit
                      </Button>
                      <Button asChild size="sm" variant="outline" className="border-[#333]">
                        <Link href={`/datasets/${ds.id}`}>
                          Analyze
                        </Link>
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}

      {exportDatasetId && (
        <ExportDialog
          datasetName={exportDatasetId}
          open={!!exportDatasetId}
          onOpenChange={(open) => !open && setExportDatasetId(null)}
        />
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

      {/* Revisión directa del bucket S3 */}
      <Card className="mt-8 bg-[#0a0a0a] border-[#222]">
        <CardHeader>
          <button
            type="button"
            onClick={() => {
              setBucketReviewOpen(prev => !prev);
              if (!bucketReviewOpen && !bucketList && !bucketLoading) fetchBucketList();
            }}
            className="flex items-center gap-2 text-left w-full"
          >
            {bucketReviewOpen ? <ChevronDown className="h-5 w-5 text-gray-400" /> : <ChevronRight className="h-5 w-5 text-gray-400" />}
            <FolderOpen className="h-5 w-5 text-gray-400" />
            <CardTitle className="text-lg text-white">Revisión directa del bucket S3 (Garritz)</CardTitle>
          </button>
          <CardDescription className="text-gray-500">
            Lista de prefijos en el bucket sin pasar por el mapeo de jobs
          </CardDescription>
        </CardHeader>
        {bucketReviewOpen && (
          <CardContent className="pt-0">
            <div className="flex items-center gap-2 mb-4">
              <Button
                variant="outline"
                size="sm"
                onClick={fetchBucketList}
                disabled={bucketLoading}
                className="border-[#333]"
              >
                {bucketLoading ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
                Actualizar lista
              </Button>
              {bucketList && (
                <span className="text-sm text-gray-500">
                  Bucket: <span className="font-mono">{bucketList.bucket}</span>
                  {bucketList.listedAt && (
                    <> · Listado: {new Date(bucketList.listedAt).toLocaleString()}</>
                  )}
                </span>
              )}
            </div>
            {bucketLoading && !bucketList && (
              <div className="flex items-center justify-center py-8">
                <Loader2 className="h-8 w-8 animate-spin text-gray-500" />
              </div>
            )}
            {bucketList?.items && (
              <div className="rounded-md border border-[#222] overflow-hidden">
                <Table>
                  <TableHeader>
                    <TableRow className="border-[#222] hover:bg-transparent">
                      <TableHead className="text-gray-400">Prefijo</TableHead>
                      <TableHead className="text-gray-400 text-right">Objetos</TableHead>
                      <TableHead className="text-gray-400 text-right">Tamaño</TableHead>
                      <TableHead className="text-gray-400 w-[80px]">Tipo</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {bucketList.items.map((item) => (
                      <TableRow key={item.prefix} className="border-[#222] hover:bg-[#111]">
                        <TableCell className="font-mono text-sm text-white">{item.prefix}</TableCell>
                        <TableCell className="text-right text-gray-400">{item.objectCount.toLocaleString()}</TableCell>
                        <TableCell className="text-right text-gray-400">{formatBytes(item.totalBytes)}</TableCell>
                        <TableCell>
                          {item.isSystem ? (
                            <span className="text-xs px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20">Sistema</span>
                          ) : (
                            <span className="text-xs px-2 py-0.5 rounded bg-green-500/10 text-green-400 border border-green-500/20">Dataset</span>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
            {bucketList && (!bucketList.items || bucketList.items.length === 0) && (
              <p className="text-sm text-gray-500 py-4">No se encontraron prefijos en el bucket.</p>
            )}
            {bucketError && (
              <p className="text-sm text-destructive py-4">{bucketError}</p>
            )}
          </CardContent>
        )}
      </Card>
    </MainLayout>
  );
}
