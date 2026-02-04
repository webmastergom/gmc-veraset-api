'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
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
  Loader2
} from 'lucide-react';

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

  useEffect(() => {
    fetch('/api/datasets', {
      credentials: 'include',
    })
      .then(r => r.json())
      .then(data => setDatasets(data.datasets || []))
      .catch(err => {
        console.error('Error fetching datasets:', err);
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
        <h1 className="text-3xl font-bold text-white">Datasets</h1>

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
            <p className="text-muted-foreground">
              No datasets available. Sync a completed job to create a dataset.
            </p>
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
                  <p className="text-xs text-gray-500 font-mono truncate mt-1">
                    {ds.id}
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
                {ds.poiCount && (
                  <div className="flex items-center gap-2 text-sm text-gray-400">
                    <MapPin className="h-4 w-4 flex-shrink-0" />
                    {ds.poiCount.toLocaleString()} POIs
                  </div>
                )}
                <Button asChild className="w-full mt-4">
                  <Link href={`/datasets/${ds.id}`}>
                    Analyze
                  </Link>
                </Button>
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
                <TableHead className="text-gray-400 w-[100px]"></TableHead>
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
                      <span>{ds.dateRange.from} → {ds.dateRange.to}</span>
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
                    <Button asChild size="sm" variant="outline" className="border-[#333]">
                      <Link href={`/datasets/${ds.id}`}>
                        Analyze
                      </Link>
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
    </MainLayout>
  );
}
