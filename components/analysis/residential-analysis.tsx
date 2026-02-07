'use client';

import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Home, MapPin, Download, Loader2, Search, Users } from 'lucide-react';

interface ResidentialZipcode {
  zipcode: string;
  city: string;
  province: string;
  region: string;
  devices: number;
  percentage: number;
}

interface ResidentialData {
  summary: {
    totalDevicesInDataset: number;
    devicesWithHomeLocation: number;
    devicesMatchedToZipcode: number;
    totalZipcodes: number;
    topZipcode: string | null;
    topCity: string | null;
  };
  zipcodes: ResidentialZipcode[];
}

interface ResidentialAnalysisProps {
  datasetName: string;
}

export default function ResidentialAnalysis({ datasetName }: ResidentialAnalysisProps) {
  const [data, setData] = useState<ResidentialData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [sortField, setSortField] = useState<'devices' | 'zipcode' | 'city' | 'province'>('devices');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const runAnalysis = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/datasets/${datasetName}/analyze/residential`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filters: {} }),
      });

      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.details || errorData.error || `Analysis failed: ${res.statusText}`);
      }

      const result = await res.json();
      setData(result);
    } catch (err: any) {
      console.error('Residential analysis error:', err);
      setError(err.message || 'Failed to analyze residential data');
    } finally {
      setLoading(false);
    }
  };

  const handleSort = (field: typeof sortField) => {
    if (sortField === field) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDir(field === 'devices' ? 'desc' : 'asc');
    }
  };

  const sortIndicator = (field: typeof sortField) => {
    if (sortField !== field) return '';
    return sortDir === 'asc' ? ' \u2191' : ' \u2193';
  };

  const downloadCSV = () => {
    if (!data) return;

    const header = 'zipcode,city,province,region,devices,percentage';
    const rows = filteredData.map(z =>
      `${z.zipcode},"${z.city}","${z.province}","${z.region}",${z.devices},${z.percentage}`
    );
    const csv = [header, ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${datasetName}-residential-zipcodes.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Filter and sort data
  const filteredData = data
    ? data.zipcodes
        .filter(z => {
          if (!search) return true;
          const q = search.toLowerCase();
          return (
            z.zipcode.includes(q) ||
            z.city.toLowerCase().includes(q) ||
            z.province.toLowerCase().includes(q)
          );
        })
        .sort((a, b) => {
          const mul = sortDir === 'asc' ? 1 : -1;
          if (sortField === 'devices') return (a.devices - b.devices) * mul;
          if (sortField === 'zipcode') return a.zipcode.localeCompare(b.zipcode) * mul;
          if (sortField === 'city') return a.city.localeCompare(b.city) * mul;
          if (sortField === 'province') return a.province.localeCompare(b.province) * mul;
          return 0;
        })
    : [];

  if (!data && !loading) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Home className="h-10 w-10 text-muted-foreground mx-auto mb-4" />
          <p className="text-muted-foreground mb-4">
            Analyze the residential zipcodes of visitors by examining their nighttime location patterns.
          </p>
          <Button onClick={runAnalysis}>
            Run Residential Analysis
          </Button>
        </CardContent>
      </Card>
    );
  }

  if (loading) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground mx-auto mb-4" />
          <p className="text-muted-foreground">
            Analyzing nighttime pings to estimate home locations...
          </p>
          <p className="text-xs text-muted-foreground mt-2">
            This may take 1-2 minutes for large datasets
          </p>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <p className="text-destructive mb-4">{error}</p>
          <Button onClick={runAnalysis} variant="outline">
            Retry
          </Button>
        </CardContent>
      </Card>
    );
  }

  if (!data) return null;

  return (
    <div className="space-y-4">
      {/* Summary Cards */}
      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Devices Analyzed</CardTitle>
            <Users className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {data.summary.devicesWithHomeLocation.toLocaleString()}
            </div>
            <p className="text-xs text-muted-foreground">
              of {data.summary.totalDevicesInDataset.toLocaleString()} total
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Matched to Zipcode</CardTitle>
            <Home className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {data.summary.devicesMatchedToZipcode.toLocaleString()}
            </div>
            <p className="text-xs text-muted-foreground">
              {data.summary.devicesWithHomeLocation > 0
                ? `${Math.round((data.summary.devicesMatchedToZipcode / data.summary.devicesWithHomeLocation) * 100)}% match rate`
                : ''}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Zipcodes Found</CardTitle>
            <MapPin className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {data.summary.totalZipcodes.toLocaleString()}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium">Top Location</CardTitle>
            <MapPin className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {data.summary.topZipcode || '-'}
            </div>
            <p className="text-xs text-muted-foreground">
              {data.summary.topCity || ''}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Search + Download */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Residential Zipcodes</CardTitle>
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search zipcode or city..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  className="pl-8 w-60"
                />
              </div>
              <Button variant="outline" size="sm" onClick={downloadCSV}>
                <Download className="h-4 w-4 mr-2" />
                CSV
              </Button>
              <Button variant="outline" size="sm" onClick={runAnalysis}>
                Refresh
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="max-h-[500px] overflow-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="sticky top-0 bg-background">#</TableHead>
                  <TableHead
                    className="sticky top-0 bg-background cursor-pointer hover:text-foreground"
                    onClick={() => handleSort('zipcode')}
                  >
                    Zipcode{sortIndicator('zipcode')}
                  </TableHead>
                  <TableHead
                    className="sticky top-0 bg-background cursor-pointer hover:text-foreground"
                    onClick={() => handleSort('city')}
                  >
                    City{sortIndicator('city')}
                  </TableHead>
                  <TableHead
                    className="sticky top-0 bg-background cursor-pointer hover:text-foreground"
                    onClick={() => handleSort('province')}
                  >
                    Province{sortIndicator('province')}
                  </TableHead>
                  <TableHead
                    className="sticky top-0 bg-background text-right cursor-pointer hover:text-foreground"
                    onClick={() => handleSort('devices')}
                  >
                    Devices{sortIndicator('devices')}
                  </TableHead>
                  <TableHead className="sticky top-0 bg-background text-right">%</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredData.map((z, i) => (
                  <TableRow key={z.zipcode}>
                    <TableCell className="text-muted-foreground">{i + 1}</TableCell>
                    <TableCell className="font-mono">{z.zipcode}</TableCell>
                    <TableCell>{z.city}</TableCell>
                    <TableCell>{z.province}</TableCell>
                    <TableCell className="text-right font-medium">{z.devices.toLocaleString()}</TableCell>
                    <TableCell className="text-right text-muted-foreground">{z.percentage}%</TableCell>
                  </TableRow>
                ))}
                {filteredData.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                      {search ? 'No results matching your search' : 'No zipcode data available'}
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
          {filteredData.length > 0 && (
            <p className="text-xs text-muted-foreground mt-2">
              Showing {filteredData.length} zipcodes
              {search ? ` matching "${search}"` : ''}
              . Min. 5 devices per zipcode for privacy.
            </p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
