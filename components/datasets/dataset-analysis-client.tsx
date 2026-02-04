'use client';

import { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ExportDialog } from '@/components/export/export-dialog';
import dynamic from 'next/dynamic';

const DailyActivityChart = dynamic(
  () => import('@/components/analysis/daily-activity-chart'),
  { 
    ssr: false,
    loading: () => <div className="h-64 flex items-center justify-center text-muted-foreground">Loading chart...</div>
  }
);

const DwellDistributionChart = dynamic(
  () => import('@/components/analysis/dwell-distribution-chart'),
  { 
    ssr: false,
    loading: () => <div className="h-64 flex items-center justify-center text-muted-foreground">Loading chart...</div>
  }
);

import { TopPoisTable } from '@/components/analysis/top-pois-table';
import { Users, MapPin, Activity, Clock, Download, Loader2 } from 'lucide-react';

interface DatasetAnalysisClientProps {
  datasetName: string;
}

export function DatasetAnalysisClient({ datasetName }: DatasetAnalysisClientProps) {
  const [analysis, setAnalysis] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);

  const runAnalysis = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/datasets/${datasetName}/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filters: {} }),
      });

      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        const errorMsg = errorData.details || errorData.error || `Analysis failed: ${res.statusText}`;
        const hint = errorData.hint ? `\n\nHint: ${errorData.hint}` : '';
        
        // Show a more user-friendly error for access denied
        if (errorMsg.includes('Access denied') || errorMsg.includes('not authorized')) {
          const setupMsg = `\n\nTo fix this:\n` +
            `1. Add Athena permissions to your IAM user (see ATHENA_SETUP.md)\n` +
            `2. Create Glue database: aws glue create-database --database-input '{"Name": "veraset"}' --region us-west-2\n` +
            `3. Retry the analysis`;
          throw new Error(`${errorMsg}${setupMsg}`);
        }
        
        throw new Error(`${errorMsg}${hint}`);
      }

      const data = await res.json();
      setAnalysis(data);
    } catch (error: any) {
      console.error('Analysis error:', error);
      const errorMessage = error.message || 'Unknown error occurred';
      
      // Show error in a more user-friendly way
      if (errorMessage.includes('Access denied') || errorMessage.includes('not authorized')) {
        alert(
          `⚠️ Athena Access Denied\n\n` +
          `${errorMessage}\n\n` +
          `Please configure AWS permissions as described in ATHENA_SETUP.md`
        );
      } else {
        alert(`Failed to analyze dataset: ${errorMessage}`);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold">Analysis</h2>
        <div className="flex gap-2">
          <Button onClick={runAnalysis} disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                Analyzing...
              </>
            ) : (
              'Run Analysis'
            )}
          </Button>
          <Button variant="outline" onClick={() => setExportOpen(true)}>
            <Download className="h-4 w-4 mr-2" />
            Export
          </Button>
        </div>
      </div>

      {analysis && (
        <>
          {/* Summary Cards */}
          <div className="grid gap-4 md:grid-cols-4 mb-6">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Total Pings</CardTitle>
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
                <CardTitle className="text-sm font-medium">Unique Devices</CardTitle>
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
                <CardTitle className="text-sm font-medium">Active POIs</CardTitle>
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
                <CardTitle className="text-sm font-medium">Days Analyzed</CardTitle>
                <Clock className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {analysis.summary.daysAnalyzed}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Charts & Tables */}
          <Tabs defaultValue="activity">
            <TabsList>
              <TabsTrigger value="activity">Daily Activity</TabsTrigger>
              <TabsTrigger value="dwell">Dwell Distribution</TabsTrigger>
              <TabsTrigger value="pois">Top POIs</TabsTrigger>
            </TabsList>

            <TabsContent value="activity">
              <Card>
                <CardHeader>
                  <CardTitle>Pings & Devices by Day</CardTitle>
                </CardHeader>
                <CardContent>
                  <DailyActivityChart data={analysis.dailyActivity} />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="dwell">
              <Card>
                <CardHeader>
                  <CardTitle>Dwell Time Distribution</CardTitle>
                </CardHeader>
                <CardContent>
                  <DwellDistributionChart data={analysis.dwellDistribution} />
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="pois">
              <Card>
                <CardHeader>
                  <CardTitle>Top 20 POIs by Unique Devices</CardTitle>
                </CardHeader>
                <CardContent>
                  <TopPoisTable data={analysis.topPois} />
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </>
      )}

      {!analysis && !loading && (
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted-foreground">
              Click &quot;Run Analysis&quot; to analyze this dataset.
            </p>
          </CardContent>
        </Card>
      )}

      <ExportDialog
        datasetName={datasetName}
        open={exportOpen}
        onOpenChange={setExportOpen}
      />
    </div>
  );
}
