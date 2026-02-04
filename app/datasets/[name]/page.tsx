'use client';

import { useState, useEffect, useCallback } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ExportDialog } from '@/components/export/export-dialog';
import { TopPoisTable } from '@/components/analysis/top-pois-table';
import { POIFilter } from '@/components/analysis/poi-filter';
import { POINameInterpreter } from '@/components/analysis/poi-name-interpreter';
import { Users, MapPin, Activity, Clock, Download, Loader2, ArrowLeft, Filter } from 'lucide-react';
import dynamic from 'next/dynamic';

// Use a more explicit dynamic import with error handling
const DailyActivityChart = dynamic(
  async () => {
    try {
      const mod = await import('@/components/analysis/daily-activity-chart');
      return { default: mod.default };
    } catch (error) {
      console.error('Failed to load DailyActivityChart:', error);
      return { default: () => <div className="h-64 flex items-center justify-center text-red-500">Failed to load chart</div> };
    }
  },
  { 
    ssr: false,
    loading: () => <div className="h-64 flex items-center justify-center text-muted-foreground">Loading chart...</div>
  }
);

const DwellDistributionChart = dynamic(
  async () => {
    try {
      const mod = await import('@/components/analysis/dwell-distribution-chart');
      return { default: mod.default };
    } catch (error) {
      console.error('Failed to load DwellDistributionChart:', error);
      return { default: () => <div className="h-64 flex items-center justify-center text-red-500">Failed to load chart</div> };
    }
  },
  { 
    ssr: false,
    loading: () => <div className="h-64 flex items-center justify-center text-muted-foreground">Loading chart...</div>
  }
);

export default function DatasetAnalysisPage() {
  const params = useParams();
  const router = useRouter();
  const datasetName = params.name as string;

  const [analysis, setAnalysis] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [datasetInfo, setDatasetInfo] = useState<{ name: string; id: string; external?: boolean } | null>(null);
  const [availablePois, setAvailablePois] = useState<Array<{ poiId: string; pings: number; devices: number; originalId?: string; displayName?: string }>>([]);
  const [selectedPois, setSelectedPois] = useState<string[]>([]);
  const [loadingPois, setLoadingPois] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);

  // Fetch dataset info to get display name and job ID
  useEffect(() => {
    fetch('/api/datasets', {
      credentials: 'include',
    })
      .then(r => r.json())
      .then(data => {
        const dataset = data.datasets?.find((ds: any) => ds.id === datasetName);
        if (dataset) {
          setDatasetInfo({
            name: dataset.name,
            id: dataset.id,
            external: dataset.external
          });
          // Store job ID for POI mapping
          if (dataset.jobId) {
            setJobId(dataset.jobId);
          }
        }
      })
      .catch(err => {
        console.error('Error fetching dataset info:', err);
      });
  }, [datasetName]);

  // Fetch available POIs for filtering
  const fetchAvailablePois = useCallback(async () => {
    setLoadingPois(true);
    try {
      // First try to get enriched POIs from Veraset API
      // This uses /v1/poi/pois to get information about POIs that visited the dataset
      let res = await fetch(`/api/datasets/${datasetName}/pois/enrich`, {
        credentials: 'include',
      });
      
      // If enrich endpoint fails, fall back to basic POI list
      if (!res.ok) {
        res = await fetch(`/api/datasets/${datasetName}/pois`, {
          credentials: 'include',
        });
      }
      
      if (res.ok) {
        const data = await res.json();
        let pois = data.pois || [];
        
        // If we have a job ID, try to get POI mapping to show original names (for GeoJSON POIs)
        if (jobId) {
          try {
            const mapRes = await fetch(`/api/datasets/${datasetName}/pois/map?jobId=${jobId}`, {
              credentials: 'include',
            });
            if (mapRes.ok) {
              const mapData = await mapRes.json();
              const mapping = mapData.mapping || {};
              const names = mapData.names || {};
              
              // Enhance POIs with original IDs and names (for GeoJSON-based jobs)
              pois = pois.map((poi: any) => {
                const originalId = mapping[poi.poiId];
                const poiName = names[poi.poiId];
                
                return {
                  ...poi,
                  originalId: originalId && originalId !== poi.poiId ? originalId : undefined,
                  // Use Veraset name if available, otherwise use mapped name or ID
                  displayName: poi.name || poiName || (originalId && originalId !== poi.poiId ? originalId : poi.poiId),
                };
              });
            }
          } catch (mapError) {
            console.warn('Could not fetch POI mapping:', mapError);
            // Continue without mapping
          }
        }
        
        setAvailablePois(pois);
      }
    } catch (error) {
      console.error('Error fetching POIs:', error);
    } finally {
      setLoadingPois(false);
    }
  }, [datasetName, jobId]);

  // Load POIs when dataset changes or when filters are shown
  useEffect(() => {
    if (showFilters && availablePois.length === 0 && !loadingPois) {
      // If we have analysis results with POIs, use those first
      if (analysis?.topPois && analysis.topPois.length > 0) {
        setAvailablePois(analysis.topPois.map((poi: any) => ({
          poiId: poi.poiId,
          pings: poi.pings || 0,
          devices: poi.devices || 0,
        })));
      } else {
        // Otherwise fetch all POIs from the dataset
        fetchAvailablePois();
      }
    }
  }, [datasetName, showFilters, analysis?.topPois, availablePois.length, loadingPois, fetchAvailablePois]);

  const runAnalysis = async () => {
    setLoading(true);
    try {
      const filters: any = {};
      
      // Add POI filter if POIs are selected
      if (selectedPois.length > 0) {
        filters.poiIds = selectedPois;
      }

      const res = await fetch(`/api/datasets/${datasetName}/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filters }),
        credentials: 'include',
      });

      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        const errorMsg = errorData.details || errorData.error || `Analysis failed: ${res.statusText}`;
        const hint = errorData.hint ? `\n\nHint: ${errorData.hint}` : '';
        throw new Error(`${errorMsg}${hint}`);
      }

      const data = await res.json();
      
      // Enhance POI data with Veraset POI information
      // First try to enrich from Veraset API (for POIs that visited the dataset)
      if (data.topPois && data.topPois.length > 0) {
        try {
          // Try to get enriched POI data from Veraset
          const enrichRes = await fetch(`/api/datasets/${datasetName}/pois/enrich`, {
            credentials: 'include',
          });
          
          if (enrichRes.ok) {
            const enrichData = await enrichRes.json();
            const enrichedPoisMap = new Map();
            (enrichData.pois || []).forEach((poi: any) => {
              enrichedPoisMap.set(String(poi.poiId), poi);
            });
            
            // Enhance topPois with Veraset data
            data.topPois = data.topPois.map((poi: any) => {
              const enriched = enrichedPoisMap.get(String(poi.poiId));
              return {
                ...poi,
                name: enriched?.name || poi.poiId,
                category: enriched?.category,
                subcategory: enriched?.subcategory,
                brand: enriched?.brand,
                address: enriched?.address,
                city: enriched?.city,
                state: enriched?.state,
                displayName: enriched?.name || poi.poiId,
              };
            });
          }
        } catch (enrichError) {
          console.warn('Could not enrich POIs from Veraset:', enrichError);
        }
      }
      
      // Also try to get POI mapping if job ID is available (for GeoJSON-based jobs)
      if (jobId && data.topPois) {
        try {
          const mapRes = await fetch(`/api/datasets/${datasetName}/pois/map?jobId=${jobId}`, {
            credentials: 'include',
          });
          if (mapRes.ok) {
            const mapData = await mapRes.json();
            const mapping = mapData.mapping || {};
            const names = mapData.names || {};
            
            data.topPois = data.topPois.map((poi: any) => {
              const originalId = mapping[poi.poiId];
              const poiName = names[poi.poiId];
              
              return {
                ...poi,
                originalId: originalId && originalId !== poi.poiId ? originalId : undefined,
                // Prefer Veraset name, then mapped name, then original ID
                displayName: poi.name || poiName || (originalId && originalId !== poi.poiId ? originalId : poi.poiId),
              };
            });
          }
        } catch (mapError) {
          console.warn('Could not fetch POI mapping for analysis:', mapError);
        }
      }
      
      setAnalysis(data);
      
      // Update available POIs from analysis results if we don't have them yet
      if (data.topPois && data.topPois.length > 0 && availablePois.length === 0) {
        setAvailablePois(data.topPois.map((poi: any) => ({
          poiId: poi.poiId,
          pings: poi.pings || 0,
          devices: poi.devices || 0,
          originalId: poi.originalId,
          displayName: poi.displayName,
        })));
      }
    } catch (error: any) {
      console.error('Analysis error:', error);
      const errorMessage = error.message || 'Unknown error occurred';
      alert(`Failed to analyze dataset: ${errorMessage}`);
    } finally {
      setLoading(false);
    }
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
            <span className="text-xs px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">
              External
            </span>
          )}
        </div>
        {datasetInfo && datasetInfo.id !== datasetInfo.name && (
          <p className="text-sm text-gray-500 font-mono mt-2">
            {datasetInfo.id}
          </p>
        )}
      </div>

      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold">Analysis</h2>
        <div className="flex gap-2">
          <Button 
            variant="outline" 
            onClick={() => setShowFilters(!showFilters)}
            className={showFilters ? 'bg-primary text-primary-foreground' : ''}
          >
            <Filter className="h-4 w-4 mr-2" />
            Filters {selectedPois.length > 0 && `(${selectedPois.length})`}
          </Button>
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

      {/* POI Filter */}
      {showFilters && (
        <div className="mb-6">
          <POIFilter
            availablePois={availablePois}
            selectedPois={selectedPois}
            onSelectionChange={setSelectedPois}
            jobId={jobId || undefined}
          />
        </div>
      )}

      {analysis && (
        <>
          {/* Show active filters info */}
          {selectedPois.length > 0 && (
            <div className="mb-4 p-3 bg-blue-500/10 border border-blue-500/20 rounded-lg">
              <div className="flex items-center gap-2 text-blue-400">
                <Filter className="h-4 w-4" />
                <span className="text-sm font-medium">
                  Analysis filtered by {selectedPois.length} POI{selectedPois.length !== 1 ? 's' : ''}
                </span>
              </div>
            </div>
          )}

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
                  <CardDescription>
                    POIs are sorted by number of unique devices. Click on a POI to see its interpretation.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Show interpretation for first POI if it's auto-generated */}
                  {analysis.topPois && analysis.topPois.length > 0 && (
                    <POINameInterpreter
                      verasetPoiId={analysis.topPois[0].poiId}
                      originalPoiId={analysis.topPois[0].originalId}
                      jobId={jobId || undefined}
                    />
                  )}
                  <TopPoisTable data={analysis.topPois} jobId={jobId || undefined} />
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
    </MainLayout>
  );
}
