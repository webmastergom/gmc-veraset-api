'use client';

import { useState, useEffect, useRef } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Download, Upload, Loader2, Users, MapPin, Play } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

interface NseRecord {
  postal_code: string;
  population: number;
  nse: number;
  region1: string;
  region2: string;
  region3: string;
}

interface BracketResult {
  label: string;
  min: number;
  max: number;
  postalCodes: number;
  population: number;
  maidCount: number;
  downloadUrl: string | null;
}

const NSE_BRACKETS = [
  { label: '0-19 (Low)', min: 0, max: 19 },
  { label: '20-39', min: 20, max: 39 },
  { label: '40-59 (Mid)', min: 40, max: 59 },
  { label: '60-79', min: 60, max: 79 },
  { label: '80-100 (High)', min: 80, max: 100 },
];

interface NseModalProps {
  open: boolean;
  onClose: () => void;
  datasetName: string;
  catchmentData: any[] | null;
  dwellMin: number;
  dwellMax: number;
  hourFrom: number;
  hourTo: number;
  jobCountry: string | null;
}

export function NseModal({ open, onClose, datasetName, dwellMin, dwellMax, hourFrom, hourTo, jobCountry }: NseModalProps) {
  const [nseData, setNseData] = useState<NseRecord[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [notFound, setNotFound] = useState(false);
  const [computing, setComputing] = useState(false);
  const [computeProgress, setComputeProgress] = useState<string | null>(null);
  const [brackets, setBrackets] = useState<BracketResult[] | null>(null);
  const [totalMaids, setTotalMaids] = useState(0);
  const [country, setCountry] = useState<string | null>(jobCountry || null);
  const { toast } = useToast();
  const loadedRef = useRef(false);

  // Load NSE data when modal opens
  useEffect(() => {
    if (!open) {
      loadedRef.current = false;
      return;
    }
    if (loadedRef.current) return;
    loadedRef.current = true;

    const load = async () => {
      setLoading(true);
      setBrackets(null);
      setTotalMaids(0);
      setNotFound(false);
      setNseData(null);

      // Resolve country
      let cc = jobCountry || null;
      if (!cc && datasetName) {
        try {
          const dsRes = await fetch('/api/datasets', { credentials: 'include' });
          if (dsRes.ok) {
            const dsData = await dsRes.json();
            const ds = dsData.datasets?.find((d: any) => d.id === datasetName);
            cc = ds?.country || null;
          }
        } catch {}
      }
      setCountry(cc);

      if (!cc) {
        setLoading(false);
        return;
      }

      // Load NSE data
      try {
        const res = await fetch(`/api/nse/${cc}`, { credentials: 'include' });
        if (res.status === 404) {
          setNotFound(true);
        } else if (res.ok) {
          const data = await res.json();
          setNseData(data.data);
        } else {
          throw new Error('Failed to load NSE data');
        }
      } catch (e: any) {
        toast({ title: 'Error', description: e.message, variant: 'destructive' });
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [open, jobCountry, datasetName]); // eslint-disable-line react-hooks/exhaustive-deps

  // Upload CSV
  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !country) return;

    setUploading(true);
    try {
      const csv = await file.text();
      const res = await fetch(`/api/nse/${country}`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ csv }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);

      toast({ title: 'NSE data uploaded', description: `${data.records} postal codes for ${country}` });

      // Reload NSE data
      setNotFound(false);
      setBrackets(null);
      const nseRes = await fetch(`/api/nse/${country}`, { credentials: 'include' });
      if (nseRes.ok) {
        const nseJson = await nseRes.json();
        setNseData(nseJson.data);
      }
    } catch (e: any) {
      toast({ title: 'Upload failed', description: e.message, variant: 'destructive' });
    } finally {
      setUploading(false);
    }
  };

  // Safe fetch that handles 504/non-JSON responses gracefully
  const safePollFetch = async (url: string, options?: RequestInit) => {
    const res = await fetch(url, options);
    if (res.status === 504) {
      // Vercel gateway timeout — backend is still working, keep polling
      return { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
    }
    let data;
    try {
      data = await res.json();
    } catch {
      // Non-JSON response (e.g., Vercel error page) — retry
      return { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
    }
    if (!res.ok) throw new Error(data.error || 'Failed');
    return data;
  };

  // Compute MAIDs per bracket via polling
  const handleComputeMaids = async () => {
    if (!country) return;

    setComputing(true);
    setComputeProgress('Starting...');
    setBrackets(null);

    try {
      let data = await safePollFetch(`/api/datasets/${datasetName}/export/nse-poll`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ country, minDwell: dwellMin, maxDwell: dwellMax, hourFrom, hourTo }),
      });

      while (data.phase !== 'done' && data.phase !== 'error') {
        setComputeProgress(data.progress?.message || 'Processing...');
        await new Promise(r => setTimeout(r, 4000));

        data = await safePollFetch(`/api/datasets/${datasetName}/export/nse-poll`, {
          method: 'POST',
          credentials: 'include',
        });
      }

      if (data.phase === 'error') {
        throw new Error(data.error || 'Analysis failed');
      }

      setBrackets(data.brackets);
      setTotalMaids(data.totalMaids || 0);
      toast({ title: 'Analysis complete', description: `${(data.totalMaids || 0).toLocaleString()} total MAIDs` });
    } catch (e: any) {
      toast({ title: 'Analysis failed', description: e.message, variant: 'destructive' });
    } finally {
      setComputing(false);
      setComputeProgress(null);
    }
  };

  const handleDownload = (bracket: BracketResult) => {
    if (!bracket.downloadUrl) return;
    const link = document.createElement('a');
    link.href = bracket.downloadUrl;
    link.download = `${datasetName}-maids-nse-${bracket.min}-${bracket.max}.csv`;
    link.click();
  };

  const handleOpenChange = (isOpen: boolean) => {
    if (!isOpen) onClose();
  };

  // Preview brackets from NSE data (before computing MAIDs)
  const previewBrackets = nseData ? NSE_BRACKETS.map(b => {
    const inBracket = nseData.filter(r => r.nse >= b.min && r.nse <= b.max);
    return {
      label: b.label,
      min: b.min,
      max: b.max,
      postalCodes: inBracket.length,
      population: inBracket.reduce((s, r) => s + r.population, 0),
    };
  }) : [];

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Users className="h-5 w-5" />
            MAIDs by NSE {country ? `— ${country}` : ''}
            {loading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
          </DialogTitle>
        </DialogHeader>

        {loading && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        )}

        {!loading && !country && (
          <div className="py-8 text-center text-muted-foreground">
            <MapPin className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p className="font-medium">No country set</p>
            <p className="text-sm mt-1">Set the country for this job in the Jobs section first.</p>
          </div>
        )}

        {!loading && country && notFound && (
          <div className="space-y-4 py-4">
            <p className="text-muted-foreground">
              No NSE data found for <strong>{country}</strong>. Upload a CSV with columns:
              <code className="ml-1 text-xs bg-muted px-1 py-0.5 rounded">
                postal_code, population, nse
              </code>
            </p>
            <Button variant="outline" disabled={uploading} asChild>
              <label className="cursor-pointer">
                {uploading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Upload className="mr-2 h-4 w-4" />}
                {uploading ? 'Uploading...' : 'Upload NSE CSV'}
                <input type="file" accept=".csv" className="hidden" onChange={handleUpload} disabled={uploading} />
              </label>
            </Button>
          </div>
        )}

        {!loading && nseData && !brackets && (
          <div className="space-y-4">
            <div className="rounded-lg border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-3 font-medium">NSE Bracket</th>
                    <th className="text-right p-3 font-medium">Postal Codes</th>
                    <th className="text-right p-3 font-medium">Population</th>
                  </tr>
                </thead>
                <tbody>
                  {previewBrackets.map(b => (
                    <tr key={b.label} className="border-b last:border-0">
                      <td className="p-3 font-mono">{b.label}</td>
                      <td className="p-3 text-right">{b.postalCodes.toLocaleString()}</td>
                      <td className="p-3 text-right">{b.population.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
                <tfoot>
                  <tr className="bg-muted/50">
                    <td className="p-3 font-medium">Total</td>
                    <td className="p-3 text-right font-medium">{nseData.length.toLocaleString()}</td>
                    <td className="p-3 text-right font-medium">{nseData.reduce((s, r) => s + r.population, 0).toLocaleString()}</td>
                  </tr>
                </tfoot>
              </table>
            </div>

            {computing && computeProgress && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground bg-muted/50 rounded-lg p-3">
                <Loader2 className="h-4 w-4 animate-spin flex-shrink-0" />
                {computeProgress}
              </div>
            )}

            <Button onClick={handleComputeMaids} disabled={computing} className="w-full">
              {computing ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Play className="mr-2 h-4 w-4" />
              )}
              {computing ? 'Computing...' : 'Compute MAIDs by NSE'}
            </Button>
          </div>
        )}

        {!loading && brackets && (
          <div className="space-y-4">
            <div className="rounded-lg border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-3 font-medium">NSE Bracket</th>
                    <th className="text-right p-3 font-medium">Postal Codes</th>
                    <th className="text-right p-3 font-medium">Population</th>
                    <th className="text-right p-3 font-medium">MAIDs</th>
                    <th className="text-right p-3 font-medium"></th>
                  </tr>
                </thead>
                <tbody>
                  {brackets.map(b => (
                    <tr key={b.label} className="border-b last:border-0 hover:bg-muted/30">
                      <td className="p-3 font-mono">{b.label}</td>
                      <td className="p-3 text-right">{b.postalCodes.toLocaleString()}</td>
                      <td className="p-3 text-right">{b.population.toLocaleString()}</td>
                      <td className="p-3 text-right font-semibold">{b.maidCount.toLocaleString()}</td>
                      <td className="p-3 text-right">
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={!b.downloadUrl}
                          onClick={() => handleDownload(b)}
                        >
                          <Download className="h-3 w-3" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
                <tfoot>
                  <tr className="bg-muted/50">
                    <td className="p-3 font-medium">Total</td>
                    <td className="p-3 text-right font-medium">
                      {brackets.reduce((s, b) => s + b.postalCodes, 0).toLocaleString()}
                    </td>
                    <td className="p-3 text-right font-medium">
                      {brackets.reduce((s, b) => s + b.population, 0).toLocaleString()}
                    </td>
                    <td className="p-3 text-right font-semibold">
                      {totalMaids.toLocaleString()}
                    </td>
                    <td></td>
                  </tr>
                </tfoot>
              </table>
            </div>

            <div className="flex items-center justify-between">
              <Button variant="outline" size="sm" onClick={handleComputeMaids} disabled={computing}>
                {computing ? <Loader2 className="mr-2 h-3 w-3 animate-spin" /> : <Play className="mr-2 h-3 w-3" />}
                Recompute
              </Button>
              <Button variant="ghost" size="sm" asChild>
                <label className="cursor-pointer text-xs">
                  <Upload className="mr-1 h-3 w-3" />
                  Replace NSE CSV
                  <input type="file" accept=".csv" className="hidden" onChange={handleUpload} disabled={uploading} />
                </label>
              </Button>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
