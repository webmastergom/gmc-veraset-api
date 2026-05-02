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

const SUPPORTED_COUNTRIES = [
  'AR', 'BE', 'CL', 'CO', 'CR', 'DE', 'DO', 'EC', 'ES', 'FR',
  'GT', 'HN', 'IE', 'IT', 'MX', 'NI', 'NL', 'PA', 'PE', 'PT',
  'SE', 'SV', 'UK', 'US',
];

interface MegaNseModalProps {
  open: boolean;
  onClose: () => void;
  megaJobId: string;
  megaJobCountry?: string | null;
}

export function MegaNseModal({ open, onClose, megaJobId, megaJobCountry }: MegaNseModalProps) {
  const [nseData, setNseData] = useState<NseRecord[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [notFound, setNotFound] = useState(false);
  const [computing, setComputing] = useState(false);
  const [computeProgress, setComputeProgress] = useState<string | null>(null);
  const [brackets, setBrackets] = useState<BracketResult[] | null>(null);
  const [totalMaids, setTotalMaids] = useState(0);
  const [country, setCountry] = useState<string | null>(megaJobCountry || null);
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

    const cc = megaJobCountry || country;
    if (!cc) {
      setLoading(false);
      return;
    }
    setCountry(cc);
    loadNseData(cc);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, megaJobCountry]);

  const loadNseData = async (cc: string) => {
    setLoading(true);
    setBrackets(null);
    setTotalMaids(0);
    setNotFound(false);
    setNseData(null);

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

  const handleCountrySelect = (cc: string) => {
    setCountry(cc);
    loadNseData(cc);
  };

  // Upload CSV (also used to replace existing data)
  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    // Always reset the input so the same file can be re-selected after a failed upload
    e.target.value = '';
    if (!file || !country) return;

    // If data already exists, ask for confirmation before overwriting
    if (nseData && nseData.length > 0) {
      const ok = window.confirm(
        `Replace existing NSE data for ${country}? This will overwrite ${nseData.length.toLocaleString()} postal codes.`
      );
      if (!ok) return;
    }

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

      setNotFound(false);
      setBrackets(null);
      setTotalMaids(0);
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

  // Safe fetch that handles 504/non-JSON responses
  const safePollFetch = async (url: string, options?: RequestInit) => {
    const res = await fetch(url, options);
    if (res.status === 504) {
      return { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
    }
    let data;
    try {
      data = await res.json();
    } catch {
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
      let data = await safePollFetch(`/api/mega-jobs/${megaJobId}/nse-poll`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ country }),
      });

      while (data.phase !== 'done' && data.phase !== 'error') {
        setComputeProgress(data.progress?.message || 'Processing...');
        await new Promise(r => setTimeout(r, 4000));

        data = await safePollFetch(`/api/mega-jobs/${megaJobId}/nse-poll`, {
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
    link.download = `mega-${megaJobId}-maids-nse-${bracket.min}-${bracket.max}.csv`;
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
          <div className="flex items-center justify-between gap-2">
            <DialogTitle className="flex items-center gap-2">
              <Users className="h-5 w-5" />
              MAIDs by NSE {country ? `— ${country}` : ''}
              {loading && <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />}
            </DialogTitle>
            {country && !loading && nseData && nseData.length > 0 && (
              <Button variant="outline" size="sm" disabled={uploading} asChild className="mr-6">
                <label className="cursor-pointer">
                  {uploading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Upload className="mr-2 h-4 w-4" />}
                  {uploading ? 'Uploading...' : 'Replace CSV'}
                  <input type="file" accept=".csv" className="hidden" onChange={handleUpload} disabled={uploading} />
                </label>
              </Button>
            )}
          </div>
        </DialogHeader>

        {/* Country selector when no country is set */}
        {!loading && !country && (
          <div className="space-y-4 py-4">
            <p className="text-muted-foreground text-sm">Select the country for NSE analysis:</p>
            <div className="flex flex-wrap gap-2">
              {SUPPORTED_COUNTRIES.map(cc => (
                <Button
                  key={cc}
                  variant="outline"
                  size="sm"
                  onClick={() => handleCountrySelect(cc)}
                  className="font-mono"
                >
                  {cc}
                </Button>
              ))}
            </div>
          </div>
        )}

        {loading && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
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
            <div className="flex gap-2">
              <Button variant="outline" disabled={uploading} asChild>
                <label className="cursor-pointer">
                  {uploading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Upload className="mr-2 h-4 w-4" />}
                  {uploading ? 'Uploading...' : 'Upload NSE CSV'}
                  <input type="file" accept=".csv" className="hidden" onChange={handleUpload} disabled={uploading} />
                </label>
              </Button>
              <Button variant="ghost" size="sm" onClick={() => { setCountry(null); setNotFound(false); }}>
                Change country
              </Button>
            </div>
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

            <div className="flex gap-2">
              <Button onClick={handleComputeMaids} disabled={computing} className="flex-1">
                {computing ? (
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                ) : (
                  <Play className="mr-2 h-4 w-4" />
                )}
                {computing ? 'Computing...' : 'Compute MAIDs by NSE'}
              </Button>
              <Button variant="ghost" size="sm" onClick={() => { setCountry(null); setNseData(null); }}>
                Change country
              </Button>
            </div>
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
              <div className="flex gap-2">
                <Button variant="ghost" size="sm" onClick={() => { setCountry(null); setNseData(null); setBrackets(null); }}>
                  Change country
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
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
