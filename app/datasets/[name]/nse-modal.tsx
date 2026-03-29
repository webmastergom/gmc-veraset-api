'use client';

import { useState, useCallback } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Download, Upload, Loader2, Users, MapPin } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

interface NseRecord {
  postal_code: string;
  population: number;
  nse: number;
  region1: string;
  region2: string;
  region3: string;
}

interface CatchmentZip {
  zipCode: string;
  city: string;
  country: string;
  deviceDays: number;
  lat: number;
  lng: number;
}

interface BracketStats {
  label: string;
  min: number;
  max: number;
  postalCodes: number;
  population: number;
  deviceDays: number;
  matchedZips: string[];
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
  catchmentData: CatchmentZip[] | null;
  selectedBucket: number;
}

export function NseModal({ open, onClose, datasetName, catchmentData, selectedBucket }: NseModalProps) {
  const [nseData, setNseData] = useState<NseRecord[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [downloading, setDownloading] = useState<string | null>(null);
  const [country, setCountry] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const { toast } = useToast();

  // Derive country from catchment data
  const inferCountry = useCallback(() => {
    if (!catchmentData?.length) return null;
    const counts: Record<string, number> = {};
    for (const z of catchmentData) {
      if (z.country) counts[z.country] = (counts[z.country] || 0) + z.deviceDays;
    }
    const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    return sorted[0]?.[0] || null;
  }, [catchmentData]);

  // Load NSE data when modal opens
  const loadNseData = useCallback(async () => {
    // If catchmentData not passed, fetch it directly
    let catchment = catchmentData;
    if (!catchment?.length) {
      try {
        const res = await fetch(`/api/datasets/${datasetName}/reports?type=catchment&bucket=${selectedBucket}`, { credentials: 'include' });
        if (res.ok) {
          const data = await res.json();
          catchment = data?.byZipCode || null;
        }
      } catch {}
    }

    const cc = (() => {
      if (!catchment?.length) return null;
      const counts: Record<string, number> = {};
      for (const z of catchment) {
        if (z.country) counts[z.country] = (counts[z.country] || 0) + z.deviceDays;
      }
      const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
      return sorted[0]?.[0] || null;
    })();

    if (!cc) {
      toast({ title: 'No catchment data', description: 'Run Analyze first to generate catchment report.', variant: 'destructive' });
      setLoading(false);
      return;
    }
    setCountry(cc);
    setLoading(true);
    setNotFound(false);

    try {
      const res = await fetch(`/api/nse/${cc}`, { credentials: 'include' });
      if (res.status === 404) {
        setNotFound(true);
        setNseData(null);
      } else if (res.ok) {
        const data = await res.json();
        setNseData(data.data);
        setNotFound(false);
      } else {
        throw new Error('Failed to load NSE data');
      }
    } catch (e: any) {
      toast({ title: 'Error', description: e.message, variant: 'destructive' });
    } finally {
      setLoading(false);
    }
  }, [inferCountry, toast]);

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
      await loadNseData();
    } catch (e: any) {
      toast({ title: 'Upload failed', description: e.message, variant: 'destructive' });
    } finally {
      setUploading(false);
    }
  };

  // Compute bracket stats
  const computeBrackets = (): BracketStats[] => {
    if (!nseData || !catchmentData) return [];

    // Build NSE lookup by postal code
    const nseLookup = new Map<string, NseRecord>();
    for (const r of nseData) {
      nseLookup.set(r.postal_code, r);
    }

    return NSE_BRACKETS.map(({ label, min, max }) => {
      // Filter NSE records in this bracket
      const inBracket = nseData.filter(r => r.nse >= min && r.nse <= max);
      const bracketCPs = new Set(inBracket.map(r => r.postal_code));

      // Cross-reference with catchment
      let deviceDays = 0;
      const matchedZips: string[] = [];
      for (const z of catchmentData) {
        const cleanZip = z.zipCode.replace(/^["']+|["']+$/g, '').replace(/^[A-Z]{2}[-\s]/, '');
        if (bracketCPs.has(cleanZip)) {
          deviceDays += z.deviceDays;
          matchedZips.push(cleanZip);
        }
      }

      return {
        label,
        min,
        max,
        postalCodes: inBracket.length,
        population: inBracket.reduce((s, r) => s + r.population, 0),
        deviceDays,
        matchedZips,
      };
    });
  };

  // Download MAIDs for a bracket
  const handleDownloadMaids = async (bracket: BracketStats) => {
    setDownloading(bracket.label);
    try {
      const res = await fetch(`/api/datasets/${datasetName}/export`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          format: 'maids-nse',
          nsePostalCodes: bracket.matchedZips,
          nseBracket: bracket.label,
          minDwell: selectedBucket,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Export failed');

      if (data.downloadUrl) {
        const link = document.createElement('a');
        link.href = data.downloadUrl;
        link.download = `${datasetName}-maids-nse-${bracket.min}-${bracket.max}.csv`;
        link.click();
        toast({ title: 'Download started', description: `${data.totalDevices?.toLocaleString() || '?'} MAIDs` });
      }
    } catch (e: any) {
      toast({ title: 'Download failed', description: e.message, variant: 'destructive' });
    } finally {
      setDownloading(null);
    }
  };

  // Load data when modal opens
  const handleOpenChange = (isOpen: boolean) => {
    if (isOpen) {
      setLoading(true);
      loadNseData();
    } else {
      onClose();
    }
  };

  const brackets = nseData ? computeBrackets() : [];
  const totalDeviceDays = brackets.reduce((s, b) => s + b.deviceDays, 0);

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

        {!loading && notFound && (
          <div className="space-y-4 py-4">
            <p className="text-muted-foreground">
              No NSE data found for <strong>{country}</strong>. Upload a CSV with columns:
              <code className="ml-1 text-xs bg-muted px-1 py-0.5 rounded">
                postal_code, population, nse, region1, region2, region3
              </code>
            </p>
            <div className="flex items-center gap-2">
              <Button variant="outline" disabled={uploading} asChild>
                <label className="cursor-pointer">
                  {uploading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Upload className="mr-2 h-4 w-4" />}
                  {uploading ? 'Uploading...' : 'Upload NSE CSV'}
                  <input
                    type="file"
                    accept=".csv"
                    className="hidden"
                    onChange={handleUpload}
                    disabled={uploading}
                  />
                </label>
              </Button>
            </div>
          </div>
        )}

        {!loading && nseData && (
          <div className="space-y-4">
            {!catchmentData?.length && (
              <p className="text-sm text-yellow-500">
                Run Analyze first to see device counts per bracket.
              </p>
            )}

            <div className="rounded-lg border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/50">
                    <th className="text-left p-3 font-medium">NSE Bracket</th>
                    <th className="text-right p-3 font-medium">Postal Codes</th>
                    <th className="text-right p-3 font-medium">Population</th>
                    <th className="text-right p-3 font-medium">Device-Days</th>
                    <th className="text-right p-3 font-medium"></th>
                  </tr>
                </thead>
                <tbody>
                  {brackets.map((b) => (
                    <tr key={b.label} className="border-b last:border-0 hover:bg-muted/30">
                      <td className="p-3 font-mono">{b.label}</td>
                      <td className="p-3 text-right">{b.postalCodes.toLocaleString()}</td>
                      <td className="p-3 text-right">{b.population.toLocaleString()}</td>
                      <td className="p-3 text-right">{b.deviceDays.toLocaleString()}</td>
                      <td className="p-3 text-right">
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={b.deviceDays === 0 || downloading === b.label}
                          onClick={() => handleDownloadMaids(b)}
                        >
                          {downloading === b.label ? (
                            <Loader2 className="h-3 w-3 animate-spin" />
                          ) : (
                            <Download className="h-3 w-3" />
                          )}
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
                <tfoot>
                  <tr className="bg-muted/50">
                    <td className="p-3 font-medium">Total</td>
                    <td className="p-3 text-right font-medium">{nseData.length.toLocaleString()}</td>
                    <td className="p-3 text-right font-medium">{nseData.reduce((s, r) => s + r.population, 0).toLocaleString()}</td>
                    <td className="p-3 text-right font-medium">{totalDeviceDays.toLocaleString()}</td>
                    <td></td>
                  </tr>
                </tfoot>
              </table>
            </div>

            <div className="flex items-center justify-between">
              <p className="text-xs text-muted-foreground">
                <MapPin className="inline h-3 w-3 mr-1" />
                {nseData.length} postal codes loaded for {country}
              </p>
              <Button variant="ghost" size="sm" asChild>
                <label className="cursor-pointer text-xs">
                  <Upload className="mr-1 h-3 w-3" />
                  Replace CSV
                  <input
                    type="file"
                    accept=".csv"
                    className="hidden"
                    onChange={handleUpload}
                    disabled={uploading}
                  />
                </label>
              </Button>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
