'use client';

import { useState } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Loader2, Download, CheckCircle2 } from 'lucide-react';
import type { ExportFilters, ExportResult } from '@/lib/types';

interface ExportDialogProps {
  datasetName: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ExportDialog({ datasetName, open, onOpenChange }: ExportDialogProps) {
  const [loading, setLoading] = useState(false);
  const [dwellFilterType, setDwellFilterType] = useState<'none' | 'seconds' | 'minutes'>('none');
  const [dwellValue, setDwellValue] = useState<number>(60);
  const [minPings, setMinPings] = useState<number | null>(1);
  const [usePingsFilter, setUsePingsFilter] = useState(true);
  const [result, setResult] = useState<ExportResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  
  const handleExport = async () => {
    setLoading(true);
    setError(null);
    setResult(null);
    
    let minDwellTime: number | null = null;
    if (dwellFilterType === 'seconds') {
      minDwellTime = dwellValue;
    } else if (dwellFilterType === 'minutes') {
      minDwellTime = dwellValue * 60;
    }
    
    const filters: ExportFilters = {
      minDwellTime,
      minPings: usePingsFilter ? minPings : null,
    };
    
    try {
      const res = await fetch(`/api/datasets/${datasetName}/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filters,
          format: 'csv',
        }),
      });
      
      const data = await res.json();
      
      if (!res.ok) {
        throw new Error(data.error || data.details || 'Export failed');
      }
      
      setResult(data);
      
    } catch (err: any) {
      console.error('Export failed:', err);
      setError(err.message || 'Failed to export devices');
    } finally {
      setLoading(false);
    }
  };
  
  const handleClose = () => {
    if (!loading) {
      setResult(null);
      setError(null);
      setDwellFilterType('none');
      setDwellValue(60);
      setMinPings(1);
      setUsePingsFilter(true);
      onOpenChange(false);
    }
  };
  
  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Export Device IDs</DialogTitle>
        </DialogHeader>
        
        <div className="space-y-6 py-4">
          {/* Dwell Time Filter */}
          <div className="space-y-3">
            <Label>Dwell Time Filter</Label>
            <RadioGroup 
              value={dwellFilterType} 
              onValueChange={(v: 'none' | 'seconds' | 'minutes') => setDwellFilterType(v)}
            >
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="none" id="dwell-none" />
                <Label htmlFor="dwell-none" className="font-normal cursor-pointer">
                  No filter (export ALL devices)
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="seconds" id="dwell-seconds" />
                <Label htmlFor="dwell-seconds" className="font-normal cursor-pointer">
                  Minimum seconds
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="minutes" id="dwell-minutes" />
                <Label htmlFor="dwell-minutes" className="font-normal cursor-pointer">
                  Minimum minutes
                </Label>
              </div>
            </RadioGroup>
            
            {dwellFilterType !== 'none' && (
              <div className="flex items-center gap-2 ml-6">
                <Input
                  type="number"
                  min={0}
                  step={dwellFilterType === 'seconds' ? 1 : 0.5}
                  value={dwellValue}
                  onChange={(e) => setDwellValue(parseFloat(e.target.value) || 0)}
                  className="w-24"
                  disabled={loading}
                />
                <span className="text-sm text-muted-foreground">
                  {dwellFilterType === 'seconds' ? 'seconds' : 'minutes'}
                  {dwellFilterType === 'minutes' && dwellValue && (
                    <span className="ml-1">({dwellValue * 60}s)</span>
                  )}
                </span>
              </div>
            )}
          </div>
          
          {/* Min Pings Filter */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <Label>Minimum Pings</Label>
              <Switch 
                checked={usePingsFilter} 
                onCheckedChange={setUsePingsFilter}
                disabled={loading}
              />
            </div>
            {usePingsFilter && (
              <Input
                type="number"
                min={1}
                value={minPings || ''}
                onChange={(e) => setMinPings(parseInt(e.target.value) || null)}
                placeholder="e.g., 2"
                className="w-24"
                disabled={loading}
              />
            )}
          </div>
          
          {/* Error */}
          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          
          {/* Result */}
          {result && result.success && (
            <div className="rounded-lg bg-muted p-4 space-y-3">
              <div className="flex items-center gap-2">
                <CheckCircle2 className="h-5 w-5 text-green-600" />
                <p className="font-medium">
                  {result.deviceCount?.toLocaleString()} devices exported
                </p>
              </div>
              {result.totalDevicesInDataset && (
                <p className="text-sm text-muted-foreground">
                  of {result.totalDevicesInDataset.toLocaleString()} total devices in dataset
                </p>
              )}
              {result.downloadUrl && (
                <Button variant="outline" size="sm" asChild className="w-full">
                  <a href={result.downloadUrl} download>
                    <Download className="h-4 w-4 mr-2" />
                    Download CSV
                  </a>
                </Button>
              )}
            </div>
          )}
        </div>
        
        <DialogFooter>
          <Button variant="outline" onClick={handleClose} disabled={loading}>
            {result ? 'Close' : 'Cancel'}
          </Button>
          <Button onClick={handleExport} disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                Exporting...
              </>
            ) : (
              'Export'
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
