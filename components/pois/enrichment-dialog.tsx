'use client';

import { useState, useCallback } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { Badge } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Loader2, CheckCircle, XCircle, Zap } from 'lucide-react';

interface EnrichmentMatch {
  originalName: string;
  status: 'matched' | 'unmatched' | 'error';
  placekey?: string;
  verasetName?: string;
  distance?: number;
  category?: string;
  error?: string;
}

interface EnrichmentResult {
  matched: number;
  unmatched: number;
  errors: number;
  total: number;
  matchRate: number;
  matches: EnrichmentMatch[];
}

interface EnrichmentDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  collectionId: string;
  collectionName: string;
  poiCount: number;
  onEnrichmentApplied?: () => void;
}

export function EnrichmentDialog({
  open,
  onOpenChange,
  collectionId,
  collectionName,
  poiCount,
  onEnrichmentApplied,
}: EnrichmentDialogProps) {
  const [phase, setPhase] = useState<'idle' | 'running' | 'review' | 'applying' | 'done'>('idle');
  const [progress, setProgress] = useState(0);
  const [result, setResult] = useState<EnrichmentResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [limit, setLimit] = useState<number | null>(null);

  const startEnrichment = useCallback(async (testLimit?: number) => {
    setPhase('running');
    setProgress(0);
    setError(null);
    setResult(null);

    try {
      const body: Record<string, any> = { action: 'enrich' };
      if (testLimit) body.limit = testLimit;

      const res = await fetch(`/api/pois/collections/${collectionId}/enrich`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || errData.details || `Enrichment failed: ${res.statusText}`);
      }

      const data = await res.json();
      setResult(data);
      setProgress(100);
      setPhase('review');
    } catch (err: any) {
      setError(err.message || 'Enrichment failed');
      setPhase('idle');
    }
  }, [collectionId]);

  const applyEnrichment = useCallback(async () => {
    if (!result) return;
    setPhase('applying');

    try {
      const res = await fetch(`/api/pois/collections/${collectionId}/enrich`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({
          action: 'apply',
          matches: result.matches,
        }),
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || 'Failed to apply enrichment');
      }

      setPhase('done');
      onEnrichmentApplied?.();
    } catch (err: any) {
      setError(err.message || 'Failed to apply enrichment');
      setPhase('review');
    }
  }, [collectionId, result, onEnrichmentApplied]);

  const reset = () => {
    setPhase('idle');
    setProgress(0);
    setResult(null);
    setError(null);
    setLimit(null);
  };

  const handleClose = (open: boolean) => {
    if (!open && phase !== 'running' && phase !== 'applying') {
      reset();
    }
    onOpenChange(open);
  };

  const matchedResults = result?.matches.filter(m => m.status === 'matched') || [];
  const unmatchedResults = result?.matches.filter(m => m.status === 'unmatched') || [];

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="max-w-3xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Zap className="h-5 w-5" />
            Enrich with Veraset Placekeys
          </DialogTitle>
          <DialogDescription>
            Match POIs in &quot;{collectionName}&quot; against Veraset&apos;s database to get placekeys for better movement data tracking.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-auto space-y-4">
          {/* Idle State */}
          {phase === 'idle' && (
            <div className="space-y-4 py-4">
              <p className="text-sm text-muted-foreground">
                This will search Veraset&apos;s POI database for each of your {poiCount.toLocaleString()} POIs
                using a 25m geo-radius match. Matched POIs will receive a <code className="text-xs bg-muted px-1 rounded">place_key</code> for
                more accurate movement data queries.
              </p>
              <p className="text-sm text-muted-foreground">
                Estimated time: ~{Math.ceil(poiCount * 0.15 / 60)} minutes for all POIs.
              </p>

              {error && (
                <div className="bg-destructive/10 text-destructive p-3 rounded text-sm">{error}</div>
              )}

              <div className="flex gap-2">
                <Button onClick={() => startEnrichment(50)} variant="outline">
                  Test with 50 POIs
                </Button>
                <Button onClick={() => startEnrichment()}>
                  Enrich All {poiCount.toLocaleString()} POIs
                </Button>
              </div>
            </div>
          )}

          {/* Running State */}
          {phase === 'running' && (
            <div className="space-y-4 py-8 text-center">
              <Loader2 className="h-8 w-8 animate-spin mx-auto text-muted-foreground" />
              <p className="text-sm text-muted-foreground">
                Matching POIs against Veraset database...
              </p>
              <p className="text-xs text-muted-foreground">
                This may take several minutes. Please don&apos;t close this dialog.
              </p>
            </div>
          )}

          {/* Review State */}
          {phase === 'review' && result && (
            <div className="space-y-4">
              {/* Summary */}
              <div className="grid grid-cols-4 gap-3">
                <div className="bg-muted p-3 rounded text-center">
                  <div className="text-2xl font-bold">{result.total}</div>
                  <div className="text-xs text-muted-foreground">Processed</div>
                </div>
                <div className="bg-green-500/10 p-3 rounded text-center">
                  <div className="text-2xl font-bold text-green-500">{result.matched}</div>
                  <div className="text-xs text-muted-foreground">Matched</div>
                </div>
                <div className="bg-muted p-3 rounded text-center">
                  <div className="text-2xl font-bold">{result.unmatched}</div>
                  <div className="text-xs text-muted-foreground">No Match</div>
                </div>
                <div className="bg-muted p-3 rounded text-center">
                  <div className="text-2xl font-bold">{result.matchRate}%</div>
                  <div className="text-xs text-muted-foreground">Match Rate</div>
                </div>
              </div>

              {error && (
                <div className="bg-destructive/10 text-destructive p-3 rounded text-sm">{error}</div>
              )}

              {/* Match Results Table */}
              {matchedResults.length > 0 && (
                <div>
                  <h4 className="text-sm font-medium mb-2">
                    Matched POIs ({matchedResults.length})
                  </h4>
                  <div className="max-h-[250px] overflow-auto border rounded">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="sticky top-0 bg-background">Original POI</TableHead>
                          <TableHead className="sticky top-0 bg-background">Veraset Match</TableHead>
                          <TableHead className="sticky top-0 bg-background text-right">Distance</TableHead>
                          <TableHead className="sticky top-0 bg-background">Status</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {matchedResults.slice(0, 100).map((m, i) => (
                          <TableRow key={i}>
                            <TableCell className="text-sm">{m.originalName}</TableCell>
                            <TableCell className="text-sm">{m.verasetName || '-'}</TableCell>
                            <TableCell className="text-right text-sm">
                              {m.distance !== undefined ? `${m.distance}m` : '-'}
                            </TableCell>
                            <TableCell>
                              <Badge variant="default" className="bg-green-600 text-xs">
                                <CheckCircle className="h-3 w-3 mr-1" />
                                Match
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                        {matchedResults.length > 100 && (
                          <TableRow>
                            <TableCell colSpan={4} className="text-center text-muted-foreground text-xs">
                              ...and {matchedResults.length - 100} more matches
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Applying State */}
          {phase === 'applying' && (
            <div className="py-8 text-center space-y-4">
              <Loader2 className="h-8 w-8 animate-spin mx-auto text-muted-foreground" />
              <p className="text-sm text-muted-foreground">Saving placekeys to collection...</p>
            </div>
          )}

          {/* Done State */}
          {phase === 'done' && result && (
            <div className="py-8 text-center space-y-4">
              <CheckCircle className="h-10 w-10 text-green-500 mx-auto" />
              <div>
                <p className="font-medium">Enrichment Applied</p>
                <p className="text-sm text-muted-foreground">
                  {result.matched} POIs now have Veraset placekeys. These will use <code className="text-xs bg-muted px-1 rounded">place_key</code> for
                  more accurate movement data, while the remaining {result.unmatched} will use <code className="text-xs bg-muted px-1 rounded">geo_radius</code>.
                </p>
              </div>
            </div>
          )}
        </div>

        <DialogFooter>
          {phase === 'review' && result && result.matched > 0 && (
            <>
              <Button variant="outline" onClick={reset}>Cancel</Button>
              <Button onClick={applyEnrichment}>
                Apply {result.matched} Placekeys
              </Button>
            </>
          )}
          {phase === 'review' && result && result.matched === 0 && (
            <Button variant="outline" onClick={() => handleClose(false)}>Close</Button>
          )}
          {phase === 'done' && (
            <Button onClick={() => handleClose(false)}>Done</Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
