'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { MainLayout } from '@/components/layout/main-layout';
import { Loader2, Sparkles, ChevronLeft, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import PersonaResults from '@/components/personas/persona-results';
import type { PersonaReport } from '@/lib/persona-types';

export default function PersonaRunPage({ params }: { params: { runId: string } }) {
  const { runId } = params;
  const [report, setReport] = useState<PersonaReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [phase, setPhase] = useState<string>('');
  const [progressMsg, setProgressMsg] = useState<string>('');
  const [megaJobNames, setMegaJobNames] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function run() {
      try {
        const r = await fetch(`/api/personas/report?runId=${encodeURIComponent(runId)}`, { credentials: 'include' });
        if (r.ok) {
          const data = await r.json();
          if (!cancelled && data.report) {
            setReport(data.report);
            setLoading(false);
          }
        }
        // Always poll the state so we know if exports are still running.
        let pollData = await fetch('/api/personas/poll', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ runId }),
        }).then((r) => r.json()).catch(() => ({ phase: 'unknown' }));

        if (!cancelled) setPhase(pollData.phase || 'unknown');

        while (!cancelled && pollData.phase !== 'done' && pollData.phase !== 'error' && pollData.phase !== 'unknown') {
          setProgressMsg(pollData.progress?.message || pollData.phase);
          if (pollData.report && !cancelled) setReport(pollData.report);
          await new Promise((r) => setTimeout(r, 4000));
          pollData = await fetch('/api/personas/poll', {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ runId }),
          }).then((r) => r.json()).catch(() => ({ phase: 'error', error: 'network' }));
          if (!cancelled) setPhase(pollData.phase || 'unknown');
        }

        if (!cancelled && pollData.phase === 'done' && pollData.report) {
          setReport(pollData.report);
        }
        if (!cancelled && pollData.phase === 'error') {
          setError(pollData.error || 'Run failed');
        }
        if (!cancelled) setLoading(false);
      } catch (e: any) {
        if (!cancelled) {
          setError(e?.message || String(e));
          setLoading(false);
        }
      }
    }
    run();
    return () => { cancelled = true; };
  }, [runId]);

  // Best-effort: resolve megajob names from report.config + the runs API.
  useEffect(() => {
    if (!report) return;
    fetch('/api/personas/runs', { credentials: 'include' })
      .then((r) => r.json())
      .then((data) => {
        const r = data?.runs?.find((x: any) => x.runId === runId);
        if (r?.megaJobNames) setMegaJobNames(r.megaJobNames);
      })
      .catch(() => {});
  }, [report, runId]);

  return (
    <MainLayout>
      <div className="max-w-6xl mx-auto space-y-6">
        <div className="flex items-center gap-3">
          <Link href="/personas" className="text-sm text-muted-foreground hover:text-foreground flex items-center gap-1">
            <ChevronLeft className="h-4 w-4" /> Back
          </Link>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Sparkles className="h-6 w-6" /> Persona Run
          </h1>
          <span className="text-xs text-muted-foreground font-mono">{runId}</span>
          {phase && phase !== 'done' && (
            <span className="text-xs px-2 py-0.5 rounded bg-blue-500/15 text-blue-500">{phase}</span>
          )}
        </div>

        {error ? (
          <div className="border border-red-500/40 bg-red-500/5 rounded-lg p-4">
            <div className="font-semibold text-red-500">Error</div>
            <div className="text-sm text-muted-foreground mt-1">{error}</div>
            <Button onClick={() => window.location.reload()} variant="outline" size="sm" className="mt-3">
              <RefreshCw className="h-4 w-4 mr-2" /> Retry
            </Button>
          </div>
        ) : loading && !report ? (
          <div className="flex flex-col items-center justify-center py-16 gap-2">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            <div className="text-sm text-muted-foreground">{progressMsg || 'Loading…'}</div>
          </div>
        ) : report ? (
          <PersonaResults report={report} megaJobNames={megaJobNames} />
        ) : (
          <div className="text-sm text-muted-foreground">No data.</div>
        )}
      </div>
    </MainLayout>
  );
}
