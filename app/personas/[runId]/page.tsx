'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { MainLayout } from '@/components/layout/main-layout';
import { Sparkles, ChevronLeft, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import PersonaResults from '@/components/personas/persona-results';
import PersonaProgress from '@/components/personas/persona-progress';
import type { PersonaReport } from '@/lib/persona-types';

interface ProgressInfo {
  step?: string;
  percent?: number;
  message?: string;
  details?: string;
  phaseLabel?: string;
  ratio?: number;
  perSource?: Record<string, string>;
}

export default function PersonaRunPage({ params }: { params: { runId: string } }) {
  const { runId } = params;
  const [report, setReport] = useState<PersonaReport | null>(null);
  const [phase, setPhase] = useState<string>('');
  const [progressInfo, setProgressInfo] = useState<ProgressInfo>({});
  const [megaJobNames, setMegaJobNames] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function run() {
      try {
        // Try to load any cached report immediately so the user sees results
        // while exports might still be finalizing in the background.
        const r = await fetch(`/api/personas/report?runId=${encodeURIComponent(runId)}`, { credentials: 'include' });
        if (r.ok) {
          const data = await r.json();
          if (!cancelled && data.report) setReport(data.report);
        }
        // Drive the state machine forward via /poll calls until done/error.
        let pollData: any = await fetch('/api/personas/poll', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ runId }),
        }).then((r) => r.json()).catch(() => ({ phase: 'unknown' }));

        if (!cancelled) {
          setPhase(pollData.phase || 'unknown');
          setProgressInfo(pollData.progress || {});
        }

        while (!cancelled && pollData.phase !== 'done' && pollData.phase !== 'error' && pollData.phase !== 'unknown') {
          if (pollData.report && !cancelled) setReport(pollData.report);
          await new Promise((r) => setTimeout(r, 4000));
          pollData = await fetch('/api/personas/poll', {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ runId }),
          }).then((r) => r.json()).catch(() => ({ phase: 'error', error: 'network' }));
          if (!cancelled) {
            setPhase(pollData.phase || 'unknown');
            setProgressInfo(pollData.progress || {});
          }
        }

        if (!cancelled && pollData.phase === 'done' && pollData.report) {
          setReport(pollData.report);
        }
        if (!cancelled && pollData.phase === 'error') {
          setError(pollData.error || 'Run failed');
        }
      } catch (e: any) {
        if (!cancelled) setError(e?.message || String(e));
      }
    }
    run();
    return () => { cancelled = true; };
  }, [runId]);

  // Best-effort: resolve source names (mega + job) from report.config + the runs API.
  useEffect(() => {
    if (!report) return;
    fetch('/api/personas/runs', { credentials: 'include' })
      .then((r) => r.json())
      .then((data) => {
        const r = data?.runs?.find((x: any) => x.runId === runId);
        if (r) {
          const all = [...(r.megaJobNames || []), ...(r.jobNames || [])];
          setMegaJobNames(all);
        }
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
          <>
            <PersonaProgress progress={progressInfo} phase="error" error={error} />
            <div className="flex justify-center">
              <Button onClick={() => window.location.reload()} variant="outline" size="sm">
                <RefreshCw className="h-4 w-4 mr-2" /> Retry
              </Button>
            </div>
          </>
        ) : !report && phase !== 'done' ? (
          <PersonaProgress progress={progressInfo} phase={phase} />
        ) : report ? (
          <>
            {/* Still polling exports? Show a slim progress strip on top of results. */}
            {phase && phase !== 'done' && phase !== 'error' && (
              <div className="border border-blue-500/30 bg-blue-500/5 rounded-lg px-4 py-2 text-xs text-muted-foreground flex items-center gap-2">
                <span className="inline-block h-2 w-2 rounded-full bg-blue-500 animate-pulse" />
                <span>
                  {progressInfo.message || progressInfo.phaseLabel || 'Finalizing…'}
                  {progressInfo.details && ` · ${progressInfo.details}`}
                </span>
              </div>
            )}
            <PersonaResults report={report} megaJobNames={megaJobNames} />
          </>
        ) : (
          <div className="text-sm text-muted-foreground">No data.</div>
        )}
      </div>
    </MainLayout>
  );
}
