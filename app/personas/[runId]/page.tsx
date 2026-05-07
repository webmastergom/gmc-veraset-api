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

    /**
     * Resilient poll: tolerates network blips (Wi-Fi switching, VPN, modem
     * reconnects → ERR_NETWORK_CHANGED, ERR_INTERNET_DISCONNECTED, etc.) and
     * Vercel 504s by surfacing a "retrying" status to the UI and retrying
     * with exponential backoff. Only gives up after MAX_CONSECUTIVE failures.
     */
    const MAX_CONSECUTIVE = 8;

    async function pollOnce(body: any): Promise<{ ok: boolean; data?: any; error?: string }> {
      try {
        const res = await fetch('/api/personas/poll', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        // 504 from Vercel = function timeout — retry instead of bailing.
        if (res.status === 504) {
          return { ok: false, error: 'Server timeout (504), will retry' };
        }
        let data: any;
        try {
          data = await res.json();
        } catch (e: any) {
          return { ok: false, error: `Bad JSON (HTTP ${res.status})` };
        }
        if (!res.ok) {
          return { ok: false, error: data?.error || `HTTP ${res.status}`, data };
        }
        return { ok: true, data };
      } catch (e: any) {
        // Network errors (ERR_NETWORK_CHANGED, offline, DNS, etc.) — retry.
        return { ok: false, error: e?.message || 'Network error' };
      }
    }

    async function run() {
      try {
        // Cached report (best-effort).
        try {
          const r = await fetch(`/api/personas/report?runId=${encodeURIComponent(runId)}`, { credentials: 'include' });
          if (r.ok) {
            const data = await r.json();
            if (!cancelled && data.report) setReport(data.report);
          }
        } catch {}

        let consecutiveErrors = 0;
        let pollData: any = null;

        while (!cancelled) {
          const result = await pollOnce({ runId });
          if (cancelled) break;

          if (result.ok) {
            consecutiveErrors = 0;
            pollData = result.data;
            setPhase(pollData.phase || 'unknown');
            setProgressInfo(pollData.progress || {});
            if (pollData.report) setReport(pollData.report);

            if (pollData.phase === 'done') break;
            if (pollData.phase === 'error') {
              setError(pollData.error || 'Run failed');
              break;
            }
            // Normal cadence between healthy polls.
            await new Promise((r) => setTimeout(r, 4000));
          } else {
            consecutiveErrors++;
            // Surface the retry state on the loader.
            setProgressInfo((prev) => ({
              ...prev,
              message: `Connection blip — retrying (${consecutiveErrors}/${MAX_CONSECUTIVE})`,
              details: result.error,
            }));
            if (consecutiveErrors >= MAX_CONSECUTIVE) {
              setError(`Lost connection to server (${result.error}). Reload to resume — the run is still running on the backend.`);
              break;
            }
            // Exponential backoff: 3s, 5s, 8s, 13s, 21s, 34s, 55s, 89s …
            const delay = Math.min(90_000, 3000 * Math.pow(1.6, consecutiveErrors - 1));
            await new Promise((r) => setTimeout(r, delay));
          }
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
