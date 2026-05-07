'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { MainLayout } from '@/components/layout/main-layout';
import { Button } from '@/components/ui/button';
import { Loader2, Sparkles, Play, ChevronRight, Layers, Briefcase } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';

interface MegaJobSummary {
  megaJobId: string;
  name: string;
  country?: string;
  status?: string;
  progress?: { synced?: number; total?: number };
  schema?: string;
  type?: string;
}

interface JobSummary {
  jobId: string;
  name: string;
  country?: string;
  status?: string;
  syncedAt?: string;
  schema?: string;
  type?: string;
  poiCount?: number;
  megaJobId?: string; // present if part of a megajob — exclude these
}

/** Unified picker item: either a megajob or a standalone job. */
interface PickerItem {
  kind: 'mega' | 'job';
  id: string;
  name: string;
  country?: string;
  status?: string;
  schema?: string;
  type?: string;
  /** human-readable progress / metadata blurb */
  blurb?: string;
}

interface RunSummary {
  runId: string;
  phase: string;
  generatedAt: string;
  megaJobIds: string[];
  megaJobNames: string[];
  jobIds: string[];
  jobNames: string[];
  totalDevices?: number;
  personaCount?: number;
  error?: string;
}

export default function PersonasIndexPage() {
  const [items, setItems] = useState<PickerItem[]>([]);
  /** keys are `mega:<id>` or `job:<id>` for a unified selection model. */
  const [selected, setSelected] = useState<string[]>([]);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [progressMsg, setProgressMsg] = useState('');
  /** Day-of-week filter (1=Mon..7=Sun). Empty = all days. */
  const [daysOfWeek, setDaysOfWeek] = useState<number[]>([]);
  const router = useRouter();
  const { toast } = useToast();

  useEffect(() => {
    Promise.all([
      fetch('/api/mega-jobs', { credentials: 'include' }).then((r) => r.json()),
      fetch('/api/jobs', { credentials: 'include' }).then((r) => r.json()),
      fetch('/api/personas/runs', { credentials: 'include' }).then((r) => r.json()),
    ])
      .then(([mjData, jobsData, runsData]) => {
        // Megajobs (from /api/mega-jobs)
        const mjArr = Array.isArray(mjData)
          ? mjData
          : mjData?.megaJobs || mjData?.entries || Object.values(mjData || {});
        const mjItems: PickerItem[] = (mjArr as MegaJobSummary[])
          .filter((mj) => mj.megaJobId)
          .map((mj) => {
            const synced = mj.progress?.synced || 0;
            const total = mj.progress?.total || 0;
            const blurb = [
              mj.country,
              mj.schema,
              mj.status,
              total > 0 ? `${synced}/${total} synced` : undefined,
            ].filter(Boolean).join(' · ');
            return {
              kind: 'mega' as const,
              id: mj.megaJobId,
              name: mj.name || mj.megaJobId,
              country: mj.country,
              status: mj.status,
              schema: mj.schema,
              type: mj.type,
              blurb,
            };
          });

        // Standalone jobs (SUCCESS + synced + NOT part of any megajob)
        const jobArr = Array.isArray(jobsData) ? (jobsData as JobSummary[]) : [];
        const jobItems: PickerItem[] = jobArr
          .filter((j) => j.status === 'SUCCESS' && j.syncedAt && !j.megaJobId)
          .map((j) => {
            const blurb = [
              j.country,
              j.schema,
              j.poiCount != null ? `${j.poiCount} POIs` : undefined,
            ].filter(Boolean).join(' · ');
            return {
              kind: 'job' as const,
              id: j.jobId,
              name: j.name || j.jobId,
              country: j.country,
              status: j.status,
              schema: j.schema,
              type: j.type,
              blurb,
            };
          });

        setItems([...mjItems, ...jobItems]);
        setRuns(runsData?.runs || []);
      })
      .finally(() => setLoading(false));
  }, []);

  const togglePick = (key: string) => {
    setSelected((cur) => {
      if (cur.includes(key)) return cur.filter((x) => x !== key);
      if (cur.length >= 2) return [cur[1], key]; // sliding window of 2
      return [...cur, key];
    });
  };

  const handleRun = async () => {
    if (selected.length === 0) {
      toast({ title: 'Pick at least one source' });
      return;
    }
    setRunning(true);
    setProgressMsg('Starting…');
    try {
      // Split unified selection into megaJobIds + jobIds for the API.
      const megaJobIds = selected.filter((s) => s.startsWith('mega:')).map((s) => s.slice(5));
      const jobIds = selected.filter((s) => s.startsWith('job:')).map((s) => s.slice(4));
      const filters: Record<string, any> = {};
      if (daysOfWeek.length > 0 && daysOfWeek.length < 7) filters.daysOfWeek = daysOfWeek;
      // Send the user to the runId page as soon as we have one — that page
      // owns the resilient polling + rich loader. We just need the first
      // response to extract runId.
      let data = await safePoll('/api/personas/poll', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ megaJobIds, jobIds, ...(Object.keys(filters).length ? { filters } : {}) }),
      });
      const runId = data.runId;
      if (runId) {
        router.push(`/personas/${runId}`);
        return;
      }
      while (data.phase !== 'done' && data.phase !== 'error') {
        setProgressMsg(data.progress?.message || data.phase);
        await new Promise((r) => setTimeout(r, 4000));
        data = await safePoll('/api/personas/poll', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ runId }),
        });
      }
      if (data.phase === 'error') throw new Error(data.error || 'Persona run failed');
      router.push(`/personas/${runId}`);
    } catch (e: any) {
      toast({ title: 'Failed', description: e.message, variant: 'destructive' });
      setRunning(false);
      setProgressMsg('');
    }
  };

  return (
    <MainLayout>
      <div className="max-w-6xl mx-auto space-y-6">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Sparkles className="h-6 w-6" /> Personas & Mobility Insights
          </h1>
          <p className="text-muted-foreground mt-1">
            Auto-discover buyer personas from device-level mobility traits.
            Pick one or two megajobs (cross-dataset analysis enabled with two).
          </p>
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <>
            <div className="border rounded-lg p-4 space-y-3 bg-card">
              <div className="flex items-center justify-between">
                <h2 className="font-semibold text-sm uppercase tracking-wider text-muted-foreground">
                  Pick sources (mega-jobs or single jobs)
                </h2>
                <span className="text-xs text-muted-foreground">{selected.length}/2 selected</span>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2 max-h-96 overflow-y-auto">
                {items.length === 0 ? (
                  <p className="text-sm text-muted-foreground p-2 col-span-2">
                    No data sources available. Create a job or mega-job first.
                  </p>
                ) : (
                  items.map((it) => {
                    const key = `${it.kind}:${it.id}`;
                    const isSel = selected.includes(key);
                    const Icon = it.kind === 'mega' ? Layers : Briefcase;
                    return (
                      <div
                        key={key}
                        onClick={() => togglePick(key)}
                        className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                          isSel ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/30'
                        }`}
                      >
                        <Icon className="h-4 w-4 text-muted-foreground" />
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-medium truncate">{it.name}</span>
                            <span className={`text-[10px] px-1.5 py-0.5 rounded uppercase tracking-wider ${
                              it.kind === 'mega'
                                ? 'bg-blue-500/15 text-blue-500'
                                : 'bg-emerald-500/15 text-emerald-500'
                            }`}>
                              {it.kind === 'mega' ? 'Mega' : 'Job'}
                            </span>
                          </div>
                          {it.blurb && (
                            <div className="text-xs text-muted-foreground truncate">{it.blurb}</div>
                          )}
                        </div>
                        {isSel && <span className="text-xs text-primary font-semibold">✓</span>}
                      </div>
                    );
                  })
                )}
              </div>

              {/* Day-of-week filter (optional; empty = all days). */}
              <div className="flex flex-wrap items-center gap-2 pt-2">
                <label className="text-xs text-muted-foreground whitespace-nowrap">Days of week:</label>
                <div className="flex gap-0.5">
                  {[
                    { d: 1, label: 'M' },
                    { d: 2, label: 'T' },
                    { d: 3, label: 'W' },
                    { d: 4, label: 'T' },
                    { d: 5, label: 'F' },
                    { d: 6, label: 'S' },
                    { d: 7, label: 'S' },
                  ].map(({ d, label }) => {
                    const active = daysOfWeek.length === 0 || daysOfWeek.includes(d);
                    return (
                      <button
                        key={d}
                        type="button"
                        onClick={() => {
                          let next: number[];
                          if (daysOfWeek.length === 0) {
                            next = [1, 2, 3, 4, 5, 6, 7].filter((x) => x !== d);
                          } else if (daysOfWeek.includes(d)) {
                            next = daysOfWeek.filter((x) => x !== d);
                          } else {
                            next = [...daysOfWeek, d];
                          }
                          if (next.length === 7) next = [];
                          setDaysOfWeek(next);
                        }}
                        className={`h-8 w-7 rounded text-xs font-medium transition-colors ${
                          active
                            ? 'bg-primary/15 text-foreground border border-primary/40'
                            : 'bg-muted/30 text-muted-foreground border border-border'
                        }`}
                        title={['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][d - 1]}
                      >
                        {label}
                      </button>
                    );
                  })}
                </div>
                <div className="flex gap-1">
                  <button
                    type="button"
                    onClick={() => setDaysOfWeek([])}
                    className="h-8 px-2 rounded text-[10px] uppercase tracking-wider bg-muted/40 hover:bg-muted text-muted-foreground"
                    title="Reset to all days"
                  >
                    All
                  </button>
                  <button
                    type="button"
                    onClick={() => setDaysOfWeek([1, 2, 3, 4, 5])}
                    className="h-8 px-2 rounded text-[10px] uppercase tracking-wider bg-muted/40 hover:bg-muted text-muted-foreground"
                    title="Weekdays only"
                  >
                    M-F
                  </button>
                  <button
                    type="button"
                    onClick={() => setDaysOfWeek([6, 7])}
                    className="h-8 px-2 rounded text-[10px] uppercase tracking-wider bg-muted/40 hover:bg-muted text-muted-foreground"
                    title="Weekend only"
                  >
                    S-S
                  </button>
                </div>
                {daysOfWeek.length > 0 && daysOfWeek.length < 7 && (
                  <span className="text-[11px] text-muted-foreground ml-auto">
                    Filtering {daysOfWeek.length}/7 day{daysOfWeek.length === 1 ? '' : 's'}
                  </span>
                )}
              </div>

              <Button onClick={handleRun} disabled={running || selected.length === 0} className="w-full">
                {running ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    {progressMsg || 'Running…'}
                  </>
                ) : (
                  <>
                    <Play className="h-4 w-4 mr-2" /> Generate personas ({selected.length} source{selected.length === 1 ? '' : 's'})
                  </>
                )}
              </Button>
              <p className="text-xs text-muted-foreground">
                Pick 2 sources to unlock cross-dataset insights: brand cohabitation matrix, persona × brand exclusivity, BK-vs-other-chains overlap.
                Standalone jobs (single-month, no megajob parent) are listed alongside mega-jobs.
                The day-of-week filter restricts the analysis to selected days only — useful for &quot;weekday lunch crowd&quot; or &quot;weekend family&quot; cohorts.
              </p>
            </div>

            {runs.length > 0 && (
              <div className="border rounded-lg overflow-hidden bg-card">
                <div className="px-4 py-2 text-sm font-semibold border-b bg-muted/30">Past runs</div>
                <div className="divide-y">
                  {runs.slice(0, 20).map((r) => (
                    <Link
                      key={r.runId}
                      href={`/personas/${r.runId}`}
                      className="flex items-center gap-3 p-3 hover:bg-muted/30 transition-colors"
                    >
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium truncate">
                          {[...(r.megaJobNames || []), ...(r.jobNames || [])].join(' + ') || r.runId}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          {new Date(r.generatedAt).toLocaleString()} ·{' '}
                          {r.phase === 'done'
                            ? `${r.totalDevices?.toLocaleString() ?? '?'} devices · ${r.personaCount ?? '?'} personas`
                            : r.phase}
                        </div>
                      </div>
                      <span className={`text-xs px-2 py-0.5 rounded ${
                        r.phase === 'done' ? 'bg-emerald-500/15 text-emerald-500' :
                        r.phase === 'error' ? 'bg-red-500/15 text-red-500' :
                        'bg-blue-500/15 text-blue-500'
                      }`}>
                        {r.phase}
                      </span>
                      <ChevronRight className="h-4 w-4 text-muted-foreground" />
                    </Link>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </MainLayout>
  );
}

/**
 * Resilient single fetch with retry on transient failures.
 * Tolerates: 504 Vercel timeouts, ERR_NETWORK_CHANGED / offline blips,
 *            non-JSON responses (HTML error pages from Vercel edge).
 * Up to 4 attempts with exponential backoff (3s, 5s, 8s).
 */
async function safePoll(url: string, init?: RequestInit) {
  const MAX_ATTEMPTS = 4;
  let lastError: any = null;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const r = await fetch(url, init);
      if (r.status === 504) {
        // Backend still working — retry.
        lastError = new Error('Server timeout (504)');
      } else {
        let data: any;
        try { data = await r.json(); } catch (e: any) {
          lastError = new Error(`Bad JSON (HTTP ${r.status})`);
          continue;
        }
        if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
        return data;
      }
    } catch (e: any) {
      // Network error (ERR_NETWORK_CHANGED, offline, DNS) — retry.
      lastError = e;
    }
    if (attempt < MAX_ATTEMPTS) {
      const delay = 3000 * Math.pow(1.6, attempt - 1);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw lastError || new Error('Unknown polling error');
}
