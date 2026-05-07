'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { MainLayout } from '@/components/layout/main-layout';
import { Button } from '@/components/ui/button';
import { Loader2, Sparkles, Play, ChevronRight, Layers } from 'lucide-react';
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

interface RunSummary {
  runId: string;
  phase: string;
  generatedAt: string;
  megaJobIds: string[];
  megaJobNames: string[];
  totalDevices?: number;
  personaCount?: number;
  error?: string;
}

export default function PersonasIndexPage() {
  const [megaJobs, setMegaJobs] = useState<MegaJobSummary[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [progressMsg, setProgressMsg] = useState('');
  const router = useRouter();
  const { toast } = useToast();

  useEffect(() => {
    Promise.all([
      fetch('/api/mega-jobs', { credentials: 'include' }).then((r) => r.json()),
      fetch('/api/personas/runs', { credentials: 'include' }).then((r) => r.json()),
    ])
      .then(([mjData, runsData]) => {
        const arr = Array.isArray(mjData) ? mjData : mjData?.megaJobs || mjData?.entries || Object.values(mjData || {});
        setMegaJobs(arr as MegaJobSummary[]);
        setRuns(runsData?.runs || []);
      })
      .finally(() => setLoading(false));
  }, []);

  const togglePick = (id: string) => {
    setSelected((cur) => {
      if (cur.includes(id)) return cur.filter((x) => x !== id);
      if (cur.length >= 2) return [cur[1], id]; // sliding window of 2
      return [...cur, id];
    });
  };

  const handleRun = async () => {
    if (selected.length === 0) {
      toast({ title: 'Pick at least one mega-job' });
      return;
    }
    setRunning(true);
    setProgressMsg('Starting…');
    try {
      let data = await safePoll('/api/personas/poll', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ megaJobIds: selected }),
      });
      const runId = data.runId;
      if (data.phase === 'done' && data.report) {
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
                <h2 className="font-semibold text-sm uppercase tracking-wider text-muted-foreground">Pick mega-jobs</h2>
                <span className="text-xs text-muted-foreground">{selected.length}/2 selected</span>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2 max-h-80 overflow-y-auto">
                {megaJobs.length === 0 ? (
                  <p className="text-sm text-muted-foreground p-2">No mega-jobs available. Create one first.</p>
                ) : (
                  megaJobs.map((mj) => {
                    const isSel = selected.includes(mj.megaJobId);
                    const synced = mj.progress?.synced || 0;
                    const total = mj.progress?.total || 0;
                    return (
                      <div
                        key={mj.megaJobId}
                        onClick={() => togglePick(mj.megaJobId)}
                        className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                          isSel ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/30'
                        }`}
                      >
                        <Layers className="h-4 w-4 text-muted-foreground" />
                        <div className="flex-1 min-w-0">
                          <div className="text-sm font-medium truncate">{mj.name}</div>
                          <div className="text-xs text-muted-foreground">
                            {mj.country || '—'} · {mj.schema || '—'} · {mj.status || '—'}
                            {total > 0 && ` · ${synced}/${total} synced`}
                          </div>
                        </div>
                        {isSel && <span className="text-xs text-primary font-semibold">✓</span>}
                      </div>
                    );
                  })
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
                    <Play className="h-4 w-4 mr-2" /> Generate personas ({selected.length} mega-job{selected.length === 1 ? '' : 's'})
                  </>
                )}
              </Button>
              <p className="text-xs text-muted-foreground">
                Two megajobs unlock cross-dataset insights: brand cohabitation matrix, BK-vs-others overlap, persona × brand exclusivity.
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
                        <div className="text-sm font-medium truncate">{r.megaJobNames.join(' + ')}</div>
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

async function safePoll(url: string, init?: RequestInit) {
  const r = await fetch(url, init);
  if (r.status === 504) return { phase: 'polling', progress: { message: 'Server processing (retrying…)' } };
  let data: any;
  try { data = await r.json(); } catch { return { phase: 'polling', progress: { message: 'Server processing (retrying…)' } }; }
  if (!r.ok) throw new Error(data.error || `HTTP ${r.status}`);
  return data;
}
