'use client';

import { Loader2, CheckCircle2, AlertCircle, Activity } from 'lucide-react';

interface ProgressInfo {
  step?: string;
  percent?: number;
  /** Subprogress label (granular), preferred over phaseLabel for the headline. */
  message?: string;
  /** Optional second-line detail (bytes scanned, rows, runtime). */
  details?: string;
  /** Phase label (e.g. "Aggregating per-device features in Athena") shown smaller. */
  phaseLabel?: string;
  /** 0-1 ratio for the in-phase mini-bar. */
  ratio?: number;
  /** Per-source status strings rendered as small chips. */
  perSource?: Record<string, string>;
}

const PHASE_ORDER: { key: string; label: string }[] = [
  { key: 'starting', label: 'Setup' },
  { key: 'feature_ctas', label: 'Feature CTAS' },
  { key: 'feature_polling', label: 'Feature CTAS' },
  { key: 'download_query', label: 'Download' },
  { key: 'download_polling', label: 'Download' },
  { key: 'download_read', label: 'Read' },
  { key: 'clustering', label: 'Cluster' },
  { key: 'aggregation', label: 'Aggregate' },
  { key: 'master_maids_export', label: 'Export' },
  { key: 'export_polling', label: 'Export' },
  { key: 'done', label: 'Done' },
];

function phaseRank(step?: string): number {
  if (!step) return 0;
  return PHASE_ORDER.findIndex((p) => p.key === step);
}

interface Props {
  progress: ProgressInfo;
  phase?: string;
  error?: string | null;
}

export default function PersonaProgress({ progress, phase, error }: Props) {
  const isError = !!error || phase === 'error';
  const isDone = phase === 'done';
  const percent = Math.max(0, Math.min(100, progress.percent ?? 0));
  const ratio = Math.max(0, Math.min(1, progress.ratio ?? 0));
  const currentRank = phaseRank(progress.step || phase);

  return (
    <div className="border rounded-lg bg-card p-6 space-y-5 max-w-2xl mx-auto">
      <div className="flex items-start gap-3">
        {isError ? (
          <AlertCircle className="h-6 w-6 text-red-500 mt-0.5 flex-shrink-0" />
        ) : isDone ? (
          <CheckCircle2 className="h-6 w-6 text-emerald-500 mt-0.5 flex-shrink-0" />
        ) : (
          <Loader2 className="h-6 w-6 animate-spin text-primary mt-0.5 flex-shrink-0" />
        )}
        <div className="flex-1 min-w-0">
          <div className="text-base font-semibold leading-tight">
            {isError
              ? 'Run failed'
              : isDone
              ? 'Personas ready'
              : progress.message || progress.phaseLabel || 'Working…'}
          </div>
          {!isError && !isDone && progress.phaseLabel && progress.message !== progress.phaseLabel && (
            <div className="text-xs text-muted-foreground mt-0.5">{progress.phaseLabel}</div>
          )}
          {progress.details && (
            <div className="text-xs text-muted-foreground mt-1.5 flex items-center gap-1.5">
              <Activity className="h-3 w-3" /> {progress.details}
            </div>
          )}
          {error && <div className="text-sm text-red-500 mt-2 break-words">{error}</div>}
        </div>
      </div>

      {/* Overall progress bar */}
      {!isError && (
        <div className="space-y-1.5">
          <div className="h-2 w-full bg-muted rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all duration-500 ease-out"
              style={{
                width: `${percent}%`,
                backgroundColor: isDone ? '#10b981' : 'hsl(var(--primary))',
              }}
            />
          </div>
          <div className="flex items-center justify-between text-xs text-muted-foreground tabular-nums">
            <span>Overall progress</span>
            <span>{percent.toFixed(0)}%</span>
          </div>
        </div>
      )}

      {/* Phase-specific sub-progress (e.g. 1/2 sources done) */}
      {!isError && !isDone && ratio > 0 && ratio < 1 && (
        <div className="space-y-1.5">
          <div className="h-1.5 w-full bg-muted rounded-full overflow-hidden">
            <div
              className="h-full rounded-full bg-blue-500 transition-all duration-300 ease-out"
              style={{ width: `${ratio * 100}%` }}
            />
          </div>
          <div className="flex items-center justify-between text-xs text-muted-foreground tabular-nums">
            <span>Current phase</span>
            <span>{(ratio * 100).toFixed(0)}%</span>
          </div>
        </div>
      )}

      {/* Per-source chips */}
      {progress.perSource && Object.keys(progress.perSource).length > 0 && (
        <div className="space-y-1.5">
          <div className="text-xs uppercase tracking-wider text-muted-foreground">By source</div>
          <div className="space-y-1">
            {Object.entries(progress.perSource).map(([src, status]) => (
              <div key={src} className="text-xs flex items-center gap-2">
                <span className="font-mono text-muted-foreground truncate max-w-[140px]">
                  {src.slice(0, 8)}…
                </span>
                <span className="text-foreground">{status}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Phase stepper */}
      {!isError && (
        <div className="grid grid-cols-7 gap-1 mt-2">
          {PHASE_ORDER.filter((p) => p.key !== 'feature_ctas' && p.key !== 'download_polling' && p.key !== 'export_polling').map((p) => {
            const idx = PHASE_ORDER.findIndex((x) => x.key === p.key);
            const isCurrent = idx === currentRank ||
              // collapsed phases: feature_polling covers feature_ctas, download_query/read/polling collapse, etc.
              (p.key === 'feature_polling' && (progress.step === 'feature_ctas')) ||
              (p.key === 'download_query' && (progress.step === 'download_polling' || progress.step === 'download_read')) ||
              (p.key === 'master_maids_export' && progress.step === 'export_polling');
            const isPast = idx < currentRank && !isCurrent;
            return (
              <div key={p.key} className="flex flex-col items-center gap-1">
                <div
                  className={`h-1.5 w-full rounded-full ${
                    isPast
                      ? 'bg-emerald-500'
                      : isCurrent
                      ? 'bg-primary animate-pulse'
                      : 'bg-muted'
                  }`}
                />
                <span
                  className={`text-[10px] uppercase tracking-wider truncate w-full text-center ${
                    isCurrent ? 'text-foreground font-semibold' : 'text-muted-foreground'
                  }`}
                >
                  {p.label}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
