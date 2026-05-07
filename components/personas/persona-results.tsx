'use client';

import { Fragment, useEffect, useMemo, useState } from 'react';
import {
  ResponsiveContainer,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Cell,
} from 'recharts';
import {
  Sparkles,
  TrendingUp,
  TrendingDown,
  Activity,
  Target,
  AlertTriangle,
  Users,
  Clock,
  MapPin,
  Compass,
  Star,
  ArrowUp,
  ArrowDown,
  ArrowUpDown,
} from 'lucide-react';
import type {
  PersonaReport,
  PersonaCluster,
  PersonaInsight,
  RfmCell,
  CohabitationEntry,
  SourceZipAffinity,
  ZipAffinityRow,
} from '@/lib/persona-types';

const PERSONA_COLORS = [
  '#3b82f6', // blue
  '#f97316', // orange
  '#10b981', // emerald
  '#a855f7', // violet
  '#ec4899', // pink
  '#facc15', // yellow
  '#14b8a6', // teal
  '#ef4444', // red
];

const RFM_COLORS: Record<string, string> = {
  Champions: '#10b981',
  'Loyal+': '#3b82f6',
  Loyal: '#6366f1',
  Promising: '#a855f7',
  'Need Attention': '#f97316',
  'At Risk': '#f59e0b',
  "Can't Lose": '#facc15',
  Hibernating: '#94a3b8',
  Lost: '#ef4444',
};

const SEVERITY_COLORS: Record<PersonaInsight['severity'], string> = {
  positive: 'border-emerald-500/40 bg-emerald-500/5',
  highlight: 'border-blue-500/40 bg-blue-500/5',
  neutral: 'border-border bg-muted/30',
  warning: 'border-amber-500/40 bg-amber-500/5',
};

const SEVERITY_ICONS: Record<PersonaInsight['severity'], JSX.Element> = {
  positive: <TrendingUp className="h-4 w-4 text-emerald-500" />,
  highlight: <Sparkles className="h-4 w-4 text-blue-500" />,
  neutral: <Activity className="h-4 w-4 text-muted-foreground" />,
  warning: <AlertTriangle className="h-4 w-4 text-amber-500" />,
};

function fmtNum(n: number): string {
  if (!Number.isFinite(n)) return '0';
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return Math.round(n).toLocaleString();
}

function pct(n: number): string {
  return `${n.toFixed(1)}%`;
}

interface Props {
  report: PersonaReport;
  megaJobNames: string[];
}

type TabKey = 'overview' | 'personas' | 'rfm' | 'cohabitation' | 'zipAffinity' | 'insights';

export default function PersonaResults({ report, megaJobNames }: Props) {
  const [tab, setTab] = useState<TabKey>('overview');
  const tabs: { key: TabKey; label: string; show: boolean }[] = [
    { key: 'overview', label: 'Overview', show: true },
    { key: 'personas', label: `Personas (${report.personas.length})`, show: true },
    { key: 'rfm', label: 'RFM Grid', show: true },
    { key: 'cohabitation', label: 'Brand Cohabitation', show: !!report.cohabitation },
    {
      key: 'zipAffinity',
      label: `Zip Affinity${
        report.zipAffinity?.length ? ` (${report.zipAffinity.length})` : ''
      }`,
      show: !!(report.zipAffinity && report.zipAffinity.length > 0),
    },
    { key: 'insights', label: `Insights (${report.insights.length})`, show: true },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-1 border-b">
        {tabs.filter((t) => t.show).map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              tab === t.key
                ? 'border-primary text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'overview' && <OverviewTab report={report} megaJobNames={megaJobNames} />}
      {tab === 'personas' && <PersonasTab report={report} />}
      {tab === 'rfm' && <RfmTab report={report} />}
      {tab === 'cohabitation' && report.cohabitation && (
        <CohabitationTab entries={report.cohabitation.entries} brands={report.cohabitation.brands} />
      )}
      {tab === 'zipAffinity' && report.zipAffinity && (
        <ZipAffinityTab sources={report.zipAffinity} runId={report.runId} />
      )}
      {tab === 'insights' && <InsightsTab insights={report.insights} />}
    </div>
  );
}

// ── Overview / Scorecard ─────────────────────────────────────────────

function OverviewTab({ report, megaJobNames }: { report: PersonaReport; megaJobNames: string[] }) {
  const sc = report.scorecard;
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Total devices" value={fmtNum(sc.totalDevices)} sub="across megajobs" icon={<Users className="h-4 w-4" />} />
        <StatCard label="High-quality" value={fmtNum(sc.highQualityDevices)} sub={`${pct((sc.highQualityDevices / Math.max(1, sc.totalDevices)) * 100)} of base`} icon={<Star className="h-4 w-4" />} />
        <StatCard label="Personas" value={String(report.personas.length)} sub="auto-discovered" icon={<Sparkles className="h-4 w-4" />} />
        <StatCard label="Exported audiences" value={String(report.exports.length)} sub="to Master MAIDs" icon={<Target className="h-4 w-4" />} />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card title="Frequency tiers">
          <DonutBars data={sc.freqTiers.map((t) => ({ label: t.tier, value: t.count, percent: t.percent }))} />
        </Card>
        <Card title="Dwell tiers">
          <DonutBars data={sc.dwellTiers.map((t) => ({ label: t.tier, value: t.count, percent: t.percent }))} />
        </Card>
      </div>

      <Card title="Hour buckets (avg share across devices)">
        <div className="space-y-2">
          {sc.hourBuckets.map((h) => (
            <div key={h.bucket} className="flex items-center gap-3 text-sm">
              <span className="w-24 capitalize text-muted-foreground">{h.bucket}</span>
              <div className="flex-1 h-3 bg-muted rounded-full overflow-hidden">
                <div className="h-full bg-blue-500 rounded-full" style={{ width: `${h.share * 100}%` }} />
              </div>
              <span className="w-16 text-right tabular-nums">{pct(h.share * 100)}</span>
            </div>
          ))}
        </div>
      </Card>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Weekend share (median)" value={pct(sc.weekendShareMedian * 100)} sub={`p90 ${pct(sc.weekendShareP90 * 100)}`} icon={<Clock className="h-4 w-4" />} />
        <StatCard label="Mobility radius (median)" value={`${sc.gyrationKmP50.toFixed(1)} km`} sub={`p90 ${sc.gyrationKmP90.toFixed(1)} km`} icon={<Compass className="h-4 w-4" />} />
        <StatCard label="Megajobs" value={String(report.config.megaJobIds.length)} sub={megaJobNames.join(' · ')} icon={<Activity className="h-4 w-4" />} />
        <StatCard label="Generated" value={new Date(report.generatedAt).toLocaleString()} sub="" icon={<Sparkles className="h-4 w-4" />} />
      </div>
    </div>
  );
}

function StatCard({ label, value, sub, icon }: { label: string; value: string; sub: string; icon?: JSX.Element }) {
  return (
    <div className="border rounded-lg p-3 bg-card">
      <div className="flex items-center gap-2 text-xs text-muted-foreground mb-1">
        {icon}
        <span>{label}</span>
      </div>
      <div className="text-xl font-bold tabular-nums leading-tight">{value}</div>
      {sub && <div className="text-xs text-muted-foreground mt-1 truncate">{sub}</div>}
    </div>
  );
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="border rounded-lg overflow-hidden bg-card">
      <div className="px-4 py-2 text-sm font-semibold border-b bg-muted/30">{title}</div>
      <div className="p-4">{children}</div>
    </div>
  );
}

function DonutBars({ data }: { data: { label: string; value: number; percent: number }[] }) {
  return (
    <div className="space-y-2">
      {data.map((d, i) => (
        <div key={d.label} className="flex items-center gap-3 text-sm">
          <span className="w-20 capitalize text-muted-foreground">{d.label}</span>
          <div className="flex-1 h-3 bg-muted rounded-full overflow-hidden">
            <div className="h-full rounded-full" style={{ width: `${d.percent}%`, backgroundColor: PERSONA_COLORS[i % PERSONA_COLORS.length] }} />
          </div>
          <span className="w-20 text-right tabular-nums text-muted-foreground">{fmtNum(d.value)}</span>
          <span className="w-14 text-right tabular-nums">{pct(d.percent)}</span>
        </div>
      ))}
    </div>
  );
}

// ── Personas tab ─────────────────────────────────────────────────────

function PersonasTab({ report }: { report: PersonaReport }) {
  const sorted = useMemo(() => [...report.personas].sort((a, b) => b.deviceCount - a.deviceCount), [report]);
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      {sorted.map((p) => (
        <PersonaCard key={p.id} persona={p} totalDevices={report.scorecard.totalDevices} />
      ))}
    </div>
  );
}

function PersonaCard({ persona, totalDevices }: { persona: PersonaCluster; totalDevices: number }) {
  const color = PERSONA_COLORS[persona.id % PERSONA_COLORS.length];
  const radarData = persona.radarAxes.map((a) => ({ axis: a.label, value: a.value }));

  return (
    <div className="border rounded-lg overflow-hidden bg-card flex flex-col">
      <div className="h-1.5" style={{ backgroundColor: color }} />
      <div className="p-4 space-y-3 flex-1">
        <div>
          <div className="text-xs text-muted-foreground uppercase tracking-wider">Persona #{persona.id + 1}</div>
          <div className="text-lg font-bold leading-tight">{persona.name}</div>
          <div className="text-xs text-muted-foreground mt-1">{persona.description}</div>
        </div>

        <div className="grid grid-cols-2 gap-2 text-xs">
          <div className="border rounded p-2">
            <div className="text-muted-foreground">Devices</div>
            <div className="text-base font-bold tabular-nums">{fmtNum(persona.deviceCount)}</div>
            <div className="text-[10px] text-muted-foreground">{pct(persona.percentOfBase)} of base</div>
          </div>
          <div className="border rounded p-2">
            <div className="text-muted-foreground">Median visits</div>
            <div className="text-base font-bold tabular-nums">{persona.medians.total_visits.toFixed(0)}</div>
            <div className="text-[10px] text-muted-foreground">{persona.medians.avg_dwell_min.toFixed(0)} min avg dwell</div>
          </div>
        </div>

        {radarData.length > 0 && (
          <div className="h-44">
            <ResponsiveContainer width="100%" height="100%">
              <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="75%">
                <PolarGrid stroke="hsl(var(--border))" />
                <PolarAngleAxis dataKey="axis" tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }} />
                <PolarRadiusAxis angle={90} domain={[0, 1]} tick={false} stroke="hsl(var(--border))" />
                <Radar dataKey="value" stroke={color} fill={color} fillOpacity={0.45} />
              </RadarChart>
            </ResponsiveContainer>
          </div>
        )}

        {persona.topZips.length > 0 && (
          <div className="text-xs">
            <div className="text-muted-foreground mb-1">Top home ZIPs</div>
            <div className="flex flex-wrap gap-1">
              {persona.topZips.slice(0, 5).map((z) => (
                <span key={z.zip} className="px-2 py-0.5 rounded bg-muted text-foreground tabular-nums">
                  {z.zip}{' '}<span className="text-muted-foreground">({z.count})</span>
                </span>
              ))}
            </div>
          </div>
        )}

        {Object.keys(persona.brandMix).length > 0 && (
          <div className="text-xs">
            <div className="text-muted-foreground mb-1">Brand mix (top)</div>
            <div className="space-y-1">
              {Object.entries(persona.brandMix)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 4)
                .map(([brand, n]) => (
                  <div key={brand} className="flex items-center gap-2">
                    <span className="capitalize w-28 truncate">{brand.replace(/_/g, ' ')}</span>
                    <div className="flex-1 h-2 bg-muted rounded overflow-hidden">
                      <div className="h-full" style={{ backgroundColor: color, width: `${(n / Math.max(...Object.values(persona.brandMix))) * 100}%` }} />
                    </div>
                    <span className="tabular-nums text-muted-foreground w-12 text-right">{fmtNum(n)}</span>
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// ── RFM tab ──────────────────────────────────────────────────────────

function RfmTab({ report }: { report: PersonaReport }) {
  const grid = report.rfm.cells;
  const cellByKey: Record<string, RfmCell> = {};
  for (const c of grid) cellByKey[`${c.rTertile}-${c.fmTertile}`] = c;
  // Render 3×3 with R rows: high (top, recent), mid, low (bottom, lapsed); FM cols: low → mid → high
  const rRows: ('high' | 'mid' | 'low')[] = ['high', 'mid', 'low'];
  const fmCols: ('low' | 'mid' | 'high')[] = ['low', 'mid', 'high'];
  const max = Math.max(1, ...grid.map((c) => c.deviceCount));
  return (
    <div className="space-y-3">
      <div className="text-sm text-muted-foreground">
        9-cell RFM grid. Rows: Recency (top = recent). Columns: Frequency + Monetary (right = highest).
      </div>
      <div className="grid grid-cols-[80px_repeat(3,1fr)] gap-1">
        <div></div>
        {fmCols.map((fm) => (
          <div key={fm} className="text-center text-xs text-muted-foreground capitalize py-1">FM {fm}</div>
        ))}
        {rRows.map((r) => (
          <Fragment key={`row-${r}`}>
            <div className="text-xs text-muted-foreground capitalize flex items-center pr-2 justify-end">R {r}</div>
            {fmCols.map((fm) => {
              const cell = cellByKey[`${r}-${fm}`];
              const intensity = cell ? cell.deviceCount / max : 0;
              const color = RFM_COLORS[cell?.label || 'Hibernating'] || '#94a3b8';
              return (
                <div
                  key={`${r}-${fm}`}
                  className="border rounded-lg p-3 bg-card relative overflow-hidden min-h-[120px]"
                >
                  <div
                    className="absolute inset-0 opacity-25"
                    style={{ backgroundColor: color, opacity: 0.1 + intensity * 0.4 }}
                  />
                  <div className="relative">
                    <div className="text-xs font-semibold" style={{ color }}>
                      {cell?.label || '—'}
                    </div>
                    <div className="text-xl font-bold tabular-nums mt-1">
                      {cell ? fmtNum(cell.deviceCount) : '0'}
                    </div>
                    <div className="text-[10px] text-muted-foreground mt-1">
                      {cell ? pct(cell.percentOfBase) : '0%'} of base
                    </div>
                    {cell && cell.deviceCount > 0 && (
                      <div className="text-[10px] text-muted-foreground mt-1">
                        Med: {cell.medianFrequency.toFixed(0)} visits · {cell.medianRecencyDays.toFixed(0)}d ago
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </Fragment>
        ))}
      </div>
    </div>
  );
}

// ── Cohabitation tab ─────────────────────────────────────────────────

type CohabSortKey = 'brandA' | 'brandB' | 'jaccard' | 'shareAtoB' | 'shareBtoA' | 'intersectionDevices';
type SortDir = 'asc' | 'desc';

function CohabitationTab({ entries, brands }: { entries: CohabitationEntry[]; brands: string[] }) {
  const [sortKey, setSortKey] = useState<CohabSortKey>('jaccard');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [showAll, setShowAll] = useState(false);
  const [brandFilter, setBrandFilter] = useState<string>('');

  // Apply brand filter first (so the row count + Show all label reflect it),
  // then sort.
  const filteredAndSorted = useMemo(() => {
    const q = brandFilter.trim().toLowerCase();
    let rows = entries;
    if (q) {
      rows = entries.filter(
        (e) =>
          e.brandA.toLowerCase().includes(q) ||
          e.brandB.toLowerCase().includes(q)
      );
    }
    const dir = sortDir === 'asc' ? 1 : -1;
    const cmp = (a: CohabitationEntry, b: CohabitationEntry): number => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === 'string' && typeof bv === 'string') return av.localeCompare(bv) * dir;
      return ((av as number) - (bv as number)) * dir;
    };
    return [...rows].sort(cmp);
  }, [entries, sortKey, sortDir, brandFilter]);

  const sorted = filteredAndSorted;
  const visible = showAll ? sorted : sorted.slice(0, 30);

  // Sorted unique brand list for the dropdown — alphabetical.
  const brandsSorted = useMemo(
    () => [...brands].sort((a, b) => a.localeCompare(b)),
    [brands]
  );

  const onSort = (key: CohabSortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      // Numeric columns default desc (largest first); text columns default asc.
      setSortDir(key === 'brandA' || key === 'brandB' ? 'asc' : 'desc');
    }
  };

  if (brands.length === 0) {
    return <div className="text-sm text-muted-foreground p-4">No brands detected (need ≥50 visitors per brand).</div>;
  }

  const SortHeader = ({
    columnKey,
    align,
    children,
  }: {
    columnKey: CohabSortKey;
    align: 'left' | 'right';
    children: React.ReactNode;
  }) => {
    const active = sortKey === columnKey;
    const Icon = active ? (sortDir === 'asc' ? ArrowUp : ArrowDown) : ArrowUpDown;
    return (
      <th className={`px-3 py-2 ${align === 'left' ? 'text-left' : 'text-right'}`}>
        <button
          type="button"
          onClick={() => onSort(columnKey)}
          className={`inline-flex items-center gap-1 hover:text-foreground transition-colors ${
            align === 'right' ? 'flex-row-reverse' : ''
          } ${active ? 'text-foreground' : ''}`}
          title={`Sort by ${children}`}
        >
          <span>{children}</span>
          <Icon className={`h-3 w-3 ${active ? 'opacity-100' : 'opacity-40'}`} />
        </button>
      </th>
    );
  };

  return (
    <div className="space-y-3">
      <div className="text-sm text-muted-foreground">
        Pairwise device-overlap between brands. Click a column to sort. Higher Jaccard = tighter cross-shopping.
      </div>
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <input
            type="text"
            value={brandFilter}
            onChange={(e) => setBrandFilter(e.target.value)}
            placeholder="Filter by brand…"
            list="cohab-brand-list"
            className="h-8 px-3 rounded-md border border-input bg-background text-sm w-56"
          />
          <datalist id="cohab-brand-list">
            {brandsSorted.map((b) => (
              <option key={b} value={b.replace(/_/g, ' ')} />
            ))}
          </datalist>
          {brandFilter && (
            <button
              type="button"
              onClick={() => setBrandFilter('')}
              className="text-xs px-2 py-1 rounded bg-muted/40 hover:bg-muted text-foreground"
              title="Clear filter"
            >
              Clear
            </button>
          )}
        </div>
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>{sorted.length.toLocaleString()} pair{sorted.length === 1 ? '' : 's'}</span>
          {sorted.length > 30 && (
            <button
              type="button"
              onClick={() => setShowAll((v) => !v)}
              className="px-2 py-1 rounded bg-muted/40 hover:bg-muted text-foreground whitespace-nowrap"
            >
              {showAll ? `Show top 30` : `Show all ${sorted.length}`}
            </button>
          )}
        </div>
      </div>
      <div className="border rounded-lg overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-muted/30 text-xs uppercase tracking-wider text-muted-foreground">
            <tr>
              <SortHeader columnKey="brandA" align="left">Brand A</SortHeader>
              <SortHeader columnKey="brandB" align="left">Brand B</SortHeader>
              <SortHeader columnKey="jaccard" align="right">Jaccard</SortHeader>
              <SortHeader columnKey="shareAtoB" align="right">% of A also visit B</SortHeader>
              <SortHeader columnKey="shareBtoA" align="right">% of B also visit A</SortHeader>
              <SortHeader columnKey="intersectionDevices" align="right">Overlap devices</SortHeader>
            </tr>
          </thead>
          <tbody>
            {visible.map((e, i) => (
              <tr key={`${e.brandA}-${e.brandB}-${i}`} className="border-t border-border/50 hover:bg-muted/30">
                <td className="px-3 py-2 capitalize">{e.brandA.replace(/_/g, ' ')}</td>
                <td className="px-3 py-2 capitalize">{e.brandB.replace(/_/g, ' ')}</td>
                <td className="px-3 py-2 text-right tabular-nums">
                  <span className="px-2 py-0.5 rounded text-xs" style={{ backgroundColor: `rgba(59,130,246,${Math.max(0.1, e.jaccard)})`, color: 'white' }}>
                    {e.jaccard.toFixed(3)}
                  </span>
                </td>
                <td className="px-3 py-2 text-right tabular-nums">{pct(e.shareAtoB * 100)}</td>
                <td className="px-3 py-2 text-right tabular-nums">{pct(e.shareBtoA * 100)}</td>
                <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">{fmtNum(e.intersectionDevices)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Zip Affinity tab ─────────────────────────────────────────────────

type AffinityMode = 'pop' | 'volume';
type ZipSortKey = 'zip' | 'affinityIndex' | 'count' | 'population';

function ZipAffinityTab({
  sources,
  runId,
}: {
  sources: SourceZipAffinity[];
  runId: string;
}) {
  const [activeSourceId, setActiveSourceId] = useState<string>(sources[0]?.sourceId || '');
  const [search, setSearch] = useState<string>('');
  const [sortKey, setSortKey] = useState<ZipSortKey>('affinityIndex');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  const active = sources.find((s) => s.sourceId === activeSourceId) || sources[0];

  // Default to population-weighted when this source has it; volume otherwise.
  const [mode, setMode] = useState<AffinityMode>(active?.hasPopulation ? 'pop' : 'volume');

  // Switch mode automatically when the user switches source to one without pop.
  useEffect(() => {
    if (!active) return;
    if (mode === 'pop' && !active.hasPopulation) setMode('volume');
  }, [activeSourceId, active, mode]);

  const indexFor = (r: ZipAffinityRow) =>
    mode === 'pop' ? r.affinityIndexPop : r.affinityIndexVolume;

  const filtered = useMemo(() => {
    if (!active) return [];
    const q = search.trim().toLowerCase();
    const dir = sortDir === 'asc' ? 1 : -1;
    const cmp = (a: ZipAffinityRow, b: ZipAffinityRow) => {
      let av: string | number;
      let bv: string | number;
      if (sortKey === 'affinityIndex') {
        av = indexFor(a);
        bv = indexFor(b);
      } else if (sortKey === 'zip') {
        av = a.zip;
        bv = b.zip;
      } else {
        av = a[sortKey] as number;
        bv = b[sortKey] as number;
      }
      if (typeof av === 'string' && typeof bv === 'string') return av.localeCompare(bv) * dir;
      return ((av as number) - (bv as number)) * dir;
    };
    let rows = active.rows;
    if (q) rows = rows.filter((r) => r.zip.toLowerCase().includes(q));
    return [...rows].sort(cmp);
  }, [active, search, sortKey, sortDir, mode]);

  const onSort = (key: ZipSortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir(key === 'zip' ? 'asc' : 'desc');
    }
  };

  const downloadCsv = () => {
    if (!active) return;
    const rows = filtered;
    const lines = ['Zip Code,Affinity Index'];
    for (const r of rows) {
      const z = /[",\n]/.test(r.zip) ? `"${r.zip.replace(/"/g, '""')}"` : r.zip;
      lines.push(`${z},${indexFor(r)}`);
    }
    const csv = lines.join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const safeLabel = active.sourceLabel.replace(/[^a-zA-Z0-9_-]+/g, '_').slice(0, 60) || 'source';
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `zip-affinity-${mode}_${safeLabel}_${runId.slice(0, 8)}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  if (!active) {
    return (
      <div className="text-sm text-muted-foreground p-4">
        No ZIP data available — devices need a resolvable home_zip (FULL schema or
        reverse-geocoded fallback) to compute affinity.
      </div>
    );
  }

  // Color ramp. Pop-mode max ≈ 200 (anything ≥ 200 saturates to vivid blue);
  // volume-mode max = 100. Normalize to [0,1] for opacity.
  const barWidthPct = (idx: number) => {
    if (mode === 'pop') return Math.min(100, (idx / 200) * 100);
    return Math.max(0, Math.min(100, idx));
  };
  const barColor = (idx: number) => {
    const norm = mode === 'pop' ? Math.min(1, idx / 200) : Math.min(1, idx / 100);
    return `rgba(59,130,246,${Math.max(0.18, norm)})`;
  };

  const SortHeader = ({
    columnKey,
    align,
    children,
  }: {
    columnKey: ZipSortKey;
    align: 'left' | 'right';
    children: React.ReactNode;
  }) => {
    const activeCol = sortKey === columnKey;
    const Icon = activeCol ? (sortDir === 'asc' ? ArrowUp : ArrowDown) : ArrowUpDown;
    return (
      <th className={`px-3 py-2 ${align === 'left' ? 'text-left' : 'text-right'}`}>
        <button
          type="button"
          onClick={() => onSort(columnKey)}
          className={`inline-flex items-center gap-1 hover:text-foreground transition-colors ${
            align === 'right' ? 'flex-row-reverse' : ''
          } ${activeCol ? 'text-foreground' : ''}`}
          title={`Sort by ${columnKey}`}
        >
          <span>{children}</span>
          <Icon className={`h-3 w-3 ${activeCol ? 'opacity-100' : 'opacity-40'}`} />
        </button>
      </th>
    );
  };

  const modeDescription =
    mode === 'pop'
      ? 'Population-weighted index (CPG-style "vs baseline"). 100 = ZIP delivers visitors proportional to its size. >100 over-indexes; <100 under-indexes. Capped at 300.'
      : 'Volume index 0..100 — share of the top ZIP\'s visitor count. Best for coverage / media buying where audience size matters most.';

  return (
    <div className="space-y-3">
      <div className="text-sm text-muted-foreground">{modeDescription}</div>

      {/* Source toggle (when 2+ sources) */}
      {sources.length > 1 && (
        <div className="flex items-center gap-1 flex-wrap">
          <span className="text-xs text-muted-foreground mr-1">Source:</span>
          {sources.map((s) => (
            <button
              key={s.sourceId}
              type="button"
              onClick={() => setActiveSourceId(s.sourceId)}
              className={`px-3 py-1 rounded-md text-xs font-medium transition-colors border ${
                s.sourceId === active.sourceId
                  ? 'bg-primary text-primary-foreground border-primary'
                  : 'bg-muted/30 text-muted-foreground border-border hover:bg-muted'
              }`}
            >
              {s.sourceLabel}
              <span className="ml-1.5 opacity-70">· {fmtNum(s.rows.length)} ZIPs</span>
              {s.hasPopulation && <span className="ml-1 text-[10px] opacity-60">📊</span>}
            </button>
          ))}
        </div>
      )}

      {/* Mode toggle */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className="text-xs text-muted-foreground">Index by:</span>
        <div className="inline-flex rounded-md border border-border overflow-hidden">
          <button
            type="button"
            onClick={() => setMode('pop')}
            disabled={!active.hasPopulation}
            className={`px-3 py-1 text-xs font-medium transition-colors ${
              mode === 'pop'
                ? 'bg-primary text-primary-foreground'
                : 'bg-muted/30 text-muted-foreground hover:bg-muted'
            } ${!active.hasPopulation ? 'opacity-40 cursor-not-allowed' : ''}`}
            title={active.hasPopulation ? 'CPG-style: visitors per resident, normalized vs baseline' : `No population data uploaded for ${active.country || 'this country'}. Upload via /datasets → NSE modal.`}
          >
            Population-weighted
          </button>
          <button
            type="button"
            onClick={() => setMode('volume')}
            className={`px-3 py-1 text-xs font-medium transition-colors border-l border-border ${
              mode === 'volume'
                ? 'bg-primary text-primary-foreground'
                : 'bg-muted/30 text-muted-foreground hover:bg-muted'
            }`}
            title="Raw visitor count normalized to top ZIP (0..100)"
          >
            Volume-only
          </button>
        </div>
        {!active.hasPopulation && (
          <span className="text-xs text-amber-500/80">
            ⚠ No population data for {active.country || 'this country'} — pop-weighted unavailable. Upload via the dataset NSE modal.
          </span>
        )}
      </div>

      {/* Toolbar: search + download CSV */}
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Filter by ZIP code…"
          className="h-8 px-3 rounded-md border border-input bg-background text-sm w-56"
        />
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>
            {filtered.length.toLocaleString()} ZIPs · {fmtNum(active.totalDevicesWithZip)} devices
          </span>
          <button
            type="button"
            onClick={downloadCsv}
            className="px-3 py-1.5 rounded-md text-xs font-medium bg-primary/10 text-foreground border border-primary/30 hover:bg-primary/20 transition-colors"
            title="Download as CSV (Zip Code, Affinity Index)"
          >
            Download CSV
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="border rounded-lg overflow-x-auto max-h-[600px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="bg-muted/30 text-xs uppercase tracking-wider text-muted-foreground sticky top-0">
            <tr>
              <SortHeader columnKey="zip" align="left">Zip Code</SortHeader>
              <SortHeader columnKey="affinityIndex" align="right">Affinity Index</SortHeader>
              <SortHeader columnKey="count" align="right">Visitors</SortHeader>
              {active.hasPopulation && (
                <SortHeader columnKey="population" align="right">Population</SortHeader>
              )}
              <th className="text-left px-3 py-2 w-1/3">Score</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((r) => {
              const idx = indexFor(r);
              const isFallback = mode === 'pop' && r.noPopulation;
              return (
                <tr key={r.zip} className="border-t border-border/50 hover:bg-muted/30">
                  <td className="px-3 py-2 font-mono text-xs">{r.zip}</td>
                  <td className="px-3 py-2 text-right tabular-nums font-medium">
                    <div className="inline-flex items-center gap-1.5 justify-end">
                      <span>{idx}</span>
                      {isFallback && (
                        <span
                          className="text-[9px] px-1 py-0.5 rounded bg-amber-500/15 text-amber-500/80 border border-amber-500/30"
                          title="No population data for this ZIP — fell back to volume-only"
                        >
                          no pop
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                    {fmtNum(r.count)}
                  </td>
                  {active.hasPopulation && (
                    <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                      {r.population > 0 ? fmtNum(r.population) : '—'}
                    </td>
                  )}
                  <td className="px-3 py-2">
                    <div className="h-2 rounded bg-muted/40 overflow-hidden relative">
                      <div
                        className="h-full rounded"
                        style={{
                          width: `${barWidthPct(idx)}%`,
                          backgroundColor: barColor(idx),
                        }}
                      />
                      {/* Reference line at index=100 (the "average" baseline) */}
                      {mode === 'pop' && (
                        <div
                          className="absolute top-0 h-full w-px bg-foreground/40"
                          style={{ left: `${barWidthPct(100)}%` }}
                          title="Baseline (index = 100)"
                        />
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Insights tab ─────────────────────────────────────────────────────

function InsightsTab({ insights }: { insights: PersonaInsight[] }) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      {insights.map((ins) => (
        <div key={ins.id} className={`border rounded-lg p-4 ${SEVERITY_COLORS[ins.severity]}`}>
          <div className="flex items-start gap-2">
            {SEVERITY_ICONS[ins.severity]}
            <div className="flex-1">
              <div className="text-xs text-muted-foreground uppercase tracking-wider">{ins.title}</div>
              <div className="text-xl font-bold leading-tight mt-0.5">{ins.value}</div>
              <div className="text-sm text-muted-foreground mt-1.5">{ins.detail}</div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
