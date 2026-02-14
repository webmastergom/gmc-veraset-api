'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Separator } from '@/components/ui/separator';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  FlaskConical,
  Globe,
  BarChart3,
  Download,
  Loader2,
  Search,
  MapPinned,
  Database,
  CheckCircle,
  TrendingUp,
  Zap,
  Flame,
  Sparkles,
  ChevronDown,
  ChevronUp,
  Filter,
  X,
  ArrowUpRight,
  Activity,
  Target,
} from 'lucide-react';
import type {
  LabAnalysisResult,
  AffinityRecord,
  ZipcodeProfile,
  PoiCategory,
  CategoryStat,
} from '@/lib/laboratory-types';
import {
  POI_CATEGORIES,
  CATEGORY_LABELS,
  CATEGORY_GROUPS,
  LAB_COUNTRIES,
} from '@/lib/laboratory-types';

// ── Affinity color scale ───────────────────────────────────────────────
function getAffinityColor(index: number): string {
  if (index >= 80) return 'text-emerald-400';
  if (index >= 60) return 'text-green-400';
  if (index >= 40) return 'text-yellow-400';
  if (index >= 20) return 'text-orange-400';
  return 'text-red-400';
}

function getAffinityBg(index: number): string {
  if (index >= 80) return 'bg-emerald-400/10 border-emerald-400/20';
  if (index >= 60) return 'bg-green-400/10 border-green-400/20';
  if (index >= 40) return 'bg-yellow-400/10 border-yellow-400/20';
  if (index >= 20) return 'bg-orange-400/10 border-orange-400/20';
  return 'bg-red-400/10 border-red-400/20';
}

function getAffinityLabel(index: number): string {
  if (index >= 80) return 'Very High';
  if (index >= 60) return 'High';
  if (index >= 40) return 'Medium';
  if (index >= 20) return 'Low';
  return 'Very Low';
}

// ── Progress step config ───────────────────────────────────────────────
const PROGRESS_STEPS = [
  { key: 'initializing', label: 'Initializing', icon: Database },
  { key: 'loading_pois', label: 'Loading POIs', icon: MapPinned },
  { key: 'querying_visits', label: 'Querying visits', icon: Search },
  { key: 'geocoding', label: 'Geocoding', icon: Globe },
  { key: 'computing_affinity', label: 'Computing affinity', icon: Zap },
  { key: 'aggregating', label: 'Aggregating', icon: BarChart3 },
];

// ── Main page component ────────────────────────────────────────────────
export default function LaboratoryPage() {
  // ── State ──────────────────────────────────────────────────────────
  const [selectedCountry, setSelectedCountry] = useState<string>('FR');
  const [selectedCategories, setSelectedCategories] = useState<PoiCategory[]>([]);
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const [minVisits, setMinVisits] = useState<number>(5);
  const [showFilters, setShowFilters] = useState(false);

  const [loading, setLoading] = useState(false);
  const [progress, setProgress] = useState<{
    step: string; percent: number; message: string; detail?: string;
  } | null>(null);
  const [result, setResult] = useState<LabAnalysisResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Result view state
  const [activeTab, setActiveTab] = useState<'hotspots' | 'profiles' | 'categories' | 'records'>('hotspots');
  const [searchQuery, setSearchQuery] = useState('');
  const [sortField, setSortField] = useState<string>('affinityIndex');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [expandedCategory, setExpandedCategory] = useState<string | null>(null);

  const countryInfo = LAB_COUNTRIES.find(c => c.code === selectedCountry);

  // ── Category selection ─────────────────────────────────────────────
  const toggleCategory = useCallback((cat: PoiCategory) => {
    setSelectedCategories(prev =>
      prev.includes(cat) ? prev.filter(c => c !== cat) : [...prev, cat]
    );
  }, []);

  const toggleCategoryGroup = useCallback((groupKey: string) => {
    const group = CATEGORY_GROUPS[groupKey];
    if (!group) return;
    const allSelected = group.categories.every(c => selectedCategories.includes(c));
    if (allSelected) {
      setSelectedCategories(prev => prev.filter(c => !group.categories.includes(c)));
    } else {
      setSelectedCategories(prev => [...new Set([...prev, ...group.categories])]);
    }
  }, [selectedCategories]);

  // ── Run analysis ───────────────────────────────────────────────────
  const runAnalysis = useCallback(() => {
    setLoading(true);
    setError(null);
    setResult(null);
    setProgress({ step: 'initializing', percent: 0, message: 'Starting analysis...' });

    const params = new URLSearchParams({ country: selectedCountry });
    if (selectedCategories.length > 0) params.set('categories', selectedCategories.join(','));
    if (dateFrom) params.set('dateFrom', dateFrom);
    if (dateTo) params.set('dateTo', dateTo);
    if (minVisits !== 5) params.set('minVisits', String(minVisits));

    const es = new EventSource(`/api/laboratory/analyze/stream?${params}`);

    es.addEventListener('progress', (event) => {
      try {
        const data = JSON.parse(event.data);
        setProgress(data);
      } catch { /* ignore */ }
    });

    es.addEventListener('result', (event) => {
      try {
        const data = JSON.parse(event.data);
        setResult(data);
        setLoading(false);
        setProgress(null);
      } catch {
        setError('Failed to parse results');
        setLoading(false);
      }
      es.close();
    });

    es.onerror = () => {
      if (!result) {
        setError('Connection lost. The analysis may still be running on the server.');
      }
      setLoading(false);
      es.close();
    };
  }, [selectedCountry, selectedCategories, dateFrom, dateTo, minVisits, result]);

  // ── CSV download ───────────────────────────────────────────────────
  const downloadCSV = useCallback(() => {
    if (!result) return;

    const headers = [
      'postal_code', 'city', 'province', 'region', 'category',
      'visits', 'unique_devices', 'frequency',
      'concentration_score', 'frequency_score', 'temporal_score',
      'affinity_index',
    ];
    const rows = result.records.map(r => [
      r.zipcode, r.city, r.province, r.region, r.category,
      r.visits, r.uniqueDevices, r.frequency,
      r.concentrationScore, r.frequencyScore, r.temporalScore,
      r.affinityIndex,
    ].map(v => typeof v === 'string' && v.includes(',') ? `"${v}"` : v).join(','));

    const csv = [headers.join(','), ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `affinity_${result.country.toLowerCase()}_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }, [result]);

  // ── Filtered data ──────────────────────────────────────────────────
  const filteredProfiles = useMemo(() => {
    if (!result) return [];
    let profiles = result.profiles;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      profiles = profiles.filter(p =>
        p.zipcode.toLowerCase().includes(q) ||
        p.city.toLowerCase().includes(q) ||
        p.province.toLowerCase().includes(q)
      );
    }
    return profiles;
  }, [result, searchQuery]);

  const filteredRecords = useMemo(() => {
    if (!result) return [];
    let records = result.records;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      records = records.filter(r =>
        r.zipcode.toLowerCase().includes(q) ||
        r.city.toLowerCase().includes(q) ||
        r.category.toLowerCase().includes(q)
      );
    }
    return records;
  }, [result, searchQuery]);

  // ── Current progress step index ────────────────────────────────────
  const currentStepIndex = progress
    ? PROGRESS_STEPS.findIndex(s => s.key === progress.step)
    : -1;

  // ── Render ─────────────────────────────────────────────────────────
  return (
    <MainLayout>
      <div className="space-y-8">
        {/* ── Hero header ──────────────────────────────────────────── */}
        <div className="relative overflow-hidden rounded-2xl bg-gradient-to-br from-background via-secondary to-background border border-border p-8">
          <div className="absolute top-0 right-0 w-96 h-96 bg-theme-accent/5 rounded-full blur-3xl -translate-y-1/2 translate-x-1/3" />
          <div className="relative z-10">
            <div className="flex items-center gap-3 mb-4">
              <div className="p-3 rounded-xl bg-theme-accent/10 border border-theme-accent/20">
                <FlaskConical className="w-7 h-7 text-theme-accent" />
              </div>
              <div>
                <h1 className="text-3xl font-bold tracking-tight">Affinity Laboratory</h1>
                <p className="text-muted-foreground text-sm mt-1">
                  Spatial affinity indices by postal code — powered by GPS movement data
                </p>
              </div>
            </div>

            <div className="flex flex-wrap gap-2 mt-6">
              <Badge variant="outline" className="text-xs px-3 py-1 border-theme-accent/30 text-theme-accent">
                <Activity className="w-3 h-3 mr-1" />
                3 signals: concentration + frequency + temporal
              </Badge>
              <Badge variant="outline" className="text-xs px-3 py-1 border-muted-foreground/30">
                <Target className="w-3 h-3 mr-1" />
                27 POI categories
              </Badge>
              <Badge variant="outline" className="text-xs px-3 py-1 border-muted-foreground/30">
                <Globe className="w-3 h-3 mr-1" />
                24-country GeoJSON geocoding
              </Badge>
            </div>
          </div>
        </div>

        {/* ── Configuration panel ──────────────────────────────────── */}
        <Card className="border-border">
          <CardHeader className="pb-4">
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="text-lg font-semibold flex items-center gap-2">
                  <Filter className="w-5 h-5 text-muted-foreground" />
                  Analysis Configuration
                </CardTitle>
                <CardDescription className="mt-1">
                  Select country, categories, and date range
                </CardDescription>
              </div>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setShowFilters(!showFilters)}
                className="text-muted-foreground"
              >
                {showFilters ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                {showFilters ? 'Collapse' : 'Expand'}
              </Button>
            </div>
          </CardHeader>

          <CardContent className="space-y-6">
            {/* Row 1: Country + Dates + Min visits */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              {/* Country selector */}
              <div>
                <label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Country</label>
                <div className="flex gap-2">
                  {LAB_COUNTRIES.map(c => (
                    <button
                      key={c.code}
                      onClick={() => setSelectedCountry(c.code)}
                      className={`flex-1 flex flex-col items-center gap-1.5 p-3 rounded-xl border transition-all ${
                        selectedCountry === c.code
                          ? 'border-theme-accent bg-theme-accent/10 text-theme-accent'
                          : 'border-border bg-secondary hover:bg-secondary/80 text-muted-foreground hover:text-foreground'
                      }`}
                    >
                      <span className="text-2xl">{c.flag}</span>
                      <span className="text-xs font-medium">{c.name}</span>
                      <span className="text-[10px] text-muted-foreground">{(c.totalPois / 1000).toFixed(0)}k POIs</span>
                    </button>
                  ))}
                </div>
              </div>

              {/* Date range */}
              <div>
                <label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">From</label>
                <Input
                  type="date"
                  value={dateFrom}
                  onChange={(e) => setDateFrom(e.target.value)}
                  className="bg-secondary border-border"
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">To</label>
                <Input
                  type="date"
                  value={dateTo}
                  onChange={(e) => setDateTo(e.target.value)}
                  className="bg-secondary border-border"
                />
              </div>

              {/* Min visits */}
              <div>
                <label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Min visits</label>
                <Input
                  type="number"
                  value={minVisits}
                  onChange={(e) => setMinVisits(parseInt(e.target.value) || 5)}
                  min={1}
                  className="bg-secondary border-border"
                />
                <p className="text-[10px] text-muted-foreground mt-1">Noise filter per postal code</p>
              </div>
            </div>

            {/* Row 2: Category groups (expandable) */}
            {showFilters && (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <label className="text-xs text-muted-foreground uppercase tracking-wider">
                    Categories {selectedCategories.length > 0 && `(${selectedCategories.length} selected)`}
                  </label>
                  {selectedCategories.length > 0 && (
                    <button
                      onClick={() => setSelectedCategories([])}
                      className="text-xs text-muted-foreground hover:text-foreground flex items-center gap-1"
                    >
                      <X className="w-3 h-3" /> Clear all
                    </button>
                  )}
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
                  {Object.entries(CATEGORY_GROUPS).map(([key, group]) => {
                    const allSelected = group.categories.every(c => selectedCategories.includes(c));
                    const someSelected = group.categories.some(c => selectedCategories.includes(c));

                    return (
                      <div
                        key={key}
                        className={`rounded-xl border p-3 transition-all cursor-pointer ${
                          allSelected
                            ? 'border-theme-accent bg-theme-accent/5'
                            : someSelected
                            ? 'border-theme-accent/40 bg-theme-accent/5'
                            : 'border-border bg-secondary hover:bg-secondary/80'
                        }`}
                        onClick={() => toggleCategoryGroup(key)}
                      >
                        <div className="flex items-center gap-2 mb-2">
                          <span className={`text-sm font-medium ${allSelected ? 'text-theme-accent' : 'text-foreground'}`}>
                            {group.label}
                          </span>
                          {allSelected && <CheckCircle className="w-3.5 h-3.5 text-theme-accent" />}
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {group.categories.map(cat => (
                            <button
                              key={cat}
                              onClick={(e) => { e.stopPropagation(); toggleCategory(cat); }}
                              className={`text-[10px] px-2 py-0.5 rounded-md transition-all ${
                                selectedCategories.includes(cat)
                                  ? 'bg-theme-accent text-black font-medium'
                                  : 'bg-muted text-muted-foreground hover:text-foreground'
                              }`}
                            >
                              {CATEGORY_LABELS[cat]}
                            </button>
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Run button */}
            <div className="flex items-center gap-4 pt-2">
              <Button
                onClick={runAnalysis}
                disabled={loading}
                className="bg-theme-accent text-black hover:bg-theme-accent/90 font-semibold px-8 h-11 rounded-xl"
              >
                {loading ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Analyzing...
                  </>
                ) : (
                  <>
                    <Sparkles className="w-4 h-4 mr-2" />
                    Run Analysis
                  </>
                )}
              </Button>

              {result && (
                <Button
                  onClick={downloadCSV}
                  variant="outline"
                  className="h-11 rounded-xl"
                >
                  <Download className="w-4 h-4 mr-2" />
                  Export CSV
                </Button>
              )}

              <div className="ml-auto text-xs text-muted-foreground">
                {countryInfo && (
                  <span>
                    {countryInfo.flag} {countryInfo.name} — {countryInfo.totalPois.toLocaleString()} POIs
                    {selectedCategories.length > 0 ? ` — ${selectedCategories.length} categories` : ' — all 27 categories'}
                  </span>
                )}
              </div>
            </div>
          </CardContent>
        </Card>

        {/* ── Progress indicator ───────────────────────────────────── */}
        {loading && progress && (
          <Card className="border-border overflow-hidden">
            <CardContent className="pt-6 pb-4">
              {/* Step indicators */}
              <div className="flex items-center justify-between mb-6">
                {PROGRESS_STEPS.map((step, i) => {
                  const isComplete = currentStepIndex > i;
                  const isActive = currentStepIndex === i;
                  const isPending = currentStepIndex < i;
                  const StepIcon = step.icon;

                  return (
                    <div key={step.key} className="flex items-center gap-2">
                      <div className={`relative flex items-center justify-center w-9 h-9 rounded-full transition-all ${
                        isComplete ? 'bg-theme-accent/20 text-theme-accent' :
                        isActive ? 'bg-theme-accent/10 text-theme-accent ring-2 ring-theme-accent/40' :
                        'bg-secondary text-muted-foreground/50'
                      }`}>
                        {isComplete ? (
                          <CheckCircle className="w-5 h-5" />
                        ) : isActive ? (
                          <Loader2 className="w-5 h-5 animate-spin" />
                        ) : (
                          <StepIcon className="w-4 h-4" />
                        )}
                      </div>
                      <span className={`text-xs font-medium hidden lg:block ${
                        isActive ? 'text-theme-accent' :
                        isComplete ? 'text-foreground' :
                        'text-muted-foreground/50'
                      }`}>
                        {step.label}
                      </span>
                      {i < PROGRESS_STEPS.length - 1 && (
                        <div className={`hidden md:block w-8 h-px mx-2 ${
                          isComplete ? 'bg-theme-accent/40' : 'bg-border'
                        }`} />
                      )}
                    </div>
                  );
                })}
              </div>

              {/* Progress bar */}
              <div className="space-y-2">
                <div className="w-full bg-secondary rounded-full h-2 overflow-hidden">
                  <div
                    className="h-full bg-theme-accent rounded-full transition-all duration-500 ease-out"
                    style={{ width: `${progress.percent}%` }}
                  />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm text-foreground font-medium">{progress.message}</span>
                  <span className="text-sm text-muted-foreground">{progress.percent}%</span>
                </div>
                {progress.detail && (
                  <p className="text-xs text-muted-foreground">{progress.detail}</p>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        {/* ── Error ────────────────────────────────────────────────── */}
        {error && (
          <Card className="border-red-500/30 bg-red-500/5">
            <CardContent className="pt-6">
              <p className="text-red-400 text-sm">{error}</p>
            </CardContent>
          </Card>
        )}

        {/* ── Results ──────────────────────────────────────────────── */}
        {result && (
          <>
            {/* Stats cards */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <StatsCard
                label="Postal Codes"
                value={result.stats.totalPostalCodes.toLocaleString()}
                icon={<MapPinned className="w-5 h-5" />}
                sub="profiled"
              />
              <StatsCard
                label="Device-Days"
                value={result.stats.totalDeviceDays.toLocaleString()}
                icon={<Activity className="w-5 h-5" />}
                sub="analyzed"
              />
              <StatsCard
                label="Categories"
                value={String(result.stats.categoriesAnalyzed)}
                icon={<Target className="w-5 h-5" />}
                sub="with data"
              />
              <StatsCard
                label="Avg Affinity"
                value={String(result.stats.avgAffinityIndex)}
                icon={<TrendingUp className="w-5 h-5" />}
                sub="/ 100"
                highlight
              />
            </div>

            {/* Tab navigation */}
            <div className="flex items-center gap-1 bg-secondary rounded-xl p-1">
              {[
                { key: 'hotspots' as const, label: 'Hotspots', icon: Flame },
                { key: 'profiles' as const, label: 'Postal Code Profiles', icon: MapPinned },
                { key: 'categories' as const, label: 'Category Breakdown', icon: BarChart3 },
                { key: 'records' as const, label: 'Raw Records', icon: Database },
              ].map(tab => (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`flex-1 flex items-center justify-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-all ${
                    activeTab === tab.key
                      ? 'bg-background text-foreground shadow-sm'
                      : 'text-muted-foreground hover:text-foreground'
                  }`}
                >
                  <tab.icon className="w-4 h-4" />
                  <span className="hidden md:inline">{tab.label}</span>
                </button>
              ))}
            </div>

            {/* Search bar */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input
                placeholder="Search by postal code, city, or category..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10 bg-secondary border-border rounded-xl"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery('')}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  <X className="w-4 h-4" />
                </button>
              )}
            </div>

            {/* ── Tab: Hotspots ──────────────────────────────────── */}
            {activeTab === 'hotspots' && (
              <Card className="border-border">
                <CardHeader>
                  <CardTitle className="text-lg flex items-center gap-2">
                    <Flame className="w-5 h-5 text-orange-400" />
                    Top Affinity Hotspots
                  </CardTitle>
                  <CardDescription>
                    Postal codes with strongest category affinity (index {'\u2265'} 70)
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {result.stats.topHotspots.length === 0 ? (
                    <p className="text-sm text-muted-foreground py-8 text-center">No hotspots found with affinity {'\u2265'} 70</p>
                  ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                      {result.stats.topHotspots.map((h, i) => (
                        <div
                          key={`${h.zipcode}-${h.category}`}
                          className={`rounded-xl border p-4 transition-all hover:scale-[1.01] ${getAffinityBg(h.affinityIndex)}`}
                        >
                          <div className="flex items-start justify-between mb-3">
                            <div>
                              <div className="text-sm font-semibold">{h.zipcode}</div>
                              <div className="text-xs text-muted-foreground">{h.city}</div>
                            </div>
                            <div className={`text-2xl font-bold tabular-nums ${getAffinityColor(h.affinityIndex)}`}>
                              {h.affinityIndex}
                            </div>
                          </div>
                          <div className="flex items-center justify-between">
                            <Badge variant="outline" className="text-[10px]">
                              {h.categoryLabel}
                            </Badge>
                            <span className="text-[10px] text-muted-foreground">
                              {h.visits.toLocaleString()} visits — {h.uniqueDevices.toLocaleString()} devices
                            </span>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            {/* ── Tab: Profiles ─────────────────────────────────── */}
            {activeTab === 'profiles' && (
              <Card className="border-border">
                <CardHeader>
                  <CardTitle className="text-lg flex items-center gap-2">
                    <MapPinned className="w-5 h-5 text-blue-400" />
                    Postal Code Profiles
                    <span className="text-sm font-normal text-muted-foreground ml-2">
                      ({filteredProfiles.length.toLocaleString()})
                    </span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-28">Postal Code</TableHead>
                          <TableHead>City</TableHead>
                          <TableHead className="text-right">Visits</TableHead>
                          <TableHead className="text-right">Devices</TableHead>
                          <TableHead>Top Category</TableHead>
                          <TableHead className="text-right">Top Affinity</TableHead>
                          <TableHead>Dominant Group</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredProfiles.slice(0, 100).map(p => (
                          <TableRow key={p.zipcode} className="hover:bg-secondary/50">
                            <TableCell className="font-mono text-sm font-medium">{p.zipcode}</TableCell>
                            <TableCell className="text-sm">{p.city}</TableCell>
                            <TableCell className="text-right tabular-nums text-sm">{p.totalVisits.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums text-sm">{p.uniqueDevices.toLocaleString()}</TableCell>
                            <TableCell>
                              <Badge variant="outline" className="text-[10px]">
                                {CATEGORY_LABELS[p.topCategory]}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right">
                              <span className={`text-sm font-bold tabular-nums ${getAffinityColor(p.topAffinity)}`}>
                                {p.topAffinity}
                              </span>
                            </TableCell>
                            <TableCell className="text-xs text-muted-foreground capitalize">
                              {CATEGORY_GROUPS[p.dominantGroup]?.label || p.dominantGroup}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                    {filteredProfiles.length > 100 && (
                      <p className="text-xs text-muted-foreground text-center py-3">
                        Showing 100 of {filteredProfiles.length.toLocaleString()} profiles — use search to filter, or export CSV for full data
                      </p>
                    )}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* ── Tab: Category breakdown ───────────────────────── */}
            {activeTab === 'categories' && (
              <Card className="border-border">
                <CardHeader>
                  <CardTitle className="text-lg flex items-center gap-2">
                    <BarChart3 className="w-5 h-5 text-purple-400" />
                    Category Breakdown
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {result.stats.categoryBreakdown.map(cat => (
                      <div
                        key={cat.category}
                        className="rounded-xl border border-border bg-secondary/50 overflow-hidden"
                      >
                        <button
                          className="w-full flex items-center gap-4 p-4 text-left hover:bg-secondary/80 transition-colors"
                          onClick={() => setExpandedCategory(expandedCategory === cat.category ? null : cat.category)}
                        >
                          {/* Category bar */}
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center justify-between mb-1.5">
                              <span className="text-sm font-medium">{cat.label}</span>
                              <span className="text-xs text-muted-foreground">{cat.percentOfTotal}%</span>
                            </div>
                            <div className="w-full bg-muted rounded-full h-1.5">
                              <div
                                className="h-full bg-theme-accent rounded-full transition-all"
                                style={{ width: `${Math.min(cat.percentOfTotal * 2, 100)}%` }}
                              />
                            </div>
                          </div>

                          {/* Stats */}
                          <div className="flex items-center gap-4 shrink-0">
                            <div className="text-center">
                              <div className="text-sm font-bold tabular-nums">{cat.visits.toLocaleString()}</div>
                              <div className="text-[10px] text-muted-foreground">visits</div>
                            </div>
                            <div className="text-center">
                              <div className="text-sm font-bold tabular-nums">{cat.postalCodesWithVisits}</div>
                              <div className="text-[10px] text-muted-foreground">zipcodes</div>
                            </div>
                            <div className="text-center">
                              <div className={`text-sm font-bold tabular-nums ${getAffinityColor(cat.avgAffinity)}`}>
                                {cat.avgAffinity}
                              </div>
                              <div className="text-[10px] text-muted-foreground">avg</div>
                            </div>
                            <div className="text-center">
                              <div className={`text-sm font-bold tabular-nums ${getAffinityColor(cat.maxAffinity)}`}>
                                {cat.maxAffinity}
                              </div>
                              <div className="text-[10px] text-muted-foreground">max</div>
                            </div>
                            {expandedCategory === cat.category ?
                              <ChevronUp className="w-4 h-4 text-muted-foreground" /> :
                              <ChevronDown className="w-4 h-4 text-muted-foreground" />
                            }
                          </div>
                        </button>

                        {expandedCategory === cat.category && (
                          <div className="border-t border-border px-4 py-3 bg-background/50">
                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-xs">
                              <div>
                                <span className="text-muted-foreground">Group:</span>{' '}
                                <span className="capitalize">{CATEGORY_GROUPS[cat.group]?.label || cat.group}</span>
                              </div>
                              <div>
                                <span className="text-muted-foreground">Top zipcode:</span>{' '}
                                <span className="font-mono">{cat.maxAffinityZipcode || '—'}</span>
                              </div>
                              <div>
                                <span className="text-muted-foreground">Top city:</span>{' '}
                                <span>{cat.maxAffinityCity || '—'}</span>
                              </div>
                              <div>
                                <span className="text-muted-foreground">National share:</span>{' '}
                                <span>{cat.percentOfTotal}%</span>
                              </div>
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* ── Tab: Raw records ──────────────────────────────── */}
            {activeTab === 'records' && (
              <Card className="border-border">
                <CardHeader>
                  <CardTitle className="text-lg flex items-center gap-2">
                    <Database className="w-5 h-5 text-muted-foreground" />
                    Raw Affinity Records
                    <span className="text-sm font-normal text-muted-foreground ml-2">
                      ({filteredRecords.length.toLocaleString()})
                    </span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Postal Code</TableHead>
                          <TableHead>City</TableHead>
                          <TableHead>Category</TableHead>
                          <TableHead className="text-right">Visits</TableHead>
                          <TableHead className="text-right">Devices</TableHead>
                          <TableHead className="text-right">Freq</TableHead>
                          <TableHead className="text-right">Conc.</TableHead>
                          <TableHead className="text-right">Freq.</TableHead>
                          <TableHead className="text-right">Temp.</TableHead>
                          <TableHead className="text-right">Affinity</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {filteredRecords.slice(0, 200).map((r, i) => (
                          <TableRow key={`${r.zipcode}-${r.category}-${i}`} className="hover:bg-secondary/50">
                            <TableCell className="font-mono text-xs">{r.zipcode}</TableCell>
                            <TableCell className="text-xs">{r.city}</TableCell>
                            <TableCell className="text-xs">{CATEGORY_LABELS[r.category]}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{r.visits.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{r.uniqueDevices.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{r.frequency.toFixed(1)}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{r.concentrationScore}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{r.frequencyScore}</TableCell>
                            <TableCell className="text-right tabular-nums text-xs">{r.temporalScore}</TableCell>
                            <TableCell className="text-right">
                              <span className={`text-xs font-bold tabular-nums ${getAffinityColor(r.affinityIndex)}`}>
                                {r.affinityIndex}
                              </span>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                    {filteredRecords.length > 200 && (
                      <p className="text-xs text-muted-foreground text-center py-3">
                        Showing 200 of {filteredRecords.length.toLocaleString()} records — export CSV for full data
                      </p>
                    )}
                  </div>
                </CardContent>
              </Card>
            )}
          </>
        )}
      </div>
    </MainLayout>
  );
}

// ── Stats card component ───────────────────────────────────────────────
function StatsCard({ label, value, icon, sub, highlight }: {
  label: string; value: string; icon: React.ReactNode; sub: string; highlight?: boolean;
}) {
  return (
    <Card className={`border-border ${highlight ? 'border-theme-accent/30 bg-theme-accent/5' : ''}`}>
      <CardContent className="pt-5 pb-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-xs text-muted-foreground uppercase tracking-wider">{label}</span>
          <div className={highlight ? 'text-theme-accent' : 'text-muted-foreground'}>{icon}</div>
        </div>
        <div className="text-2xl font-bold tabular-nums">{value}</div>
        <span className="text-xs text-muted-foreground">{sub}</span>
      </CardContent>
    </Card>
  );
}
