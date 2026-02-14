'use client';

import { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';
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
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from '@/components/ui/tabs';
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
  X,
  Activity,
  Target,
  Plus,
  Trash2,
  Clock,
  Timer,
  Users,
  ArrowRight,
  ArrowDown,
  Hash,
  Settings2,
  Play,
  Beaker,
  FileDown,
  ListFilter,
  RotateCcw,
} from 'lucide-react';
import type {
  LabConfig,
  LabAnalysisResult,
  AffinityRecord,
  ZipcodeProfile,
  PoiCategory,
  CategoryStat,
  RecipeStep,
  Recipe,
  SegmentDevice,
} from '@/lib/laboratory-types';
import {
  POI_CATEGORIES,
  CATEGORY_LABELS,
  CATEGORY_GROUPS,
  MIN_VISITS_DEFAULT,
  SPATIAL_JOIN_RADIUS_DEFAULT,
} from '@/lib/laboratory-types';

// ── Types ─────────────────────────────────────────────────────────────
interface DatasetInfo {
  id: string;
  name: string;
  jobId: string | null;
  objectCount: number;
  totalBytes: number;
  dateRange: { from: string; to: string } | null;
  syncedAt: string | null;
}

type WizardPhase = 'setup' | 'recipe' | 'running' | 'results';

// ── Helpers ───────────────────────────────────────────────────────────
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

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(0)} KB`;
  if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`;
  return `${(bytes / 1073741824).toFixed(2)} GB`;
}

function newStepId(): string {
  return `step_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`;
}

const COUNTRY_FLAGS: Record<string, string> = {
  FR: '🇫🇷', DE: '🇩🇪', ES: '🇪🇸', IT: '🇮🇹', GB: '🇬🇧', US: '🇺🇸',
  PT: '🇵🇹', NL: '🇳🇱', BE: '🇧🇪', AT: '🇦🇹', CH: '🇨🇭', PL: '🇵🇱',
  SE: '🇸🇪', NO: '🇳🇴', DK: '🇩🇰', FI: '🇫🇮', IE: '🇮🇪', CZ: '🇨🇿',
  GR: '🇬🇷', RO: '🇷🇴', HU: '🇭🇺', BG: '🇧🇬', HR: '🇭🇷', MX: '🇲🇽',
};

// ── Progress step config ──────────────────────────────────────────────
const PROGRESS_STEPS = [
  { key: 'initializing', label: 'Init', icon: Database },
  { key: 'loading_pois', label: 'POIs', icon: MapPinned },
  { key: 'spatial_join', label: 'Spatial Join', icon: Target },
  { key: 'computing_dwell', label: 'Dwell', icon: Timer },
  { key: 'building_segments', label: 'Segment', icon: Users },
  { key: 'geocoding', label: 'Geocode', icon: Globe },
  { key: 'computing_affinity', label: 'Affinity', icon: Zap },
];

// ── Main page ─────────────────────────────────────────────────────────
export default function LaboratoryPage() {
  // ── Wizard state ────────────────────────────────────────────────────
  const [phase, setPhase] = useState<WizardPhase>('setup');
  const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
  const [loadingDatasets, setLoadingDatasets] = useState(true);

  // Config
  const [selectedDatasetId, setSelectedDatasetId] = useState<string>('');
  const [selectedCountry, setSelectedCountry] = useState<string>('');
  const [dateFrom, setDateFrom] = useState<string>('');
  const [dateTo, setDateTo] = useState<string>('');
  const [minVisits, setMinVisits] = useState<number>(MIN_VISITS_DEFAULT);
  const [spatialRadius, setSpatialRadius] = useState<number>(SPATIAL_JOIN_RADIUS_DEFAULT);

  // Recipe
  const [recipeSteps, setRecipeSteps] = useState<RecipeStep[]>([{
    id: newStepId(),
    categories: [],
  }]);
  const [recipeLogic, setRecipeLogic] = useState<'AND' | 'OR'>('OR');
  const [recipeOrdered, setRecipeOrdered] = useState(false);
  const [recipeName, setRecipeName] = useState('');

  // Running state
  const [progress, setProgress] = useState<{
    step: string; percent: number; message: string; detail?: string;
  } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  // Results
  const [result, setResult] = useState<LabAnalysisResult | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedCategory, setExpandedCategory] = useState<string | null>(null);

  // ── Fetch datasets ──────────────────────────────────────────────────
  useEffect(() => {
    fetch('/api/datasets', { credentials: 'include' })
      .then(r => r.json())
      .then(data => {
        const ds = (data.datasets || []).filter((d: any) =>
          d.jobId && d.objectCount > 0 && d.totalBytes > 0
        );
        setDatasets(ds);
        setLoadingDatasets(false);
      })
      .catch(() => setLoadingDatasets(false));
  }, []);

  // ── Derived ─────────────────────────────────────────────────────────
  const selectedDataset = datasets.find(d => d.id === selectedDatasetId);

  const totalRecipeCategories = useMemo(() => {
    const cats = new Set<PoiCategory>();
    for (const step of recipeSteps) {
      for (const c of step.categories) cats.add(c);
    }
    return cats.size;
  }, [recipeSteps]);

  const canRun = selectedDatasetId && selectedCountry && recipeSteps.some(s => s.categories.length > 0);

  // ── Recipe step management ──────────────────────────────────────────
  const addStep = () => {
    setRecipeSteps(prev => [...prev, { id: newStepId(), categories: [] }]);
  };

  const removeStep = (stepId: string) => {
    setRecipeSteps(prev => prev.filter(s => s.id !== stepId));
  };

  const updateStep = (stepId: string, update: Partial<RecipeStep>) => {
    setRecipeSteps(prev => prev.map(s =>
      s.id === stepId ? { ...s, ...update } : s
    ));
  };

  const toggleStepCategory = (stepId: string, cat: PoiCategory) => {
    setRecipeSteps(prev => prev.map(s => {
      if (s.id !== stepId) return s;
      const cats = s.categories.includes(cat)
        ? s.categories.filter(c => c !== cat)
        : [...s.categories, cat];
      return { ...s, categories: cats };
    }));
  };

  const toggleStepCategoryGroup = (stepId: string, groupKey: string) => {
    const group = CATEGORY_GROUPS[groupKey];
    if (!group) return;
    setRecipeSteps(prev => prev.map(s => {
      if (s.id !== stepId) return s;
      const allSelected = group.categories.every(c => s.categories.includes(c));
      const cats = allSelected
        ? s.categories.filter(c => !group.categories.includes(c as PoiCategory))
        : [...new Set([...s.categories, ...group.categories])];
      return { ...s, categories: cats as PoiCategory[] };
    }));
  };

  // ── Run analysis ────────────────────────────────────────────────────
  const runAnalysis = async () => {
    if (!canRun || !selectedDataset) return;

    setPhase('running');
    setError(null);
    setResult(null);
    setProgress({ step: 'initializing', percent: 0, message: 'Starting analysis...' });

    const recipe: Recipe = {
      id: `recipe_${Date.now()}`,
      name: recipeName || 'Experiment',
      steps: recipeSteps.filter(s => s.categories.length > 0),
      logic: recipeLogic,
      ordered: recipeOrdered,
    };

    const config: LabConfig = {
      datasetId: selectedDatasetId,
      datasetName: selectedDataset.name,
      jobId: selectedDataset.jobId || '',
      country: selectedCountry,
      dateFrom: dateFrom || undefined,
      dateTo: dateTo || undefined,
      recipe,
      minVisitsPerZipcode: minVisits,
      spatialJoinRadiusMeters: spatialRadius,
    };

    const abort = new AbortController();
    abortRef.current = abort;

    try {
      const response = await fetch('/api/laboratory/analyze/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config),
        credentials: 'include',
        signal: abort.signal,
      });

      if (!response.ok || !response.body) {
        throw new Error(`Server error: ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        let eventType = '';
        for (const line of lines) {
          if (line.startsWith('event: ')) {
            eventType = line.slice(7).trim();
          } else if (line.startsWith('data: ') && eventType) {
            try {
              const data = JSON.parse(line.slice(6));
              if (eventType === 'progress') {
                setProgress(data);
              } else if (eventType === 'result') {
                setResult(data);
                setPhase('results');
                setProgress(null);
              }
            } catch { /* ignore parse errors */ }
            eventType = '';
          }
        }
      }

      // If stream ended without result event
      if (!result) {
        setPhase('results');
        setProgress(null);
      }
    } catch (err: any) {
      if (err.name === 'AbortError') {
        setPhase('recipe');
        setProgress(null);
        return;
      }
      setError(err.message || 'Analysis failed');
      setPhase('recipe');
      setProgress(null);
    }
  };

  const cancelAnalysis = () => {
    abortRef.current?.abort();
    setPhase('recipe');
    setProgress(null);
  };

  // ── CSV downloads ───────────────────────────────────────────────────
  const downloadAffinityCSV = () => {
    if (!result) return;
    const headers = [
      'postal_code', 'city', 'province', 'region', 'category',
      'visits', 'unique_devices', 'avg_dwell_minutes', 'frequency',
      'concentration_score', 'frequency_score', 'dwell_score',
      'affinity_index',
    ];
    const rows = result.records.map(r => [
      r.zipcode, r.city, r.province, r.region, r.category,
      r.visits, r.uniqueDevices, r.avgDwellMinutes, r.frequency,
      r.concentrationScore, r.frequencyScore, r.dwellScore,
      r.affinityIndex,
    ].map(v => typeof v === 'string' && v.includes(',') ? `"${v}"` : v).join(','));
    downloadBlob([headers.join(','), ...rows].join('\n'), `affinity_${selectedCountry.toLowerCase()}_${new Date().toISOString().slice(0, 10)}.csv`);
  };

  const downloadSegmentCSV = () => {
    if (!result) return;
    const headers = ['ad_id', 'matched_steps', 'total_visits', 'avg_dwell_minutes', 'categories'];
    const rows = result.segment.devices.map(d => [
      d.adId, d.matchedSteps, d.totalVisits, d.avgDwellMinutes,
      d.categories.join(';'),
    ].join(','));
    downloadBlob([headers.join(','), ...rows].join('\n'), `segment_${selectedCountry.toLowerCase()}_${new Date().toISOString().slice(0, 10)}.csv`);
  };

  function downloadBlob(content: string, filename: string) {
    const blob = new Blob([content], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ── Filtered data ───────────────────────────────────────────────────
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

  const currentStepIndex = progress
    ? PROGRESS_STEPS.findIndex(s => s.key === progress.step)
    : -1;

  // ── Render ──────────────────────────────────────────────────────────
  return (
    <MainLayout>
      <div className="space-y-6">
        {/* ── Hero ──────────────────────────────────────────────────── */}
        <div className="relative overflow-hidden rounded-2xl bg-gradient-to-br from-background via-secondary to-background border border-border p-8">
          <div className="absolute top-0 right-0 w-96 h-96 bg-theme-accent/5 rounded-full blur-3xl -translate-y-1/2 translate-x-1/3" />
          <div className="relative z-10">
            <div className="flex items-center gap-3 mb-3">
              <div className="p-3 rounded-xl bg-theme-accent/10 border border-theme-accent/20">
                <FlaskConical className="w-7 h-7 text-theme-accent" />
              </div>
              <div>
                <h1 className="text-3xl font-bold tracking-tight">Affinity Laboratory</h1>
                <p className="text-muted-foreground text-sm mt-1">
                  Build recipes to segment devices and compute spatial affinity indices
                </p>
              </div>
            </div>
            <div className="flex flex-wrap gap-2 mt-4">
              <Badge variant="outline" className="text-xs px-3 py-1 border-theme-accent/30 text-theme-accent">
                <Activity className="w-3 h-3 mr-1" />
                Concentration + Frequency + Dwell
              </Badge>
              <Badge variant="outline" className="text-xs px-3 py-1 border-muted-foreground/30">
                <Target className="w-3 h-3 mr-1" />
                27 POI categories
              </Badge>
              <Badge variant="outline" className="text-xs px-3 py-1 border-muted-foreground/30">
                <Beaker className="w-3 h-3 mr-1" />
                Recipe builder
              </Badge>
            </div>
          </div>
        </div>

        {/* ── Phase: Setup ──────────────────────────────────────────── */}
        {(phase === 'setup' || phase === 'recipe') && (
          <>
            {/* Dataset + Country selector */}
            <Card className="border-border">
              <CardHeader className="pb-3">
                <CardTitle className="text-lg flex items-center gap-2">
                  <Database className="w-5 h-5 text-muted-foreground" />
                  Dataset & Country
                </CardTitle>
                <CardDescription>Select a synced mobility dataset and target country</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {/* Dataset selector */}
                  <div>
                    <Label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Mobility Dataset</Label>
                    {loadingDatasets ? (
                      <div className="flex items-center gap-2 text-sm text-muted-foreground py-3">
                        <Loader2 className="w-4 h-4 animate-spin" />
                        Loading datasets...
                      </div>
                    ) : datasets.length === 0 ? (
                      <p className="text-sm text-muted-foreground py-3">No synced datasets found. Create a job first.</p>
                    ) : (
                      <Select value={selectedDatasetId} onValueChange={(v) => { setSelectedDatasetId(v); setPhase('recipe'); }}>
                        <SelectTrigger className="bg-secondary border-border">
                          <SelectValue placeholder="Choose dataset..." />
                        </SelectTrigger>
                        <SelectContent>
                          {datasets.map(d => (
                            <SelectItem key={d.id} value={d.id}>
                              <div className="flex items-center gap-2">
                                <span className="font-medium">{d.name}</span>
                                <span className="text-xs text-muted-foreground">
                                  {formatBytes(d.totalBytes)}
                                </span>
                                {d.dateRange && (
                                  <span className="text-xs text-muted-foreground">
                                    {d.dateRange.from} → {d.dateRange.to}
                                  </span>
                                )}
                              </div>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    )}
                    {selectedDataset && (
                      <div className="flex gap-3 mt-2 text-xs text-muted-foreground">
                        <span>{selectedDataset.objectCount} files</span>
                        <span>{formatBytes(selectedDataset.totalBytes)}</span>
                        {selectedDataset.dateRange && (
                          <span>{selectedDataset.dateRange.from} → {selectedDataset.dateRange.to}</span>
                        )}
                      </div>
                    )}
                  </div>

                  {/* Country */}
                  <div>
                    <Label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Country (POI catalog)</Label>
                    <Select value={selectedCountry} onValueChange={(v) => { setSelectedCountry(v); setPhase('recipe'); }}>
                      <SelectTrigger className="bg-secondary border-border">
                        <SelectValue placeholder="Choose country..." />
                      </SelectTrigger>
                      <SelectContent>
                        {Object.entries(COUNTRY_FLAGS).map(([code, flag]) => (
                          <SelectItem key={code} value={code}>
                            <span className="mr-2">{flag}</span>
                            {code}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>

                {/* Date range + advanced */}
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div>
                    <Label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Date From</Label>
                    <Input
                      type="date"
                      value={dateFrom}
                      onChange={(e) => setDateFrom(e.target.value)}
                      className="bg-secondary border-border"
                    />
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Date To</Label>
                    <Input
                      type="date"
                      value={dateTo}
                      onChange={(e) => setDateTo(e.target.value)}
                      className="bg-secondary border-border"
                    />
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Min Visits / ZIP</Label>
                    <Input
                      type="number"
                      value={minVisits}
                      onChange={(e) => setMinVisits(parseInt(e.target.value) || MIN_VISITS_DEFAULT)}
                      min={1}
                      className="bg-secondary border-border"
                    />
                  </div>
                  <div>
                    <Label className="text-xs text-muted-foreground uppercase tracking-wider mb-2 block">Spatial Radius (m)</Label>
                    <Input
                      type="number"
                      value={spatialRadius}
                      onChange={(e) => setSpatialRadius(parseInt(e.target.value) || SPATIAL_JOIN_RADIUS_DEFAULT)}
                      min={50}
                      max={2000}
                      step={50}
                      className="bg-secondary border-border"
                    />
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* ── Recipe Builder ────────────────────────────────────── */}
            {phase === 'recipe' && (
              <Card className="border-border">
                <CardHeader className="pb-3">
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle className="text-lg flex items-center gap-2">
                        <Beaker className="w-5 h-5 text-theme-accent" />
                        Recipe Builder
                      </CardTitle>
                      <CardDescription>
                        Define steps combining categories, time windows, and dwell thresholds
                      </CardDescription>
                    </div>
                    <div className="flex items-center gap-3">
                      <Input
                        placeholder="Recipe name..."
                        value={recipeName}
                        onChange={(e) => setRecipeName(e.target.value)}
                        className="w-48 bg-secondary border-border text-sm"
                      />
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Logic controls */}
                  <div className="flex items-center gap-6 p-3 rounded-xl bg-secondary/50 border border-border">
                    <div className="flex items-center gap-3">
                      <Label className="text-xs text-muted-foreground uppercase tracking-wider">Step Logic</Label>
                      <div className="flex bg-muted rounded-lg p-0.5">
                        <button
                          onClick={() => setRecipeLogic('OR')}
                          className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                            recipeLogic === 'OR' ? 'bg-theme-accent text-black' : 'text-muted-foreground hover:text-foreground'
                          }`}
                        >
                          OR (any)
                        </button>
                        <button
                          onClick={() => setRecipeLogic('AND')}
                          className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                            recipeLogic === 'AND' ? 'bg-theme-accent text-black' : 'text-muted-foreground hover:text-foreground'
                          }`}
                        >
                          AND (all)
                        </button>
                      </div>
                    </div>

                    {recipeLogic === 'AND' && recipeSteps.length > 1 && (
                      <div className="flex items-center gap-2">
                        <Switch
                          checked={recipeOrdered}
                          onCheckedChange={setRecipeOrdered}
                          id="ordered"
                        />
                        <Label htmlFor="ordered" className="text-xs text-muted-foreground cursor-pointer">
                          Enforce visit order
                        </Label>
                      </div>
                    )}

                    <div className="ml-auto text-xs text-muted-foreground">
                      {recipeSteps.length} step{recipeSteps.length > 1 ? 's' : ''} — {totalRecipeCategories} categories
                    </div>
                  </div>

                  {/* Steps */}
                  <div className="space-y-3">
                    {recipeSteps.map((step, idx) => (
                      <div key={step.id}>
                        {/* Step connector */}
                        {idx > 0 && (
                          <div className="flex items-center justify-center py-1">
                            <div className={`px-3 py-0.5 rounded-full text-[10px] font-bold uppercase tracking-wider ${
                              recipeLogic === 'AND' ? 'bg-blue-500/10 text-blue-400 border border-blue-500/20' : 'bg-orange-500/10 text-orange-400 border border-orange-500/20'
                            }`}>
                              {recipeLogic}
                            </div>
                            {recipeLogic === 'AND' && recipeOrdered && (
                              <ArrowDown className="w-3 h-3 text-muted-foreground ml-1" />
                            )}
                          </div>
                        )}

                        <RecipeStepCard
                          step={step}
                          index={idx}
                          onUpdate={(update) => updateStep(step.id, update)}
                          onToggleCategory={(cat) => toggleStepCategory(step.id, cat)}
                          onToggleCategoryGroup={(g) => toggleStepCategoryGroup(step.id, g)}
                          onRemove={recipeSteps.length > 1 ? () => removeStep(step.id) : undefined}
                        />
                      </div>
                    ))}
                  </div>

                  {/* Add step + Run */}
                  <div className="flex items-center gap-3 pt-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={addStep}
                      className="rounded-xl"
                    >
                      <Plus className="w-4 h-4 mr-1" />
                      Add Step
                    </Button>

                    <div className="flex-1" />

                    {error && (
                      <span className="text-xs text-red-400 mr-2">{error}</span>
                    )}

                    <Button
                      onClick={runAnalysis}
                      disabled={!canRun}
                      className="bg-theme-accent text-black hover:bg-theme-accent/90 font-semibold px-8 h-11 rounded-xl"
                    >
                      <Play className="w-4 h-4 mr-2" />
                      Run Experiment
                    </Button>
                  </div>
                </CardContent>
              </Card>
            )}
          </>
        )}

        {/* ── Phase: Running ────────────────────────────────────────── */}
        {phase === 'running' && progress && (
          <Card className="border-border overflow-hidden">
            <CardContent className="pt-8 pb-6">
              <div className="flex items-center justify-between mb-8">
                {PROGRESS_STEPS.map((step, i) => {
                  const isComplete = currentStepIndex > i;
                  const isActive = currentStepIndex === i;
                  const StepIcon = step.icon;
                  return (
                    <div key={step.key} className="flex items-center gap-2">
                      <div className={`relative flex items-center justify-center w-10 h-10 rounded-full transition-all ${
                        isComplete ? 'bg-theme-accent/20 text-theme-accent' :
                        isActive ? 'bg-theme-accent/10 text-theme-accent ring-2 ring-theme-accent/40' :
                        'bg-secondary text-muted-foreground/40'
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
                        isActive ? 'text-theme-accent' : isComplete ? 'text-foreground' : 'text-muted-foreground/40'
                      }`}>{step.label}</span>
                      {i < PROGRESS_STEPS.length - 1 && (
                        <div className={`hidden md:block w-6 h-px mx-1 ${isComplete ? 'bg-theme-accent/40' : 'bg-border'}`} />
                      )}
                    </div>
                  );
                })}
              </div>

              <div className="space-y-3">
                <div className="w-full bg-secondary rounded-full h-2.5 overflow-hidden">
                  <div
                    className="h-full bg-theme-accent rounded-full transition-all duration-700 ease-out"
                    style={{ width: `${progress.percent}%` }}
                  />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">{progress.message}</span>
                  <span className="text-sm text-muted-foreground tabular-nums">{progress.percent}%</span>
                </div>
                {progress.detail && (
                  <p className="text-xs text-muted-foreground">{progress.detail}</p>
                )}
              </div>

              <div className="flex justify-center mt-6">
                <Button variant="outline" size="sm" onClick={cancelAnalysis} className="rounded-xl">
                  <X className="w-4 h-4 mr-1" />
                  Cancel
                </Button>
              </div>
            </CardContent>
          </Card>
        )}

        {/* ── Phase: Results ────────────────────────────────────────── */}
        {phase === 'results' && result && (
          <>
            {/* Summary bar */}
            <div className="flex items-center gap-3 p-4 rounded-xl bg-secondary/50 border border-border">
              <CheckCircle className="w-5 h-5 text-theme-accent shrink-0" />
              <div className="flex-1 min-w-0">
                <span className="text-sm font-medium">
                  {result.config.recipe?.name || 'Experiment'} — {result.stats.segmentSize.toLocaleString()} devices in segment
                </span>
                <span className="text-xs text-muted-foreground ml-3">
                  {result.stats.totalPostalCodes} postal codes — {result.stats.categoriesAnalyzed} categories — avg affinity {result.stats.avgAffinityIndex}
                </span>
              </div>
              <div className="flex items-center gap-2 shrink-0">
                <Button variant="outline" size="sm" onClick={downloadSegmentCSV} className="rounded-xl">
                  <Users className="w-3.5 h-3.5 mr-1" />
                  Segment CSV
                </Button>
                <Button variant="outline" size="sm" onClick={downloadAffinityCSV} className="rounded-xl">
                  <FileDown className="w-3.5 h-3.5 mr-1" />
                  Affinity CSV
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => { setPhase('recipe'); setResult(null); }}
                  className="rounded-xl text-muted-foreground"
                >
                  <RotateCcw className="w-3.5 h-3.5 mr-1" />
                  New
                </Button>
              </div>
            </div>

            {/* Stats cards */}
            <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
              <StatsCard
                label="Segment"
                value={result.stats.segmentSize.toLocaleString()}
                icon={<Users className="w-4 h-4" />}
                sub={`${result.stats.segmentPercent}% of ${result.stats.totalDevicesInDataset.toLocaleString()}`}
                highlight
              />
              <StatsCard
                label="Postal Codes"
                value={result.stats.totalPostalCodes.toLocaleString()}
                icon={<MapPinned className="w-4 h-4" />}
                sub="profiled"
              />
              <StatsCard
                label="Avg Dwell"
                value={`${result.stats.avgDwellMinutes} min`}
                icon={<Timer className="w-4 h-4" />}
                sub="per visit"
              />
              <StatsCard
                label="Categories"
                value={String(result.stats.categoriesAnalyzed)}
                icon={<Target className="w-4 h-4" />}
                sub="with data"
              />
              <StatsCard
                label="Avg Affinity"
                value={String(result.stats.avgAffinityIndex)}
                icon={<TrendingUp className="w-4 h-4" />}
                sub="/ 100"
                highlight
              />
            </div>

            {/* Search */}
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

            {/* Results tabs */}
            <Tabs defaultValue="hotspots">
              <TabsList className="bg-secondary rounded-xl p-1 w-full">
                <TabsTrigger value="hotspots" className="flex-1 rounded-lg text-xs data-[state=active]:bg-background">
                  <Flame className="w-3.5 h-3.5 mr-1.5" />
                  Hotspots
                </TabsTrigger>
                <TabsTrigger value="segment" className="flex-1 rounded-lg text-xs data-[state=active]:bg-background">
                  <Users className="w-3.5 h-3.5 mr-1.5" />
                  Segment
                </TabsTrigger>
                <TabsTrigger value="profiles" className="flex-1 rounded-lg text-xs data-[state=active]:bg-background">
                  <MapPinned className="w-3.5 h-3.5 mr-1.5" />
                  Profiles
                </TabsTrigger>
                <TabsTrigger value="categories" className="flex-1 rounded-lg text-xs data-[state=active]:bg-background">
                  <BarChart3 className="w-3.5 h-3.5 mr-1.5" />
                  Categories
                </TabsTrigger>
                <TabsTrigger value="records" className="flex-1 rounded-lg text-xs data-[state=active]:bg-background">
                  <Database className="w-3.5 h-3.5 mr-1.5" />
                  Records
                </TabsTrigger>
              </TabsList>

              {/* Hotspots */}
              <TabsContent value="hotspots" className="mt-4">
                <Card className="border-border">
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base flex items-center gap-2">
                      <Flame className="w-4 h-4 text-orange-400" />
                      Top Affinity Hotspots
                    </CardTitle>
                    <CardDescription>Postal codes with strongest affinity (index &ge; 60)</CardDescription>
                  </CardHeader>
                  <CardContent>
                    {result.stats.topHotspots.length === 0 ? (
                      <p className="text-sm text-muted-foreground py-8 text-center">No hotspots found with high affinity</p>
                    ) : (
                      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                        {result.stats.topHotspots.map((h) => (
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
                              <div className="text-[10px] text-muted-foreground space-x-2">
                                <span>{h.visits} visits</span>
                                <span>{h.uniqueDevices} devices</span>
                                <span>{h.avgDwellMinutes} min dwell</span>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Segment */}
              <TabsContent value="segment" className="mt-4">
                <Card className="border-border">
                  <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                      <div>
                        <CardTitle className="text-base flex items-center gap-2">
                          <Users className="w-4 h-4 text-blue-400" />
                          Device Segment
                          <span className="text-sm font-normal text-muted-foreground ml-2">
                            ({result.segment.totalDevices.toLocaleString()} total)
                          </span>
                        </CardTitle>
                        <CardDescription>Devices matching the recipe criteria</CardDescription>
                      </div>
                      <Button variant="outline" size="sm" onClick={downloadSegmentCSV} className="rounded-xl">
                        <Download className="w-3.5 h-3.5 mr-1" />
                        Export All
                      </Button>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Ad ID</TableHead>
                            <TableHead className="text-right">Steps</TableHead>
                            <TableHead className="text-right">Visits</TableHead>
                            <TableHead className="text-right">Avg Dwell</TableHead>
                            <TableHead>Categories</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {result.segment.devices.slice(0, 100).map(d => (
                            <TableRow key={d.adId} className="hover:bg-secondary/50">
                              <TableCell className="font-mono text-xs">{d.adId}</TableCell>
                              <TableCell className="text-right tabular-nums text-sm">{d.matchedSteps}</TableCell>
                              <TableCell className="text-right tabular-nums text-sm">{d.totalVisits}</TableCell>
                              <TableCell className="text-right tabular-nums text-sm">{d.avgDwellMinutes} min</TableCell>
                              <TableCell>
                                <div className="flex flex-wrap gap-1">
                                  {d.categories.slice(0, 5).map(c => (
                                    <Badge key={c} variant="outline" className="text-[9px]">
                                      {CATEGORY_LABELS[c]}
                                    </Badge>
                                  ))}
                                  {d.categories.length > 5 && (
                                    <Badge variant="outline" className="text-[9px]">+{d.categories.length - 5}</Badge>
                                  )}
                                </div>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                      {result.segment.devices.length > 100 && (
                        <p className="text-xs text-muted-foreground text-center py-3">
                          Showing 100 of {result.segment.devices.length} — export CSV for full segment
                        </p>
                      )}
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Profiles */}
              <TabsContent value="profiles" className="mt-4">
                <Card className="border-border">
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base flex items-center gap-2">
                      <MapPinned className="w-4 h-4 text-blue-400" />
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
                            <TableHead className="w-24">Postal Code</TableHead>
                            <TableHead>City</TableHead>
                            <TableHead className="text-right">Visits</TableHead>
                            <TableHead className="text-right">Devices</TableHead>
                            <TableHead className="text-right">Avg Dwell</TableHead>
                            <TableHead>Top Category</TableHead>
                            <TableHead className="text-right">Affinity</TableHead>
                            <TableHead>Group</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {filteredProfiles.slice(0, 100).map(p => (
                            <TableRow key={p.zipcode} className="hover:bg-secondary/50">
                              <TableCell className="font-mono text-sm font-medium">{p.zipcode}</TableCell>
                              <TableCell className="text-sm">{p.city}</TableCell>
                              <TableCell className="text-right tabular-nums text-sm">{p.totalVisits.toLocaleString()}</TableCell>
                              <TableCell className="text-right tabular-nums text-sm">{p.uniqueDevices.toLocaleString()}</TableCell>
                              <TableCell className="text-right tabular-nums text-sm">{p.avgDwellMinutes} min</TableCell>
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
                              <TableCell className="text-xs text-muted-foreground">
                                {CATEGORY_GROUPS[p.dominantGroup]?.label || p.dominantGroup}
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                      {filteredProfiles.length > 100 && (
                        <p className="text-xs text-muted-foreground text-center py-3">
                          Showing 100 of {filteredProfiles.length.toLocaleString()} — export CSV for all
                        </p>
                      )}
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Categories */}
              <TabsContent value="categories" className="mt-4">
                <Card className="border-border">
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base flex items-center gap-2">
                      <BarChart3 className="w-4 h-4 text-purple-400" />
                      Category Breakdown
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {result.stats.categoryBreakdown.map(cat => (
                        <div key={cat.category} className="rounded-xl border border-border bg-secondary/50 overflow-hidden">
                          <button
                            className="w-full flex items-center gap-4 p-4 text-left hover:bg-secondary/80 transition-colors"
                            onClick={() => setExpandedCategory(expandedCategory === cat.category ? null : cat.category)}
                          >
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
                            <div className="flex items-center gap-4 shrink-0">
                              <div className="text-center">
                                <div className="text-sm font-bold tabular-nums">{cat.visits.toLocaleString()}</div>
                                <div className="text-[10px] text-muted-foreground">visits</div>
                              </div>
                              <div className="text-center">
                                <div className="text-sm font-bold tabular-nums">{cat.avgDwellMinutes}</div>
                                <div className="text-[10px] text-muted-foreground">min dwell</div>
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
                              <div className="grid grid-cols-2 md:grid-cols-5 gap-4 text-xs">
                                <div>
                                  <span className="text-muted-foreground">Group:</span>{' '}
                                  <span>{CATEGORY_GROUPS[cat.group]?.label || cat.group}</span>
                                </div>
                                <div>
                                  <span className="text-muted-foreground">Zipcodes:</span>{' '}
                                  <span>{cat.postalCodesWithVisits}</span>
                                </div>
                                <div>
                                  <span className="text-muted-foreground">Top ZIP:</span>{' '}
                                  <span className="font-mono">{cat.maxAffinityZipcode || '—'}</span>
                                </div>
                                <div>
                                  <span className="text-muted-foreground">Top city:</span>{' '}
                                  <span>{cat.maxAffinityCity || '—'}</span>
                                </div>
                                <div>
                                  <span className="text-muted-foreground">Devices:</span>{' '}
                                  <span>{cat.uniqueDevices.toLocaleString()}</span>
                                </div>
                              </div>
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              {/* Records */}
              <TabsContent value="records" className="mt-4">
                <Card className="border-border">
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base flex items-center gap-2">
                      <Database className="w-4 h-4 text-muted-foreground" />
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
                            <TableHead>ZIP</TableHead>
                            <TableHead>City</TableHead>
                            <TableHead>Category</TableHead>
                            <TableHead className="text-right">Visits</TableHead>
                            <TableHead className="text-right">Devices</TableHead>
                            <TableHead className="text-right">Dwell</TableHead>
                            <TableHead className="text-right">Freq</TableHead>
                            <TableHead className="text-right">Conc.</TableHead>
                            <TableHead className="text-right">Freq.</TableHead>
                            <TableHead className="text-right">Dwell.</TableHead>
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
                              <TableCell className="text-right tabular-nums text-xs">{r.avgDwellMinutes} min</TableCell>
                              <TableCell className="text-right tabular-nums text-xs">{r.frequency.toFixed(1)}</TableCell>
                              <TableCell className="text-right tabular-nums text-xs">{r.concentrationScore}</TableCell>
                              <TableCell className="text-right tabular-nums text-xs">{r.frequencyScore}</TableCell>
                              <TableCell className="text-right tabular-nums text-xs">{r.dwellScore}</TableCell>
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
                          Showing 200 of {filteredRecords.length.toLocaleString()} — export CSV for all
                        </p>
                      )}
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          </>
        )}
      </div>
    </MainLayout>
  );
}


// ── Recipe Step Card ──────────────────────────────────────────────────
function RecipeStepCard({
  step,
  index,
  onUpdate,
  onToggleCategory,
  onToggleCategoryGroup,
  onRemove,
}: {
  step: RecipeStep;
  index: number;
  onUpdate: (update: Partial<RecipeStep>) => void;
  onToggleCategory: (cat: PoiCategory) => void;
  onToggleCategoryGroup: (group: string) => void;
  onRemove?: () => void;
}) {
  const [showTimeWindow, setShowTimeWindow] = useState(!!step.timeWindow);
  const [showDwell, setShowDwell] = useState(step.minDwellMinutes != null || step.maxDwellMinutes != null);
  const [showFrequency, setShowFrequency] = useState(step.minFrequency != null && step.minFrequency > 1);

  return (
    <div className="rounded-xl border border-border bg-secondary/30 overflow-hidden">
      {/* Step header */}
      <div className="flex items-center justify-between px-4 py-3 bg-secondary/50 border-b border-border">
        <div className="flex items-center gap-2">
          <div className="w-6 h-6 rounded-full bg-theme-accent/10 text-theme-accent flex items-center justify-center text-xs font-bold">
            {index + 1}
          </div>
          <span className="text-sm font-medium">
            Step {index + 1}
            {step.categories.length > 0 && (
              <span className="text-muted-foreground font-normal ml-2">
                — {step.categories.length} {step.categories.length === 1 ? 'category' : 'categories'}
              </span>
            )}
          </span>
        </div>
        <div className="flex items-center gap-1">
          {/* Toggle filters */}
          <button
            onClick={() => {
              setShowTimeWindow(!showTimeWindow);
              if (showTimeWindow) onUpdate({ timeWindow: undefined });
            }}
            className={`p-1.5 rounded-lg text-xs transition-all ${
              showTimeWindow ? 'bg-blue-500/10 text-blue-400' : 'text-muted-foreground hover:text-foreground hover:bg-secondary'
            }`}
            title="Time window"
          >
            <Clock className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => {
              setShowDwell(!showDwell);
              if (showDwell) onUpdate({ minDwellMinutes: undefined, maxDwellMinutes: undefined });
            }}
            className={`p-1.5 rounded-lg text-xs transition-all ${
              showDwell ? 'bg-purple-500/10 text-purple-400' : 'text-muted-foreground hover:text-foreground hover:bg-secondary'
            }`}
            title="Dwell time"
          >
            <Timer className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => {
              setShowFrequency(!showFrequency);
              if (showFrequency) onUpdate({ minFrequency: undefined });
            }}
            className={`p-1.5 rounded-lg text-xs transition-all ${
              showFrequency ? 'bg-green-500/10 text-green-400' : 'text-muted-foreground hover:text-foreground hover:bg-secondary'
            }`}
            title="Min frequency"
          >
            <Hash className="w-3.5 h-3.5" />
          </button>

          {onRemove && (
            <>
              <Separator orientation="vertical" className="h-5 mx-1" />
              <button
                onClick={onRemove}
                className="p-1.5 rounded-lg text-muted-foreground hover:text-red-400 hover:bg-red-500/10 transition-all"
              >
                <Trash2 className="w-3.5 h-3.5" />
              </button>
            </>
          )}
        </div>
      </div>

      {/* Category groups */}
      <div className="p-4 space-y-3">
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
          {Object.entries(CATEGORY_GROUPS).map(([key, group]) => {
            const allSelected = group.categories.every(c => step.categories.includes(c));
            const someSelected = group.categories.some(c => step.categories.includes(c));

            return (
              <div
                key={key}
                className={`rounded-lg border p-2.5 transition-all cursor-pointer ${
                  allSelected
                    ? 'border-theme-accent bg-theme-accent/5'
                    : someSelected
                    ? 'border-theme-accent/40 bg-theme-accent/5'
                    : 'border-border/60 bg-background/50 hover:bg-secondary/50'
                }`}
                onClick={() => onToggleCategoryGroup(key)}
              >
                <div className="flex items-center gap-1.5 mb-1.5">
                  <span className={`text-xs font-medium ${allSelected ? 'text-theme-accent' : 'text-foreground'}`}>
                    {group.label}
                  </span>
                  {allSelected && <CheckCircle className="w-3 h-3 text-theme-accent" />}
                </div>
                <div className="flex flex-wrap gap-0.5">
                  {group.categories.map(cat => (
                    <button
                      key={cat}
                      onClick={(e) => { e.stopPropagation(); onToggleCategory(cat); }}
                      className={`text-[9px] px-1.5 py-0.5 rounded transition-all ${
                        step.categories.includes(cat)
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

        {/* Optional filters */}
        {(showTimeWindow || showDwell || showFrequency) && (
          <div className="flex flex-wrap gap-4 pt-2 border-t border-border/50">
            {showTimeWindow && (
              <div className="flex items-center gap-2">
                <Clock className="w-3.5 h-3.5 text-blue-400" />
                <span className="text-xs text-muted-foreground">Hours:</span>
                <Input
                  type="number"
                  min={0} max={23}
                  placeholder="From"
                  value={step.timeWindow?.hourFrom ?? ''}
                  onChange={(e) => {
                    const v = parseInt(e.target.value);
                    onUpdate({
                      timeWindow: {
                        hourFrom: isNaN(v) ? 0 : v,
                        hourTo: step.timeWindow?.hourTo ?? 23,
                      },
                    });
                  }}
                  className="w-16 h-7 text-xs bg-background border-border"
                />
                <span className="text-xs text-muted-foreground">—</span>
                <Input
                  type="number"
                  min={0} max={23}
                  placeholder="To"
                  value={step.timeWindow?.hourTo ?? ''}
                  onChange={(e) => {
                    const v = parseInt(e.target.value);
                    onUpdate({
                      timeWindow: {
                        hourFrom: step.timeWindow?.hourFrom ?? 0,
                        hourTo: isNaN(v) ? 23 : v,
                      },
                    });
                  }}
                  className="w-16 h-7 text-xs bg-background border-border"
                />
                <span className="text-[10px] text-muted-foreground">UTC</span>
              </div>
            )}

            {showDwell && (
              <div className="flex items-center gap-2">
                <Timer className="w-3.5 h-3.5 text-purple-400" />
                <span className="text-xs text-muted-foreground">Dwell:</span>
                <Input
                  type="number"
                  min={0}
                  placeholder="Min"
                  value={step.minDwellMinutes ?? ''}
                  onChange={(e) => {
                    const v = parseInt(e.target.value);
                    onUpdate({ minDwellMinutes: isNaN(v) ? undefined : v });
                  }}
                  className="w-16 h-7 text-xs bg-background border-border"
                />
                <span className="text-xs text-muted-foreground">—</span>
                <Input
                  type="number"
                  min={0}
                  placeholder="Max"
                  value={step.maxDwellMinutes ?? ''}
                  onChange={(e) => {
                    const v = parseInt(e.target.value);
                    onUpdate({ maxDwellMinutes: isNaN(v) ? undefined : v });
                  }}
                  className="w-16 h-7 text-xs bg-background border-border"
                />
                <span className="text-[10px] text-muted-foreground">min</span>
              </div>
            )}

            {showFrequency && (
              <div className="flex items-center gap-2">
                <Hash className="w-3.5 h-3.5 text-green-400" />
                <span className="text-xs text-muted-foreground">Min visits:</span>
                <Input
                  type="number"
                  min={1}
                  value={step.minFrequency ?? 1}
                  onChange={(e) => {
                    const v = parseInt(e.target.value);
                    onUpdate({ minFrequency: isNaN(v) ? 1 : v });
                  }}
                  className="w-16 h-7 text-xs bg-background border-border"
                />
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}


// ── Stats card ────────────────────────────────────────────────────────
function StatsCard({ label, value, icon, sub, highlight }: {
  label: string; value: string; icon: React.ReactNode; sub: string; highlight?: boolean;
}) {
  return (
    <Card className={`border-border ${highlight ? 'border-theme-accent/30 bg-theme-accent/5' : ''}`}>
      <CardContent className="pt-4 pb-3">
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-[10px] text-muted-foreground uppercase tracking-wider">{label}</span>
          <div className={highlight ? 'text-theme-accent' : 'text-muted-foreground'}>{icon}</div>
        </div>
        <div className="text-xl font-bold tabular-nums">{value}</div>
        <span className="text-[10px] text-muted-foreground">{sub}</span>
      </CardContent>
    </Card>
  );
}
