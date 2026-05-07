'use client';

import { useEffect, useMemo, useState } from 'react';
import { useParams } from 'next/navigation';
import Link from 'next/link';
import { MainLayout } from '@/components/layout/main-layout';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { useToast } from '@/hooks/use-toast';
import {
  ArrowLeft,
  Loader2,
  Save,
  Tag,
  Wand2,
  Check,
  X,
  ChevronDown,
  ChevronRight,
  Search,
  Trash2,
} from 'lucide-react';

interface BrandedPoi {
  poiId: string;
  name: string;
  currentBrand: string;
  source: 'override' | 'discovered' | 'rules' | 'other';
}

interface BrandSummary {
  brand: string;
  count: number;
}

interface BrandsResponse {
  collectionId: string;
  poiCount: number;
  pois: BrandedPoi[];
  summary: BrandSummary[];
  candidates: { brand: string; count: number; exemplar: string }[];
}

const SOURCE_LABELS: Record<BrandedPoi['source'], string> = {
  override: 'override',
  discovered: 'discovered',
  rules: 'rules',
  other: 'unmatched',
};

const SOURCE_COLORS: Record<BrandedPoi['source'], string> = {
  override: 'bg-emerald-500/15 text-emerald-500 border-emerald-500/30',
  discovered: 'bg-blue-500/15 text-blue-500 border-blue-500/30',
  rules: 'bg-violet-500/15 text-violet-500 border-violet-500/30',
  other: 'bg-amber-500/15 text-amber-500 border-amber-500/30',
};

export default function BrandEditorPage() {
  const params = useParams();
  const collectionId = params.id as string;
  const { toast } = useToast();

  const [data, setData] = useState<BrandsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  /** Pending edits keyed by poiId. Value '' clears the override. */
  const [edits, setEdits] = useState<Record<string, string>>({});
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selectedBrands, setSelectedBrands] = useState<Set<string>>(new Set());
  const [mergeTarget, setMergeTarget] = useState('');

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setLoading(true);
        const res = await fetch(`/api/pois/collections/${collectionId}/brands`, {
          credentials: 'include',
        });
        if (!res.ok) throw new Error(await res.text());
        const json = (await res.json()) as BrandsResponse;
        if (!cancelled) setData(json);
      } catch (e: any) {
        if (!cancelled) {
          toast({ title: 'Failed to load brands', description: e?.message || String(e), variant: 'destructive' });
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [collectionId, toast]);

  /** Effective brand per POI = pending edit overrides existing one. */
  const effectiveBrand = (p: BrandedPoi) =>
    p.poiId in edits
      ? edits[p.poiId] || 'other'
      : p.currentBrand;

  /** Brand → POI list, using effective (after pending edits) brand. */
  const brandGroups = useMemo(() => {
    const groups = new Map<string, BrandedPoi[]>();
    if (!data) return groups;
    const q = search.trim().toLowerCase();
    for (const p of data.pois) {
      if (q && !p.name.toLowerCase().includes(q) && !p.poiId.toLowerCase().includes(q)) continue;
      const brand = effectiveBrand(p);
      let list = groups.get(brand);
      if (!list) {
        list = [];
        groups.set(brand, list);
      }
      list.push(p);
    }
    return groups;
  }, [data, edits, search]);

  /** Sorted summary per brand using effective brand. */
  const effectiveSummary = useMemo(() => {
    if (!data) return [];
    const counts = new Map<string, number>();
    for (const p of data.pois) {
      const b = effectiveBrand(p);
      counts.set(b, (counts.get(b) || 0) + 1);
    }
    return Array.from(counts.entries())
      .map(([brand, count]) => ({ brand, count }))
      .sort((a, b) => b.count - a.count);
  }, [data, edits]);

  const dirty = Object.keys(edits).length > 0;

  const toggleExpanded = (brand: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(brand)) next.delete(brand);
      else next.add(brand);
      return next;
    });
  };

  const setBrandFor = (poiId: string, brand: string) => {
    setEdits((prev) => ({ ...prev, [poiId]: brand }));
  };

  /** Bulk: assign newBrand to every POI currently in `brand`. Empty string = clear. */
  const reassignBrand = (brand: string, newBrand: string) => {
    if (!data) return;
    const newEdits = { ...edits };
    for (const p of data.pois) {
      if (effectiveBrand(p) === brand) {
        newEdits[p.poiId] = newBrand;
      }
    }
    setEdits(newEdits);
  };

  /** Bulk: merge all selected brands into mergeTarget. */
  const doMerge = () => {
    if (!mergeTarget.trim() || selectedBrands.size === 0) return;
    if (!data) return;
    const target = mergeTarget.trim();
    const newEdits = { ...edits };
    for (const p of data.pois) {
      if (selectedBrands.has(effectiveBrand(p))) {
        newEdits[p.poiId] = target;
      }
    }
    setEdits(newEdits);
    setSelectedBrands(new Set());
    setMergeTarget('');
    toast({ title: `Merged ${selectedBrands.size} brand(s) into "${target}"` });
  };

  const undoEdit = (poiId: string) => {
    setEdits((prev) => {
      const next = { ...prev };
      delete next[poiId];
      return next;
    });
  };

  const undoAll = () => {
    setEdits({});
  };

  const save = async () => {
    if (!dirty) return;
    setSaving(true);
    try {
      const res = await fetch(`/api/pois/collections/${collectionId}/brands`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ updates: edits }),
      });
      if (!res.ok) throw new Error(await res.text());
      const json = await res.json();
      toast({ title: 'Saved', description: `${json.touched} POI(s) updated. Re-run personas to see fresh brand mix.` });
      // Reload fresh data so the source labels reflect the override.
      setEdits({});
      const fresh = await fetch(`/api/pois/collections/${collectionId}/brands`, {
        credentials: 'include',
      }).then((r) => r.json());
      setData(fresh);
    } catch (e: any) {
      toast({ title: 'Save failed', description: e?.message || String(e), variant: 'destructive' });
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <MainLayout>
        <div className="max-w-6xl mx-auto p-6">
          <Loader2 className="h-6 w-6 animate-spin" />
        </div>
      </MainLayout>
    );
  }

  if (!data) {
    return (
      <MainLayout>
        <div className="max-w-6xl mx-auto p-6">
          <div className="text-sm text-muted-foreground">No data.</div>
        </div>
      </MainLayout>
    );
  }

  const brandsAlpha = effectiveSummary.map((s) => s.brand).filter((b) => b !== 'other').sort();

  return (
    <MainLayout>
      <div className="max-w-6xl mx-auto space-y-4 p-4">
        {/* Header */}
        <div className="flex items-center gap-3">
          <Link href={`/pois/${collectionId}`} className="text-sm text-muted-foreground hover:text-foreground flex items-center gap-1">
            <ArrowLeft className="h-4 w-4" /> Back
          </Link>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Tag className="h-6 w-6" /> Brand Editor
          </h1>
          <span className="text-xs text-muted-foreground">
            {data.poiCount.toLocaleString()} POIs · {effectiveSummary.length} brands
          </span>
          <div className="ml-auto flex items-center gap-2">
            {dirty && (
              <Button variant="outline" size="sm" onClick={undoAll}>
                <X className="h-4 w-4 mr-1" /> Discard {Object.keys(edits).length}
              </Button>
            )}
            <Button onClick={save} disabled={!dirty || saving}>
              {saving ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Save className="h-4 w-4 mr-2" />}
              Save {dirty ? `(${Object.keys(edits).length})` : ''}
            </Button>
          </div>
        </div>

        <p className="text-sm text-muted-foreground">
          Brands are auto-detected from POI names (frequency discovery + hardcoded chains) and from explicit override
          properties (<code className="text-xs">brand</code>, <code className="text-xs">cadena</code>,{' '}
          <code className="text-xs">marca</code>, <code className="text-xs">concesionaria</code>, …). Edit here to clean
          up garbage / merge duplicates / add manually. Saved values land in the collection&apos;s GeoJSON as{' '}
          <code className="text-xs">properties.brand</code>, taking precedence on the next persona run.
        </p>

        {/* Toolbar: search + bulk merge */}
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-2 flex-1 max-w-md">
            <Search className="h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search POI name or id…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="h-8"
            />
          </div>
          {selectedBrands.size > 0 && (
            <div className="flex items-center gap-2 ml-auto bg-blue-500/10 border border-blue-500/30 rounded-md px-3 py-1.5">
              <span className="text-xs">{selectedBrands.size} brand(s) selected — merge into:</span>
              <Input
                value={mergeTarget}
                onChange={(e) => setMergeTarget(e.target.value)}
                placeholder="target brand"
                list="merge-target-list"
                className="h-7 w-40"
              />
              <datalist id="merge-target-list">
                {brandsAlpha.map((b) => (
                  <option key={b} value={b} />
                ))}
              </datalist>
              <Button size="sm" onClick={doMerge} disabled={!mergeTarget.trim()}>
                <Wand2 className="h-3.5 w-3.5 mr-1" /> Merge
              </Button>
              <Button size="sm" variant="ghost" onClick={() => setSelectedBrands(new Set())}>
                <X className="h-3.5 w-3.5" />
              </Button>
            </div>
          )}
        </div>

        {/* Brand groups */}
        <Card>
          <CardHeader className="py-3">
            <CardTitle className="text-sm">Brands</CardTitle>
          </CardHeader>
          <CardContent className="px-0 pb-0">
            <div className="divide-y">
              {effectiveSummary.map((s) => {
                const pois = brandGroups.get(s.brand) || [];
                const isExpanded = expanded.has(s.brand);
                const isSelected = selectedBrands.has(s.brand);
                return (
                  <div key={s.brand}>
                    {/* Brand row */}
                    <div className={`px-4 py-2 flex items-center gap-2 hover:bg-muted/30 transition-colors ${isSelected ? 'bg-blue-500/5' : ''}`}>
                      <input
                        type="checkbox"
                        checked={isSelected}
                        onChange={(e) => {
                          setSelectedBrands((prev) => {
                            const next = new Set(prev);
                            if (e.target.checked) next.add(s.brand);
                            else next.delete(s.brand);
                            return next;
                          });
                        }}
                        className="h-3.5 w-3.5"
                        title="Select for bulk merge"
                      />
                      <button
                        type="button"
                        onClick={() => toggleExpanded(s.brand)}
                        className="flex items-center gap-2 flex-1 min-w-0"
                      >
                        {isExpanded ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
                        <span className={`font-medium capitalize ${s.brand === 'other' ? 'text-amber-500' : ''}`}>
                          {s.brand.replace(/_/g, ' ')}
                        </span>
                        <span className="text-xs text-muted-foreground tabular-nums">{s.count} POI{s.count === 1 ? '' : 's'}</span>
                      </button>
                      <BulkRenameButton
                        brand={s.brand}
                        suggestions={brandsAlpha}
                        onApply={(next) => reassignBrand(s.brand, next)}
                      />
                    </div>

                    {/* Expanded POI list */}
                    {isExpanded && pois.length > 0 && (
                      <div className="bg-muted/10 border-t border-border/50">
                        <div className="max-h-[400px] overflow-y-auto">
                          {pois.slice(0, 200).map((p) => (
                            <PoiRow
                              key={p.poiId}
                              poi={p}
                              edited={p.poiId in edits}
                              effective={effectiveBrand(p)}
                              suggestions={brandsAlpha}
                              onChange={(b) => setBrandFor(p.poiId, b)}
                              onClear={() => setBrandFor(p.poiId, '')}
                              onUndo={() => undoEdit(p.poiId)}
                            />
                          ))}
                          {pois.length > 200 && (
                            <div className="px-4 py-2 text-xs text-muted-foreground border-t border-border/30">
                              Showing first 200 of {pois.length}. Use search to narrow down.
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </CardContent>
        </Card>
      </div>
    </MainLayout>
  );
}

// ── Sub-components ──────────────────────────────────────────────────

function BulkRenameButton({
  brand,
  suggestions,
  onApply,
}: {
  brand: string;
  suggestions: string[];
  onApply: (next: string) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(brand === 'other' ? '' : brand);

  if (!editing) {
    return (
      <Button
        size="sm"
        variant="outline"
        className="h-7 text-xs"
        onClick={() => setEditing(true)}
        title="Reassign all POIs in this brand to a different one"
      >
        Rename / Reassign
      </Button>
    );
  }

  const apply = () => {
    onApply(value.trim());
    setEditing(false);
  };

  return (
    <div className="flex items-center gap-1">
      <Input
        value={value}
        onChange={(e) => setValue(e.target.value)}
        list={`bulk-list-${brand}`}
        placeholder="new brand (empty = clear)"
        className="h-7 w-44 text-xs"
        autoFocus
      />
      <datalist id={`bulk-list-${brand}`}>
        {suggestions.map((b) => (
          <option key={b} value={b} />
        ))}
      </datalist>
      <Button size="sm" className="h-7" onClick={apply}>
        <Check className="h-3.5 w-3.5" />
      </Button>
      <Button size="sm" variant="ghost" className="h-7" onClick={() => setEditing(false)}>
        <X className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}

function PoiRow({
  poi,
  edited,
  effective,
  suggestions,
  onChange,
  onClear,
  onUndo,
}: {
  poi: BrandedPoi;
  edited: boolean;
  effective: string;
  suggestions: string[];
  onChange: (brand: string) => void;
  onClear: () => void;
  onUndo: () => void;
}) {
  return (
    <div className={`px-4 py-1.5 flex items-center gap-3 hover:bg-muted/30 ${edited ? 'bg-blue-500/5' : ''}`}>
      <div className="flex-1 min-w-0">
        <div className="text-sm truncate">{poi.name || <span className="text-muted-foreground">(no name)</span>}</div>
        <div className="text-[10px] text-muted-foreground font-mono truncate">{poi.poiId}</div>
      </div>
      <span
        className={`text-[10px] px-1.5 py-0.5 rounded border ${SOURCE_COLORS[poi.source]}`}
        title={`Original source: ${SOURCE_LABELS[poi.source]} (before any pending edits)`}
      >
        {SOURCE_LABELS[poi.source]}
      </span>
      <Input
        value={effective === 'other' && !edited ? '' : effective}
        onChange={(e) => onChange(e.target.value)}
        placeholder="brand"
        list={`poi-list-${poi.poiId}`}
        className="h-7 w-44 text-xs"
      />
      <datalist id={`poi-list-${poi.poiId}`}>
        {suggestions.map((b) => (
          <option key={b} value={b} />
        ))}
      </datalist>
      {edited ? (
        <Button size="sm" variant="ghost" className="h-7 px-2" onClick={onUndo} title="Undo this edit">
          <X className="h-3.5 w-3.5" />
        </Button>
      ) : (
        <Button size="sm" variant="ghost" className="h-7 px-2" onClick={onClear} title="Clear override (fall back to discovery)">
          <Trash2 className="h-3.5 w-3.5 text-muted-foreground" />
        </Button>
      )}
    </div>
  );
}
