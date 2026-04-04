'use client';

import { useState } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Download, Loader2, Target, MapPin, Play, ChevronDown, ChevronRight, Check } from 'lucide-react';
import { useToast } from '@/hooks/use-toast';
import { CATEGORY_GROUPS, CATEGORY_LABELS } from '@/lib/laboratory-types';
import type { PoiCategory } from '@/lib/laboratory-types';

const DWELL_OPTIONS = [
  { value: 0, label: 'Any (no minimum)' },
  { value: 2, label: '2+ minutes' },
  { value: 5, label: '5+ minutes' },
  { value: 10, label: '10+ minutes' },
  { value: 15, label: '15+ minutes' },
  { value: 30, label: '30+ minutes' },
  { value: 60, label: '60+ minutes' },
];

interface CategoryMaidModalProps {
  open: boolean;
  onClose: () => void;
  datasetName: string;
  jobCountry: string | null;
}

export function CategoryMaidModal({ open, onClose, datasetName, jobCountry }: CategoryMaidModalProps) {
  const [selectedGroup, setSelectedGroup] = useState<string | null>(null);
  const [selectedCategories, setSelectedCategories] = useState<Set<string>>(new Set());
  const [minDwell, setMinDwell] = useState(5);
  const [computing, setComputing] = useState(false);
  const [computeProgress, setComputeProgress] = useState<string | null>(null);
  const [result, setResult] = useState<{ maidCount: number; downloadKey: string } | null>(null);
  const [expandedGroup, setExpandedGroup] = useState<string | null>(null);
  const { toast } = useToast();

  const handleOpenChange = (isOpen: boolean) => {
    if (!isOpen) onClose();
  };

  const handleSelectGroup = (groupKey: string) => {
    const group = CATEGORY_GROUPS[groupKey];
    if (!group) return;

    if (selectedGroup === groupKey) {
      // Deselect
      setSelectedGroup(null);
      setSelectedCategories(new Set());
    } else {
      // Select all categories from this group
      setSelectedGroup(groupKey);
      setSelectedCategories(new Set(group.categories));
    }
    setResult(null);
  };

  const toggleCategory = (cat: string) => {
    const next = new Set(selectedCategories);
    if (next.has(cat)) {
      next.delete(cat);
    } else {
      next.add(cat);
    }
    setSelectedCategories(next);
    setResult(null);
  };

  const toggleExpandGroup = (groupKey: string) => {
    setExpandedGroup(expandedGroup === groupKey ? null : groupKey);
  };

  // Safe fetch that handles 504/non-JSON responses gracefully
  const safePollFetch = async (url: string, options?: RequestInit) => {
    const res = await fetch(url, options);
    if (res.status === 504) {
      return { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
    }
    let data;
    try {
      data = await res.json();
    } catch {
      return { phase: 'polling', progress: { message: 'Server processing (retrying...)' } };
    }
    if (!res.ok) throw new Error(data.error || 'Failed');
    return data;
  };

  const handleCompute = async () => {
    if (!jobCountry || selectedCategories.size === 0) return;

    setComputing(true);
    setComputeProgress('Starting spatial join...');
    setResult(null);

    try {
      let data = await safePollFetch(`/api/datasets/${datasetName}/export/category-poll`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          categories: Array.from(selectedCategories),
          groupKey: selectedGroup || 'custom',
          minDwell,
          country: jobCountry,
        }),
      });

      while (data.phase !== 'done' && data.phase !== 'error') {
        setComputeProgress(data.progress?.message || 'Processing...');
        await new Promise(r => setTimeout(r, 4000));

        data = await safePollFetch(`/api/datasets/${datasetName}/export/category-poll`, {
          method: 'POST',
          credentials: 'include',
        });
      }

      if (data.phase === 'error') {
        throw new Error(data.error || 'Analysis failed');
      }

      setResult(data.result);
      toast({ title: 'Analysis complete', description: `${(data.result?.maidCount || 0).toLocaleString()} MAIDs found` });
    } catch (e: any) {
      toast({ title: 'Analysis failed', description: e.message, variant: 'destructive' });
    } finally {
      setComputing(false);
      setComputeProgress(null);
    }
  };

  const handleDownload = () => {
    if (!result?.downloadKey) return;
    const link = document.createElement('a');
    link.href = result.downloadKey;
    link.download = `${datasetName}-category-${selectedGroup || 'custom'}.csv`;
    link.click();
  };

  const groupEntries = Object.entries(CATEGORY_GROUPS);

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Target className="h-5 w-5" />
            MAIDs by POI Category {jobCountry ? `— ${jobCountry}` : ''}
          </DialogTitle>
        </DialogHeader>

        {!jobCountry && (
          <div className="py-8 text-center text-muted-foreground">
            <MapPin className="h-8 w-8 mx-auto mb-3 opacity-50" />
            <p className="font-medium">No country set</p>
            <p className="text-sm mt-1">Set the country for this job in the Jobs section first.</p>
          </div>
        )}

        {jobCountry && (
          <div className="space-y-4">
            {/* Category Group Selector */}
            <div>
              <label className="text-sm font-medium text-muted-foreground mb-2 block">
                Select POI Category Group
              </label>
              <div className="grid grid-cols-2 gap-2 max-h-[300px] overflow-y-auto pr-1">
                {groupEntries.map(([key, group]) => {
                  const isSelected = selectedGroup === key;
                  const isExpanded = expandedGroup === key;
                  const selectedInGroup = group.categories.filter(c => selectedCategories.has(c)).length;

                  return (
                    <div key={key} className="col-span-2">
                      <div className="flex items-center gap-1">
                        <Button
                          variant={isSelected ? 'default' : 'outline'}
                          size="sm"
                          className="flex-1 justify-start text-left h-auto py-1.5"
                          onClick={() => handleSelectGroup(key)}
                        >
                          {isSelected && <Check className="h-3 w-3 mr-1 shrink-0" />}
                          <span className="truncate">{group.label}</span>
                          <span className="ml-auto text-xs opacity-60 shrink-0">
                            {isSelected && selectedInGroup < group.categories.length
                              ? `${selectedInGroup}/${group.categories.length}`
                              : `${group.categories.length} cats`
                            }
                          </span>
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-8 w-8 p-0 shrink-0"
                          onClick={() => {
                            if (!isSelected) handleSelectGroup(key);
                            toggleExpandGroup(key);
                          }}
                        >
                          {isExpanded ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                        </Button>
                      </div>

                      {/* Individual categories within expanded group */}
                      {isExpanded && isSelected && (
                        <div className="ml-4 mt-1 mb-2 flex flex-wrap gap-1">
                          {group.categories.map(cat => (
                            <button
                              key={cat}
                              onClick={() => toggleCategory(cat)}
                              className={`text-xs px-2 py-0.5 rounded-full border transition-colors ${
                                selectedCategories.has(cat)
                                  ? 'bg-primary text-primary-foreground border-primary'
                                  : 'bg-muted/50 text-muted-foreground border-border hover:bg-muted'
                              }`}
                            >
                              {CATEGORY_LABELS[cat as PoiCategory] || cat.replace(/_/g, ' ')}
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Dwell Time Selector */}
            <div>
              <label className="text-sm font-medium text-muted-foreground mb-1 block">
                Minimum Dwell Time
              </label>
              <select
                value={minDwell}
                onChange={e => { setMinDwell(Number(e.target.value)); setResult(null); }}
                className="w-full h-9 rounded-md border border-input bg-background px-3 text-sm"
              >
                {DWELL_OPTIONS.map(opt => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
            </div>

            {/* Compute Button */}
            <Button
              onClick={handleCompute}
              disabled={computing || selectedCategories.size === 0}
              className="w-full"
            >
              {computing ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  {computeProgress || 'Computing...'}
                </>
              ) : (
                <>
                  <Play className="mr-2 h-4 w-4" />
                  Find MAIDs ({selectedCategories.size} categories selected)
                </>
              )}
            </Button>

            {/* Results */}
            {result && (
              <div className="border rounded-lg p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-2xl font-bold">{result.maidCount.toLocaleString()}</p>
                    <p className="text-sm text-muted-foreground">
                      Unique MAIDs visiting {selectedGroup ? CATEGORY_GROUPS[selectedGroup]?.label : 'selected categories'}
                      {minDwell > 0 ? ` (≥${minDwell} min dwell)` : ''}
                    </p>
                  </div>
                  {result.downloadKey && (
                    <Button variant="outline" onClick={handleDownload}>
                      <Download className="mr-2 h-4 w-4" />
                      Download CSV
                    </Button>
                  )}
                </div>
              </div>
            )}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
