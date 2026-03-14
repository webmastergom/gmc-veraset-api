'use client';

import { useState, useRef, useEffect } from 'react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { MapPin, X, ChevronDown, Check } from 'lucide-react';

interface PoiOption {
  id: string;
  name: string;
}

interface PoiFilterProps {
  pois: PoiOption[];
  selectedIds: string[];
  onChange: (ids: string[]) => void;
}

export function PoiFilter({ pois, selectedIds, onChange }: PoiFilterProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const ref = useRef<HTMLDivElement>(null);

  // Close on click outside
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const togglePoi = (id: string) => {
    if (selectedIds.includes(id)) {
      onChange(selectedIds.filter((x) => x !== id));
    } else {
      onChange([...selectedIds, id]);
    }
  };

  const filtered = pois.filter((p) =>
    p.name.toLowerCase().includes(search.toLowerCase())
  );

  const selectedNames = selectedIds
    .map((id) => pois.find((p) => p.id === id)?.name || id)
    .slice(0, 3);

  return (
    <div className="flex items-center gap-3">
      <span className="text-sm font-medium text-muted-foreground flex items-center gap-1 shrink-0">
        <MapPin className="h-3.5 w-3.5" /> Filter POIs:
      </span>

      <div ref={ref} className="relative flex-1 max-w-md">
        <Button
          variant="outline"
          className="w-full justify-between text-left font-normal"
          onClick={() => setOpen(!open)}
        >
          <span className="truncate">
            {selectedIds.length === 0
              ? 'All POIs'
              : `${selectedIds.length} selected`}
          </span>
          <ChevronDown className="h-4 w-4 shrink-0 ml-2 opacity-50" />
        </Button>

        {open && (
          <div className="absolute z-50 top-full mt-1 w-full rounded-md border bg-popover shadow-md">
            <div className="p-2">
              <input
                type="text"
                placeholder="Search POIs..."
                className="w-full rounded-md border bg-background px-3 py-1.5 text-sm outline-none"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                autoFocus
              />
            </div>
            <div className="max-h-60 overflow-y-auto px-1 pb-1">
              <button
                className="flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-sm hover:bg-accent cursor-pointer"
                onClick={() => { onChange([]); setOpen(false); }}
              >
                <Check className={`h-3.5 w-3.5 ${selectedIds.length === 0 ? 'opacity-100' : 'opacity-0'}`} />
                All POIs
              </button>
              {filtered.map((poi) => (
                <button
                  key={poi.id}
                  className="flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-sm hover:bg-accent cursor-pointer"
                  onClick={() => togglePoi(poi.id)}
                >
                  <Check className={`h-3.5 w-3.5 ${selectedIds.includes(poi.id) ? 'opacity-100' : 'opacity-0'}`} />
                  <span className="truncate">{poi.name}</span>
                </button>
              ))}
              {filtered.length === 0 && (
                <p className="text-xs text-muted-foreground text-center py-2">No matches</p>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Selected badges (compact) */}
      {selectedIds.length > 0 && (
        <div className="flex items-center gap-1.5 flex-wrap">
          {selectedNames.map((name, i) => (
            <Badge key={i} variant="secondary" className="text-xs gap-1">
              {name.length > 20 ? name.slice(0, 18) + '...' : name}
              <X
                className="h-3 w-3 cursor-pointer"
                onClick={() => togglePoi(selectedIds[i])}
              />
            </Badge>
          ))}
          {selectedIds.length > 3 && (
            <Badge variant="outline" className="text-xs">
              +{selectedIds.length - 3} more
            </Badge>
          )}
          <Button variant="ghost" size="sm" className="text-xs h-6" onClick={() => onChange([])}>
            Clear
          </Button>
        </div>
      )}
    </div>
  );
}
