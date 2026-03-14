'use client';

import { Badge } from '@/components/ui/badge';
import { MapPin } from 'lucide-react';

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
  const allSelected = selectedIds.length === 0;

  const togglePoi = (id: string) => {
    if (selectedIds.includes(id)) {
      onChange(selectedIds.filter((x) => x !== id));
    } else {
      onChange([...selectedIds, id]);
    }
  };

  return (
    <div className="flex items-center gap-2 flex-wrap">
      <span className="text-sm font-medium text-muted-foreground flex items-center gap-1">
        <MapPin className="h-3.5 w-3.5" /> Filter:
      </span>
      <Badge
        variant={allSelected ? 'default' : 'outline'}
        className="cursor-pointer"
        onClick={() => onChange([])}
      >
        All POIs
      </Badge>
      {pois.map((poi) => (
        <Badge
          key={poi.id}
          variant={selectedIds.includes(poi.id) ? 'default' : 'outline'}
          className="cursor-pointer"
          onClick={() => togglePoi(poi.id)}
        >
          {poi.name}
        </Badge>
      ))}
    </div>
  );
}
