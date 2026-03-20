import { Layers } from 'lucide-react';

export function MegaJobBadge({ megaJobId, megaJobIndex }: { megaJobId?: string; megaJobIndex?: number }) {
  if (!megaJobId) return null;

  return (
    <span className="text-xs px-2 py-0.5 rounded-full bg-purple-500/10 text-purple-400 border border-purple-500/20 flex items-center gap-1">
      <Layers className="h-3 w-3" />
      {megaJobIndex !== undefined ? `Mega #${megaJobIndex + 1}` : 'Mega-job'}
    </span>
  );
}
