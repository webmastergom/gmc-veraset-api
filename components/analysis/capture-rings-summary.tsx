'use client';

import { Target } from 'lucide-react';

interface CaptureRing {
  zips: number;
  deviceDays: number;
  share: number;
}

interface CaptureRingsSummaryProps {
  totalZips: number;
  totalDeviceDays: number;
  captureRings?: {
    p70: CaptureRing;
    p80: CaptureRing;
    p90: CaptureRing;
  };
}

/**
 * "Trade area" summary card — shows how many top zips concentrate
 * 70/80/90% of the catchment's device-days. The headline question for
 * sales pitches: how addressable is this audience geographically?
 */
export function CaptureRingsSummary({
  totalZips,
  totalDeviceDays,
  captureRings,
}: CaptureRingsSummaryProps) {
  if (!captureRings) return null;

  const fmt = (n: number) => n.toLocaleString();
  const pct = (s: number) => `${s.toFixed(1)}%`;

  // What fraction of TOTAL zips is needed to capture each tier?
  const compaction = (ring: CaptureRing) =>
    totalZips > 0 ? Math.round((ring.zips / totalZips) * 100) : 0;

  const tiers: Array<{ key: 'p70' | 'p80' | 'p90'; label: string; ring: CaptureRing; color: string }> = [
    { key: 'p70', label: '70% capture', ring: captureRings.p70, color: 'bg-emerald-500/15 border-emerald-500/40 text-emerald-500' },
    { key: 'p80', label: '80% capture', ring: captureRings.p80, color: 'bg-amber-500/15 border-amber-500/40 text-amber-500' },
    { key: 'p90', label: '90% capture', ring: captureRings.p90, color: 'bg-orange-500/15 border-orange-500/40 text-orange-500' },
  ];

  return (
    <div className="mb-4 space-y-2">
      <div className="flex items-center gap-2 text-sm font-medium">
        <Target className="h-4 w-4" />
        Trade Area — Capture Rings
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
        {tiers.map(({ key, label, ring, color }) => (
          <div key={key} className={`rounded-lg border p-3 ${color}`}>
            <div className="text-xs uppercase tracking-wider opacity-80">{label}</div>
            <div className="mt-1 text-2xl font-bold tabular-nums">
              {fmt(ring.zips)} <span className="text-sm font-normal opacity-70">zips</span>
            </div>
            <div className="text-xs opacity-80 mt-0.5">
              {fmt(ring.deviceDays)} device-days · {pct(ring.share)} of catchment
            </div>
            <div className="text-[10px] mt-1 opacity-60">
              ({compaction(ring)}% of {fmt(totalZips)} total zips)
            </div>
          </div>
        ))}
      </div>
      <p className="text-xs text-muted-foreground italic">
        Trade-area rings show the smallest set of top zips that cumulatively account for 70/80/90% of device-days — the "addressable core" for geo-targeted campaigns.
      </p>
    </div>
  );
}
