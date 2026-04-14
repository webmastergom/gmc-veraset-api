'use client';

import { useMemo, useState } from 'react';

interface SankeyFlow {
  direction: 'before' | 'after';
  group_key: string;
  group_label: string;
  devices: number;
  pings: number;
}

interface SankeyChartProps {
  data: SankeyFlow[];
  targetLabel?: string;
  maxNodes?: number;
}

// Colors for category groups — matches CATEGORY_GROUPS colors
const GROUP_COLORS: Record<string, string> = {
  retail: '#f472b6',        // pink-400
  food_and_beverage: '#fb923c', // orange-400
  automotive: '#94a3b8',    // slate-400
  beauty: '#e879f9',        // fuchsia-400
  healthcare: '#f87171',    // red-400
  finance: '#34d399',       // emerald-400
  sports: '#a3e635',        // lime-400
  entertainment: '#a78bfa', // purple-400
  accommodation: '#60a5fa', // blue-400
  education: '#fbbf24',     // amber-400
  luxury: '#c084fc',        // purple-400 lighter
  home: '#a78bfa',          // violet
  electronics: '#22d3ee',   // cyan-400
  pets: '#fb7185',          // rose-400
  pharma: '#2dd4bf',        // teal-400
  transport: '#38bdf8',     // sky-400
  logistics: '#818cf8',     // indigo-400
  government: '#a1a1aa',    // zinc-400
  energy: '#facc15',        // yellow-400
  gaming: '#4ade80',        // green-400
  moviegoers: '#c084fc',    // purple-300
  corporate: '#64748b',     // slate-500
  attractions: '#f59e0b',   // amber-500
  other: '#71717a',         // zinc-500
};

export function SankeyChart({ data, targetLabel = 'Target POI', maxNodes = 12 }: SankeyChartProps) {
  const [hoveredGroup, setHoveredGroup] = useState<string | null>(null);

  const { beforeNodes, afterNodes, maxDevices, totalBefore, totalAfter } = useMemo(() => {
    const before = data
      .filter(d => d.direction === 'before')
      .sort((a, b) => b.devices - a.devices)
      .slice(0, maxNodes);
    const after = data
      .filter(d => d.direction === 'after')
      .sort((a, b) => b.devices - a.devices)
      .slice(0, maxNodes);

    const maxD = Math.max(
      ...before.map(d => d.devices),
      ...after.map(d => d.devices),
      1,
    );
    const tBefore = before.reduce((s, d) => s + d.devices, 0);
    const tAfter = after.reduce((s, d) => s + d.devices, 0);

    return { beforeNodes: before, afterNodes: after, maxDevices: maxD, totalBefore: tBefore, totalAfter: tAfter };
  }, [data, maxNodes]);

  if (!data?.length) {
    return (
      <div className="h-60 flex items-center justify-center text-muted-foreground">
        No route data available.
      </div>
    );
  }

  // Layout dimensions
  const W = 900;
  const nodeColW = 180;
  const centerW = 120;
  const gapX = (W - 2 * nodeColW - centerW) / 2;
  const rowH = 36;
  const padTop = 30;
  const maxRows = Math.max(beforeNodes.length, afterNodes.length, 1);
  const H = padTop + maxRows * rowH + 30;

  // Center box
  const cx = nodeColW + gapX;
  const centerY = padTop;
  const centerH = maxRows * rowH;

  // Render a set of category nodes + bezier links to center
  const renderSide = (nodes: SankeyFlow[], side: 'left' | 'right') => {
    const total = side === 'left' ? totalBefore : totalAfter;
    return nodes.map((node, i) => {
      const y = padTop + i * rowH;
      const barW = Math.max(8, (node.devices / maxDevices) * (nodeColW - 10));
      const color = GROUP_COLORS[node.group_key] || GROUP_COLORS.other;
      const pct = total > 0 ? ((node.devices / total) * 100).toFixed(1) : '0';
      const isHovered = hoveredGroup === node.group_key;
      const opacity = hoveredGroup ? (isHovered ? 1 : 0.2) : 0.7;

      // Bar position
      const barX = side === 'left' ? nodeColW - barW : cx + centerW;
      // Link endpoints
      const linkStartX = side === 'left' ? nodeColW : cx + centerW;
      const linkEndX = side === 'left' ? cx : cx + centerW;
      // Y midpoint for the bar
      const barMidY = y + rowH / 2;
      // Center Y point — proportional vertical position
      const centerMidY = centerY + (i / Math.max(nodes.length - 1, 1)) * centerH;

      // Bezier control points
      const cpx1 = side === 'left' ? linkStartX + gapX * 0.4 : linkEndX + gapX * 0.6;
      const cpx2 = side === 'left' ? linkEndX - gapX * 0.1 : linkStartX + gapX * 0.1;

      // Flow thickness proportional to device count
      const thickness = Math.max(2, (node.devices / maxDevices) * 18);

      return (
        <g
          key={`${side}-${node.group_key}`}
          onMouseEnter={() => setHoveredGroup(node.group_key)}
          onMouseLeave={() => setHoveredGroup(null)}
          className="cursor-pointer"
        >
          {/* Bezier link */}
          <path
            d={side === 'left'
              ? `M${linkStartX},${barMidY} C${cpx1},${barMidY} ${cpx2},${centerMidY} ${linkEndX},${centerMidY}`
              : `M${linkEndX},${centerMidY} C${cpx2},${centerMidY} ${cpx1},${barMidY} ${linkStartX},${barMidY}`
            }
            fill="none"
            stroke={color}
            strokeWidth={thickness}
            opacity={opacity}
            strokeLinecap="round"
          />
          {/* Category bar */}
          <rect
            x={barX}
            y={y + 4}
            width={barW}
            height={rowH - 8}
            rx={4}
            fill={color}
            opacity={isHovered ? 1 : 0.85}
          />
          {/* Label */}
          <text
            x={side === 'left' ? barX - 6 : barX + barW + 6}
            y={y + rowH / 2 + 1}
            textAnchor={side === 'left' ? 'end' : 'start'}
            fill="currentColor"
            fontSize={11}
            dominantBaseline="middle"
            className="fill-foreground"
            opacity={hoveredGroup ? (isHovered ? 1 : 0.4) : 1}
          >
            {node.group_label} ({pct}%)
          </text>
          {/* Device count on hover */}
          {isHovered && (
            <text
              x={side === 'left' ? barX - 6 : barX + barW + 6}
              y={y + rowH / 2 + 14}
              textAnchor={side === 'left' ? 'end' : 'start'}
              fill="currentColor"
              fontSize={10}
              dominantBaseline="middle"
              className="fill-muted-foreground"
            >
              {node.devices.toLocaleString()} devices · {node.pings.toLocaleString()} visits
            </text>
          )}
        </g>
      );
    });
  };

  return (
    <div className="w-full overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ minWidth: 600, maxHeight: 600 }}>
        {/* Column headers */}
        <text x={nodeColW / 2} y={16} textAnchor="middle" fontSize={12} fontWeight="bold" className="fill-muted-foreground">
          Before Visit
        </text>
        <text x={cx + centerW / 2} y={16} textAnchor="middle" fontSize={12} fontWeight="bold" className="fill-foreground">
          {targetLabel}
        </text>
        <text x={cx + centerW + gapX + nodeColW / 2} y={16} textAnchor="middle" fontSize={12} fontWeight="bold" className="fill-muted-foreground">
          After Visit
        </text>

        {/* Center node */}
        <rect
          x={cx}
          y={centerY}
          width={centerW}
          height={centerH}
          rx={8}
          className="fill-primary/20 stroke-primary"
          strokeWidth={2}
        />
        <text
          x={cx + centerW / 2}
          y={centerY + centerH / 2}
          textAnchor="middle"
          dominantBaseline="middle"
          fontSize={11}
          fontWeight="bold"
          className="fill-primary"
        >
          🎯
        </text>

        {/* Left side (before) */}
        {renderSide(beforeNodes, 'left')}

        {/* Right side (after) */}
        {renderSide(afterNodes, 'right')}
      </svg>
    </div>
  );
}
