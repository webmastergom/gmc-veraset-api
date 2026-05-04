'use client';

import { useEffect } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import type { LatLngExpression } from 'leaflet';
import 'leaflet/dist/leaflet.css';

export interface ComparePoi {
  side: 'A' | 'B';
  poiId: string;
  name?: string;
  lat?: number;
  lng?: number;
  overlapDevices: number;
}

interface Props {
  pois: ComparePoi[];
  dsALabel: string;
  dsBLabel: string;
  /**
   * Selection keys of the form `${side}-${poiId}` shared with the chart.
   * Selected POIs render bigger / yellow / on top; unselected ones dim
   * when there's any active selection. Clicking a SELECTED marker on the
   * map calls onTogglePoi to deselect it (per UX agreement: chart is the
   * only "input" for selection, the map can only reduce it).
   */
  selectedPoiKeys?: Set<string>;
  onTogglePoi?: (key: string) => void;
}

const SELECTED_FILL = '#facc15';     // yellow-400 (matches chart highlight)
const SELECTED_BORDER = '#ffffff';

function FitBounds({ pois }: { pois: ComparePoi[] }) {
  const map = useMap();
  useEffect(() => {
    const pts: LatLngExpression[] = pois
      .filter(p => Number.isFinite(p.lat) && Number.isFinite(p.lng))
      .map(p => [p.lat as number, p.lng as number] as LatLngExpression);
    if (pts.length === 0) return;
    map.fitBounds(L.latLngBounds(pts), { padding: [40, 40], maxZoom: 14 });
  }, [map, pois]);
  return null;
}

function radiusFor(overlapDevices: number, max: number): number {
  if (max <= 0) return 6;
  const minR = 5, maxR = 22;
  const t = Math.min(1, Math.log(overlapDevices + 1) / Math.log(max + 1));
  return minR + t * (maxR - minR);
}

export default function CompareMapInner({ pois, dsALabel, dsBLabel, selectedPoiKeys, onTogglePoi }: Props) {
  const placed = pois.filter(p => Number.isFinite(p.lat) && Number.isFinite(p.lng));
  const max = placed.reduce((m, p) => Math.max(m, p.overlapDevices), 0);
  const center: LatLngExpression = placed[0] ? [placed[0].lat as number, placed[0].lng as number] : [0, 0];
  const hasSelection = !!selectedPoiKeys && selectedPoiKeys.size > 0;

  // Render unselected first, then selected — so highlighted markers naturally
  // sit on top of the dimmed ones (Leaflet draws in render order).
  const sorted = [...placed].sort((a, b) => {
    const aSel = selectedPoiKeys?.has(`${a.side}-${a.poiId}`) ? 1 : 0;
    const bSel = selectedPoiKeys?.has(`${b.side}-${b.poiId}`) ? 1 : 0;
    return aSel - bSel;
  });

  return (
    <MapContainer center={center} zoom={4} style={{ height: '100%', width: '100%' }} scrollWheelZoom>
      <TileLayer
        attribution='&copy; OpenStreetMap contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <FitBounds pois={placed} />
      {sorted.map((p, i) => {
        const sideColor = p.side === 'A' ? '#3b82f6' : '#f97316'; // blue / orange
        const baseR = radiusFor(p.overlapDevices, max);
        const key = `${p.side}-${p.poiId}`;
        const isSel = selectedPoiKeys?.has(key) ?? false;

        const fillColor = isSel ? SELECTED_FILL : sideColor;
        const borderColor = isSel ? SELECTED_BORDER : sideColor;
        const radius = isSel ? baseR * 1.6 : baseR;
        const fillOpacity = isSel ? 0.9 : (hasSelection ? 0.2 : 0.55);
        const weight = isSel ? 3 : 1.5;

        return (
          <CircleMarker
            key={`${key}-${i}`}
            center={[p.lat as number, p.lng as number]}
            radius={radius}
            pathOptions={{ color: borderColor, fillColor, fillOpacity, weight }}
            eventHandlers={isSel && onTogglePoi ? {
              click: () => onTogglePoi(key),
            } : undefined}
          >
            <Popup>
              <div className="text-xs">
                <div className="font-semibold">{p.name || p.poiId}</div>
                <div className="text-muted-foreground">Side {p.side} — {p.side === 'A' ? dsALabel : dsBLabel}</div>
                <div>Overlap devices: <b>{p.overlapDevices.toLocaleString()}</b></div>
                <div className="text-[10px] text-muted-foreground">{p.poiId}</div>
                {isSel && (
                  <div className="text-[10px] text-yellow-600 mt-1">
                    Highlighted — click to deselect
                  </div>
                )}
              </div>
            </Popup>
          </CircleMarker>
        );
      })}
    </MapContainer>
  );
}
