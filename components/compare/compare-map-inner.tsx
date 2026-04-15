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
}

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

export default function CompareMapInner({ pois, dsALabel, dsBLabel }: Props) {
  const placed = pois.filter(p => Number.isFinite(p.lat) && Number.isFinite(p.lng));
  const max = placed.reduce((m, p) => Math.max(m, p.overlapDevices), 0);
  const center: LatLngExpression = placed[0] ? [placed[0].lat as number, placed[0].lng as number] : [0, 0];

  return (
    <MapContainer center={center} zoom={4} style={{ height: '100%', width: '100%' }} scrollWheelZoom>
      <TileLayer
        attribution='&copy; OpenStreetMap contributors'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <FitBounds pois={placed} />
      {placed.map((p, i) => {
        const color = p.side === 'A' ? '#3b82f6' : '#f97316'; // blue / orange
        const r = radiusFor(p.overlapDevices, max);
        return (
          <CircleMarker
            key={`${p.side}-${p.poiId}-${i}`}
            center={[p.lat as number, p.lng as number]}
            radius={r}
            pathOptions={{ color, fillColor: color, fillOpacity: 0.55, weight: 1.5 }}
          >
            <Popup>
              <div className="text-xs">
                <div className="font-semibold">{p.name || p.poiId}</div>
                <div className="text-muted-foreground">Side {p.side} — {p.side === 'A' ? dsALabel : dsBLabel}</div>
                <div>Overlap devices: <b>{p.overlapDevices.toLocaleString()}</b></div>
                <div className="text-[10px] text-muted-foreground">{p.poiId}</div>
              </div>
            </Popup>
          </CircleMarker>
        );
      })}
    </MapContainer>
  );
}
