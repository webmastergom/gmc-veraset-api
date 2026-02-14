'use client';

import { useEffect, useRef } from 'react';
import { MapContainer, TileLayer, Polyline, CircleMarker, Popup, useMap } from 'react-leaflet';
import L from 'leaflet';
import type { LatLngExpression } from 'leaflet';

import 'leaflet/dist/leaflet.css';

export interface DeviceMovement {
  adId: string;
  points: { lat: number; lng: number; utc: string }[];
}

export interface PoiPosition {
  poiId: string;
  lat: number;
  lng: number;
  name?: string;
}

interface MovementMapInnerProps {
  devices: DeviceMovement[];
  deviceIndices: number[];
  getColor: (index: number) => string;
  pois?: PoiPosition[];
  poiRadius?: number;
  poiFillColor?: string;
  poiBorderColor?: string;
}

function FitBounds({ devices }: { devices: DeviceMovement[] }) {
  const map = useMap();

  useEffect(() => {
    if (devices.length === 0) return;
    const allPoints: LatLngExpression[] = [];
    for (const d of devices) {
      for (const p of d.points) {
        allPoints.push([p.lat, p.lng]);
      }
    }
    if (allPoints.length === 0) return;
    const b = L.latLngBounds(allPoints);
    map.fitBounds(b, { padding: [40, 40], maxZoom: 14 });
  }, [map, devices]);

  return null;
}

export function MovementMapInner({
  devices,
  deviceIndices,
  getColor,
  pois = [],
  poiRadius = 8,
  poiFillColor = '#ef4444',
  poiBorderColor = '#b91c1c',
}: MovementMapInnerProps) {
  return (
    <MapContainer
      center={[40.4, -3.7]}
      zoom={6}
      className="h-96 w-full"
      style={{ minHeight: 384 }}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />
      <FitBounds devices={devices} />
      {devices.map((d, i) => {
        const colorIndex = deviceIndices[i] ?? i;
        const positions: LatLngExpression[] = d.points.map((p) => [p.lat, p.lng] as LatLngExpression);
        if (positions.length < 2) return null;
        return (
          <Polyline
            key={d.adId}
            positions={positions}
            pathOptions={{
              color: getColor(colorIndex),
              weight: 2,
              opacity: 0.8,
            }}
          />
        );
      })}
      {pois.map((p) => (
        <CircleMarker
          key={p.poiId}
          center={[p.lat, p.lng]}
          radius={poiRadius}
          pathOptions={{
            color: poiBorderColor,
            fillColor: poiFillColor,
            fillOpacity: 0.85,
            weight: 2,
          }}
        >
          <Popup>
            <span className="font-medium">{p.name || p.poiId}</span>
            <br />
            <span className="text-xs text-muted-foreground font-mono">{p.poiId}</span>
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
}
