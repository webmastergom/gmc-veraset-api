'use client';

import { useEffect, useRef, useState } from 'react';
import dynamic from 'next/dynamic';
import 'leaflet/dist/leaflet.css';

interface ZipEntry {
  zipCode: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  deviceDays: number;
  /** Trade-area capture ring tier. When present, the map can be toggled
   *  to color polygons by ring (70/80/90) instead of by raw heat. */
  captureRing?: 70 | 80 | 90 | null;
}

interface CatchmentMapProps {
  data: ZipEntry[];
}

type ViewMode = 'heat' | 'trade-area';

/** Trade-area ring colors. Sequential green→amber→orange→gray maps to
 *  "core" / "secondary" / "fringe" / "outside trade area". */
function getRingColor(ring: 70 | 80 | 90 | null | undefined): string {
  if (ring === 70) return '#10b981'; // emerald — core
  if (ring === 80) return '#f59e0b'; // amber — secondary
  if (ring === 90) return '#f97316'; // orange — fringe
  return '#6b7280';                  // gray — long tail
}

function ringLabel(ring: 70 | 80 | 90 | null | undefined): string {
  if (ring === 70) return '70% capture (core)';
  if (ring === 80) return '80% capture (secondary)';
  if (ring === 90) return '90% capture (fringe)';
  return 'Long tail';
}

/**
 * Heatmap color scale: green → yellow → orange → red
 * Uses log scale for better distribution across wide ranges.
 */
function getHeatColor(value: number, maxVal: number): string {
  if (maxVal <= 0 || value <= 0) return '#1a9641'; // green for zero/min
  const logRatio = Math.min(Math.log(value + 1) / Math.log(maxVal + 1), 1);

  // 4-stop gradient: green → yellow → orange → red
  const stops = [
    { pos: 0, r: 26, g: 150, b: 65 },   // green
    { pos: 0.33, r: 166, g: 217, b: 106 }, // light green
    { pos: 0.66, r: 253, g: 174, b: 97 },  // orange
    { pos: 1, r: 215, g: 25, b: 28 },    // red
  ];

  let i = 0;
  while (i < stops.length - 1 && stops[i + 1].pos < logRatio) i++;
  const a = stops[i];
  const b = stops[Math.min(i + 1, stops.length - 1)];
  const t = a.pos === b.pos ? 0 : (logRatio - a.pos) / (b.pos - a.pos);

  const r = Math.round(a.r + t * (b.r - a.r));
  const g = Math.round(a.g + t * (b.g - a.g));
  const bl = Math.round(a.b + t * (b.b - a.b));
  return `rgb(${r},${g},${bl})`;
}

/**
 * Choropleth heatmap showing catchment origins as colored postal code polygons.
 * Falls back to circle markers for zip codes without GeoJSON polygons.
 */
function CatchmentMapInner({ data }: CatchmentMapProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstance = useRef<any>(null);
  const [showMarkers, setShowMarkers] = useState(false);
  const markersLayerRef = useRef<any>(null);
  const [loading, setLoading] = useState(false);
  // Switch between continuous heat (device-days gradient) and discrete
  // trade-area rings (70/80/90% capture). Default = heat.
  const [viewMode, setViewMode] = useState<ViewMode>('heat');
  const hasRings = data.some((d) => d.captureRing != null);
  const choroplethLayerRef = useRef<any>(null);
  const legendRef = useRef<any>(null);
  const LRef = useRef<any>(null);
  const maxValRef = useRef<number>(1);

  useEffect(() => {
    if (!mapRef.current || typeof window === 'undefined') return;
    let cancelled = false;

    import('leaflet').then(async (L) => {
      if (cancelled || !mapRef.current) return;
      if (mapInstance.current) {
        mapInstance.current.remove();
      }

      const validData = data.filter((d) => d.lat !== 0 && d.lng !== 0 && d.zipCode !== 'UNKNOWN');
      if (validData.length === 0) return;

      // Calculate bounds from data
      const lats = validData.map((d) => d.lat);
      const lngs = validData.map((d) => d.lng);
      const bounds = L.latLngBounds(
        [Math.min(...lats) - 0.5, Math.min(...lngs) - 0.5],
        [Math.max(...lats) + 0.5, Math.max(...lngs) + 0.5],
      );

      const map = L.map(mapRef.current!, {
        zoomControl: true,
        attributionControl: false,
      }).fitBounds(bounds, { padding: [30, 30] });

      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        maxZoom: 18,
      }).addTo(map);

      mapInstance.current = map;

      const maxVal = Math.max(...validData.map((d) => d.deviceDays));

      // ── Fetch GeoJSON polygons ──────────────────────────────────────
      setLoading(true);
      let geojsonData: any = null;
      const matchedZips = new Set<string>();

      try {
        const entries = validData.map((d) => ({
          zipCode: d.zipCode,
          deviceDays: d.deviceDays,
          city: d.city,
          country: d.country,
          captureRing: d.captureRing ?? null,
        }));

        const res = await fetch('/api/geojson/catchment', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ entries }),
        });

        if (res.ok) {
          geojsonData = await res.json();
        }
      } catch (err) {
        console.warn('[CatchmentMap] Failed to fetch GeoJSON polygons:', err);
      }
      if (cancelled || !mapRef.current) return;
      setLoading(false);

      // ── Render GeoJSON choropleth (heat or trade-area) ─────────────
      LRef.current = L;
      maxValRef.current = maxVal;
      if (geojsonData?.features?.length) {
        for (const f of geojsonData.features) {
          matchedZips.add(f.properties.zipCode);
        }

        const choropleth = L.geoJSON(geojsonData, {
          style: (feature: any) => {
            const p = feature?.properties || {};
            const dd = p.deviceDays || 0;
            const fillColor = viewMode === 'trade-area'
              ? getRingColor(p.captureRing)
              : getHeatColor(dd, maxVal);
            return { fillColor, fillOpacity: 0.7, color: '#333', weight: 0.5 };
          },
          onEachFeature: (feature: any, layer: any) => {
            const p = feature.properties;
            const ringRow = p.captureRing != null
              ? `<br/><span style="opacity:0.75">${ringLabel(p.captureRing)}</span>`
              : '';
            layer.bindPopup(
              `<strong>${p.zipCode}</strong><br/>${p.city || ''}, ${p.country || ''}<br/><b>${(p.deviceDays || 0).toLocaleString()}</b> device-days${ringRow}`
            );
          },
        }).addTo(map);
        choroplethLayerRef.current = choropleth;

        if (geojsonData.features.length > 5) {
          const gjBounds = choropleth.getBounds();
          if (gjBounds.isValid()) {
            map.fitBounds(gjBounds, { padding: [30, 30] });
          }
        }
      }

      // ── Circle markers: unmatched always visible, all data on toggle ─
      const unmatchedGroup = L.layerGroup();
      const allMarkersGroup = L.layerGroup();

      for (const d of validData) {
        const ratio = d.deviceDays / maxVal;
        const radius = Math.max(5, Math.min(25, ratio * 25));
        const isMatched = matchedZips.has(d.zipCode);

        const marker = L.circleMarker([d.lat, d.lng], {
          radius,
          fillColor: getHeatColor(d.deviceDays, maxVal),
          fillOpacity: 0.7,
          color: '#fff',
          weight: 1,
        }).bindPopup(
          `<strong>${d.zipCode}</strong><br/>${d.city}, ${d.country}<br/><b>${d.deviceDays.toLocaleString()}</b> device-days`
        );

        allMarkersGroup.addLayer(marker);
        if (!isMatched) {
          unmatchedGroup.addLayer(marker);
        }
      }

      // Always show unmatched as fallback
      unmatchedGroup.addTo(map);
      markersLayerRef.current = { all: allMarkersGroup, unmatched: unmatchedGroup };

      // ── Legend (rebuilt on mode change via the effect below) ───────
      const buildLegend = (mode: ViewMode) => new (L.Control.extend({
        onAdd: () => {
          const div = L.DomUtil.create('div', 'leaflet-control');
          div.style.cssText =
            'background: rgba(0,0,0,0.8); padding: 8px 12px; border-radius: 6px; color: #fff; font-size: 12px;';
          if (mode === 'trade-area') {
            const tiers: Array<[70 | 80 | 90 | null, string]> = [
              [70, 'Core (top 70%)'],
              [80, 'Secondary (70–80%)'],
              [90, 'Fringe (80–90%)'],
              [null, 'Long tail'],
            ];
            const rows = tiers.map(([r, label]) =>
              `<div style="display:flex;align-items:center;gap:6px;margin-top:2px;">
                <span style="display:inline-block;width:14px;height:14px;background:${getRingColor(r)};border:1px solid #555"></span>
                <span>${label}</span>
              </div>`
            ).join('');
            div.innerHTML = `<div style="margin-bottom:4px;font-weight:600;">Trade Area</div>${rows}`;
          } else {
            const steps = 5;
            let gradient = '';
            for (let i = 0; i <= steps; i++) {
              const val = (maxVal / steps) * i;
              gradient += `<span style="display:inline-block;width:${100 / (steps + 1)}%;height:14px;background:${getHeatColor(val, maxVal)}"></span>`;
            }
            div.innerHTML = `
              <div style="margin-bottom:4px;font-weight:600;">Device-Days</div>
              <div style="display:flex;">${gradient}</div>
              <div style="display:flex;justify-content:space-between;margin-top:2px;">
                <span>0</span>
                <span>${maxVal.toLocaleString()}</span>
              </div>
            `;
          }
          return div;
        },
      }))({ position: 'bottomright' });
      const legend = buildLegend(viewMode);
      legend.addTo(map);
      legendRef.current = { instance: legend, build: buildLegend };
    });

    return () => {
      cancelled = true;
      if (mapInstance.current) {
        mapInstance.current.remove();
        mapInstance.current = null;
      }
    };
  }, [data]);

  // Toggle markers overlay: show ALL markers on top of polygons
  useEffect(() => {
    const map = mapInstance.current;
    const layers = markersLayerRef.current;
    if (!map || !layers) return;

    if (showMarkers) {
      // Remove unmatched-only, show all
      if (map.hasLayer(layers.unmatched)) map.removeLayer(layers.unmatched);
      layers.all.addTo(map);
    } else {
      // Remove all, show unmatched-only
      if (map.hasLayer(layers.all)) map.removeLayer(layers.all);
      layers.unmatched.addTo(map);
    }
  }, [showMarkers]);

  // Re-style polygons + rebuild legend when viewMode flips. No re-fetch
  // of the GeoJSON, no map remount — just an instant color swap.
  useEffect(() => {
    const map = mapInstance.current;
    const choropleth = choroplethLayerRef.current;
    const legendInfo = legendRef.current;
    const maxVal = maxValRef.current;
    if (!map || !choropleth) return;
    choropleth.setStyle((feature: any) => {
      const p = feature?.properties || {};
      const dd = p.deviceDays || 0;
      const fillColor = viewMode === 'trade-area'
        ? getRingColor(p.captureRing)
        : getHeatColor(dd, maxVal);
      return { fillColor, fillOpacity: 0.7, color: '#333', weight: 0.5 };
    });
    if (legendInfo?.instance && legendInfo?.build) {
      legendInfo.instance.remove();
      const fresh = legendInfo.build(viewMode);
      fresh.addTo(map);
      legendRef.current = { instance: fresh, build: legendInfo.build };
    }
  }, [viewMode]);

  if (!data?.length) {
    return (
      <div className="h-[500px] flex items-center justify-center text-muted-foreground">
        No catchment map data available.
      </div>
    );
  }

  return (
    <div className="relative">
      {loading && (
        <div className="absolute top-2 left-1/2 -translate-x-1/2 z-[1000] bg-background/80 px-3 py-1 rounded text-sm text-muted-foreground">
          Loading polygons...
        </div>
      )}
      <div className="absolute top-2 left-2 z-[1000] flex items-center gap-2">
        <button
          onClick={() => setShowMarkers((v) => !v)}
          className="bg-background/90 px-3 py-1.5 rounded text-xs font-medium border border-border hover:bg-accent transition-colors"
        >
          {showMarkers ? 'Hide markers' : 'Show markers'}
        </button>
        {hasRings && (
          <div className="inline-flex rounded-md border border-border bg-background/90 overflow-hidden">
            <button
              type="button"
              onClick={() => setViewMode('heat')}
              className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                viewMode === 'heat'
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:bg-accent'
              }`}
              title="Continuous heat gradient by device-days"
            >
              Heat
            </button>
            <button
              type="button"
              onClick={() => setViewMode('trade-area')}
              className={`px-3 py-1.5 text-xs font-medium transition-colors border-l border-border ${
                viewMode === 'trade-area'
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:bg-accent'
              }`}
              title="Discrete colors by 70/80/90% capture ring"
            >
              Trade Area
            </button>
          </div>
        )}
      </div>
      <div ref={mapRef} className="h-[500px] w-full rounded-lg" />
    </div>
  );
}

// Export with SSR disabled (Leaflet requires window)
export const CatchmentMap = dynamic(() => Promise.resolve(CatchmentMapInner), {
  ssr: false,
  loading: () => (
    <div className="h-[500px] flex items-center justify-center text-muted-foreground">
      Loading map...
    </div>
  ),
});
