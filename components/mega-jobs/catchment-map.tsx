'use client';

import { useEffect, useRef } from 'react';
import dynamic from 'next/dynamic';
import 'leaflet/dist/leaflet.css';

interface ZipEntry {
  zipCode: string;
  city: string;
  country: string;
  lat: number;
  lng: number;
  deviceDays: number;
}

interface CatchmentMapProps {
  data: ZipEntry[];
}

/**
 * Choropleth-style map showing catchment origins as circles
 * sized and colored by device-days. Uses Leaflet.
 */
function CatchmentMapInner({ data }: CatchmentMapProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstance = useRef<any>(null);

  useEffect(() => {
    if (!mapRef.current || typeof window === 'undefined') return;

    // Dynamic import of leaflet
    import('leaflet').then((L) => {


      if (mapInstance.current) {
        mapInstance.current.remove();
      }

      // Filter valid coordinates
      const validData = data.filter((d) => d.lat !== 0 && d.lng !== 0 && d.zipCode !== 'UNKNOWN');
      if (validData.length === 0) return;

      // Calculate bounds
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

      // Color scale
      const maxVal = Math.max(...validData.map((d) => d.deviceDays));
      const getColor = (val: number) => {
        const ratio = Math.min(val / maxVal, 1);
        const r = Math.round(59 + ratio * (239 - 59));
        const g = Math.round(130 + ratio * (68 - 130));
        const b = Math.round(246 + ratio * (68 - 246));
        return `rgb(${r},${g},${b})`;
      };

      const getRadius = (val: number) => {
        const ratio = val / maxVal;
        return Math.max(5, Math.min(30, ratio * 30));
      };

      // Add circles
      for (const d of validData) {
        L.circleMarker([d.lat, d.lng], {
          radius: getRadius(d.deviceDays),
          fillColor: getColor(d.deviceDays),
          fillOpacity: 0.7,
          color: '#fff',
          weight: 1,
        })
          .bindPopup(
            `<strong>${d.zipCode}</strong><br/>${d.city}, ${d.country}<br/>${d.deviceDays.toLocaleString()} device-days`
          )
          .addTo(map);
      }

      mapInstance.current = map;
    });

    return () => {
      if (mapInstance.current) {
        mapInstance.current.remove();
        mapInstance.current = null;
      }
    };
  }, [data]);

  if (!data?.length) {
    return (
      <div className="h-96 flex items-center justify-center text-muted-foreground">
        No catchment map data available.
      </div>
    );
  }

  return <div ref={mapRef} className="h-96 w-full rounded-lg" />;
}

// Export with SSR disabled (Leaflet requires window)
export const CatchmentMap = dynamic(() => Promise.resolve(CatchmentMapInner), {
  ssr: false,
  loading: () => (
    <div className="h-96 flex items-center justify-center text-muted-foreground">
      Loading map...
    </div>
  ),
});
