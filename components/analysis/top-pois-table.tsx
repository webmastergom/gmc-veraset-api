import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';

interface PoiStat {
  poiId: string;
  pings: number;
  devices: number;
  originalId?: string;
  displayName?: string;
}

interface TopPoisTableProps {
  data: PoiStat[];
  jobId?: string;
}

export function TopPoisTable({ data, jobId }: TopPoisTableProps) {
  if (!data || data.length === 0) {
    return (
      <div className="py-8 text-center text-muted-foreground">
        No POI data available
      </div>
    );
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>#</TableHead>
          <TableHead>POI ID</TableHead>
          <TableHead className="text-right">Devices</TableHead>
          <TableHead className="text-right">Pings</TableHead>
          <TableHead className="text-right">Avg Pings/Device</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {data.map((poi, i) => {
          const displayName = poi.displayName || poi.originalId || poi.poiId;
          const showVerasetId = poi.originalId && poi.originalId !== poi.poiId;
          
          return (
            <TableRow key={poi.poiId}>
              <TableCell>{i + 1}</TableCell>
              <TableCell>
                <div className="font-mono text-sm">{displayName}</div>
                {showVerasetId && (
                  <div className="text-xs text-muted-foreground italic">
                    Veraset ID: {poi.poiId}
                  </div>
                )}
              </TableCell>
              <TableCell className="text-right">{poi.devices.toLocaleString()}</TableCell>
              <TableCell className="text-right">{poi.pings.toLocaleString()}</TableCell>
              <TableCell className="text-right">
                {poi.devices > 0 ? (poi.pings / poi.devices).toFixed(1) : '0'}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
