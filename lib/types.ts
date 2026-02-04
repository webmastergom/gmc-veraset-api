/**
 * Type definitions for export functionality
 */

export interface ExportFilters {
  minDwellTime?: number | null;   // In SECONDS. null = no filter (all devices)
  minPings?: number | null;        // Minimum pings per device. null = no filter
  dateFrom?: string;               // Filter within dataset date range
  dateTo?: string;
  poiIds?: string[];               // Filter specific POIs
}

export interface ExportResult {
  success: boolean;
  deviceCount: number;
  totalDevicesInDataset?: number;
  filters: ExportFilters;
  filePath: string;                // S3 path to exported CSV
  downloadUrl?: string;
  createdAt: string;
  error?: string;
  details?: string;
}
