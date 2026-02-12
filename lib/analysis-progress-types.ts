/**
 * Types for analysis progress tracking
 * Separated to allow client-side imports
 */

export interface AnalysisProgress {
  step: AnalysisStep;
  progress: number; // 0-100
  message: string;
  details?: string;
  error?: string;
}

export type AnalysisStep =
  | 'initializing'
  | 'checking_table'
  | 'creating_table'
  | 'discovering_partitions'
  | 'adding_partitions'
  | 'running_queries'
  | 'processing_results'
  | 'completed'
  | 'error';
