'use client';

import { useState, useEffect, useCallback } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Settings,
  Globe,
  Database,
  Save,
  Plus,
  Trash2,
  Loader2,
  CheckCircle,
  AlertTriangle,
  FolderSync,
  FileText,
  FileCheck,
  AlertCircle,
  RefreshCw,
  Import,
} from 'lucide-react';

// ── Types ──────────────────────────────────────────────────────────────

interface CountryDatasetEntry {
  dataset: string;
  label: string;
}

interface CountryDatasetConfig {
  entries: Record<string, CountryDatasetEntry>;
  updatedAt: string;
}

interface StagingListing {
  handle: string;
  csv: { size: number; lastModified: string } | null;
  spec: boolean;
  error: string | null;
  importing: boolean;
}

const ALL_COUNTRIES: Record<string, string> = {
  AR: 'Argentina', BE: 'Belgium', CL: 'Chile', CO: 'Colombia',
  CR: 'Costa Rica', DE: 'Germany', DO: 'Dominican Republic', EC: 'Ecuador',
  ES: 'Spain', FR: 'France', GT: 'Guatemala', HN: 'Honduras',
  IE: 'Ireland', IT: 'Italy', MX: 'Mexico', NI: 'Nicaragua',
  NL: 'Netherlands', PA: 'Panama', PE: 'Peru', PT: 'Portugal',
  SE: 'Sweden', SV: 'El Salvador', UK: 'United Kingdom', US: 'United States',
};

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function timeAgo(dateStr: string): string {
  if (!dateStr) return '';
  const diff = Date.now() - new Date(dateStr).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

// ── Main Component ─────────────────────────────────────────────────────

export default function SettingsPage() {
  // Country-Dataset state
  const [config, setConfig] = useState<CountryDatasetConfig>({ entries: {}, updatedAt: '' });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [datasets, setDatasets] = useState<{ id: string; name: string }[]>([]);
  const [newCountry, setNewCountry] = useState('');
  const [newDataset, setNewDataset] = useState('');

  // Staging state
  const [stagingListings, setStagingListings] = useState<StagingListing[]>([]);
  const [stagingLoading, setStagingLoading] = useState(false);
  const [deletingHandle, setDeletingHandle] = useState<string | null>(null);

  const loadConfig = useCallback(async () => {
    try {
      const res = await fetch('/api/settings/country-datasets');
      const data = await res.json();
      setConfig(data);
    } catch {
      setError('Failed to load config');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadDatasets = useCallback(async () => {
    try {
      const res = await fetch('/api/datasets');
      if (res.ok) {
        const data = await res.json();
        const list = (data.datasets || [])
          .map((d: any) => ({ id: d.id || d.name || d, name: d.name || d.id || d }))
          .filter((d: any) => d.id);
        setDatasets(list);
      }
    } catch {
      // Datasets list is optional
    }
  }, []);

  const loadStaging = useCallback(async () => {
    setStagingLoading(true);
    try {
      const res = await fetch('/api/settings/staging');
      if (res.ok) {
        const data = await res.json();
        setStagingListings(data.listings || []);
      }
    } catch {
      // Staging list is optional
    } finally {
      setStagingLoading(false);
    }
  }, []);

  useEffect(() => {
    loadConfig();
    loadDatasets();
    loadStaging();
  }, [loadConfig, loadDatasets, loadStaging]);

  const handleDeleteStaging = async (handle: string) => {
    if (!confirm(`Delete "${handle}" and all associated files from staging?`)) return;
    setDeletingHandle(handle);
    try {
      const res = await fetch(`/api/settings/staging?handle=${encodeURIComponent(handle)}`, { method: 'DELETE' });
      if (res.ok) {
        setStagingListings(prev => prev.filter(l => l.handle !== handle));
      }
    } catch {
      // ignore
    } finally {
      setDeletingHandle(null);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    setSaved(false);
    setError(null);
    try {
      const res = await fetch('/api/settings/country-datasets', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ entries: config.entries }),
      });
      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || 'Failed to save');
      }
      const data = await res.json();
      setConfig(data);
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  };

  const handleAdd = () => {
    if (!newCountry || !newDataset.trim()) return;
    setConfig(prev => ({
      ...prev,
      entries: {
        ...prev.entries,
        [newCountry]: {
          dataset: newDataset.trim(),
          label: ALL_COUNTRIES[newCountry] || newCountry,
        },
      },
    }));
    setNewCountry('');
    setNewDataset('');
  };

  const handleRemove = (code: string) => {
    setConfig(prev => {
      const next = { ...prev, entries: { ...prev.entries } };
      delete next.entries[code];
      return next;
    });
  };

  const handleDatasetChange = (code: string, dataset: string) => {
    setConfig(prev => ({
      ...prev,
      entries: {
        ...prev.entries,
        [code]: { ...prev.entries[code], dataset },
      },
    }));
  };

  const configuredCodes = Object.keys(config.entries).sort();
  const availableCodes = Object.keys(ALL_COUNTRIES)
    .filter(c => !config.entries[c])
    .sort();

  return (
    <MainLayout>
      <div className="space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Settings className="w-6 h-6" />
            Settings
          </h1>
          <p className="text-muted-foreground mt-1">
            Configure system-wide settings
          </p>
        </div>

        <Tabs defaultValue="country-datasets" className="space-y-4">
          <TabsList>
            <TabsTrigger value="country-datasets" className="flex items-center gap-1.5">
              <Globe className="w-4 h-4" />
              Country → Dataset
            </TabsTrigger>
            <TabsTrigger value="karlsgate" className="flex items-center gap-1.5">
              <FolderSync className="w-4 h-4" />
              Karlsgate Staging
            </TabsTrigger>
          </TabsList>

          {/* ── Tab 1: Country → Dataset ─────────────────────────────── */}
          <TabsContent value="country-datasets">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      <Globe className="w-5 h-5" />
                      Country → Dataset Mapping
                    </CardTitle>
                    <CardDescription>
                      Assign a default dataset to each country for the external postal-maid API.
                      When an external client sends a country code, the system will use the assigned dataset.
                    </CardDescription>
                  </div>
                  <Button onClick={handleSave} disabled={saving}>
                    {saving ? (
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    ) : saved ? (
                      <CheckCircle className="w-4 h-4 mr-2" />
                    ) : (
                      <Save className="w-4 h-4 mr-2" />
                    )}
                    {saving ? 'Saving...' : saved ? 'Saved!' : 'Save'}
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {loading ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
                  </div>
                ) : (
                  <div className="space-y-4">
                    {error && (
                      <div className="flex items-center gap-2 text-destructive text-sm bg-destructive/10 rounded-lg p-3">
                        <AlertTriangle className="w-4 h-4" />
                        {error}
                      </div>
                    )}

                    {/* Add new country */}
                    <div className="flex items-end gap-3 p-4 bg-secondary/50 rounded-lg">
                      <div className="flex-shrink-0 w-48">
                        <Label className="text-xs mb-1 block">Country</Label>
                        <Select value={newCountry} onValueChange={setNewCountry}>
                          <SelectTrigger>
                            <SelectValue placeholder="Select country..." />
                          </SelectTrigger>
                          <SelectContent>
                            {availableCodes.map(code => (
                              <SelectItem key={code} value={code}>
                                {code} — {ALL_COUNTRIES[code]}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="flex-1">
                        <Label className="text-xs mb-1 block">Dataset</Label>
                        {datasets.length > 0 ? (
                          <Select value={newDataset} onValueChange={setNewDataset}>
                            <SelectTrigger>
                              <SelectValue placeholder="Select dataset..." />
                            </SelectTrigger>
                            <SelectContent>
                              {datasets.map(ds => (
                                <SelectItem key={ds.id} value={ds.id}>
                                  {ds.name !== ds.id ? `${ds.name} (${ds.id})` : ds.id}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        ) : (
                          <Input
                            value={newDataset}
                            onChange={e => setNewDataset(e.target.value)}
                            placeholder="e.g. Spain-Cities-Pois-March-2026"
                          />
                        )}
                      </div>
                      <Button onClick={handleAdd} disabled={!newCountry || !newDataset.trim()}>
                        <Plus className="w-4 h-4 mr-1" /> Add
                      </Button>
                    </div>

                    {/* Configured countries table */}
                    {configuredCodes.length > 0 ? (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead className="w-24">Code</TableHead>
                            <TableHead className="w-48">Country</TableHead>
                            <TableHead>Dataset</TableHead>
                            <TableHead className="w-20"></TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {configuredCodes.map(code => {
                            const entry = config.entries[code];
                            return (
                              <TableRow key={code}>
                                <TableCell>
                                  <Badge variant="outline" className="font-mono">{code}</Badge>
                                </TableCell>
                                <TableCell className="font-medium">{entry.label}</TableCell>
                                <TableCell>
                                  <div className="flex items-center gap-2">
                                    <Database className="w-4 h-4 text-muted-foreground" />
                                    {datasets.length > 0 ? (
                                      <Select
                                        value={entry.dataset}
                                        onValueChange={v => handleDatasetChange(code, v)}
                                      >
                                        <SelectTrigger className="w-full">
                                          <SelectValue />
                                        </SelectTrigger>
                                        <SelectContent>
                                          {datasets.map(ds => (
                                            <SelectItem key={ds.id} value={ds.id}>
                                              {ds.name !== ds.id ? `${ds.name} (${ds.id})` : ds.id}
                                            </SelectItem>
                                          ))}
                                        </SelectContent>
                                      </Select>
                                    ) : (
                                      <Input
                                        value={entry.dataset}
                                        onChange={e => handleDatasetChange(code, e.target.value)}
                                        className="font-mono text-sm"
                                      />
                                    )}
                                  </div>
                                </TableCell>
                                <TableCell>
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => handleRemove(code)}
                                    className="text-destructive hover:text-destructive"
                                  >
                                    <Trash2 className="w-4 h-4" />
                                  </Button>
                                </TableCell>
                              </TableRow>
                            );
                          })}
                        </TableBody>
                      </Table>
                    ) : (
                      <div className="text-center py-8 text-muted-foreground">
                        <Globe className="w-8 h-8 mx-auto mb-2 opacity-50" />
                        <p>No countries configured yet.</p>
                        <p className="text-sm">Add a country above to assign a dataset.</p>
                      </div>
                    )}

                    {config.updatedAt && (
                      <p className="text-xs text-muted-foreground text-right">
                        Last saved: {new Date(config.updatedAt).toLocaleString()}
                      </p>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          {/* ── Tab 2: Karlsgate Staging ─────────────────────────────── */}
          <TabsContent value="karlsgate">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="flex items-center gap-2">
                      <FolderSync className="w-5 h-5" />
                      Karlsgate Staging
                    </CardTitle>
                    <CardDescription>
                      Files in the S3 staging folder waiting to be processed by the Karlsgate node.
                      The node polls every ~10 minutes and auto-deletes files after successful import.
                    </CardDescription>
                  </div>
                  <Button variant="outline" onClick={loadStaging} disabled={stagingLoading}>
                    {stagingLoading ? (
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    ) : (
                      <RefreshCw className="w-4 h-4 mr-2" />
                    )}
                    Refresh
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {stagingLoading && stagingListings.length === 0 ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
                  </div>
                ) : stagingListings.length > 0 ? (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Listing Handle</TableHead>
                        <TableHead className="w-24">CSV</TableHead>
                        <TableHead className="w-24">Spec</TableHead>
                        <TableHead className="w-32">Status</TableHead>
                        <TableHead className="w-32">Uploaded</TableHead>
                        <TableHead className="w-16"></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {stagingListings.map(listing => (
                        <TableRow key={listing.handle}>
                          <TableCell className="font-mono text-sm">
                            {listing.handle}
                          </TableCell>
                          <TableCell>
                            {listing.csv ? (
                              <span className="flex items-center gap-1 text-sm">
                                <FileText className="w-3.5 h-3.5 text-muted-foreground" />
                                {formatBytes(listing.csv.size)}
                              </span>
                            ) : (
                              <span className="text-muted-foreground text-sm">—</span>
                            )}
                          </TableCell>
                          <TableCell>
                            {listing.spec ? (
                              <FileCheck className="w-4 h-4 text-green-500" />
                            ) : (
                              <span className="text-muted-foreground text-sm">—</span>
                            )}
                          </TableCell>
                          <TableCell>
                            {listing.error ? (
                              <Badge variant="destructive" className="text-xs gap-1">
                                <AlertCircle className="w-3 h-3" />
                                Error
                              </Badge>
                            ) : listing.importing ? (
                              <Badge className="text-xs gap-1 bg-blue-600">
                                <Import className="w-3 h-3" />
                                Importing
                              </Badge>
                            ) : listing.csv && listing.spec ? (
                              <Badge variant="outline" className="text-xs gap-1">
                                <Loader2 className="w-3 h-3 animate-spin" />
                                Pending
                              </Badge>
                            ) : (
                              <Badge variant="secondary" className="text-xs">
                                Incomplete
                              </Badge>
                            )}
                          </TableCell>
                          <TableCell className="text-sm text-muted-foreground">
                            {listing.csv ? timeAgo(listing.csv.lastModified) : '—'}
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleDeleteStaging(listing.handle)}
                              disabled={deletingHandle === listing.handle}
                              className="text-destructive hover:text-destructive"
                            >
                              {deletingHandle === listing.handle ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                              ) : (
                                <Trash2 className="w-4 h-4" />
                              )}
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                ) : (
                  <div className="text-center py-8 text-muted-foreground">
                    <FolderSync className="w-8 h-8 mx-auto mb-2 opacity-50" />
                    <p>No files in staging.</p>
                    <p className="text-sm">Files appear here when you activate a dataset. Karlsgate auto-deletes them after processing.</p>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </MainLayout>
  );
}
