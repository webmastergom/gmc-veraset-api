'use client';

import { useState, useEffect, useCallback } from 'react';
import { MainLayout } from '@/components/layout/main-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
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
} from 'lucide-react';

interface CountryDatasetEntry {
  dataset: string;
  label: string;
}

interface CountryDatasetConfig {
  entries: Record<string, CountryDatasetEntry>;
  updatedAt: string;
}

const ALL_COUNTRIES: Record<string, string> = {
  AR: 'Argentina', BE: 'Belgium', CL: 'Chile', CO: 'Colombia',
  CR: 'Costa Rica', DE: 'Germany', DO: 'Dominican Republic', EC: 'Ecuador',
  ES: 'Spain', FR: 'France', GT: 'Guatemala', HN: 'Honduras',
  IE: 'Ireland', IT: 'Italy', MX: 'Mexico', NI: 'Nicaragua',
  NL: 'Netherlands', PA: 'Panama', PE: 'Peru', PT: 'Portugal',
  SE: 'Sweden', SV: 'El Salvador', UK: 'United Kingdom', US: 'United States',
};

export default function SettingsPage() {
  const [config, setConfig] = useState<CountryDatasetConfig>({ entries: {}, updatedAt: '' });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [datasets, setDatasets] = useState<string[]>([]);
  const [newCountry, setNewCountry] = useState('');
  const [newDataset, setNewDataset] = useState('');

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
        const names = (data.datasets || []).map((d: any) => d.name || d).filter(Boolean);
        setDatasets(names);
      }
    } catch {
      // Datasets list is optional
    }
  }, []);

  useEffect(() => {
    loadConfig();
    loadDatasets();
  }, [loadConfig, loadDatasets]);

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
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-2">
              <Settings className="w-6 h-6" />
              Settings
            </h1>
            <p className="text-muted-foreground mt-1">
              Configure system-wide settings
            </p>
          </div>
        </div>

        {/* Country → Dataset Config */}
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
                    <Label className="text-xs mb-1 block">Dataset Name</Label>
                    {datasets.length > 0 ? (
                      <Select value={newDataset} onValueChange={setNewDataset}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select dataset..." />
                        </SelectTrigger>
                        <SelectContent>
                          {datasets.map(ds => (
                            <SelectItem key={ds} value={ds}>{ds}</SelectItem>
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
                                        <SelectItem key={ds} value={ds}>{ds}</SelectItem>
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
      </div>
    </MainLayout>
  );
}
