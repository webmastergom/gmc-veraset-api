'use client';

import { useState, useCallback } from 'react';
import { Switch } from '@/components/ui/switch';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Users, CheckCircle, Loader2, Globe } from 'lucide-react';

// GMC supported countries
const GMC_COUNTRIES = [
  { code: 'AR', name: 'Argentina' },
  { code: 'BE', name: 'Belgium' },
  { code: 'CL', name: 'Chile' },
  { code: 'CO', name: 'Colombia' },
  { code: 'CR', name: 'Costa Rica' },
  { code: 'DO', name: 'Dominican Republic' },
  { code: 'EC', name: 'Ecuador' },
  { code: 'SV', name: 'El Salvador' },
  { code: 'FR', name: 'France' },
  { code: 'DE', name: 'Germany' },
  { code: 'GT', name: 'Guatemala' },
  { code: 'HN', name: 'Honduras' },
  { code: 'IE', name: 'Ireland' },
  { code: 'IT', name: 'Italy' },
  { code: 'MX', name: 'Mexico' },
  { code: 'NL', name: 'Netherlands' },
  { code: 'NI', name: 'Nicaragua' },
  { code: 'PA', name: 'Panama' },
  { code: 'PT', name: 'Portugal' },
  { code: 'ES', name: 'Spain' },
  { code: 'SE', name: 'Sweden' },
  { code: 'UK', name: 'United Kingdom' },
] as const;

interface AudienceAgentToggleProps {
  jobId: string;
  initialEnabled: boolean;
  initialCountry?: string;
  disabled?: boolean;
}

const MAX_RETRIES = 3;

async function patchWithRetry(
  url: string,
  body: Record<string, unknown>,
  retries = MAX_RETRIES,
): Promise<Response> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify(body),
      });

      if (res.ok) return res;

      if (res.status >= 500 && attempt < retries) {
        console.warn(`PATCH attempt ${attempt} failed (${res.status}), retrying...`);
        await new Promise(r => setTimeout(r, 500 * attempt));
        continue;
      }

      return res;
    } catch (err) {
      if (attempt < retries) {
        console.warn(`PATCH attempt ${attempt} network error, retrying...`);
        await new Promise(r => setTimeout(r, 500 * attempt));
        continue;
      }
      throw err;
    }
  }
  throw new Error('Unreachable');
}

export function AudienceAgentToggle({ jobId, initialEnabled, initialCountry, disabled }: AudienceAgentToggleProps) {
  const [enabled, setEnabled] = useState(initialEnabled);
  const [country, setCountry] = useState(initialCountry || '');
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const save = useCallback(async (newEnabled: boolean, newCountry: string) => {
    setSaving(true);
    setSaved(false);
    setError(null);

    // Require country when enabling
    if (newEnabled && !newCountry) {
      setSaving(false);
      setError('Select a country first');
      setEnabled(false);
      return;
    }

    try {
      const body: Record<string, unknown> = { audienceAgentEnabled: newEnabled };
      if (newCountry) body.country = newCountry;

      const res = await patchWithRetry(`/api/jobs/${jobId}`, body);

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setEnabled(!newEnabled);
        setError(data.error || `Error ${res.status}`);
        return;
      }

      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (err: any) {
      console.error('Failed to update toggle:', err);
      setEnabled(!newEnabled);
      setError('Network error — please retry');
    } finally {
      setSaving(false);
    }
  }, [jobId]);

  const handleToggle = useCallback((checked: boolean) => {
    setEnabled(checked);
    save(checked, country);
  }, [country, save]);

  const handleCountryChange = useCallback((value: string) => {
    setCountry(value);
    // If already enabled, save the new country immediately
    if (enabled) {
      save(true, value);
    }
  }, [enabled, save]);

  return (
    <div className="space-y-4">
      {/* Toggle row */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Users className="h-4 w-4 text-muted-foreground" />
          <div>
            <div className="flex items-center gap-2">
              <p className="text-sm font-medium">Enable Roamy</p>
              {saving && <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />}
              {saved && <CheckCircle className="h-3 w-3 text-emerald-400" />}
            </div>
            <p className="text-xs text-muted-foreground">
              {enabled
                ? 'Roamy is tracking this dataset'
                : 'Allow Roamy to analyze this dataset for audience segments'}
            </p>
          </div>
        </div>
        <Switch
          checked={enabled}
          onCheckedChange={handleToggle}
          disabled={disabled || saving}
        />
      </div>

      {/* Country selector */}
      <div className="flex items-center gap-3">
        <Globe className="h-4 w-4 text-muted-foreground" />
        <div className="flex-1">
          <label className="text-xs text-muted-foreground uppercase tracking-wider mb-1 block">
            Country
          </label>
          <Select value={country} onValueChange={handleCountryChange} disabled={disabled || saving}>
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Select country..." />
            </SelectTrigger>
            <SelectContent>
              {GMC_COUNTRIES.map(c => (
                <SelectItem key={c.code} value={c.code}>
                  {c.code} — {c.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {error && <p className="text-xs text-red-400">{error}</p>}
    </div>
  );
}
