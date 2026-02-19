'use client';

import { useState, useCallback } from 'react';
import { Switch } from '@/components/ui/switch';
import { Users, CheckCircle, Loader2 } from 'lucide-react';

interface AudienceAgentToggleProps {
  jobId: string;
  initialEnabled: boolean;
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

      // Server error — retry with backoff
      if (res.status >= 500 && attempt < retries) {
        console.warn(`PATCH attempt ${attempt} failed (${res.status}), retrying...`);
        await new Promise(r => setTimeout(r, 500 * attempt));
        continue;
      }

      return res; // 4xx or last attempt — return as-is
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

export function AudienceAgentToggle({ jobId, initialEnabled, disabled }: AudienceAgentToggleProps) {
  const [enabled, setEnabled] = useState(initialEnabled);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const toggle = useCallback(async (checked: boolean) => {
    // Optimistic update — flip immediately so the UI feels instant
    setEnabled(checked);
    setSaving(true);
    setSaved(false);
    setError(null);

    try {
      const res = await patchWithRetry(
        `/api/jobs/${jobId}`,
        { audienceAgentEnabled: checked },
      );

      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setEnabled(!checked);
        setError(data.error || `Error ${res.status}`);
        return;
      }

      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (err: any) {
      console.error('Failed to update toggle:', err);
      setEnabled(!checked);
      setError('Network error — please retry');
    } finally {
      setSaving(false);
    }
  }, [jobId]);

  return (
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
          {error && <p className="text-xs text-red-400 mt-0.5">{error}</p>}
        </div>
      </div>
      <Switch
        checked={enabled}
        onCheckedChange={toggle}
        disabled={disabled || saving}
      />
    </div>
  );
}
