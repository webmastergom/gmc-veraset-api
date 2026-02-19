'use client';

import { useState } from 'react';
import { Switch } from '@/components/ui/switch';
import { Users, CheckCircle, Loader2 } from 'lucide-react';

interface AudienceAgentToggleProps {
  jobId: string;
  initialEnabled: boolean;
  disabled?: boolean;
}

export function AudienceAgentToggle({ jobId, initialEnabled, disabled }: AudienceAgentToggleProps) {
  const [enabled, setEnabled] = useState(initialEnabled);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const toggle = async (checked: boolean) => {
    // Optimistic update — flip immediately so the UI feels instant
    setEnabled(checked);
    setSaving(true);
    setSaved(false);
    setError(null);

    try {
      const res = await fetch(`/api/jobs/${jobId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ audienceAgentEnabled: checked }),
      });

      if (!res.ok) {
        // Revert on failure
        setEnabled(!checked);
        setError('Failed to save');
        return;
      }

      // Show brief success indicator
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (err) {
      console.error('Failed to update audience agent toggle:', err);
      // Revert on failure
      setEnabled(!checked);
      setError('Network error');
    } finally {
      setSaving(false);
    }
  };

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
