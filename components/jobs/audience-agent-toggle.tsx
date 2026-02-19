'use client';

import { useState } from 'react';
import { Switch } from '@/components/ui/switch';
import { Users } from 'lucide-react';

interface AudienceAgentToggleProps {
  jobId: string;
  initialEnabled: boolean;
  disabled?: boolean;
}

export function AudienceAgentToggle({ jobId, initialEnabled, disabled }: AudienceAgentToggleProps) {
  const [enabled, setEnabled] = useState(initialEnabled);
  const [saving, setSaving] = useState(false);

  const toggle = async (checked: boolean) => {
    setSaving(true);
    try {
      const res = await fetch(`/api/jobs/${jobId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ audienceAgentEnabled: checked }),
      });
      if (res.ok) {
        setEnabled(checked);
      }
    } catch (err) {
      console.error('Failed to update audience agent toggle:', err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-3">
        <Users className="h-4 w-4 text-muted-foreground" />
        <div>
          <p className="text-sm font-medium">Enable Audience Agent</p>
          <p className="text-xs text-muted-foreground">
            Allow this dataset to be used for audience segment analysis
          </p>
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
