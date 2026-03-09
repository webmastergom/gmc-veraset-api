import { getApiKeyColor } from '@/lib/api-key-colors';

export function ExternalBadge({ apiKeyName }: { apiKeyName?: string }) {
  const color = getApiKeyColor(apiKeyName);

  return (
    <span className={`text-xs px-2 py-0.5 rounded-full ${color.bg} ${color.text} border ${color.border}`}>
      {apiKeyName || 'External'}
    </span>
  );
}
