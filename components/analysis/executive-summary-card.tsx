'use client';

import { useState } from 'react';
import { Copy, Check, FileText } from 'lucide-react';

interface ExecutiveSummary {
  generatedAt: string;
  headline: string;
  bullets: string[];
  paragraph: string;
}

interface ExecutiveSummaryCardProps {
  summary?: ExecutiveSummary | null;
}

/**
 * Sales-ready executive summary block — headline + bullets + narrative
 * paragraph, all in English and pre-formatted for copy-paste into a
 * commercial proposal slide. The Markdown-style ** wrappers in
 * bullets are rendered as <strong>.
 */
export function ExecutiveSummaryCard({ summary }: ExecutiveSummaryCardProps) {
  const [copiedBullets, setCopiedBullets] = useState(false);
  const [copiedParagraph, setCopiedParagraph] = useState(false);

  if (!summary) return null;

  const copyText = async (text: string, setter: (b: boolean) => void) => {
    try {
      await navigator.clipboard.writeText(text);
      setter(true);
      setTimeout(() => setter(false), 1500);
    } catch {}
  };

  // Render **bold** markers as <strong>.
  const renderInline = (s: string) => {
    const parts = s.split(/(\*\*[^*]+\*\*|\*[^*]+\*)/g);
    return parts.map((p, idx) => {
      if (p.startsWith('**') && p.endsWith('**')) {
        return <strong key={idx} className="text-foreground">{p.slice(2, -2)}</strong>;
      }
      if (p.startsWith('*') && p.endsWith('*')) {
        return <em key={idx}>{p.slice(1, -1)}</em>;
      }
      return <span key={idx}>{p}</span>;
    });
  };

  // Plain-text copy (markdown stripped).
  const bulletsAsText = summary.bullets
    .map((b) => `• ${b.replace(/\*\*/g, '').replace(/\*/g, '')}`)
    .join('\n');

  return (
    <div className="border rounded-lg p-4 bg-muted/20 space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm font-medium">
          <FileText className="h-4 w-4" />
          Executive Summary
          <span className="text-xs text-muted-foreground/70 ml-2 font-normal">
            (auto-generated, in English)
          </span>
        </div>
        <button
          type="button"
          onClick={() => copyText(bulletsAsText, setCopiedBullets)}
          className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded border border-border hover:bg-accent transition-colors"
          title="Copy bullets to clipboard"
        >
          {copiedBullets ? <><Check className="h-3 w-3" /> Copied</> : <><Copy className="h-3 w-3" /> Copy bullets</>}
        </button>
      </div>

      <div className="text-base font-medium leading-relaxed">
        {renderInline(summary.headline)}
      </div>

      {summary.bullets.length > 0 && (
        <ul className="space-y-1.5 text-sm">
          {summary.bullets.map((b, idx) => (
            <li key={idx} className="leading-relaxed flex gap-2">
              <span className="text-muted-foreground/60 select-none">•</span>
              <span>{renderInline(b)}</span>
            </li>
          ))}
        </ul>
      )}

      {summary.paragraph && (
        <div className="pt-3 border-t border-border/40 space-y-2">
          <div className="flex items-center justify-between">
            <div className="text-xs uppercase tracking-wider text-muted-foreground">Narrative version</div>
            <button
              type="button"
              onClick={() => copyText(summary.paragraph, setCopiedParagraph)}
              className="inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded border border-border hover:bg-accent transition-colors"
              title="Copy paragraph to clipboard"
            >
              {copiedParagraph ? <><Check className="h-3 w-3" /> Copied</> : <><Copy className="h-3 w-3" /> Copy</>}
            </button>
          </div>
          <p className="text-sm text-muted-foreground leading-relaxed italic">
            {summary.paragraph}
          </p>
        </div>
      )}
    </div>
  );
}
