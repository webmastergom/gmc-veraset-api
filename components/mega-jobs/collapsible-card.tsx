'use client';

import { useState, type ReactNode } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { ChevronDown, ChevronRight, Download } from 'lucide-react';

interface CollapsibleCardProps {
  title: string;
  icon?: ReactNode;
  children: ReactNode;
  defaultOpen?: boolean;
  downloadHref?: string;
  downloadLabel?: string;
  /** Extra actions in the header */
  headerActions?: ReactNode;
}

export function CollapsibleCard({
  title,
  icon,
  children,
  defaultOpen = true,
  downloadHref,
  downloadLabel = 'CSV',
  headerActions,
}: CollapsibleCardProps) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <Card>
      <CardHeader
        className="flex flex-row items-center justify-between cursor-pointer select-none"
        onClick={() => setOpen(!open)}
      >
        <CardTitle className="flex items-center gap-2 text-base">
          {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          {icon}
          {title}
        </CardTitle>
        <div className="flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
          {headerActions}
          {downloadHref && (
            <a href={downloadHref}>
              <Button variant="outline" size="sm">
                <Download className="h-4 w-4 mr-1" /> {downloadLabel}
              </Button>
            </a>
          )}
        </div>
      </CardHeader>
      {open && <CardContent>{children}</CardContent>}
    </Card>
  );
}
