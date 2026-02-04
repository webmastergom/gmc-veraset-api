'use client';

import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { ExportDialog } from './export-dialog';
import { Download } from 'lucide-react';

interface ExportButtonProps {
  datasetName: string;
  variant?: 'default' | 'outline' | 'ghost' | 'link' | 'destructive' | 'secondary';
  size?: 'default' | 'sm' | 'lg' | 'icon';
}

export function ExportButton({ datasetName, variant = 'outline', size = 'default' }: ExportButtonProps) {
  const [open, setOpen] = useState(false);
  
  return (
    <>
      <Button variant={variant} size={size} onClick={() => setOpen(true)}>
        <Download className="h-4 w-4 mr-2" />
        Export Devices
      </Button>
      <ExportDialog
        datasetName={datasetName}
        open={open}
        onOpenChange={setOpen}
      />
    </>
  );
}
