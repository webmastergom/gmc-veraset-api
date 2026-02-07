'use client';

import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import Link from 'next/link';
import { MapPin, Zap } from 'lucide-react';
import { EnrichmentDialog } from './enrichment-dialog';

interface CollectionCardProps {
  collection: {
    id: string;
    name: string;
    description: string;
    poi_count: number;
    sources?: Record<string, number | string>;
    enrichedCount?: number;
    enrichedAt?: string;
  };
}

export function CollectionCard({ collection }: CollectionCardProps) {
  const [enrichOpen, setEnrichOpen] = useState(false);
  const [enrichedCount, setEnrichedCount] = useState(collection.enrichedCount || 0);

  return (
    <>
      <Card className="hover:shadow-lg transition-shadow">
        <CardHeader>
          <div className="flex items-start justify-between">
            <div className="flex-1">
              <CardTitle>{collection.name}</CardTitle>
              <CardDescription className="mt-2">
                {collection.description}
              </CardDescription>
            </div>
            <MapPin className="h-5 w-5 text-muted-foreground" />
          </div>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <div className="flex items-end justify-between">
              <div>
                <p className="text-2xl font-bold">{collection.poi_count.toLocaleString()}</p>
                <p className="text-sm text-muted-foreground">POIs</p>
              </div>
              {enrichedCount > 0 && (
                <Badge variant="secondary" className="text-xs">
                  <Zap className="h-3 w-3 mr-1" />
                  {enrichedCount} enriched
                </Badge>
              )}
            </div>

            {collection.sources && (
              <div className="space-y-1">
                <p className="text-sm font-medium">Sources:</p>
                <div className="flex flex-wrap gap-2">
                  {Object.entries(collection.sources).map(([source, count]) => (
                    <span key={source} className="text-xs bg-secondary px-2 py-1 rounded">
                      {source}: {typeof count === 'number' ? count : String(count)}
                    </span>
                  ))}
                </div>
              </div>
            )}

            <div className="flex space-x-2 pt-2">
              <Button
                variant="outline"
                size="sm"
                className="flex-1"
                onClick={() => setEnrichOpen(true)}
              >
                <Zap className="h-4 w-4 mr-1" />
                Enrich
              </Button>
              <Link href={`/pois/${collection.id}`} className="flex-1">
                <Button variant="outline" size="sm" className="w-full">
                  View Details
                </Button>
              </Link>
            </div>
          </div>
        </CardContent>
      </Card>

      <EnrichmentDialog
        open={enrichOpen}
        onOpenChange={setEnrichOpen}
        collectionId={collection.id}
        collectionName={collection.name}
        poiCount={collection.poi_count}
        onEnrichmentApplied={() => {
          // Refresh the enrichment count
          setEnrichedCount(prev => prev + 1); // Will get actual count on page refresh
        }}
      />
    </>
  );
}
