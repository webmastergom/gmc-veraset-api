'use client'

import { useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { MainLayout } from '@/components/layout/main-layout'
import { Card, CardContent } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  Target, Download, Search, X, Layers, Database, Loader2, ExternalLink,
} from 'lucide-react'

interface LibraryItem {
  sourceType: 'dataset' | 'mega'
  sourceId: string
  sourceLabel: string
  slug: string
  label: string
  groupKey: string | null
  categories: string[]
  matchMode: 'OR' | 'AND'
  country: string | null
  generatedAt: string
  totalZips: number
  totalDevicesWithZip: number
  totalMaids?: number
  downloadUrl: string
}

export default function AffinityLibraryPage() {
  const [items, setItems] = useState<LibraryItem[]>([])
  const [loading, setLoading] = useState(true)
  const [query, setQuery] = useState('')
  const [sourceFilter, setSourceFilter] = useState<'all' | 'dataset' | 'mega'>('all')

  useEffect(() => {
    fetch('/api/affinity-library', { credentials: 'include' })
      .then((r) => r.json())
      .then((data) => setItems(Array.isArray(data?.items) ? data.items : []))
      .catch(() => setItems([]))
      .finally(() => setLoading(false))
  }, [])

  // Filter on label + source name + categories (case-insensitive). Memoized
  // so we only re-filter when items, query, or sourceFilter change.
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    return items.filter((it) => {
      if (sourceFilter !== 'all' && it.sourceType !== sourceFilter) return false
      if (!q) return true
      return (
        it.label.toLowerCase().includes(q) ||
        it.sourceLabel.toLowerCase().includes(q) ||
        (it.country || '').toLowerCase().includes(q) ||
        it.categories.some((c) => c.toLowerCase().includes(q))
      )
    })
  }, [items, query, sourceFilter])

  // Parent href so the user can jump to the originating job/megajob.
  const parentHref = (it: LibraryItem) =>
    it.sourceType === 'mega' ? `/mega-jobs/${it.sourceId}` : `/datasets/${it.sourceId}`

  return (
    <MainLayout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold flex items-center gap-2">
            <Target className="h-6 w-6" /> Affinity Library
          </h1>
          <p className="text-muted-foreground mt-1">
            Every category-affinity index generated across jobs and mega-jobs. Download the canonical 8-column CSV with one click.
          </p>
        </div>

        {/* Filters — search + source toggle. The source toggle is a tabbed
            row so it's obvious there are three states. */}
        {!loading && items.length > 0 && (
          <div className="flex items-center gap-3 flex-wrap">
            <div className="relative flex-1 min-w-[260px]">
              <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
              <Input
                type="search"
                placeholder={`Search ${items.length} affinity export${items.length === 1 ? '' : 's'}…`}
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="pl-9 pr-9"
                aria-label="Search affinity exports"
              />
              {query && (
                <button
                  type="button"
                  onClick={() => setQuery('')}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  aria-label="Clear search"
                >
                  <X className="h-4 w-4" />
                </button>
              )}
            </div>
            <div className="inline-flex rounded-md border border-input overflow-hidden text-sm">
              {(['all', 'mega', 'dataset'] as const).map((opt) => (
                <button
                  key={opt}
                  type="button"
                  onClick={() => setSourceFilter(opt)}
                  className={`px-3 py-1.5 ${sourceFilter === opt ? 'bg-primary text-primary-foreground' : 'bg-background hover:bg-muted'} ${opt !== 'all' ? 'border-l border-input' : ''}`}
                >
                  {opt === 'all' ? 'All sources' : opt === 'mega' ? 'Mega-jobs' : 'Datasets'}
                </button>
              ))}
            </div>
          </div>
        )}

        {loading ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : items.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              <Target className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p className="text-lg font-medium">No affinity indices yet</p>
              <p className="mt-1">
                Generate one from any job or mega-job via{' '}
                <span className="font-medium">MAIDs by POI Category → Generate Affinity CSV</span>.
              </p>
            </CardContent>
          </Card>
        ) : filtered.length === 0 ? (
          <div className="text-center py-16 text-muted-foreground">
            <p className="text-lg">No affinity exports match your filter.</p>
            <p className="text-sm mt-2">
              <button
                type="button"
                className="underline hover:text-foreground"
                onClick={() => { setQuery(''); setSourceFilter('all') }}
              >
                Clear filters
              </button>
            </p>
          </div>
        ) : (
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-muted/50 border-b">
                    <tr>
                      <th className="text-left px-4 py-2 font-medium">Affinity</th>
                      <th className="text-left px-4 py-2 font-medium">Source</th>
                      <th className="text-left px-4 py-2 font-medium">Country</th>
                      <th className="text-left px-4 py-2 font-medium">Mode</th>
                      <th className="text-right px-4 py-2 font-medium">Zips</th>
                      <th className="text-right px-4 py-2 font-medium" title="Devices placed by zip — and coverage % of the originating MAID count when known">
                        Devices (placed)
                      </th>
                      <th className="text-left px-4 py-2 font-medium">Generated</th>
                      <th className="text-right px-4 py-2 font-medium">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((it) => (
                      <tr key={`${it.sourceType}:${it.sourceId}:${it.slug}`} className="border-b border-border/50 hover:bg-muted/30">
                        <td className="px-4 py-2">
                          <div className="font-medium">{it.label}</div>
                          {it.categories.length > 0 && (
                            <div className="text-xs text-muted-foreground truncate max-w-[280px]" title={it.categories.join(', ')}>
                              {it.categories.slice(0, 3).join(', ')}
                              {it.categories.length > 3 && ` +${it.categories.length - 3} more`}
                            </div>
                          )}
                        </td>
                        <td className="px-4 py-2">
                          <Link href={parentHref(it)} className="inline-flex items-center gap-1 hover:underline">
                            {it.sourceType === 'mega'
                              ? <Layers className="h-3.5 w-3.5 text-purple-400" />
                              : <Database className="h-3.5 w-3.5 text-blue-400" />}
                            <span className="truncate max-w-[200px]">{it.sourceLabel}</span>
                            <ExternalLink className="h-3 w-3 opacity-50" />
                          </Link>
                        </td>
                        <td className="px-4 py-2 text-muted-foreground">{it.country || '—'}</td>
                        <td className="px-4 py-2">
                          <Badge variant="outline" className="font-mono text-[10px]">
                            {it.matchMode}
                          </Badge>
                        </td>
                        <td className="px-4 py-2 text-right tabular-nums">{it.totalZips.toLocaleString()}</td>
                        <td className="px-4 py-2 text-right tabular-nums">
                          {it.totalDevicesWithZip.toLocaleString()}
                          {it.totalMaids && it.totalMaids > 0 && (
                            <span className="ml-1 text-xs text-muted-foreground">
                              / {it.totalMaids.toLocaleString()} ({Math.round((it.totalDevicesWithZip / it.totalMaids) * 100)}%)
                            </span>
                          )}
                        </td>
                        <td className="px-4 py-2 text-muted-foreground whitespace-nowrap">
                          {it.generatedAt ? new Date(it.generatedAt).toLocaleString() : '—'}
                        </td>
                        <td className="px-4 py-2 text-right">
                          <a
                            href={it.downloadUrl}
                            className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md border border-input hover:bg-muted"
                          >
                            <Download className="h-3 w-3" />
                            CSV
                          </a>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {(query || sourceFilter !== 'all') && (
                <div className="px-4 py-2 border-t text-xs text-muted-foreground">
                  Showing {filtered.length} of {items.length}
                </div>
              )}
            </CardContent>
          </Card>
        )}
      </div>
    </MainLayout>
  )
}
