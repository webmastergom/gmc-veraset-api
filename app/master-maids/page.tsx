'use client'

import { useEffect, useState, useCallback } from 'react'
import { MainLayout } from '@/components/layout/main-layout'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Fingerprint,
  RefreshCw,
  Download,
  ChevronDown,
  ChevronRight,
  Loader2,
  Trash2,
  Database,
  Tag,
  Calendar,
} from 'lucide-react'

const COUNTRY_FLAGS: Record<string, string> = {
  ES: '🇪🇸', MX: '🇲🇽', FR: '🇫🇷', PA: '🇵🇦', CR: '🇨🇷',
  GB: '🇬🇧', UK: '🇬🇧', IT: '🇮🇹', NL: '🇳🇱', DE: '🇩🇪', US: '🇺🇸',
  CO: '🇨🇴', AR: '🇦🇷', CL: '🇨🇱', BR: '🇧🇷', PE: '🇵🇪',
  PT: '🇵🇹', BE: '🇧🇪', CH: '🇨🇭', AT: '🇦🇹', IE: '🇮🇪',
  DO: '🇩🇴', GT: '🇬🇹', HN: '🇭🇳', SV: '🇸🇻', EC: '🇪🇨',
}

const COUNTRY_NAMES: Record<string, string> = {
  ES: 'Spain', MX: 'Mexico', FR: 'France', PA: 'Panama', CR: 'Costa Rica',
  GB: 'United Kingdom', UK: 'United Kingdom', IT: 'Italy', NL: 'Netherlands', DE: 'Germany', US: 'United States',
  CO: 'Colombia', AR: 'Argentina', CL: 'Chile', BR: 'Brazil', PE: 'Peru',
  PT: 'Portugal', BE: 'Belgium', CH: 'Switzerland', AT: 'Austria', IE: 'Ireland',
  DO: 'Dominican Republic', GT: 'Guatemala', HN: 'Honduras', SV: 'El Salvador', EC: 'Ecuador',
}

const ATTR_COLORS: Record<string, string> = {
  plain: 'bg-gray-500/20 text-gray-400',
  nse_bracket: 'bg-purple-500/20 text-purple-400',
  category: 'bg-blue-500/20 text-blue-400',
  catchment: 'bg-green-500/20 text-green-400',
}

interface CountrySummary {
  country: string
  contributionCount: number
  lastConsolidatedAt: string | null
  totalMaids: number | null
  isEstimate?: boolean
  attributeCount: number
  datasetCount: number
  dateRange: { from: string | null; to: string | null } | null
}

interface Contribution {
  id: string
  s3Key: string
  attributeType: string
  attributeValue: string
  sourceDataset: string
  dateRange: { from: string; to: string }
  registeredAt: string
}

interface AttributeStat {
  attributeType: string
  attributeValue: string
  maidCount: number
  oldestData: string
  newestData: string
}

interface CountryDetail {
  country: string
  lastConsolidatedAt: string | null
  stats: {
    totalMaids: number
    byAttribute: AttributeStat[]
    byDataset: Record<string, number>
  } | null
  contributions: Contribution[]
}

function formatNumber(n: number | null): string {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString()
}

function formatDate(d: string | null): string {
  if (!d || d === 'unknown') return '—'
  return new Date(d).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

function formatShortDate(d: string | null): string {
  if (!d || d === 'unknown') return '—'
  return d.slice(0, 10)
}

export default function MasterMaidsPage() {
  const [countries, setCountries] = useState<CountrySummary[]>([])
  const [globalTotal, setGlobalTotal] = useState(0)
  const [hasEstimates, setHasEstimates] = useState(false)
  const [loading, setLoading] = useState(true)
  const [expandedCountry, setExpandedCountry] = useState<string | null>(null)
  const [countryDetail, setCountryDetail] = useState<CountryDetail | null>(null)
  const [jobNames, setJobNames] = useState<Record<string, string>>({})
  const [loadingDetail, setLoadingDetail] = useState(false)
  const [consolidating, setConsolidating] = useState<string | null>(null)
  const [consolidateProgress, setConsolidateProgress] = useState<string>('')
  const [deduplicating, setDeduplicating] = useState(false)

  const loadCountries = useCallback(async () => {
    try {
      const res = await fetch('/api/master-maids', { credentials: 'include' })
      const data = await res.json()
      const cs = data.countries || []
      setCountries(cs)
      setGlobalTotal(data.globalTotal || 0)
      setHasEstimates(cs.some((c: CountrySummary) => c.isEstimate))
    } catch (e) {
      console.error('Failed to load master MAIDs:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { loadCountries() }, [loadCountries])

  const loadDetail = async (cc: string) => {
    if (expandedCountry === cc) {
      setExpandedCountry(null)
      setCountryDetail(null)
      return
    }
    setExpandedCountry(cc)
    setLoadingDetail(true)
    try {
      const res = await fetch(`/api/master-maids/${cc}`, { credentials: 'include' })
      const data = await res.json()
      setCountryDetail(data)
      setJobNames(data.jobNames || {})
    } catch (e) {
      console.error('Failed to load detail:', e)
    } finally {
      setLoadingDetail(false)
    }
  }

  const handleConsolidate = async (cc: string) => {
    setConsolidating(cc)
    setConsolidateProgress('Starting...')

    try {
      // Reset any previous state
      let data = await fetch(`/api/master-maids/${cc}/consolidate`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ reset: true }),
      }).then(r => r.json())

      while (data.phase !== 'done' && data.phase !== 'error') {
        setConsolidateProgress(data.progress?.message || 'Processing...')
        await new Promise(r => setTimeout(r, 4000))

        data = await fetch(`/api/master-maids/${cc}/consolidate`, {
          method: 'POST',
          credentials: 'include',
        }).then(r => r.json())
      }

      if (data.phase === 'error') {
        setConsolidateProgress(`Error: ${data.error}`)
        setTimeout(() => setConsolidateProgress(''), 5000)
      } else {
        setConsolidateProgress(`Done! ${formatNumber(data.totalMaids)} unique MAIDs`)
        // Reload everything
        await loadCountries()
        if (expandedCountry === cc) {
          await loadDetail(cc)
        }
        setTimeout(() => setConsolidateProgress(''), 5000)
      }
    } catch (e: any) {
      setConsolidateProgress(`Error: ${e.message}`)
      setTimeout(() => setConsolidateProgress(''), 5000)
    } finally {
      setConsolidating(null)
    }
  }

  const handleDeduplicate = async () => {
    setDeduplicating(true)
    try {
      const res = await fetch('/api/master-maids', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'deduplicate' }),
      })
      const data = await res.json()
      if (data.totalRemoved > 0) {
        alert(`Removed ${data.totalRemoved} duplicate contributions. Please re-consolidate each country.`)
      } else {
        alert('No duplicates found.')
      }
      await loadCountries()
      if (expandedCountry) await loadDetail(expandedCountry)
    } catch (e: any) {
      alert(`Error: ${e.message}`)
    } finally {
      setDeduplicating(false)
    }
  }

  const handleRemoveContribution = async (cc: string, id: string) => {
    try {
      await fetch(`/api/master-maids/${cc}?id=${id}`, {
        method: 'DELETE',
        credentials: 'include',
      })
      await loadDetail(cc)
      await loadCountries()
    } catch (e) {
      console.error('Failed to remove contribution:', e)
    }
  }

  return (
    <MainLayout>
      <div className="max-w-6xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center gap-3">
            <Fingerprint className="h-8 w-8 text-theme-accent" />
            <div>
              <h1 className="text-2xl font-bold">Master MAIDs by Country</h1>
              <p className="text-sm text-muted-foreground">
                Deduplicated device lists enriched with attributes across all datasets
              </p>
            </div>
          </div>
          {countries.length > 0 && (
            <Button
              size="sm"
              variant="outline"
              onClick={handleDeduplicate}
              disabled={deduplicating}
            >
              {deduplicating ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <RefreshCw className="h-4 w-4 mr-2" />}
              Deduplicate Index
            </Button>
          )}
        </div>

        {!loading && globalTotal > 0 && (
          <Card className="mb-6 bg-gradient-to-r from-theme-accent/10 to-transparent border-theme-accent/30">
            <CardContent className="py-4 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <span className="text-2xl">🌍</span>
                <div>
                  <div className="text-sm text-muted-foreground">Global Total</div>
                  <div className="text-sm text-muted-foreground">{countries.length} countries</div>
                </div>
              </div>
              <div className="text-right">
                <div className="text-3xl font-bold tabular-nums text-theme-accent">
                  {hasEstimates ? '~' : ''}{formatNumber(globalTotal)}
                </div>
                <div className="text-sm text-muted-foreground">
                  {hasEstimates ? 'MAIDs worldwide (consolidate for exact count)' : 'unique MAIDs worldwide'}
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {loading ? (
          <div className="flex items-center gap-2 text-muted-foreground py-12 justify-center">
            <Loader2 className="w-5 h-5 animate-spin" />
            Loading...
          </div>
        ) : countries.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              <Fingerprint className="h-12 w-12 mx-auto mb-4 opacity-30" />
              <p className="text-lg font-medium">No master MAID lists yet</p>
              <p className="text-sm mt-1">
                Export MAIDs (Download MAIDs, MAIDs by NSE, or MAIDs by Category) from any dataset to start building your master lists.
              </p>
            </CardContent>
          </Card>
        ) : (
          <div className="space-y-4">
            {countries.map(c => (
              <Card key={c.country} className="overflow-hidden">
                <CardHeader
                  className="cursor-pointer hover:bg-secondary/50 transition-colors"
                  onClick={() => loadDetail(c.country)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      {expandedCountry === c.country
                        ? <ChevronDown className="h-4 w-4 text-muted-foreground" />
                        : <ChevronRight className="h-4 w-4 text-muted-foreground" />
                      }
                      <span className="text-2xl">{COUNTRY_FLAGS[c.country] || '🌍'}</span>
                      <div>
                        <CardTitle className="text-lg">
                          {COUNTRY_NAMES[c.country] || c.country}
                          <span className="ml-2 text-sm font-normal text-muted-foreground">{c.country}</span>
                        </CardTitle>
                        <CardDescription className="flex items-center gap-4 mt-1">
                          <span className="flex items-center gap-1">
                            <Database className="h-3 w-3" />
                            {c.datasetCount} dataset{c.datasetCount !== 1 ? 's' : ''}
                          </span>
                          <span className="flex items-center gap-1">
                            <Tag className="h-3 w-3" />
                            {c.contributionCount} contribution{c.contributionCount !== 1 ? 's' : ''}
                          </span>
                          {c.dateRange?.from && (
                            <span className="flex items-center gap-1">
                              <Calendar className="h-3 w-3" />
                              {formatShortDate(c.dateRange.from)} → {formatShortDate(c.dateRange.to)}
                            </span>
                          )}
                        </CardDescription>
                      </div>
                    </div>

                    <div className="flex items-center gap-3">
                      {c.totalMaids !== null && (
                        <div className="text-right">
                          <div className="text-xl font-bold tabular-nums">
                            {c.isEstimate ? '~' : ''}{formatNumber(c.totalMaids)}
                          </div>
                          <div className="text-xs text-muted-foreground">
                            {c.isEstimate ? 'MAIDs (not deduplicated)' : 'unique MAIDs'}
                          </div>
                        </div>
                      )}
                      {c.attributeCount > 0 && (
                        <Badge variant="outline" className="text-xs">
                          {c.attributeCount} attributes
                        </Badge>
                      )}
                      {!c.lastConsolidatedAt && c.contributionCount > 0 && (
                        <Badge className="bg-yellow-500/20 text-yellow-400 text-xs">
                          Not consolidated
                        </Badge>
                      )}
                    </div>
                  </div>
                </CardHeader>

                {expandedCountry === c.country && (
                  <CardContent className="border-t border-border pt-4">
                    {loadingDetail ? (
                      <div className="flex items-center gap-2 text-muted-foreground py-4">
                        <Loader2 className="w-4 h-4 animate-spin" />
                        Loading details...
                      </div>
                    ) : countryDetail ? (
                      <div className="space-y-6">
                        {/* Actions */}
                        <div className="flex items-center gap-3">
                          <Button
                            size="sm"
                            onClick={(e) => { e.stopPropagation(); handleConsolidate(c.country) }}
                            disabled={consolidating === c.country}
                          >
                            {consolidating === c.country ? (
                              <Loader2 className="h-4 w-4 animate-spin mr-2" />
                            ) : (
                              <RefreshCw className="h-4 w-4 mr-2" />
                            )}
                            Consolidate
                          </Button>
                          {countryDetail.lastConsolidatedAt && (
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={(e) => {
                                e.stopPropagation()
                                window.open(`/api/master-maids/${c.country}/download`, '_blank')
                              }}
                            >
                              <Download className="h-4 w-4 mr-2" />
                              Download Parquet
                            </Button>
                          )}
                          {consolidateProgress && (
                            <span className="text-sm text-muted-foreground">{consolidateProgress}</span>
                          )}
                          {countryDetail.lastConsolidatedAt && (
                            <span className="text-xs text-muted-foreground ml-auto">
                              Last consolidated: {formatDate(countryDetail.lastConsolidatedAt)}
                            </span>
                          )}
                        </div>

                        {/* Attribute Breakdown */}
                        {countryDetail.stats?.byAttribute && countryDetail.stats.byAttribute.length > 0 && (
                          <div>
                            <h3 className="text-sm font-semibold mb-2">Attribute Breakdown</h3>
                            <Table>
                              <TableHeader>
                                <TableRow>
                                  <TableHead>Type</TableHead>
                                  <TableHead>Value</TableHead>
                                  <TableHead className="text-right">MAIDs</TableHead>
                                  <TableHead className="text-right">%</TableHead>
                                  <TableHead className="text-right">Data Period</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {countryDetail.stats.byAttribute.map((attr, i) => (
                                  <TableRow key={i}>
                                    <TableCell>
                                      <Badge className={ATTR_COLORS[attr.attributeType] || 'bg-gray-500/20 text-gray-400'}>
                                        {attr.attributeType}
                                      </Badge>
                                    </TableCell>
                                    <TableCell className="font-medium">
                                      {attr.attributeValue || '(all devices)'}
                                    </TableCell>
                                    <TableCell className="text-right tabular-nums">
                                      {formatNumber(attr.maidCount)}
                                    </TableCell>
                                    <TableCell className="text-right tabular-nums text-muted-foreground">
                                      {countryDetail.stats!.totalMaids > 0
                                        ? ((attr.maidCount / countryDetail.stats!.totalMaids) * 100).toFixed(1) + '%'
                                        : '—'}
                                    </TableCell>
                                    <TableCell className="text-right text-xs text-muted-foreground">
                                      {formatShortDate(attr.oldestData)} → {formatShortDate(attr.newestData)}
                                    </TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        )}

                        {/* Dataset Breakdown */}
                        {countryDetail.stats?.byDataset && Object.keys(countryDetail.stats.byDataset).length > 0 && (
                          <div>
                            <h3 className="text-sm font-semibold mb-2">Contributing Datasets</h3>
                            <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                              {Object.entries(countryDetail.stats.byDataset).map(([ds, count]) => (
                                <div key={ds} className="flex items-center justify-between px-3 py-2 bg-secondary rounded text-sm">
                                  <span className="truncate">{jobNames[ds] || ds}</span>
                                  <Badge variant="outline" className="ml-2 text-xs">{count}</Badge>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Contributions List */}
                        <div>
                          <h3 className="text-sm font-semibold mb-2">
                            Contributions ({countryDetail.contributions.length})
                          </h3>
                          <div className="max-h-[300px] overflow-y-auto border rounded">
                            <Table>
                              <TableHeader>
                                <TableRow>
                                  <TableHead>Type</TableHead>
                                  <TableHead>Value</TableHead>
                                  <TableHead>Dataset</TableHead>
                                  <TableHead>Date Range</TableHead>
                                  <TableHead>Registered</TableHead>
                                  <TableHead></TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {countryDetail.contributions.map(contrib => (
                                  <TableRow key={contrib.id}>
                                    <TableCell>
                                      <Badge className={`${ATTR_COLORS[contrib.attributeType] || ''} text-xs`}>
                                        {contrib.attributeType}
                                      </Badge>
                                    </TableCell>
                                    <TableCell className="text-sm">{contrib.attributeValue || '—'}</TableCell>
                                    <TableCell className="text-sm">{jobNames[contrib.sourceDataset] || contrib.sourceDataset}</TableCell>
                                    <TableCell className="text-xs text-muted-foreground">
                                      {formatShortDate(contrib.dateRange?.from)} → {formatShortDate(contrib.dateRange?.to)}
                                    </TableCell>
                                    <TableCell className="text-xs text-muted-foreground">
                                      {formatDate(contrib.registeredAt)}
                                    </TableCell>
                                    <TableCell>
                                      <Button
                                        size="sm"
                                        variant="ghost"
                                        className="h-6 w-6 p-0 text-muted-foreground hover:text-red-400"
                                        onClick={(e) => {
                                          e.stopPropagation()
                                          handleRemoveContribution(c.country, contrib.id)
                                        }}
                                      >
                                        <Trash2 className="h-3 w-3" />
                                      </Button>
                                    </TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        </div>
                      </div>
                    ) : null}
                  </CardContent>
                )}
              </Card>
            ))}
          </div>
        )}
      </div>
    </MainLayout>
  )
}
