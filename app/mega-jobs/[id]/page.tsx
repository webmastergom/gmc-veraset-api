'use client'

import { useEffect, useState, useCallback } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { MainLayout } from '@/components/layout/main-layout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Layers, ExternalLink, Loader2, Play,
  CheckCircle2, XCircle, Clock,
  BarChart3, TrendingUp, MapPin, Navigation,
  Compass, Timer, Activity,
} from 'lucide-react'

import { CollapsibleCard } from '@/components/mega-jobs/collapsible-card'
import { PoiFilter } from '@/components/mega-jobs/poi-filter'
import { SummaryCards } from '@/components/mega-jobs/summary-cards'
import { MegaDailyChart } from '@/components/mega-jobs/daily-chart'
import { CatchmentPie } from '@/components/mega-jobs/catchment-pie'
import { CatchmentMap } from '@/components/mega-jobs/catchment-map'
import { ODTables } from '@/components/mega-jobs/od-tables'
import { MobilityBar } from '@/components/mega-jobs/mobility-bar'
import { HourlyChart } from '@/components/mega-jobs/hourly-chart'

const statusColors: Record<string, string> = {
  planning: 'bg-blue-500/20 text-blue-400',
  creating: 'bg-yellow-500/20 text-yellow-400',
  running: 'bg-orange-500/20 text-orange-400',
  consolidating: 'bg-purple-500/20 text-purple-400',
  completed: 'bg-green-500/20 text-green-400',
  partial: 'bg-amber-500/20 text-amber-400',
  error: 'bg-red-500/20 text-red-400',
}

const jobStatusIcon = (status: string) => {
  switch (status) {
    case 'SUCCESS': return <CheckCircle2 className="h-4 w-4 text-green-400" />
    case 'FAILED': return <XCircle className="h-4 w-4 text-red-400" />
    case 'RUNNING': return <Loader2 className="h-4 w-4 text-yellow-400 animate-spin" />
    default: return <Clock className="h-4 w-4 text-muted-foreground" />
  }
}

export default function MegaJobDetailPage() {
  const params = useParams()
  const id = params.id as string

  const [megaJob, setMegaJob] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [consolidating, setConsolidating] = useState(false)
  const [consolidateProgress, setConsolidateProgress] = useState<string>('')

  // Reports
  const [visitsReport, setVisitsReport] = useState<any>(null)
  const [temporalReport, setTemporalReport] = useState<any>(null)
  const [odReport, setODReport] = useState<any>(null)
  const [hourlyReport, setHourlyReport] = useState<any>(null)
  const [catchmentReport, setCatchmentReport] = useState<any>(null)
  const [mobilityReport, setMobilityReport] = useState<any>(null)

  // POI filter
  const [selectedPoiIds, setSelectedPoiIds] = useState<string[]>([])

  const loadMegaJob = useCallback(async () => {
    try {
      const res = await fetch(`/api/mega-jobs/${id}`, { credentials: 'include' })
      if (res.ok) {
        const data = await res.json()
        setMegaJob(data)
      }
    } catch { }
    setLoading(false)
  }, [id])

  useEffect(() => {
    loadMegaJob()
  }, [loadMegaJob])

  // Auto-refresh while running
  useEffect(() => {
    if (!megaJob || megaJob.status === 'completed' || megaJob.status === 'error') return
    const interval = setInterval(loadMegaJob, 10000)
    return () => clearInterval(interval)
  }, [megaJob?.status, loadMegaJob])

  // Load all reports if completed
  useEffect(() => {
    if (megaJob?.status !== 'completed') return

    const reportTypes = ['visits', 'temporal', 'od', 'hourly', 'catchment', 'mobility']
    const setters: Record<string, (d: any) => void> = {
      visits: setVisitsReport,
      temporal: setTemporalReport,
      od: setODReport,
      hourly: setHourlyReport,
      catchment: setCatchmentReport,
      mobility: setMobilityReport,
    }

    for (const type of reportTypes) {
      fetch(`/api/mega-jobs/${id}/reports?type=${type}`, { credentials: 'include' })
        .then((r) => r.ok ? r.json() : null)
        .then((data) => setters[type](data))
        .catch(() => { })
    }
  }, [megaJob?.status, id])

  // ── Consolidation ───────────────────────────────────────────────
  const handleConsolidate = async () => {
    setConsolidating(true)
    setConsolidateProgress('Starting consolidation...')

    try {
      let done = false
      let attempts = 0
      while (!done && attempts < 60) {
        attempts++
        const resetParam = attempts === 1 ? '?reset=true' : ''
        const res = await fetch(`/api/mega-jobs/${id}/consolidate${resetParam}`, {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(selectedPoiIds.length > 0 ? { poiIds: selectedPoiIds } : {}),
        })
        const data = await res.json()

        if (data.error) {
          setConsolidateProgress(`Error: ${data.error}`)
          break
        }

        setConsolidateProgress(data.progress?.message || data.phase)

        if (data.phase === 'done') {
          done = true
          await loadMegaJob()
        } else {
          await new Promise((r) => setTimeout(r, 3000))
        }
      }
    } catch (err: any) {
      setConsolidateProgress(`Error: ${err.message}`)
    } finally {
      setConsolidating(false)
    }
  }

  if (loading) {
    return (
      <MainLayout>
        <div className="flex items-center justify-center py-20">
          <Loader2 className="h-8 w-8 animate-spin" />
        </div>
      </MainLayout>
    )
  }

  if (!megaJob) {
    return (
      <MainLayout>
        <p className="text-muted-foreground">Mega-job not found.</p>
      </MainLayout>
    )
  }

  const canConsolidate = (megaJob.status === 'running' || megaJob.status === 'partial' || megaJob.status === 'completed') && megaJob.progress.synced > 0
  const isCompleted = megaJob.status === 'completed'

  // Build POI list from visits report for filter
  const poiOptions = (visitsReport?.visitsByPoi || []).map((v: any) => ({
    id: v.poiId,
    name: v.poiName || v.poiId,
  }))

  // Compute summary stats
  const totalPings = temporalReport?.daily?.reduce((s: number, d: any) => s + d.pings, 0) || 0
  const totalDevices = temporalReport?.daily?.reduce((s: number, d: any) => s + d.devices, 0) || 0
  const dateRange = {
    from: temporalReport?.daily?.[0]?.date || '—',
    to: temporalReport?.daily?.[temporalReport.daily.length - 1]?.date || '—',
  }
  const totalPois = visitsReport?.totalPois || 0

  return (
    <MainLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-2">
              <Layers className="h-6 w-6" /> {megaJob.name}
            </h1>
            <div className="flex items-center gap-3 mt-2">
              <Badge className={statusColors[megaJob.status] || ''}>
                {megaJob.status}
              </Badge>
              <Badge variant="outline">
                {megaJob.mode === 'auto-split' ? 'Auto-split' : 'Manual group'}
              </Badge>
              <span className="text-sm text-muted-foreground">
                Created {new Date(megaJob.createdAt).toLocaleDateString()}
              </span>
            </div>
          </div>

          {canConsolidate && (
            <Button onClick={handleConsolidate} disabled={consolidating}>
              {consolidating ? (
                <Loader2 className="h-4 w-4 mr-2 animate-spin" />
              ) : (
                <Play className="h-4 w-4 mr-2" />
              )}
              {isCompleted ? 'Re-consolidate' : 'Consolidate reports'}
            </Button>
          )}
        </div>

        {consolidateProgress && (
          <Card className="border-purple-500/30">
            <CardContent className="py-3 text-sm">
              <Loader2 className="h-4 w-4 inline mr-2 animate-spin" />
              {consolidateProgress}
            </CardContent>
          </Card>
        )}

        {/* Progress overview (pre-consolidation) */}
        {!isCompleted && (
          <div className="grid grid-cols-4 gap-4">
            {[
              { label: 'Total', value: megaJob.progress.total },
              { label: 'Created', value: megaJob.progress.created },
              { label: 'Synced', value: megaJob.progress.synced },
              { label: 'Failed', value: megaJob.progress.failed },
            ].map((s) => (
              <Card key={s.label}>
                <CardContent className="py-4 text-center">
                  <p className="text-2xl font-bold">{s.value}</p>
                  <p className="text-sm text-muted-foreground">{s.label}</p>
                </CardContent>
              </Card>
            ))}
          </div>
        )}

        {/* Sub-jobs grid (collapsible when completed) */}
        <CollapsibleCard
          title="Sub-jobs"
          icon={<Layers className="h-4 w-4" />}
          defaultOpen={!isCompleted}
        >
          <div className="space-y-2">
            {(megaJob.subJobs || []).map((job: any) => (
              <div
                key={job.jobId}
                className="flex items-center justify-between p-3 rounded-lg border"
              >
                <div className="flex items-center gap-3">
                  {jobStatusIcon(job.status)}
                  <div>
                    <p className="font-medium text-sm">{job.name}</p>
                    <p className="text-xs text-muted-foreground">
                      {job.dateRange?.from} to {job.dateRange?.to} | {job.poiCount} POIs
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline" className="text-xs">
                    {job.status}
                  </Badge>
                  {job.syncedAt && job.s3DestPath && (
                    <Link href={`/datasets/${job.s3DestPath.replace(/\/$/, '').split('/').pop()}`}>
                      <Button variant="ghost" size="sm">
                        <ExternalLink className="h-3 w-3" />
                      </Button>
                    </Link>
                  )}
                </div>
              </div>
            ))}
            {(!megaJob.subJobs || megaJob.subJobs.length === 0) && (
              <p className="text-sm text-muted-foreground text-center py-4">
                No sub-jobs yet
              </p>
            )}
          </div>
        </CollapsibleCard>

        {/* ── CONSOLIDATED DASHBOARD ────────────────────────────────── */}
        {isCompleted && (
          <>
            {/* POI filter */}
            {poiOptions.length > 1 && (
              <PoiFilter
                pois={poiOptions}
                selectedIds={selectedPoiIds}
                onChange={setSelectedPoiIds}
              />
            )}

            {/* 1. Summary Cards */}
            {(temporalReport || visitsReport) && (
              <SummaryCards
                totalPings={totalPings}
                uniqueDevices={totalDevices}
                dateRange={dateRange}
                totalPois={totalPois}
                subJobCount={megaJob.progress.synced}
              />
            )}

            {/* 2. Daily Activity Chart */}
            {temporalReport?.daily && (
              <CollapsibleCard
                title="Daily Activity"
                icon={<TrendingUp className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=temporal`}
              >
                <MegaDailyChart data={temporalReport.daily} />
              </CollapsibleCard>
            )}

            {/* 3. Catchment Pie Chart (by zip code) */}
            {catchmentReport?.byZipCode && (
              <CollapsibleCard
                title="Catchment by Zip Code"
                icon={<MapPin className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=catchment`}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  {catchmentReport.totalDeviceDays.toLocaleString()} total device-days across{' '}
                  {catchmentReport.byZipCode.length} zip codes
                </p>
                <CatchmentPie data={catchmentReport.byZipCode} />
              </CollapsibleCard>
            )}

            {/* 4. Catchment Map */}
            {catchmentReport?.byZipCode && (
              <CollapsibleCard
                title="Catchment Map"
                icon={<Compass className="h-4 w-4" />}
                defaultOpen={false}
              >
                <CatchmentMap data={catchmentReport.byZipCode} />
              </CollapsibleCard>
            )}

            {/* 5. Origin & Destination */}
            {odReport && (
              <CollapsibleCard
                title="Origin & Destination"
                icon={<Navigation className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=od`}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  {odReport.totalDeviceDays.toLocaleString()} device-days analyzed
                </p>
                <ODTables
                  origins={odReport.origins}
                  destinations={odReport.destinations}
                />
              </CollapsibleCard>
            )}

            {/* 6. Mobility Trends (POI categories ±2h) */}
            {mobilityReport?.categories && (
              <CollapsibleCard
                title="Mobility Trends (±2h of visit)"
                icon={<Activity className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=mobility`}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  Top POI categories visited within 2 hours of visiting target POIs
                </p>
                <MobilityBar data={mobilityReport.categories} />
              </CollapsibleCard>
            )}

            {/* 7. Catchment Hour-of-Day (departure from home) */}
            {catchmentReport?.departureByHour && (
              <CollapsibleCard
                title="Departure Hour (Catchment)"
                icon={<Timer className="h-4 w-4" />}
                defaultOpen={false}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  Hour of first ping of the day (proxy for when visitors leave home)
                </p>
                <HourlyChart
                  data={catchmentReport.departureByHour}
                  dataKey="deviceDays"
                  label="Device-Days"
                  color="#f59e0b"
                />
              </CollapsibleCard>
            )}

            {/* 8. POI Visit Hour-of-Day */}
            {hourlyReport?.hourly && (
              <CollapsibleCard
                title="POI Activity by Hour"
                icon={<BarChart3 className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=hourly`}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  When POIs are busiest throughout the day
                </p>
                <HourlyChart
                  data={hourlyReport.hourly}
                  dataKey="devices"
                  label="Devices"
                  color="#3b82f6"
                />
              </CollapsibleCard>
            )}

            {/* Visits by POI table */}
            {visitsReport && (
              <CollapsibleCard
                title="Visits by POI"
                icon={<BarChart3 className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=visits`}
                defaultOpen={false}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  {visitsReport.totalPois} POIs across {megaJob.progress.synced} sub-jobs
                </p>
                <div className="max-h-96 overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-background">
                      <tr className="border-b">
                        <th className="text-left py-2">POI</th>
                        <th className="text-right py-2">Visits</th>
                        <th className="text-right py-2">Devices</th>
                      </tr>
                    </thead>
                    <tbody>
                      {visitsReport.visitsByPoi.slice(0, 50).map((v: any) => (
                        <tr key={v.poiId} className="border-b border-border/50">
                          <td className="py-2">
                            <p className="font-medium">{v.poiName}</p>
                            <p className="text-xs text-muted-foreground">{v.poiId}</p>
                          </td>
                          <td className="text-right">{v.visits.toLocaleString()}</td>
                          <td className="text-right">{v.devices.toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {visitsReport.visitsByPoi.length > 50 && (
                    <p className="text-sm text-muted-foreground text-center py-2">
                      Showing top 50 of {visitsReport.totalPois}. Download CSV for full data.
                    </p>
                  )}
                </div>
              </CollapsibleCard>
            )}
          </>
        )}
      </div>
    </MainLayout>
  )
}
