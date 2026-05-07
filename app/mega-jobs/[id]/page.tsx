'use client'

import { useEffect, useState, useCallback } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { MainLayout } from '@/components/layout/main-layout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Layers, ExternalLink, Loader2, Play, Download, Users,
  CheckCircle2, XCircle, Clock, Map,
  BarChart3, TrendingUp, MapPin, Navigation,
  Compass, Timer, Activity, Target,
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
import { MovementMap } from '@/components/analysis/movement-map'
import { MegaNseModal } from '@/components/mega-jobs/nse-modal'
import { MegaCategoryMaidModal } from '@/components/mega-jobs/category-maid-modal'
import { MegaCountrySelector } from '@/components/mega-jobs/country-selector'
import { DayHourHeatmap } from '@/components/analysis/day-hour-heatmap'

/** Dwell time options (in minutes) — kept in sync with the dataset page list */
const DWELL_OPTIONS = [
  { value: 0, label: 'No limit' },
  { value: 2, label: '2 min' },
  { value: 5, label: '5 min' },
  { value: 10, label: '10 min' },
  { value: 15, label: '15 min' },
  { value: 30, label: '30 min' },
  { value: 45, label: '45 min' },
  { value: 60, label: '1 hr' },
  { value: 90, label: '1.5 hr' },
  { value: 120, label: '2 hr' },
  { value: 180, label: '3 hr' },
  { value: 240, label: '4 hr' },
  { value: 360, label: '6 hr' },
  { value: 480, label: '8 hr' },
];

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
  const [dayhourReport, setDayhourReport] = useState<any>(null)
  const [catchmentReport, setCatchmentReport] = useState<any>(null)
  const [mobilityReport, setMobilityReport] = useState<any>(null)
  const [affinityReport, setAffinityReport] = useState<any>(null)

  // Dwell filter (numeric minutes, 0 = no limit) — matches dataset page UX
  const [dwellMin, setDwellMin] = useState<number>(0)
  const [dwellMax, setDwellMax] = useState<number>(0)
  // Hour-of-day filter (0..23 inclusive)
  const [hourFrom, setHourFrom] = useState<number>(0)
  const [hourTo, setHourTo] = useState<number>(23)
  // Minimum number of distinct visit-days per ad_id
  const [minVisits, setMinVisits] = useState<number>(1)
  // FULL-schema GPS-only filter (no-op on BASIC datasets)
  const [gpsOnly, setGpsOnly] = useState<boolean>(false)
  // FULL-schema ping_circle_score threshold (0 = off; lower = tighter)
  const [maxCircleScore, setMaxCircleScore] = useState<number>(0)
  // Day-of-week filter (1=Mon..7=Sun ISO 8601). Empty = all days.
  const [daysOfWeek, setDaysOfWeek] = useState<number[]>([])
  // Discard employees (15+ days, ≥4h avg dwell, ≥0.6 work-hour share, ≤0.3 overnight share)
  const [discardEmployees, setDiscardEmployees] = useState<boolean>(false)
  // Discard residents (15+ days, ≥4h avg dwell, ≥0.5 overnight share)
  const [discardResidents, setDiscardResidents] = useState<boolean>(false)

  // NSE modal
  const [nseModalOpen, setNseModalOpen] = useState(false)
  // MAIDs by Category modal
  const [categoryModalOpen, setCategoryModalOpen] = useState(false)

  // POI filter
  const [selectedPoiIds, setSelectedPoiIds] = useState<string[]>([])
  // Bump to force report reload after re-consolidation
  const [reportVersion, setReportVersion] = useState(0)
  // Movement map sub-job selector
  const [movementSubJob, setMovementSubJob] = useState<string>('')

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

  // Auto-create sub-jobs while status is 'planning' or 'creating'
  useEffect(() => {
    if (!megaJob) return
    if (megaJob.status !== 'planning' && megaJob.status !== 'creating') return
    if (megaJob.progress?.created >= megaJob.progress?.total) return

    let cancelled = false
    const pollCreateSubJobs = async () => {
      while (!cancelled) {
        try {
          const res = await fetch(`/api/mega-jobs/${id}/create-poll`, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
          })
          if (!res.ok) break
          const data = await res.json()
          if (data.megaJob) setMegaJob(data.megaJob)
          if (data.done) break
        } catch {
          break
        }
        // Brief pause between sub-job creations
        await new Promise(r => setTimeout(r, 2000))
      }
      // Refresh final state
      if (!cancelled) loadMegaJob()
    }

    pollCreateSubJobs()
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [megaJob?.status, id])

  // Auto-resume consolidation if page loads with status 'consolidating'
  useEffect(() => {
    if (!megaJob || megaJob.status !== 'consolidating' || consolidating) return
    handleConsolidate(true)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [megaJob?.status])

  // Load all reports if completed
  useEffect(() => {
    if (megaJob?.status !== 'completed') return

    const reportTypes = ['visits', 'temporal', 'od', 'hourly', 'dayhour', 'catchment', 'mobility', 'affinity']
    const setters: Record<string, (d: any) => void> = {
      visits: setVisitsReport,
      temporal: setTemporalReport,
      od: setODReport,
      hourly: setHourlyReport,
      dayhour: setDayhourReport,
      catchment: setCatchmentReport,
      mobility: setMobilityReport,
      affinity: setAffinityReport,
    }

    for (const type of reportTypes) {
      fetch(`/api/mega-jobs/${id}/reports?type=${type}`, { credentials: 'include' })
        .then((r) => r.ok ? r.json() : null)
        .then((data) => setters[type](data))
        .catch(() => { })
    }
  }, [megaJob?.status, id, reportVersion])

  // ── Consolidation ───────────────────────────────────────────────
  const handleConsolidate = async (resume = false) => {
    setConsolidating(true)
    setConsolidateProgress(resume ? 'Resuming consolidation...' : 'Starting consolidation...')

    try {
      let done = false
      let attempts = 0
      while (!done && attempts < 60) {
        attempts++
        const resetParam = (attempts === 1 && !resume) ? '?reset=true' : ''
        const res = await fetch(`/api/mega-jobs/${id}/consolidate${resetParam}`, {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            ...(selectedPoiIds.length > 0 ? { poiIds: selectedPoiIds } : {}),
            ...(dwellMin > 0 || dwellMax > 0 ? {
              dwellFilter: {
                ...(dwellMin > 0 ? { minMinutes: dwellMin } : {}),
                ...(dwellMax > 0 ? { maxMinutes: dwellMax } : {}),
              }
            } : {}),
            ...(hourFrom > 0 || hourTo < 23 ? { hourFrom, hourTo } : {}),
            ...(minVisits > 1 ? { minVisits } : {}),
            ...(gpsOnly ? { gpsOnly: true } : {}),
            ...(maxCircleScore > 0 ? { maxCircleScore } : {}),
            ...(daysOfWeek.length > 0 && daysOfWeek.length < 7 ? { daysOfWeek } : {}),
            ...(discardEmployees ? { discardEmployees: true } : {}),
            ...(discardResidents ? { discardResidents: true } : {}),
          }),
        })
        if (!res.ok) {
          // Vercel timeout (504) or server error returns HTML, not JSON
          let errMsg = `HTTP ${res.status}`
          try {
            const body = await res.json()
            if (body.error) errMsg = body.error
            if (body.progress?.message) errMsg = body.progress.message
          } catch { /* HTML response, ignore parse error */ }
          setConsolidateProgress(`Error: ${errMsg}. Click Re-consolidate to retry.`)
          break
        }
        const data = await res.json()

        if (data.error) {
          setConsolidateProgress(`Error: ${data.error}`)
          break
        }

        setConsolidateProgress(data.progress?.message || data.phase)

        if (data.phase === 'done') {
          done = true
          await loadMegaJob()
          setReportVersion((v) => v + 1)
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

  const canConsolidate = (megaJob.status === 'running' || megaJob.status === 'partial' || megaJob.status === 'completed' || megaJob.status === 'consolidating') && megaJob.progress.synced > 0
  const isCompleted = megaJob.status === 'completed'

  // Build POI list from visits report for filter
  const poiOptions = (visitsReport?.visitsByPoi || []).map((v: any) => ({
    id: v.poiId,
    name: v.poiName || v.poiId,
  }))

  // Compute summary stats — prefer temporal report, fallback to hourly
  const totalPings = temporalReport?.daily?.reduce((s: number, d: any) => s + d.pings, 0)
    || hourlyReport?.hourly?.reduce((s: number, h: any) => s + (h.pings || 0), 0)
    || 0
  // Prefer totalUniqueDevices (true unique), fallback to daily sum (device-days)
  const totalDevices = temporalReport?.totalUniqueDevices
    || temporalReport?.daily?.reduce((s: number, d: any) => s + d.devices, 0)
    || hourlyReport?.hourly?.reduce((s: number, h: any) => s + (h.devices || 0), 0)
    || 0
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
            <div className="flex flex-wrap items-center gap-3">
              <div className="flex items-center gap-1">
                <label className="text-xs text-muted-foreground whitespace-nowrap">Dwell:</label>
                <select
                  value={dwellMin}
                  onChange={(e) => setDwellMin(parseInt(e.target.value, 10))}
                  className="h-8 w-20 rounded-md border border-input bg-background px-1 text-sm text-center"
                >
                  {DWELL_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.value === 0 ? 'Min' : opt.label}</option>
                  ))}
                </select>
                <span className="text-xs text-muted-foreground">to</span>
                <select
                  value={dwellMax}
                  onChange={(e) => setDwellMax(parseInt(e.target.value, 10))}
                  className="h-8 w-20 rounded-md border border-input bg-background px-1 text-sm text-center"
                >
                  {DWELL_OPTIONS.map((opt) => (
                    <option key={opt.value} value={opt.value}>{opt.value === 0 ? 'Max' : opt.label}</option>
                  ))}
                </select>
              </div>
              <div className="flex items-center gap-1">
                <label className="text-xs text-muted-foreground whitespace-nowrap">Hours:</label>
                <select
                  value={hourFrom}
                  onChange={(e) => setHourFrom(parseInt(e.target.value, 10))}
                  className="h-8 w-16 rounded-md border border-input bg-background px-1 text-sm text-center"
                >
                  {Array.from({ length: 24 }, (_, i) => (
                    <option key={i} value={i}>{String(i).padStart(2, '0')}h</option>
                  ))}
                </select>
                <span className="text-xs text-muted-foreground">to</span>
                <select
                  value={hourTo}
                  onChange={(e) => setHourTo(parseInt(e.target.value, 10))}
                  className="h-8 w-16 rounded-md border border-input bg-background px-1 text-sm text-center"
                >
                  {Array.from({ length: 24 }, (_, i) => (
                    <option key={i} value={i}>{String(i).padStart(2, '0')}h</option>
                  ))}
                </select>
              </div>
              <div className="flex items-center gap-1">
                <label className="text-xs text-muted-foreground whitespace-nowrap">Min Visits:</label>
                <select
                  value={minVisits}
                  onChange={(e) => setMinVisits(parseInt(e.target.value, 10))}
                  className="h-8 w-16 rounded-md border border-input bg-background px-1 text-sm text-center"
                >
                  {[1, 2, 3, 5, 10, 15, 20].map((n) => (
                    <option key={n} value={n}>{n}+</option>
                  ))}
                </select>
              </div>
              <label className="flex items-center gap-1 text-xs text-muted-foreground select-none cursor-pointer" title="Drop non-GPS pings (cell-tower / Wi-Fi). Requires FULL schema; no effect on BASIC sub-jobs.">
                <input
                  type="checkbox"
                  checked={gpsOnly}
                  onChange={(e) => setGpsOnly(e.target.checked)}
                  className="h-3.5 w-3.5"
                />
                GPS only
              </label>
              <div className="flex items-center gap-1" title="Drop pings with ping_circle_score above this threshold (lower = tighter uncertainty). 0 = off. Typical: 0.5-1.0. Requires FULL schema; no effect on BASIC.">
                <label className="text-xs text-muted-foreground whitespace-nowrap">Max score:</label>
                <select
                  value={maxCircleScore}
                  onChange={(e) => setMaxCircleScore(parseFloat(e.target.value))}
                  className="h-8 w-16 rounded-md border border-input bg-background px-1 text-sm text-center"
                >
                  {[0, 0.1, 0.25, 0.5, 1, 2].map((v) => (
                    <option key={v} value={v}>{v === 0 ? 'off' : v}</option>
                  ))}
                </select>
              </div>
              <div
                className="flex items-center gap-1"
                title="Restrict the analysis to specific days of the week. All selected = no filter."
              >
                <label className="text-xs text-muted-foreground whitespace-nowrap">Days:</label>
                <div className="flex gap-0.5">
                  {[
                    { d: 1, label: 'M' },
                    { d: 2, label: 'T' },
                    { d: 3, label: 'W' },
                    { d: 4, label: 'T' },
                    { d: 5, label: 'F' },
                    { d: 6, label: 'S' },
                    { d: 7, label: 'S' },
                  ].map(({ d, label }) => {
                    const active = daysOfWeek.length === 0 || daysOfWeek.includes(d)
                    return (
                      <button
                        key={d}
                        type="button"
                        onClick={() => {
                          let next: number[]
                          if (daysOfWeek.length === 0) {
                            next = [1, 2, 3, 4, 5, 6, 7].filter((x) => x !== d)
                          } else if (daysOfWeek.includes(d)) {
                            next = daysOfWeek.filter((x) => x !== d)
                          } else {
                            next = [...daysOfWeek, d]
                          }
                          if (next.length === 7) next = []
                          setDaysOfWeek(next)
                        }}
                        className={`h-8 w-7 rounded text-xs font-medium transition-colors ${
                          active
                            ? 'bg-primary/15 text-foreground border border-primary/40'
                            : 'bg-muted/30 text-muted-foreground border border-border'
                        }`}
                        title={['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][d - 1]}
                      >
                        {label}
                      </button>
                    )
                  })}
                </div>
                <div className="flex gap-1 ml-1">
                  <button
                    type="button"
                    onClick={() => setDaysOfWeek([])}
                    className="h-8 px-2 rounded text-[10px] uppercase tracking-wider bg-muted/40 hover:bg-muted text-muted-foreground"
                    title="Reset to all days"
                  >
                    All
                  </button>
                  <button
                    type="button"
                    onClick={() => setDaysOfWeek([1, 2, 3, 4, 5])}
                    className="h-8 px-2 rounded text-[10px] uppercase tracking-wider bg-muted/40 hover:bg-muted text-muted-foreground"
                    title="Weekdays only"
                  >
                    M-F
                  </button>
                  <button
                    type="button"
                    onClick={() => setDaysOfWeek([6, 7])}
                    className="h-8 px-2 rounded text-[10px] uppercase tracking-wider bg-muted/40 hover:bg-muted text-muted-foreground"
                    title="Weekend only"
                  >
                    S-S
                  </button>
                </div>
              </div>
              <label className="flex items-center gap-1 text-xs text-muted-foreground select-none cursor-pointer" title="Drop devices that look like employees: 15+ visit-days, 4+ hours avg dwell, mostly 8h-20h, no overnight presence. Residents are kept (they distribute pings across 24h, including 2-5am).">
                <input
                  type="checkbox"
                  checked={discardEmployees}
                  onChange={(e) => setDiscardEmployees(e.target.checked)}
                  className="h-3.5 w-3.5"
                />
                No employees
              </label>
              <label className="flex items-center gap-1 text-xs text-muted-foreground select-none cursor-pointer" title="Drop devices that LIVE inside the POI radius: 15+ visit-days, 4+ hours avg dwell, ≥50% of days include an overnight ping (2h-5h). Composes with No employees.">
                <input
                  type="checkbox"
                  checked={discardResidents}
                  onChange={(e) => setDiscardResidents(e.target.checked)}
                  className="h-3.5 w-3.5"
                />
                No residents
              </label>
              <Button onClick={() => handleConsolidate(megaJob.status === 'consolidating')} disabled={consolidating}>
                {consolidating ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                {megaJob.status === 'consolidating' ? 'Resume consolidation' : isCompleted ? 'Re-consolidate' : 'Consolidate reports'}
              </Button>
            </div>
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

            {/* Export buttons */}
            <div className="flex items-center gap-3">
              <span className="text-sm font-medium text-muted-foreground">Exports:</span>
              <a href={`/api/mega-jobs/${id}/reports/download?type=maids`}>
                <Button variant="outline" size="sm">
                  <Download className="h-4 w-4 mr-1" /> MAIDs
                </Button>
              </a>
              {catchmentReport && (
                <>
                  <a href={`/api/mega-jobs/${id}/reports/download?type=catchment`}>
                    <Button variant="outline" size="sm">
                      <Download className="h-4 w-4 mr-1" /> Catchment
                    </Button>
                  </a>
                  <a href={`/api/mega-jobs/${id}/reports/download?type=postcodes`}>
                    <Button variant="outline" size="sm">
                      <Download className="h-4 w-4 mr-1" /> Postal Codes
                    </Button>
                  </a>
                </>
              )}
              {affinityReport && (
                <a href={`/api/mega-jobs/${id}/reports/download?type=affinity`}>
                  <Button variant="outline" size="sm">
                    <Download className="h-4 w-4 mr-1" /> Affinity Index
                  </Button>
                </a>
              )}
              <Button variant="outline" size="sm" onClick={() => setNseModalOpen(true)}>
                <Users className="h-4 w-4 mr-1" /> MAIDs by NSE
              </Button>
              <Button variant="outline" size="sm" onClick={() => setCategoryModalOpen(true)}>
                <Target className="h-4 w-4 mr-1" /> MAIDs by Category
              </Button>
              <div className="flex items-center gap-2 ml-2 pl-2 border-l border-border">
                <span className="text-xs text-muted-foreground">Country:</span>
                <MegaCountrySelector
                  megaJobId={id}
                  initialCountry={megaJob?.country || null}
                  onChanged={(c) => setMegaJob((prev: any) => prev ? { ...prev, country: c } : prev)}
                />
              </div>
            </div>

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

            {/* 2b. Movement Map (select sub-job) */}
            {(() => {
              const syncedJobs = (megaJob.subJobs || []).filter((j: any) => j.syncedAt && j.s3DestPath);
              if (syncedJobs.length === 0) return null;
              const selectedJob = syncedJobs.find((j: any) => j.jobId === movementSubJob) || syncedJobs[0];
              const dsName = selectedJob.s3DestPath.replace(/\/$/, '').split('/').pop();
              return (
                <CollapsibleCard
                  title="Device Movements (50 sample)"
                  icon={<Map className="h-4 w-4" />}
                  defaultOpen={false}
                >
                  <div className="flex items-center gap-3 mb-4">
                    <label className="text-sm text-muted-foreground whitespace-nowrap">Sub-job:</label>
                    <select
                      value={selectedJob.jobId}
                      onChange={(e) => setMovementSubJob(e.target.value)}
                      className="h-8 rounded-md border border-input bg-background px-2 text-sm flex-1 max-w-md"
                    >
                      {syncedJobs.map((j: any) => (
                        <option key={j.jobId} value={j.jobId}>
                          {j.name} ({j.dateRange?.from} → {j.dateRange?.to})
                        </option>
                      ))}
                    </select>
                  </div>
                  <MovementMap
                    key={selectedJob.jobId}
                    datasetName={dsName}
                    dateFrom={selectedJob.dateRange?.from || ''}
                    dateTo={selectedJob.dateRange?.to || ''}
                  />
                </CollapsibleCard>
              );
            })()}

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

            {/* 6. Mobility Trends (POI categories ±2h) — Before & After */}
            {mobilityReport?.categories ? (
              <CollapsibleCard
                title="Mobility Trends (±2h of visit)"
                icon={<Activity className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=mobility`}
              >
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <div>
                    <p className="text-sm text-muted-foreground mb-3">
                      🕐 Places visited <span className="font-semibold text-foreground">before</span> arriving at target POIs
                    </p>
                    <MobilityBar data={mobilityReport.before || mobilityReport.categories} />
                  </div>
                  <div>
                    <p className="text-sm text-muted-foreground mb-3">
                      🕐 Places visited <span className="font-semibold text-foreground">after</span> leaving target POIs
                    </p>
                    <MobilityBar data={mobilityReport.after || mobilityReport.categories} />
                  </div>
                </div>
              </CollapsibleCard>
            ) : (megaJob?.consolidationNotes || []).some((n: string) => n.toLowerCase().includes('mobility')) && (
              <CollapsibleCard
                title="Mobility Trends (±2h of visit) — not available"
                icon={<Activity className="h-4 w-4" />}
                defaultOpen={true}
              >
                <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-4 text-sm text-amber-200">
                  {(megaJob?.consolidationNotes || []).find((n: string) => n.toLowerCase().includes('mobility'))}
                </div>
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

            {/* 8b. POI Activity by Day × Hour heatmap */}
            {dayhourReport?.cells?.length > 0 && (
              <CollapsibleCard
                title="POI Activity by Day × Hour"
                icon={<BarChart3 className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=dayhour`}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  Heatmap of unique devices (or pings) by day-of-week and hour-of-day. Darker = quieter, brighter cyan = busier.
                </p>
                <DayHourHeatmap cells={dayhourReport.cells} />
              </CollapsibleCard>
            )}

            {/* 9. Affinity Heatmap */}
            {affinityReport?.byZipCode?.length > 0 && (
              <CollapsibleCard
                title="Affinity Heatmap"
                icon={<Target className="h-4 w-4" />}
                defaultOpen={false}
              >
                <CatchmentMap
                  // Megajob ConsolidatedAffinityReport uses `postalCode` (not `zipCode`),
                  // `avgDwell`/`totalVisits` (not `avgDwellMinutes`/`totalVisitDays`).
                  // Map to the field names CatchmentMap expects.
                  data={affinityReport.byZipCode.map((z: any) => ({
                    zipCode: z.postalCode,
                    city: z.city,
                    country: z.country,
                    lat: z.lat,
                    lng: z.lng,
                    deviceDays: z.affinityIndex,
                  }))}
                />
              </CollapsibleCard>
            )}

            {/* 10. Affinity Index by Postal Code */}
            {affinityReport?.byZipCode?.length > 0 && (
              <CollapsibleCard
                title="Affinity Index by Postal Code"
                icon={<Target className="h-4 w-4" />}
                downloadHref={`/api/mega-jobs/${id}/reports/download?type=affinity`}
              >
                <p className="text-sm text-muted-foreground mb-4">
                  Affinity = 50% dwell time + 50% visit frequency. Scale 0-100.
                  {' '}{affinityReport.byZipCode.length} postal codes analyzed.
                </p>
                <div className="flex justify-end mb-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      const sorted = [...affinityReport.byZipCode].sort((a: any, b: any) => b.affinityIndex - a.affinityIndex);
                      const csv = 'postal_code,city,country,affinity_index,avg_dwell_min,avg_frequency,unique_devices,total_visits\n' +
                        sorted.map((z: any) => `${z.postalCode},${z.city},${z.country},${z.affinityIndex},${z.avgDwell},${z.avgFrequency},${z.uniqueDevices},${z.totalVisits}`).join('\n');
                      const blob = new Blob([csv], { type: 'text/csv' });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement('a');
                      a.href = url; a.download = `mega-${id}-affinity-index.csv`; a.click();
                    }}
                  >
                    <Download className="h-3 w-3 mr-1" /> Download CSV
                  </Button>
                </div>
                <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead className="sticky top-0 bg-background border-b">
                      <tr>
                        <th className="text-left py-2 px-3">Postal Code</th>
                        <th className="text-left py-2 px-3">City</th>
                        <th className="text-right py-2 px-3">Affinity</th>
                        <th className="text-right py-2 px-3">Avg Dwell (min)</th>
                        <th className="text-right py-2 px-3">Avg Frequency</th>
                        <th className="text-right py-2 px-3">Devices</th>
                        <th className="text-right py-2 px-3">Total Visits</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[...affinityReport.byZipCode]
                        .sort((a: any, b: any) => b.affinityIndex - a.affinityIndex)
                        .slice(0, 100)
                        .map((z: any) => (
                          // Megajob report uses postalCode/avgDwell/totalVisits (vs dataset's zipCode/avgDwellMinutes/totalVisitDays)
                          <tr key={z.postalCode} className="border-b border-border/50 hover:bg-muted/30">
                            <td className="py-2 px-3 font-mono">{z.postalCode}</td>
                            <td className="py-2 px-3 text-muted-foreground">{z.city}</td>
                            <td className="py-2 px-3 text-right">
                              <div className="flex items-center justify-end gap-2">
                                <div className="w-16 h-2 bg-muted rounded-full overflow-hidden">
                                  <div
                                    className="h-full rounded-full"
                                    style={{
                                      width: `${z.affinityIndex}%`,
                                      backgroundColor: z.affinityIndex >= 70 ? '#22c55e' : z.affinityIndex >= 40 ? '#eab308' : '#ef4444',
                                    }}
                                  />
                                </div>
                                <span className="font-semibold w-8 text-right">{z.affinityIndex}</span>
                              </div>
                            </td>
                            <td className="py-2 px-3 text-right text-muted-foreground">{z.avgDwell}</td>
                            <td className="py-2 px-3 text-right text-muted-foreground">{z.avgFrequency}</td>
                            <td className="py-2 px-3 text-right">{z.uniqueDevices?.toLocaleString()}</td>
                            <td className="py-2 px-3 text-right text-muted-foreground">{z.totalVisits?.toLocaleString()}</td>
                          </tr>
                        ))}
                    </tbody>
                  </table>
                </div>
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

      <MegaNseModal
        open={nseModalOpen}
        onClose={() => setNseModalOpen(false)}
        megaJobId={id}
        megaJobCountry={megaJob?.country || null}
      />

      <MegaCategoryMaidModal
        open={categoryModalOpen}
        onClose={() => setCategoryModalOpen(false)}
        megaJobId={id}
        megaJobCountry={megaJob?.country || null}
        dwellMin={dwellMin}
        dwellMax={dwellMax}
        hourFrom={hourFrom}
        hourTo={hourTo}
      />
    </MainLayout>
  )
}
