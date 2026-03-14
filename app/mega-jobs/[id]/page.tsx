'use client'

import { useEffect, useState, useCallback } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { MainLayout } from '@/components/layout/main-layout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Layers, ExternalLink, Loader2, Download, Play,
  CheckCircle2, XCircle, Clock, BarChart3, TrendingUp,
} from 'lucide-react'

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
  const [visitsReport, setVisitsReport] = useState<any>(null)
  const [temporalReport, setTemporalReport] = useState<any>(null)

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

  // Load reports if completed
  useEffect(() => {
    if (megaJob?.status !== 'completed') return

    fetch(`/api/mega-jobs/${id}/reports?type=visits`, { credentials: 'include' })
      .then((r) => r.ok ? r.json() : null)
      .then(setVisitsReport)
      .catch(() => { })

    fetch(`/api/mega-jobs/${id}/reports?type=temporal`, { credentials: 'include' })
      .then((r) => r.ok ? r.json() : null)
      .then(setTemporalReport)
      .catch(() => { })
  }, [megaJob?.status, id])

  // ── Consolidation ───────────────────────────────────────────────
  const handleConsolidate = async () => {
    setConsolidating(true)
    setConsolidateProgress('Starting consolidation...')

    try {
      let done = false
      let attempts = 0
      while (!done && attempts < 30) {
        attempts++
        const res = await fetch(`/api/mega-jobs/${id}/consolidate`, {
          method: 'POST',
          credentials: 'include',
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

  const allSynced = megaJob.progress.synced === megaJob.progress.total
  const canConsolidate = (megaJob.status === 'running' || megaJob.status === 'partial') && megaJob.progress.synced > 0

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
              Consolidate reports
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

        {/* Progress overview */}
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

        {/* Sub-jobs grid */}
        <Card>
          <CardHeader>
            <CardTitle>Sub-jobs</CardTitle>
          </CardHeader>
          <CardContent>
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
          </CardContent>
        </Card>

        {/* Consolidated reports */}
        {megaJob.status === 'completed' && (
          <>
            {/* Visits by POI */}
            {visitsReport && (
              <Card>
                <CardHeader className="flex flex-row items-center justify-between">
                  <CardTitle className="flex items-center gap-2">
                    <BarChart3 className="h-5 w-5" /> Consolidated Visits by POI
                  </CardTitle>
                  <a href={`/api/mega-jobs/${id}/reports/download?type=visits`}>
                    <Button variant="outline" size="sm">
                      <Download className="h-4 w-4 mr-2" /> CSV
                    </Button>
                  </a>
                </CardHeader>
                <CardContent>
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
                </CardContent>
              </Card>
            )}

            {/* Temporal trends */}
            {temporalReport && (
              <Card>
                <CardHeader className="flex flex-row items-center justify-between">
                  <CardTitle className="flex items-center gap-2">
                    <TrendingUp className="h-5 w-5" /> Temporal Trends
                  </CardTitle>
                  <a href={`/api/mega-jobs/${id}/reports/download?type=temporal`}>
                    <Button variant="outline" size="sm">
                      <Download className="h-4 w-4 mr-2" /> CSV
                    </Button>
                  </a>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-3 gap-4 mb-4">
                    <div>
                      <p className="text-sm font-medium">Daily range</p>
                      <p className="text-xs text-muted-foreground">
                        {temporalReport.daily[0]?.date} to{' '}
                        {temporalReport.daily[temporalReport.daily.length - 1]?.date}
                      </p>
                      <p className="text-sm">{temporalReport.daily.length} days</p>
                    </div>
                    <div>
                      <p className="text-sm font-medium">Weekly</p>
                      <p className="text-sm">{temporalReport.weekly.length} weeks</p>
                    </div>
                    <div>
                      <p className="text-sm font-medium">Monthly</p>
                      <p className="text-sm">{temporalReport.monthly.length} months</p>
                    </div>
                  </div>

                  {/* Day of week pattern */}
                  <div>
                    <p className="text-sm font-medium mb-2">Average by day of week</p>
                    <div className="grid grid-cols-7 gap-2">
                      {temporalReport.dayOfWeek.map((d: any) => (
                        <div key={d.day} className="text-center p-2 rounded bg-muted/50">
                          <p className="text-xs font-medium">{d.dayName.slice(0, 3)}</p>
                          <p className="text-sm">{d.avgDevices.toLocaleString()}</p>
                          <p className="text-xs text-muted-foreground">devices</p>
                        </div>
                      ))}
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}
          </>
        )}
      </div>
    </MainLayout>
  )
}
