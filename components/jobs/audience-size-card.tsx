'use client'

import { useEffect, useState } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Users, Loader2 } from 'lucide-react'
import { estimateRealAudience, ESTIMATE_TOOLTIP } from '@/lib/audience-estimator'

interface AudienceSizeCardProps {
  /** Last segment of the job's s3_dest_path — the dataset name we use
   *  to look up the basic analysis (totalPings, uniqueDevices, etc.). */
  datasetName: string | null
  /** Job's actual date range — used for the decay/churn calculations. */
  dateFrom?: string | null
  dateTo?: string | null
}

/**
 * Renders the "Audience Size" card on a job's detail page.
 *
 * The job page proper is a Server Component without device data, but
 * the dataset that the job lands in DOES have basic analysis. We pull
 * the totalPings + uniqueDevices via the cached basic-analysis API and
 * then layer on the decay-aware real-audience estimate.
 *
 * If no analysis exists yet (user hasn't run it), we render nothing —
 * the dataset page itself can kick the analysis off.
 */
export function AudienceSizeCard({ datasetName, dateFrom, dateTo }: AudienceSizeCardProps) {
  const [loading, setLoading] = useState(true)
  const [summary, setSummary] = useState<{
    totalPings: number
    uniqueDevices: number
  } | null>(null)

  useEffect(() => {
    if (!datasetName) {
      setLoading(false)
      return
    }
    let cancelled = false
    // Cached-hit path: the analyze endpoint returns the existing summary
    // if one's already been computed, otherwise it would kick off a query.
    // We only want the cached version here — don't trigger work.
    fetch(`/api/datasets/${encodeURIComponent(datasetName)}/analyze/cached`, {
      credentials: 'include',
    })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (cancelled) return
        if (data?.summary?.uniqueDevices) {
          setSummary({
            totalPings: data.summary.totalPings || 0,
            uniqueDevices: data.summary.uniqueDevices,
          })
        }
      })
      .catch(() => { })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [datasetName])

  if (loading) {
    return (
      <Card className="mt-6">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Users className="h-5 w-5" /> Audience Size
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-2 text-muted-foreground text-sm">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading…
          </div>
        </CardContent>
      </Card>
    )
  }

  // No cached analysis → don't show the card. The dataset page is where
  // the user runs the analysis; surfacing a "Run analysis" CTA here would
  // duplicate that affordance.
  if (!summary) return null

  const realAudience = estimateRealAudience({
    totalMaids: summary.uniqueDevices,
    dateFrom,
    dateTo,
  })

  return (
    <Card className="mt-6">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Users className="h-5 w-5" /> Audience Size
        </CardTitle>
        <CardDescription>From the dataset basic analysis</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-3 gap-4">
          <div>
            <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1">Total Pings</p>
            <p className="text-2xl font-bold tabular-nums">{summary.totalPings.toLocaleString()}</p>
          </div>
          <div>
            <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1">Unique Devices (MAIDs)</p>
            <p className="text-2xl font-bold tabular-nums">{summary.uniqueDevices.toLocaleString()}</p>
          </div>
          {realAudience !== null && (
            <div title={ESTIMATE_TOOLTIP} className="cursor-help">
              <p className="text-xs text-muted-foreground uppercase tracking-wide mb-1 flex items-center gap-1">
                Estimated Real Audience
              </p>
              <p className="text-2xl font-bold tabular-nums text-amber-400">
                ~{realAudience.toLocaleString()}
              </p>
              <p className="text-[10px] text-muted-foreground mt-0.5">
                with decay since {dateTo || 'data end'}
              </p>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
