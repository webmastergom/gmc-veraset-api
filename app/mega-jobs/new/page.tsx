'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { MainLayout } from '@/components/layout/main-layout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Layers, Wand2, Link2, Calendar, MapPin, Loader2, CheckCircle2 } from 'lucide-react'

interface POICollection {
  id: string
  name: string
  poiCount: number
}

interface JobSummary {
  jobId: string
  name: string
  status: string
  poiCount: number
  dateRange: { from: string; to: string }
  syncedAt?: string
}

export default function NewMegaJobPage() {
  const router = useRouter()

  // ── Auto-split state ────────────────────────────────────────────
  const [name, setName] = useState('')
  const [collections, setCollections] = useState<POICollection[]>([])
  const [selectedCollection, setSelectedCollection] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [splitPreview, setSplitPreview] = useState<any>(null)
  const [creating, setCreating] = useState(false)
  const [creatingPoll, setCreatingPoll] = useState(false)
  const [createProgress, setCreateProgress] = useState<string>('')

  // ── Manual group state ──────────────────────────────────────────
  const [groupName, setGroupName] = useState('')
  const [jobs, setJobs] = useState<JobSummary[]>([])
  const [selectedJobIds, setSelectedJobIds] = useState<Set<string>>(new Set())
  const [grouping, setGrouping] = useState(false)

  // ── Load collections + jobs ─────────────────────────────────────
  useEffect(() => {
    fetch('/api/pois/collections', { credentials: 'include' })
      .then((r) => r.json())
      .then((data) => {
        const arr = Array.isArray(data) ? data : Object.values(data || {})
        setCollections(arr as POICollection[])
      })
      .catch(() => {})

    fetch('/api/jobs', { credentials: 'include' })
      .then((r) => r.json())
      .then((data) => {
        const arr = Array.isArray(data) ? data : []
        setJobs(arr.filter((j: any) => j.status === 'SUCCESS' && j.syncedAt))
      })
      .catch(() => {})
  }, [])

  // ── Auto-split: compute preview ─────────────────────────────────
  const handlePreview = async () => {
    if (!name || !selectedCollection || !dateFrom || !dateTo) return
    setCreating(true)
    setSplitPreview(null)

    try {
      const res = await fetch('/api/mega-jobs', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mode: 'auto-split',
          name,
          poiCollectionId: selectedCollection,
          dateRange: { from: dateFrom, to: dateTo },
        }),
      })
      const data = await res.json()
      if (!res.ok) {
        alert(data.error || 'Failed to create mega-job')
        return
      }
      setSplitPreview(data)
    } catch (err: any) {
      alert(err.message)
    } finally {
      setCreating(false)
    }
  }

  // ── Auto-split: start creating sub-jobs ─────────────────────────
  const handleStartCreation = async () => {
    if (!splitPreview?.megaJob?.megaJobId) return
    setCreatingPoll(true)

    const megaJobId = splitPreview.megaJob.megaJobId
    const total = splitPreview.splitPreview.totalSubJobs

    try {
      for (let i = 0; i < total; i++) {
        setCreateProgress(`Creating sub-job ${i + 1} of ${total}...`)
        const res = await fetch(`/api/mega-jobs/${megaJobId}/create-poll`, {
          method: 'POST',
          credentials: 'include',
        })
        const data = await res.json()
        if (data.done) break
        if (data.subJobError) {
          setCreateProgress(`Sub-job ${i + 1} failed: ${data.subJobError}. Continuing...`)
          await new Promise((r) => setTimeout(r, 1000))
        }
      }
      router.push(`/mega-jobs/${megaJobId}`)
    } catch (err: any) {
      alert(err.message)
      setCreatingPoll(false)
    }
  }

  // ── Manual group: create ────────────────────────────────────────
  const handleGroup = async () => {
    if (!groupName || selectedJobIds.size < 2) return
    setGrouping(true)

    try {
      const res = await fetch('/api/mega-jobs', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mode: 'manual-group',
          name: groupName,
          subJobIds: Array.from(selectedJobIds),
        }),
      })
      const data = await res.json()
      if (!res.ok) {
        alert(data.error || JSON.stringify(data.details))
        return
      }
      router.push(`/mega-jobs/${data.megaJob.megaJobId}`)
    } catch (err: any) {
      alert(err.message)
    } finally {
      setGrouping(false)
    }
  }

  const toggleJob = (jobId: string) => {
    setSelectedJobIds((prev) => {
      const next = new Set(prev)
      if (next.has(jobId)) next.delete(jobId)
      else next.add(jobId)
      return next
    })
  }

  const selectedPOICount = collections.find((c) => c.id === selectedCollection)?.poiCount || 0

  return (
    <MainLayout>
      <div className="max-w-3xl mx-auto space-y-6">
        <h1 className="text-2xl font-bold flex items-center gap-2">
          <Layers className="h-6 w-6" /> New Mega-Job
        </h1>

        <Tabs defaultValue="auto-split">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="auto-split" className="flex items-center gap-2">
              <Wand2 className="h-4 w-4" /> Auto-split
            </TabsTrigger>
            <TabsTrigger value="manual-group" className="flex items-center gap-2">
              <Link2 className="h-4 w-4" /> Group existing jobs
            </TabsTrigger>
          </TabsList>

          {/* ── Auto-split tab ─────────────────────────────────────── */}
          <TabsContent value="auto-split" className="space-y-4 mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Define scope</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label>Name</Label>
                  <Input
                    value={name}
                    onChange={(e) => setName(e.target.value)}
                    placeholder="e.g. France Q1 2026"
                  />
                </div>

                <div>
                  <Label>POI Collection</Label>
                  <select
                    className="w-full rounded-md border bg-background px-3 py-2 text-sm"
                    value={selectedCollection}
                    onChange={(e) => setSelectedCollection(e.target.value)}
                  >
                    <option value="">Select collection...</option>
                    {collections.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.name} ({c.poiCount.toLocaleString()} POIs)
                      </option>
                    ))}
                  </select>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label>From</Label>
                    <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
                  </div>
                  <div>
                    <Label>To</Label>
                    <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
                  </div>
                </div>

                {selectedPOICount > 25000 && (
                  <p className="text-sm text-amber-400">
                    <MapPin className="h-4 w-4 inline mr-1" />
                    {selectedPOICount.toLocaleString()} POIs will be split into{' '}
                    {Math.ceil(selectedPOICount / 25000)} chunks of max 25K each.
                  </p>
                )}

                <Button
                  onClick={handlePreview}
                  disabled={!name || !selectedCollection || !dateFrom || !dateTo || creating}
                  className="w-full"
                >
                  {creating ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                  Preview split plan
                </Button>
              </CardContent>
            </Card>

            {/* Split preview */}
            {splitPreview && (
              <Card className="border-primary/50">
                <CardHeader>
                  <CardTitle>Split plan</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-3 gap-4 text-center">
                    <div>
                      <p className="text-2xl font-bold">{splitPreview.splitPreview.totalSubJobs}</p>
                      <p className="text-sm text-muted-foreground">Sub-jobs</p>
                    </div>
                    <div>
                      <p className="text-2xl font-bold">{splitPreview.splitPreview.dateChunks.length}</p>
                      <p className="text-sm text-muted-foreground">Date chunks</p>
                    </div>
                    <div>
                      <p className="text-2xl font-bold">{splitPreview.splitPreview.poiChunks.length}</p>
                      <p className="text-sm text-muted-foreground">POI chunks</p>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <p className="text-sm font-medium">Date chunks:</p>
                    {splitPreview.splitPreview.dateChunks.map((c: any, i: number) => (
                      <Badge key={i} variant="outline" className="mr-2">
                        <Calendar className="h-3 w-3 mr-1" />
                        {c.from} to {c.to}
                      </Badge>
                    ))}
                  </div>

                  {splitPreview.splitPreview.poiChunks.length > 1 && (
                    <div className="space-y-2">
                      <p className="text-sm font-medium">POI chunks:</p>
                      {splitPreview.splitPreview.poiChunks.map((c: any, i: number) => (
                        <Badge key={i} variant="outline" className="mr-2">
                          <MapPin className="h-3 w-3 mr-1" />
                          {c.label}
                        </Badge>
                      ))}
                    </div>
                  )}

                  <p className="text-sm text-muted-foreground">
                    Quota remaining: {splitPreview.splitPreview.quotaRemaining} calls
                  </p>

                  {creatingPoll ? (
                    <div className="text-center py-4">
                      <Loader2 className="h-6 w-6 animate-spin mx-auto mb-2" />
                      <p className="text-sm">{createProgress}</p>
                    </div>
                  ) : (
                    <Button onClick={handleStartCreation} className="w-full">
                      Create {splitPreview.splitPreview.totalSubJobs} sub-jobs via Veraset
                    </Button>
                  )}
                </CardContent>
              </Card>
            )}
          </TabsContent>

          {/* ── Manual group tab ───────────────────────────────────── */}
          <TabsContent value="manual-group" className="space-y-4 mt-4">
            <Card>
              <CardHeader>
                <CardTitle>Group existing jobs</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <Label>Mega-Job Name</Label>
                  <Input
                    value={groupName}
                    onChange={(e) => setGroupName(e.target.value)}
                    placeholder="e.g. France Full Analysis"
                  />
                </div>

                <div>
                  <Label>Select jobs to group (min 2)</Label>
                  <p className="text-xs text-muted-foreground mb-2">
                    Only SUCCESS + synced jobs are shown
                  </p>
                  <div className="space-y-2 max-h-80 overflow-y-auto">
                    {jobs.length === 0 ? (
                      <p className="text-sm text-muted-foreground py-4 text-center">
                        No synced jobs available
                      </p>
                    ) : (
                      jobs.map((job) => (
                        <div
                          key={job.jobId}
                          onClick={() => toggleJob(job.jobId)}
                          className={`flex items-center justify-between p-3 rounded-lg border cursor-pointer transition-colors ${
                            selectedJobIds.has(job.jobId)
                              ? 'border-primary bg-primary/5'
                              : 'border-border hover:border-primary/30'
                          }`}
                        >
                          <div>
                            <p className="font-medium text-sm">{job.name}</p>
                            <p className="text-xs text-muted-foreground">
                              {job.dateRange?.from} to {job.dateRange?.to} | {job.poiCount} POIs
                            </p>
                          </div>
                          {selectedJobIds.has(job.jobId) && (
                            <CheckCircle2 className="h-5 w-5 text-primary" />
                          )}
                        </div>
                      ))
                    )}
                  </div>
                </div>

                <Button
                  onClick={handleGroup}
                  disabled={!groupName || selectedJobIds.size < 2 || grouping}
                  className="w-full"
                >
                  {grouping ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : null}
                  Group {selectedJobIds.size} jobs
                </Button>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </MainLayout>
  )
}
