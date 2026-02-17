"use client"

import { useState, useEffect, useRef, useCallback, Suspense } from "react"
import { useSearchParams } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Progress } from "@/components/ui/progress"
import { useToast } from "@/hooks/use-toast"
import { Loader2, RefreshCw, CheckCircle, AlertCircle, Square } from "lucide-react"
import { ProfessionalLoader } from "@/components/sync/professional-loader"
import { useRouter } from "next/navigation"

interface SyncStatus {
  status: 'not_started' | 'syncing' | 'completed' | 'cancelled' | 'error'
  message: string
  progress: number
  total: number
  totalBytes: number
  copied: number
  copiedBytes: number
  syncProgress?: {
    currentDay?: string
    currentFile?: number
    totalFilesInCurrentDay?: number
    dayProgress?: Record<string, {
      date: string
      totalFiles: number
      copiedFiles: number
      failedFiles: number
      totalBytes: number
      copiedBytes: number
      status: 'pending' | 'copying' | 'completed' | 'failed'
      errors?: Array<{ file: string; error: string }>
    }>
    lastUpdated?: string
  } | null
}

function SyncPageContent() {
  const searchParams = useSearchParams()
  const jobId = searchParams.get("jobId")
  const [loading, setLoading] = useState(false)
  const [destPath, setDestPath] = useState("")
  const [syncStatus, setSyncStatus] = useState<SyncStatus | null>(null)
  const [isPolling, setIsPolling] = useState(false)
  const intervalRef = useRef<NodeJS.Timeout | null>(null)
  const [retryDelay, setRetryDelay] = useState(15000)
  const [sseFailed, setSseFailed] = useState(false) // Fallback to polling if SSE errors
  const [syncConflict, setSyncConflict] = useState(false) // 409: lock held, need to cancel first
  const consecutiveErrorsRef = useRef(0)
  const MAX_CONSECUTIVE_ERRORS = 5
  // Auto-retry: track progress changes to detect stalled syncs
  const lastProgressRef = useRef<{ copied: number; time: number }>({ copied: 0, time: Date.now() })
  const autoRetryCountRef = useRef(0)
  const MAX_AUTO_RETRIES = 30 // Enough for ~50 GB at ~2 GB per 10-min run
  const STALL_THRESHOLD_MS = 90_000 // 90s without progress = stalled
  const [autoRetrying, setAutoRetrying] = useState(false)
  const autoRetryTimerRef = useRef<NodeJS.Timeout | null>(null)
  const { toast } = useToast()
  const router = useRouter()

  useEffect(() => {
    if (jobId) {
      const destFromQuery = searchParams.get("destPath")
      if (destFromQuery) {
        try {
          setDestPath(decodeURIComponent(destFromQuery))
        } catch {
          setDestPath(`s3://garritz-veraset-data-us-west-2/job-${jobId.slice(0, 8)}/`)
        }
      } else {
        setDestPath(`s3://garritz-veraset-data-us-west-2/job-${jobId.slice(0, 8)}/`)
      }
      checkSyncStatus()
    }
    
    // Cleanup on unmount
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
      if (autoRetryTimerRef.current) {
        clearTimeout(autoRetryTimerRef.current)
        autoRetryTimerRef.current = null
      }
      // Don't abort the sync stream on unmount — let the backend finish
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId])

  const applyStatus = useCallback((status: SyncStatus) => {
    setSyncStatus(status)
    if (status.status === 'completed') {
      setIsPolling(false)
      setLoading(false)
      setSyncConflict(false)
      autoRetryCountRef.current = 0
      toast({
        title: "Sync Complete",
        description: `Synced ${status.copied} objects (${(status.totalBytes / 1024 / 1024 / 1024).toFixed(2)} GB)`,
      })
      setTimeout(() => router.push(`/jobs/${jobId}`), 2000)
    } else if (status.status === 'error') {
      // Don't stop polling if it's a stall — auto-retry will handle it
      if (!status.message?.includes('stalled')) {
        setIsPolling(false)
        setLoading(false)
        setSyncConflict(false)
        toast({
          title: "Sync Error",
          description: status.message || "Sync failed",
          variant: "destructive",
        })
      }
    } else if (status.status === 'cancelled') {
      setIsPolling(false)
      setLoading(false)
      setSyncConflict(false)
      autoRetryCountRef.current = 0
      toast({
        title: "Sync Stopped",
        description: status.message || "Sync was stopped by user",
      })
    }
  }, [toast, router, jobId])

  const checkSyncStatus = useCallback(async () => {
    if (!jobId) return
    try {
      const response = await fetch(`/api/jobs/${jobId}/sync/status`, { cache: 'no-store' })
      if (response.status === 429) {
        setRetryDelay(prev => Math.min(prev * 2, 60000))
        return
      }
      if (response.ok) {
        consecutiveErrorsRef.current = 0
        setRetryDelay(15000)
        const status = await response.json() as SyncStatus
        setSyncStatus(status)
        if (status.status === 'syncing' && !isPolling) setIsPolling(true)
        // For stalled errors, keep polling so auto-retry can detect and handle
        if (status.status === 'error' && status.message?.includes('stalled')) {
          if (!isPolling) setIsPolling(true)
          applyStatus(status)
        } else if (status.status === 'completed' || status.status === 'error' || status.status === 'cancelled') {
          applyStatus(status)
        }
      } else {
        consecutiveErrorsRef.current++
      }
    } catch (error) {
      consecutiveErrorsRef.current++
      console.error(`Error checking sync status (${consecutiveErrorsRef.current}/${MAX_CONSECUTIVE_ERRORS}):`, error)
      if (consecutiveErrorsRef.current >= MAX_CONSECUTIVE_ERRORS) {
        console.error('Too many consecutive network errors, stopping polling')
        setIsPolling(false)
        setLoading(false)
        if (intervalRef.current) {
          clearInterval(intervalRef.current)
          intervalRef.current = null
        }
        toast({
          title: "Connection Lost",
          description: "Could not check sync status after several attempts. Check your connection and reload the page.",
          variant: "destructive",
        })
      }
    }
  }, [jobId, isPolling, applyStatus, toast])

  // SSE for progress when syncing; fallback to polling if EventSource fails
  const eventSourceRef = useRef<EventSource | null>(null)
  useEffect(() => {
    if (!isPolling || !jobId) {
      eventSourceRef.current?.close()
      eventSourceRef.current = null
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
      return
    }

    if (sseFailed) {
      if (intervalRef.current) clearInterval(intervalRef.current)
      intervalRef.current = setInterval(checkSyncStatus, Math.max(retryDelay, 15000))
      return () => {
        if (intervalRef.current) {
          clearInterval(intervalRef.current)
          intervalRef.current = null
        }
      }
    }

    const url = `/api/jobs/${jobId}/sync/stream`
    const es = new EventSource(url)
    eventSourceRef.current = es
    es.addEventListener('progress', (e: MessageEvent) => {
      try {
        const status = JSON.parse(e.data) as SyncStatus
        setSyncStatus(status)
        if (status.status === 'completed' || status.status === 'cancelled') {
          es.close()
          eventSourceRef.current = null
          setIsPolling(false)
          applyStatus(status)
        } else if (status.status === 'error') {
          // For stalled syncs, don't stop polling — auto-retry will handle it
          // For other errors, stop polling
          if (status.message?.includes('stalled')) {
            // Keep polling, switch to fallback since SSE stream died
            es.close()
            eventSourceRef.current = null
            setSseFailed(true)
          } else {
            es.close()
            eventSourceRef.current = null
            setIsPolling(false)
          }
          applyStatus(status)
        }
      } catch {
        // ignore parse errors
      }
    })
    es.onerror = () => {
      es.close()
      eventSourceRef.current = null
      setSseFailed(true)
    }

    return () => {
      es.close()
      eventSourceRef.current = null
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
    }
  }, [isPolling, jobId, retryDelay, sseFailed, checkSyncStatus, applyStatus])

  // ---------- Auto-retry: detect stalled syncs and re-trigger ----------
  // When a Vercel function times out (10 min), the sync stalls. Since we now
  // have incremental sync, we can safely re-trigger to continue where we left off.
  const autoRetryInProgressRef = useRef(false)
  const handleAutoRetry = useCallback(async () => {
    // Guard: prevent concurrent retries
    if (autoRetryInProgressRef.current) return
    if (!jobId || !destPath) return
    if (autoRetryCountRef.current >= MAX_AUTO_RETRIES) {
      console.warn('[AUTO-RETRY] Max retries reached, stopping auto-retry')
      toast({
        title: "Auto-retry limit reached",
        description: `Sync has been auto-retried ${MAX_AUTO_RETRIES} times. Please check the sync manually.`,
        variant: "destructive",
      })
      return
    }
    autoRetryInProgressRef.current = true
    autoRetryCountRef.current++
    console.log(`[AUTO-RETRY] Triggering auto-retry #${autoRetryCountRef.current}`)
    setAutoRetrying(true)
    try {
      // Small delay to let the lock expire
      await new Promise(r => setTimeout(r, 3000))
      setAutoRetrying(false)
      await handleSync(true)
    } finally {
      autoRetryInProgressRef.current = false
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId, destPath, toast])

  useEffect(() => {
    if (!syncStatus || !isPolling) return
    // Don't trigger while an auto-retry is already in progress
    if (autoRetryInProgressRef.current) return

    if (syncStatus.status !== 'syncing') {
      // If the status switched to 'error' with a stall message, auto-retry
      if (syncStatus.status === 'error' && syncStatus.message?.includes('stalled')) {
        console.log('[AUTO-RETRY] Detected stalled sync from status, triggering retry...')
        handleAutoRetry()
      }
      return
    }

    const currentCopied = syncStatus.copied ?? 0
    const now = Date.now()

    // Update last progress timestamp when copied count increases
    if (currentCopied > lastProgressRef.current.copied) {
      lastProgressRef.current = { copied: currentCopied, time: now }
      return
    }

    // Check if progress has stalled
    const elapsed = now - lastProgressRef.current.time
    if (elapsed >= STALL_THRESHOLD_MS && currentCopied > 0 && currentCopied < (syncStatus.total ?? 0)) {
      console.log(`[AUTO-RETRY] Progress stalled for ${Math.round(elapsed / 1000)}s at ${currentCopied}/${syncStatus.total}, triggering retry...`)
      lastProgressRef.current = { copied: currentCopied, time: now } // Reset to avoid re-triggering immediately
      handleAutoRetry()
    }
  }, [syncStatus, isPolling, handleAutoRetry])

  // Ref to keep the sync stream connection alive (AbortController)
  const syncAbortRef = useRef<AbortController | null>(null)

  const handleSync = async (force = false) => {
    if (!jobId || !destPath) {
      toast({
        title: "Error",
        description: "Job ID and destination path are required",
        variant: "destructive",
      })
      return
    }

    setLoading(true)
    setIsPolling(true)
    setSseFailed(false)

    try {
      // Abort any previous sync stream
      syncAbortRef.current?.abort()
      const controller = new AbortController()
      syncAbortRef.current = controller

      const response = await fetch(`/api/jobs/${jobId}/sync`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ destPath, force }),
        signal: controller.signal,
      })

      if (response.status === 409) {
        const data = await response.json()
        setLoading(false)
        setIsPolling(false)
        setSyncConflict(true)
        toast({
          title: "Sync Blocked",
          description: data.error || "Another sync is in progress or locked. Use 'Release & Resync' to unblock.",
          variant: "destructive",
        })
        await checkSyncStatus()
        return
      }

      setSyncConflict(false)

      if (!response.ok) {
        const error = await response.json()
        throw new Error(error.error || "Failed to sync")
      }

      // The POST now returns a streaming response that keeps the Vercel
      // function alive. We need to consume the stream in the background
      // (otherwise the browser may close the connection and Vercel kills the function).
      // Progress is tracked via the separate SSE /stream endpoint.
      if (response.body) {
        const reader = response.body.getReader()
        // Read in background — don't await, just keep the connection open
        const readStream = async () => {
          try {
            while (true) {
              const { done } = await reader.read()
              if (done) break
            }
          } catch {
            // Stream closed (abort, network error) — expected
          }
        }
        readStream() // fire and forget — keeps connection alive
      }

      await checkSyncStatus()

      // Polling/SSE is handled by the useEffect hook above

    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') return
      setIsPolling(false)
      setLoading(false)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to sync",
        variant: "destructive",
      })
    }
  }

  const handleStopSync = async () => {
    if (!jobId) return

    try {
      const response = await fetch(`/api/jobs/${jobId}/sync/cancel`, {
        method: "POST",
      })

      if (!response.ok) {
        const error = await response.json()
        throw new Error(error.error || "Failed to stop sync")
      }

      const data = await response.json()
      setIsPolling(false)
      setLoading(false)
      setSyncConflict(false)
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }

      toast({
        title: data.action === "completed" ? "Sync Complete" : "Sync Stopped",
        description: data.message,
      })

      await checkSyncStatus()
    } catch (error) {
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to stop sync",
        variant: "destructive",
      })
    }
  }

  const handleForceResync = async () => {
    if (!jobId || !destPath) return
    setSyncConflict(false)
    await handleSync(true)
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold">S3 Sync</h1>
        <p className="text-muted-foreground mt-2">
          Sync Veraset job output to GMC S3 bucket
        </p>
      </div>

      <Card className="max-w-2xl">
        <CardHeader>
          <CardTitle>Sync Configuration</CardTitle>
          <CardDescription>
            Copy data from Veraset bucket to GMC destination bucket
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="space-y-2">
            <Label htmlFor="jobId">Job ID</Label>
            <Input
              id="jobId"
              value={jobId || ""}
              disabled
              className="font-mono"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="destPath">Destination S3 Path *</Label>
            <Input
              id="destPath"
              value={destPath}
              onChange={(e) => setDestPath(e.target.value)}
              placeholder="s3://garritz-veraset-data-us-west-2/project-name/"
              required
            />
            <p className="text-sm text-muted-foreground">
              Full S3 path including bucket and prefix
            </p>
          </div>

          <div className="rounded-md bg-muted p-4">
            <p className="text-sm font-medium mb-2">Sync Details</p>
            <div className="space-y-1 text-sm text-muted-foreground">
              <p>Source: <span className="font-mono text-xs">s3://veraset-prd-platform-us-west-2/output/garritz/{jobId}/</span></p>
              <p>Destination: <span className="font-mono text-xs">{destPath || "Not set"}</span></p>
            </div>
          </div>

          {/* Sync Status */}
          {syncStatus && (
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base">Sync Status</CardTitle>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={checkSyncStatus}
                    disabled={loading}
                  >
                    <RefreshCw className={`h-4 w-4 ${isPolling ? 'animate-spin' : ''}`} />
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="flex items-center space-x-2">
                  {syncStatus.status === 'completed' && (
                    <CheckCircle className="h-5 w-5 text-green-500" />
                  )}
                  {syncStatus.status === 'error' && (
                    <AlertCircle className="h-5 w-5 text-red-500" />
                  )}
                  {syncStatus.status === 'cancelled' && (
                    <Square className="h-5 w-5 text-amber-500" />
                  )}
                  {(syncStatus.status === 'syncing' || syncStatus.status === 'not_started') && (
                    <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
                  )}
                  <p className="text-sm font-medium">{syncStatus.message}</p>
                </div>

                {/* Auto-retry indicator */}
                {autoRetrying && (
                  <div className="flex items-center space-x-2 text-sm text-amber-400 bg-amber-950/30 rounded-md p-2">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span>Auto-retrying sync (attempt #{autoRetryCountRef.current})... The sync continues from where it left off.</span>
                  </div>
                )}

                {/* Professional Loader - Shows detailed day-by-day progress */}
                {syncStatus.status === 'syncing' && syncStatus.syncProgress && (
                  <ProfessionalLoader
                    syncProgress={syncStatus.syncProgress}
                    overallProgress={syncStatus.progress}
                    copied={syncStatus.copied}
                    total={syncStatus.total}
                    copiedBytes={syncStatus.copiedBytes}
                    totalBytes={syncStatus.totalBytes}
                  />
                )}
                
                {/* Fallback: Simple progress if detailed progress not available */}
                {syncStatus.total > 0 && (!syncStatus.syncProgress || syncStatus.status !== 'syncing') && (
                  <>
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Progress</span>
                        <span className="font-medium">{syncStatus.progress}%</span>
                      </div>
                      <Progress value={syncStatus.progress} className="h-2" />
                    </div>
                    
                    <div className="grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <p className="text-muted-foreground">Objects</p>
                        <p className="font-medium">
                          {syncStatus.copied.toLocaleString()} / {syncStatus.total.toLocaleString()}
                        </p>
                      </div>
                      <div>
                        <p className="text-muted-foreground">Data</p>
                        <p className="font-medium">
                          {(syncStatus.copiedBytes / 1024 / 1024 / 1024).toFixed(2)} GB / {(syncStatus.totalBytes / 1024 / 1024 / 1024).toFixed(2)} GB
                        </p>
                      </div>
                    </div>
                  </>
                )}
              </CardContent>
            </Card>
          )}

          <div className="flex justify-end flex-wrap gap-2">
            <Button
              variant="outline"
              onClick={() => router.back()}
              disabled={loading || isPolling}
            >
              Back
            </Button>
            {(syncStatus?.status === 'syncing' || isPolling || syncConflict) && (
              <Button
                variant="destructive"
                onClick={handleStopSync}
                disabled={loading}
              >
                <Square className="mr-2 h-4 w-4" />
                Stop Sync
              </Button>
            )}
            {(syncConflict || syncStatus?.status === 'completed' || syncStatus?.status === 'error' || syncStatus?.status === 'cancelled') && !isPolling && (
              <Button
                variant="default"
                onClick={handleForceResync}
                disabled={loading}
              >
                {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <RefreshCw className="mr-2 h-4 w-4" />}
                {syncConflict ? 'Release & Resync' : 'Resync'}
              </Button>
            )}
            {!syncStatus && (
              <Button onClick={() => handleSync()} disabled={loading || !destPath || isPolling}>
                {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Start Sync
              </Button>
            )}
            {syncStatus?.status === 'not_started' && (
              <Button onClick={() => handleSync()} disabled={loading || !destPath || isPolling}>
                {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Start Sync
              </Button>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

export default function SyncPage() {
  return (
    <Suspense fallback={
      <div className="container mx-auto px-4 py-8">
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      </div>
    }>
      <SyncPageContent />
    </Suspense>
  )
}
