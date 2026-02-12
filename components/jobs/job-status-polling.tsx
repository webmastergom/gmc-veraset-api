"use client"

import { useEffect, useState, useRef, useCallback } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { RefreshCw } from "lucide-react"
import { getJobStatus, type VerasetJobStatus } from "@/lib/veraset-client"

function StatusBadge({ status }: { status: string }) {
  const variants: Record<string, "default" | "success" | "warning" | "destructive"> = {
    SUCCESS: "success",
    RUNNING: "warning",
    QUEUED: "default",
    FAILED: "destructive",
  }
  
  return <Badge variant={variants[status] || "default"}>{status}</Badge>
}

export function JobStatusPolling({ jobId }: { jobId: string }) {
  const [status, setStatus] = useState<VerasetJobStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const intervalRef = useRef<NodeJS.Timeout | null>(null)
  const isMountedRef = useRef(true)

  useEffect(() => {
    isMountedRef.current = true
    return () => {
      isMountedRef.current = false
    }
  }, [])

  const fetchStatus = useCallback(async (forceRefresh = false) => {
    if (!isMountedRef.current) return
    
    try {
      setLoading(true)
      setError(null)
      console.log(`🔄 Fetching status for job ${jobId}${forceRefresh ? ' (force refresh)' : ''}...`)
      
      // If force refresh, call the refresh endpoint first
      if (forceRefresh) {
        try {
          const refreshResponse = await fetch(`/api/jobs/${jobId}/refresh`, {
            method: 'POST',
            cache: 'no-store',
            headers: {
              'Cache-Control': 'no-cache',
            },
          })
          
          if (!refreshResponse.ok) {
            const errorData = await refreshResponse.json().catch(() => ({}))
            console.warn(`⚠️ Refresh endpoint returned ${refreshResponse.status}:`, errorData)
            // Continue with normal fetch even if refresh fails
          } else {
            const refreshData = await refreshResponse.json()
            console.log(`✅ Refresh successful:`, refreshData)
            // Small delay to ensure DB is updated
            await new Promise(resolve => setTimeout(resolve, 100))
          }
        } catch (refreshError) {
          console.warn(`⚠️ Refresh endpoint error (continuing with normal fetch):`, refreshError)
          // Continue with normal fetch even if refresh fails
        }
      }
      
      const jobStatus = await getJobStatus(jobId)
      
      if (!isMountedRef.current) return
      
      console.log(`✅ Job ${jobId} status: ${jobStatus.status}`)
      setStatus(jobStatus)
      
      // Stop polling if job is complete
      if (jobStatus.status === "SUCCESS" || jobStatus.status === "FAILED") {
        console.log(`🛑 Job ${jobId} completed, stopping polling`)
        if (intervalRef.current) {
          clearInterval(intervalRef.current)
          intervalRef.current = null
        }
      }
      
      // Continue polling for SCHEDULED, QUEUED, or RUNNING
      if (jobStatus.status === "SCHEDULED" || jobStatus.status === "QUEUED" || jobStatus.status === "RUNNING") {
        console.log(`🔄 Job ${jobId} is ${jobStatus.status}, continuing to poll...`)
      }
      
      return jobStatus
    } catch (err) {
      if (!isMountedRef.current) return
      
      console.error(`❌ Error fetching job status for ${jobId}:`, err)
      const errorMessage = err instanceof Error ? err.message : "Failed to fetch job status"
      setError(errorMessage)
      // Don't clear status on error - keep showing last known status
    } finally {
      if (isMountedRef.current) {
        setLoading(false)
      }
    }
  }, [jobId])

  useEffect(() => {
    // Initial fetch
    fetchStatus()
    
    // Poll every 30 seconds
    intervalRef.current = setInterval(() => {
      if (isMountedRef.current) {
        fetchStatus()
      }
    }, 30000)

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
    }
  }, [jobId, fetchStatus])

  if (loading && !status) {
    return <p className="text-sm text-muted-foreground">Loading status...</p>
  }

  if (error) {
    return (
      <div className="space-y-2">
        <p className="text-sm text-destructive">{error}</p>
        <Button onClick={() => { void fetchStatus(); }} variant="outline" size="sm">
          <RefreshCw className="mr-2 h-4 w-4" />
          Retry
        </Button>
      </div>
    )
  }

  if (!status) {
    return null
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-3">
          <StatusBadge status={status.status} />
          {status.updated_at && (
            <p className="text-sm text-muted-foreground">
              Last updated: {new Date(status.updated_at).toLocaleTimeString()}
            </p>
          )}
        </div>
        <Button 
          onClick={() => fetchStatus(true)} 
          variant="outline" 
          size="sm" 
          disabled={loading}
        >
          <RefreshCw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>
      
      {status.error_message && (
        <div className="rounded-md bg-destructive/10 p-3">
          <p className="text-sm font-medium text-destructive">Error</p>
          <p className="text-sm text-destructive mt-1">{status.error_message}</p>
        </div>
      )}
      
      {(status.status === "QUEUED" || status.status === "RUNNING" || status.status === "SCHEDULED") && (
        <p className="text-sm text-muted-foreground">
          Status will auto-refresh every 30 seconds. This page will update when the job completes.
        </p>
      )}
    </div>
  )
}
