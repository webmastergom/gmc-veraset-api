"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { Database, RefreshCw } from "lucide-react"
import { getJobStatus, type VerasetJobStatus } from "@/lib/veraset-client"

interface S3StorageSectionProps {
  jobId: string
  initialStatus: string
  s3SourcePath: string
  s3DestPath: string | null
}

export function S3StorageSection({ 
  jobId, 
  initialStatus, 
  s3SourcePath, 
  s3DestPath: initialS3DestPath 
}: S3StorageSectionProps) {
  const [status, setStatus] = useState<string>(initialStatus)
  const [s3DestPath, setS3DestPath] = useState<string | null>(initialS3DestPath)
  const [loading, setLoading] = useState(false)

  // Poll for status updates if job is not complete
  useEffect(() => {
    if (status === "SUCCESS" || status === "FAILED") {
      return // Stop polling if job is complete
    }

    const interval = setInterval(async () => {
      try {
        const jobStatus = await getJobStatus(jobId)
        setStatus(jobStatus.status)
        
        // If status changed to SUCCESS, fetch updated job data
        if (jobStatus.status === "SUCCESS" && status !== "SUCCESS") {
          // Fetch full job data to get updated s3DestPath
          const jobResponse = await fetch(`/api/jobs/${jobId}`, {
            cache: 'no-store',
            headers: {
              'Cache-Control': 'no-cache, no-store, must-revalidate',
            },
          })
          if (jobResponse.ok) {
            const jobData = await jobResponse.json()
            setS3DestPath(jobData.s3DestPath || jobData.s3_dest_path || null)
          }
        }
      } catch (error) {
        console.error('Error polling job status:', error)
      }
    }, 10000) // Poll every 10 seconds

    return () => clearInterval(interval)
  }, [jobId, status])

  // Manual refresh handler
  const handleRefresh = async () => {
    setLoading(true)
    try {
      // Force refresh from Veraset
      const refreshResponse = await fetch(`/api/jobs/${jobId}/refresh`, {
        method: 'POST',
        cache: 'no-store',
      })
      
      if (refreshResponse.ok) {
        const refreshData = await refreshResponse.json()
        if (refreshData.job) {
          setStatus(refreshData.job.status)
          setS3DestPath(refreshData.job.s3DestPath || refreshData.job.s3_dest_path || null)
        }
      }
      
      // Also fetch full job data
      const jobResponse = await fetch(`/api/jobs/${jobId}`, {
        cache: 'no-store',
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
        },
      })
      if (jobResponse.ok) {
        const jobData = await jobResponse.json()
        setStatus(jobData.status)
        setS3DestPath(jobData.s3DestPath || jobData.s3_dest_path || null)
      }
    } catch (error) {
      console.error('Error refreshing job:', error)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>S3 Storage</CardTitle>
        <CardDescription>Data location and sync status</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div>
          <p className="text-sm font-medium text-muted-foreground">Source Path</p>
          <p className="mt-1 font-mono text-sm break-all">
            {s3SourcePath}
          </p>
        </div>
        {s3DestPath ? (
          <>
            <div>
              <p className="text-sm font-medium text-muted-foreground">Destination Path</p>
              <p className="mt-1 font-mono text-sm break-all">
                {s3DestPath}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Link href={`/datasets/${(s3DestPath.replace(/\/$/, '').split('/').filter(Boolean).pop()) || `job-${jobId.slice(0, 8)}`}`}>
                <Button>
                  <Database className="mr-2 h-4 w-4" />
                  View Dataset
                </Button>
              </Link>
              <Link href={`/sync?jobId=${jobId}${s3DestPath ? `&destPath=${encodeURIComponent(s3DestPath)}` : ''}`}>
                <Button variant="outline">
                  <RefreshCw className="mr-2 h-4 w-4" />
                  Re-sync
                </Button>
              </Link>
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              Use Re-sync to run the sync again and verify all data is downloaded.
            </p>
          </>
        ) : status === "SUCCESS" ? (
          <>
            <Link href={`/sync?jobId=${jobId}`}>
              <Button disabled={loading}>
                <Database className="mr-2 h-4 w-4" />
                {loading ? "Refreshing..." : "Sync to S3"}
              </Button>
            </Link>
            <Button 
              variant="outline" 
              size="sm" 
              onClick={handleRefresh}
              disabled={loading}
              className="ml-2"
            >
              Refresh
            </Button>
          </>
        ) : (
          <>
            <p className="text-sm text-muted-foreground">
              Sync available after job completes
            </p>
            <Button 
              variant="outline" 
              size="sm" 
              onClick={handleRefresh}
              disabled={loading}
            >
              {loading ? "Refreshing..." : "Check Status"}
            </Button>
          </>
        )}
      </CardContent>
    </Card>
  )
}
