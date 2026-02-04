"use client"

import { useState, useEffect } from "react"
import { useSearchParams } from "next/navigation"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { useToast } from "@/hooks/use-toast"
import { Loader2 } from "lucide-react"
import { useRouter } from "next/navigation"

export default function SyncPage() {
  const searchParams = useSearchParams()
  const router = useRouter()
  const { toast } = useToast()
  const jobId = searchParams.get("jobId")
  const [loading, setLoading] = useState(false)
  const [destPath, setDestPath] = useState("")

  useEffect(() => {
    if (jobId) {
      // Generate default path from job ID
      setDestPath(`s3://garritz-veraset-data-us-west-2/job-${jobId.slice(0, 8)}/`)
    }
  }, [jobId])

  const handleSync = async () => {
    if (!jobId || !destPath) {
      toast({
        title: "Error",
        description: "Job ID and destination path are required",
        variant: "destructive",
      })
      return
    }

    setLoading(true)

    try {
      const response = await fetch(`/api/jobs/${jobId}/sync`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ destPath }),
      })

      if (!response.ok) {
        const error = await response.json()
        throw new Error(error.error || "Failed to sync")
      }

      const result = await response.json()

      toast({
        title: "Sync Complete",
        description: `Synced ${result.copied} objects (${(result.totalBytes / 1024 / 1024 / 1024).toFixed(2)} GB)`,
      })

      router.push(`/jobs/${jobId}`)
    } catch (error) {
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to sync",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
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

          <div className="flex justify-end space-x-4">
            <Button
              variant="outline"
              onClick={() => router.back()}
              disabled={loading}
            >
              Cancel
            </Button>
            <Button onClick={handleSync} disabled={loading || !destPath}>
              {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              Start Sync
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
