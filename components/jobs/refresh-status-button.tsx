"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { RefreshCw, Loader2, CheckCircle, XCircle } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

export function RefreshStatusButton({ jobId }: { jobId: string }) {
  const [loading, setLoading] = useState(false)
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
  const router = useRouter()
  const { toast } = useToast()

  const handleRefresh = async () => {
    setLoading(true)
    try {
      console.log(`🔄 Manually refreshing status for job ${jobId}...`)
      const response = await fetch(`/api/jobs/${jobId}/refresh`, {
        method: 'POST',
        cache: 'no-store',
      })

      const data = await response.json()

      if (data.success) {
        setLastRefresh(new Date())
        
        if (data.statusChanged) {
          toast({
            title: "Status Updated",
            description: `Job status changed from ${data.oldStatus} to ${data.newStatus}`,
          })
          // Refresh the page to show updated status
          router.refresh()
        } else {
          toast({
            title: "Status Checked",
            description: `Job status is still ${data.newStatus}`,
          })
          // Still refresh to show latest data
          router.refresh()
        }
      } else {
        toast({
          title: "Refresh Failed",
          description: data.error || "Failed to refresh job status",
          variant: "destructive",
        })
      }
    } catch (error: any) {
      console.error('Error refreshing status:', error)
      toast({
        title: "Error",
        description: error.message || "Failed to refresh job status",
        variant: "destructive",
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-4">
      <Button 
        onClick={handleRefresh} 
        disabled={loading}
        variant="outline"
        className="w-full sm:w-auto"
      >
        {loading ? (
          <>
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            Refreshing...
          </>
        ) : (
          <>
            <RefreshCw className="mr-2 h-4 w-4" />
            Refresh Status from Veraset
          </>
        )}
      </Button>
      
      {lastRefresh && (
        <p className="text-sm text-muted-foreground">
          Last refreshed: {lastRefresh.toLocaleTimeString()}
        </p>
      )}
      
      <div className="text-sm text-muted-foreground space-y-1">
        <p>This will:</p>
        <ul className="list-disc list-inside space-y-1 ml-2">
          <li>Query Veraset API for the latest job status</li>
          <li>Update the job status in our database</li>
          <li>Refresh this page to show the updated status</li>
        </ul>
      </div>
    </div>
  )
}
