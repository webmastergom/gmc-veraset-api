import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { ArrowLeft, RefreshCw, Database, ExternalLink, Info, AlertTriangle, ShieldCheck, Users } from "lucide-react"
import { JobStatusPolling } from "@/components/jobs/job-status-polling"
import { S3StorageSection } from "@/components/jobs/s3-storage-section"
import { JobAuditDialog } from "@/components/jobs/job-audit-dialog"
import { MainLayout } from "@/components/layout/main-layout"
import { AudienceAgentToggle } from "@/components/jobs/audience-agent-toggle"
import { inferCountryFromName } from "@/lib/country-inference"
import dynamic from "next/dynamic"

// Dynamic import to avoid bundling server-side code in client
const RefreshStatusButton = dynamic(
  () => import("@/components/jobs/refresh-status-button").then(mod => ({ default: mod.RefreshStatusButton })),
  { ssr: false }
)

async function getJob(id: string) {
  try {
    const { getJob: getJobById } = await import("@/lib/jobs");
    const job = await getJobById(id);
    
    if (!job) {
      return null;
    }
    
    return {
      job_id: job.jobId,
      name: job.name,
      status: job.status,
      type: job.type,
      poi_count: job.poiCount || 0,
      external: job.external || false,
      config: {
        dateRange: job.dateRange,
        radius: job.radius,
        schema: job.schema,
      },
      created_at: job.createdAt,
      updated_at: job.updatedAt,
      error_message: job.errorMessage,
      s3_source_path: job.s3SourcePath,
      s3_dest_path: job.s3DestPath || null,
      veraset_payload: job.verasetPayload,
      actual_date_range: job.actualDateRange,
      date_range_discrepancy: job.dateRangeDiscrepancy,
      has_audit_trail: !!job.auditTrail,
      audience_agent_enabled: job.audienceAgentEnabled || false,
      country: job.country || inferCountryFromName(job.name),
    };
  } catch (error) {
    console.error("Error fetching job:", error);
    return null;
  }
}

function StatusBadge({ status }: { status: string }) {
  const variants: Record<string, "default" | "success" | "warning" | "destructive"> = {
    SUCCESS: "success",
    RUNNING: "warning",
    QUEUED: "default",
    SCHEDULED: "warning", // SCHEDULED is similar to RUNNING
    FAILED: "destructive",
  }
  
  return <Badge variant={variants[status] || "default"}>{status}</Badge>
}

export default async function JobDetailPage({
  params,
}: {
  params: { id: string }
}) {
  const job = await getJob(params.id)

  if (!job) {
    return (
      <MainLayout>
        <Card>
          <CardContent className="pt-6">
            <p className="text-center text-muted-foreground">
              Job not found
            </p>
            <div className="mt-4 text-center">
              <Link href="/jobs">
                <Button variant="outline">
                  <ArrowLeft className="mr-2 h-4 w-4" />
                  Back to Jobs
                </Button>
              </Link>
            </div>
          </CardContent>
        </Card>
      </MainLayout>
    )
  }

  return (
    <MainLayout>
      <div className="mb-6">
        <Link href="/jobs">
          <Button variant="ghost" className="mb-4">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Back to Jobs
          </Button>
        </Link>
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold">{job.name}</h1>
            <p className="text-muted-foreground mt-2">
              Job ID: <span className="font-mono">{job.job_id}</span>
            </p>
          </div>
          <div className="flex items-center space-x-2">
            <StatusBadge status={job.status} />
            <Badge variant="secondary">{job.type}</Badge>
          </div>
        </div>
      </div>

      {/* External Job Notice */}
      {job.external && (
        <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-4 mb-6">
          <div className="flex items-center gap-2 text-blue-400">
            <Info className="w-4 h-4" />
            <span className="font-medium">External Job</span>
          </div>
          <p className="text-sm text-blue-300/80 mt-1">
            This job was created via the external API.
          </p>
        </div>
      )}

      {/* Date Range Discrepancy Warning */}
      {job.date_range_discrepancy && job.date_range_discrepancy.missingDays > 0 && (
        <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-lg p-4 mb-6">
          <div className="flex items-center gap-2 text-yellow-400">
            <AlertTriangle className="w-4 h-4" />
            <span className="font-medium">Date Range Discrepancy</span>
          </div>
          <p className="text-sm text-yellow-300/80 mt-1">
            <strong>Requested:</strong> {job.date_range_discrepancy.requestedDays} days
            {job.veraset_payload?.date_range && (
              <> ({job.veraset_payload.date_range.from_date} to {job.veraset_payload.date_range.to_date})</>
            )}
          </p>
          <p className="text-sm text-yellow-300/80">
            <strong>Received:</strong> {job.date_range_discrepancy.actualDays} days
            {job.actual_date_range && (
              <> ({job.actual_date_range.from} to {job.actual_date_range.to})</>
            )}
          </p>
          <p className="text-sm text-yellow-300/80 font-semibold mt-2">
            ⚠️ Missing {job.date_range_discrepancy.missingDays} day{job.date_range_discrepancy.missingDays !== 1 ? 's' : ''} of data
          </p>
          <p className="text-xs text-yellow-300/60 mt-2">
            This discrepancy indicates that Veraset did not return all requested days. Check the Veraset API response or contact support.
          </p>
        </div>
      )}

      <div className="grid gap-6 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Job Details</CardTitle>
            <CardDescription>Configuration and metadata</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Status</p>
              <div className="mt-1">
                <StatusBadge status={job.status} />
              </div>
            </div>
            <div>
              <p className="text-sm font-medium text-muted-foreground">Type</p>
              <p className="mt-1">{job.type}</p>
            </div>
            <div>
              <p className="text-sm font-medium text-muted-foreground">POI Count</p>
              <p className="mt-1">{job.poi_count.toLocaleString()}</p>
            </div>
            <div>
              <p className="text-sm font-medium text-muted-foreground">Date Range</p>
              <p className="mt-1">
                {job.config.dateRange.from} to {job.config.dateRange.to}
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-muted-foreground">Radius</p>
              <p className="mt-1">{job.config.radius}m</p>
            </div>
            <div>
              <p className="text-sm font-medium text-muted-foreground">Schema</p>
              <p className="mt-1">{job.config.schema}</p>
            </div>
            <div>
              <p className="text-sm font-medium text-muted-foreground">Created</p>
              <p className="mt-1 text-sm">
                {new Date(job.created_at).toLocaleString()}
              </p>
            </div>
            {job.updated_at && (
              <div>
                <p className="text-sm font-medium text-muted-foreground">Last Updated</p>
                <p className="mt-1 text-sm">
                  {new Date(job.updated_at).toLocaleString()}
                </p>
              </div>
            )}
            {job.error_message && (
              <div>
                <p className="text-sm font-medium text-destructive">Error</p>
                <p className="mt-1 text-sm text-destructive">{job.error_message}</p>
              </div>
            )}
          </CardContent>
        </Card>

        <S3StorageSection
          jobId={job.job_id}
          initialStatus={job.status}
          s3SourcePath={job.s3_source_path || ''}
          s3DestPath={job.s3_dest_path}
        />
      </div>

      {/* Status Polling Component */}
      {(job.status === "QUEUED" || job.status === "RUNNING" || job.status === "SCHEDULED") && (
        <Card className="mt-6">
          <CardHeader>
            <CardTitle>Status Monitor</CardTitle>
            <CardDescription>Auto-refreshing job status</CardDescription>
          </CardHeader>
          <CardContent>
            <JobStatusPolling jobId={job.job_id} />
          </CardContent>
        </Card>
      )}
      
      {/* Manual Refresh Button for all jobs */}
      <Card className="mt-6">
        <CardHeader>
          <CardTitle>Manual Status Refresh</CardTitle>
          <CardDescription>Force check current status from Veraset API</CardDescription>
        </CardHeader>
        <CardContent>
          <RefreshStatusButton jobId={job.job_id} />
        </CardContent>
      </Card>

      {/* Roamy Toggle */}
      {job.status === 'SUCCESS' && (
        <Card className="mt-6">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Users className="h-5 w-5" />
              Roamy
            </CardTitle>
            <CardDescription>
              Roaming Observation Agent for Mobility Yield
            </CardDescription>
          </CardHeader>
          <CardContent>
            <AudienceAgentToggle
              jobId={job.job_id}
              initialEnabled={job.audience_agent_enabled}
              initialCountry={job.country}
            />
          </CardContent>
        </Card>
      )}

      {/* Payload Verification Audit */}
      <Card className="mt-6">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <ShieldCheck className="h-5 w-5" />
            Payload Verification
          </CardTitle>
          <CardDescription>
            Verify that Veraset received exactly what you requested. Compare user input with the payload sent to Veraset.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <JobAuditDialog jobId={job.job_id} />
          <p className="text-xs text-muted-foreground mt-2">
            This audit trail captures the exact payload sent to Veraset and compares it with your request to ensure data integrity.
          </p>
        </CardContent>
      </Card>
    </MainLayout>
  )
}
