import { MainLayout } from "@/components/layout/main-layout"
import { StatsCard } from "@/components/dashboard/stats-card"
import { RecentJobs } from "@/components/dashboard/recent-jobs"
import { Zap, MapPin, Database, HardDrive } from "lucide-react"

async function getDashboardStats() {
  try {
    // Use server-side imports instead of fetch for better reliability
    const { getUsage } = await import("@/lib/usage");
    const { getAllJobs } = await import("@/lib/jobs");
    const { getConfig, initConfigIfNeeded } = await import("@/lib/s3-config");
    const { initialPOICollectionsData } = await import("@/lib/seed-jobs");
    
    // Get API usage
    const usage = await getUsage();
    
    // Get jobs
    const jobs = await getAllJobs();
    
    // Get active jobs
    const activeJobs = jobs.filter(job => 
      job.status === "QUEUED" || job.status === "RUNNING"
    ).length
    
    // Get POI collections
    const collectionsData = await initConfigIfNeeded('poi-collections', initialPOICollectionsData);
    const collections = Object.values(collectionsData);
    const totalPOIs = collections.reduce((sum: number, col: any) => sum + (col.poiCount || 0), 0);
    
    // Calculate total data volume from synced jobs
    const syncedJobs = jobs.filter(job => job.s3DestPath && job.totalBytes);
    const totalBytes = syncedJobs.reduce((sum: number, job) => sum + (job.totalBytes || 0), 0);
    const dataVolume = totalBytes > 0 
      ? `${(totalBytes / 1024 / 1024 / 1024).toFixed(2)} GB`
      : "0 GB";
    
    return {
      apiUsage: {
        used: usage.used,
        limit: usage.limit,
        remaining: usage.remaining,
        percentage: Math.round((usage.used / usage.limit) * 100),
        externalJobs: usage.externalJobs || 0,
      },
      activeJobs,
      totalPOIs,
      totalDevices: 0, // TODO: Calculate from datasets
      dataVolume,
    };
  } catch (error) {
    console.error("Error fetching dashboard stats:", error);
    return {
      apiUsage: { used: 0, limit: 200, remaining: 200, percentage: 0, externalJobs: 0 },
      activeJobs: 0,
      totalPOIs: 0,
      totalDevices: 0,
      dataVolume: "0 GB",
    };
  }
}

async function getRecentJobs() {
  try {
    const { getAllJobs } = await import("@/lib/jobs");
    const jobs = await getAllJobs();
    
      return jobs
      .slice(0, 5)
      .map((job) => ({
        jobId: job.jobId,
        name: job.name,
        status: job.status,
        type: job.type,
        poiCount: job.poiCount || 0,
        external: job.external || false,
        createdAt: job.createdAt,
      }));
  } catch (error) {
    console.error("Error fetching recent jobs:", error);
    return [];
  }
}

export const dynamic = 'force-dynamic'
export const revalidate = 0

export default async function DashboardPage() {
  const stats = await getDashboardStats()
  const recentJobs = await getRecentJobs()

  return (
    <MainLayout>
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-foreground mb-2">Dashboard</h1>
        <p className="text-muted-foreground">Geospatial mobility intelligence at your fingertips</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-4 gap-6 mb-8">
        <StatsCard
          title="API Calls"
          value={stats.apiUsage.remaining.toString()}
          subtitle="remaining this month"
          icon={Zap}
          trend={{ value: stats.apiUsage.used, label: 'used' }}
          color="lime"
        />
        <StatsCard
          title="Active Jobs"
          value={stats.activeJobs.toString()}
          subtitle="in progress"
          icon={Database}
          color="blue"
        />
        <StatsCard
          title="Total POIs"
          value={stats.totalPOIs.toLocaleString()}
          subtitle="points of interest"
          icon={MapPin}
          color="purple"
        />
        <StatsCard
          title="Data Volume"
          value={stats.dataVolume}
          subtitle="synced datasets"
          icon={HardDrive}
          color="orange"
        />
      </div>

      {/* Main Content Grid */}
      <div className="grid grid-cols-3 gap-6">
        {/* Recent Jobs - Full width for now */}
        <div className="col-span-3">
          <div className="bg-card border border-border rounded-2xl p-6">
            <h2 className="text-lg font-semibold text-foreground mb-6">Recent Jobs</h2>
            <RecentJobs jobs={recentJobs} />
          </div>
        </div>
      </div>
    </MainLayout>
  )
}
