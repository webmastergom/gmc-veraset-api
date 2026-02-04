import Link from 'next/link'
import { CheckCircle, Clock, AlertCircle } from 'lucide-react'

const statusConfig = {
  SUCCESS: { icon: CheckCircle, color: 'text-green-600 dark:text-green-400', bg: 'bg-green-500/10' },
  QUEUED: { icon: Clock, color: 'text-yellow-600 dark:text-yellow-400', bg: 'bg-yellow-500/10' },
  RUNNING: { icon: Clock, color: 'text-blue-600 dark:text-blue-400', bg: 'bg-blue-500/10' },
  FAILED: { icon: AlertCircle, color: 'text-red-600 dark:text-red-400', bg: 'bg-red-500/10' },
}

export function RecentJobs({ jobs }: { jobs: any[] }) {
  const recentJobs = jobs.slice(0, 5)

  return (
    <div className="space-y-3">
      {recentJobs.length === 0 ? (
        <p className="text-sm text-muted-foreground text-center py-8">No jobs yet</p>
      ) : (
        recentJobs.map((job) => {
          const status = statusConfig[job.status as keyof typeof statusConfig] || statusConfig.QUEUED
          const StatusIcon = status.icon

          return (
            <Link
              key={job.jobId}
              href={`/jobs/${job.jobId}`}
              className="flex items-center gap-4 p-4 rounded-xl bg-secondary/50 hover:bg-secondary border border-transparent hover:border-border transition-all group"
            >
              <div className={`p-2 rounded-lg ${status.bg}`}>
                <StatusIcon className={`w-4 h-4 ${status.color}`} />
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <p className="text-sm font-medium text-foreground truncate">
                    {job.name}
                  </p>
                  {(job as any).external && (
                    <span className="text-xs px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">
                      External
                    </span>
                  )}
                </div>
                <p className="text-xs text-muted-foreground">
                  {job.poiCount?.toLocaleString() || 0} POIs • {job.type || 'pings'}
                </p>
              </div>

              <span className={`text-xs px-2 py-1 rounded-full ${status.bg} ${status.color}`}>
                {job.status}
              </span>
            </Link>
          )
        })
      )}

      <Link
        href="/jobs"
        className="block text-center text-sm text-theme-accent hover:opacity-80 py-3 transition-colors"
      >
        View all jobs →
      </Link>
    </div>
  )
}
