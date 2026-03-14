'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { MainLayout } from '@/components/layout/main-layout'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Plus, Layers, Calendar, MapPin, ArrowRight } from 'lucide-react'

interface MegaJobSummary {
  megaJobId: string
  name: string
  mode: 'auto-split' | 'manual-group'
  status: string
  progress: { created: number; synced: number; failed: number; total: number }
  subJobIds: string[]
  createdAt: string
}

const statusColors: Record<string, string> = {
  planning: 'bg-blue-500/20 text-blue-400',
  creating: 'bg-yellow-500/20 text-yellow-400',
  running: 'bg-orange-500/20 text-orange-400',
  consolidating: 'bg-purple-500/20 text-purple-400',
  completed: 'bg-green-500/20 text-green-400',
  partial: 'bg-amber-500/20 text-amber-400',
  error: 'bg-red-500/20 text-red-400',
}

export default function MegaJobsPage() {
  const [megaJobs, setMegaJobs] = useState<MegaJobSummary[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/mega-jobs', { credentials: 'include' })
      .then((r) => r.json())
      .then((data) => setMegaJobs(Array.isArray(data) ? data : []))
      .catch(() => setMegaJobs([]))
      .finally(() => setLoading(false))
  }, [])

  return (
    <MainLayout>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-2">
              <Layers className="h-6 w-6" /> Mega-Jobs
            </h1>
            <p className="text-muted-foreground mt-1">
              Consolidated analysis beyond Veraset limits (31 days / 25K POIs)
            </p>
          </div>
          <Link href="/mega-jobs/new">
            <Button>
              <Plus className="h-4 w-4 mr-2" /> New Mega-Job
            </Button>
          </Link>
        </div>

        {loading ? (
          <p className="text-muted-foreground">Loading...</p>
        ) : megaJobs.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center text-muted-foreground">
              <Layers className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p className="text-lg font-medium">No mega-jobs yet</p>
              <p className="mt-1">Create one to analyze datasets beyond Veraset limits.</p>
              <Link href="/mega-jobs/new">
                <Button className="mt-4" variant="outline">
                  <Plus className="h-4 w-4 mr-2" /> Create Mega-Job
                </Button>
              </Link>
            </CardContent>
          </Card>
        ) : (
          <div className="grid gap-4">
            {megaJobs.map((mj) => (
              <Link key={mj.megaJobId} href={`/mega-jobs/${mj.megaJobId}`}>
                <Card className="hover:border-primary/50 transition-colors cursor-pointer">
                  <CardContent className="py-4">
                    <div className="flex items-center justify-between">
                      <div className="flex-1">
                        <div className="flex items-center gap-3">
                          <h3 className="font-semibold text-lg">{mj.name}</h3>
                          <Badge className={statusColors[mj.status] || 'bg-gray-500/20 text-gray-400'}>
                            {mj.status}
                          </Badge>
                          <Badge variant="outline">
                            {mj.mode === 'auto-split' ? 'Auto-split' : 'Manual group'}
                          </Badge>
                        </div>
                        <div className="flex items-center gap-4 mt-2 text-sm text-muted-foreground">
                          <span>
                            {mj.progress.synced}/{mj.progress.total} sub-jobs synced
                            {mj.progress.failed > 0 && ` (${mj.progress.failed} failed)`}
                          </span>
                          <span>{new Date(mj.createdAt).toLocaleDateString()}</span>
                        </div>
                      </div>
                      <ArrowRight className="h-5 w-5 text-muted-foreground" />
                    </div>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>
        )}
      </div>
    </MainLayout>
  )
}
