'use client';

import { useState, useEffect, useMemo } from 'react';
import { MainLayout } from "@/components/layout/main-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import Link from "next/link"
import { Plus, LayoutGrid, List, Search, Calendar, MapPin, Database, ExternalLink, Clock, Users } from "lucide-react"

type ViewMode = 'modern' | 'classic';

interface Job {
  id: string;
  job_id: string;
  name: string;
  status: string;
  type: string;
  poi_count: number;
  created_at: string;
  s3_dest_path?: string;
  dateRange?: { from: string; to: string };
  summaryMetrics?: string;
  objectCount?: number;
  external?: boolean;
  audienceAgentEnabled?: boolean;
}

function StatusBadge({ status }: { status: string }) {
  const variants: Record<string, "default" | "success" | "warning" | "destructive"> = {
    SUCCESS: "success",
    RUNNING: "warning",
    QUEUED: "default",
    SCHEDULED: "warning",
    FAILED: "destructive",
  }
  
  return <Badge variant={variants[status] || "default"}>{status}</Badge>
}

export default function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [viewMode, setViewMode] = useState<ViewMode>('modern');

  useEffect(() => {
    fetch('/api/jobs', {
      credentials: 'include',
    })
      .then(r => r.json())
      .then(data => {
        // Normalize job data - ensure job_id exists
        const normalizedJobs = (data || []).map((job: any) => ({
          id: job.jobId || job.job_id || job.id,
          job_id: job.jobId || job.job_id || job.id,
          name: job.name || 'Unnamed Job',
          status: job.status || 'QUEUED',
          type: job.type || 'pings',
          poi_count: job.poiCount || job.poi_count || 0,
          created_at: job.createdAt || job.created_at || new Date().toISOString(),
          s3_dest_path: job.s3DestPath || job.s3_dest_path,
          dateRange: job.dateRange || job.date_range,
          summaryMetrics: job.summaryMetrics || job.summary_metrics,
          objectCount: job.objectCount || job.object_count,
          external: job.external || false,
          audienceAgentEnabled: job.audienceAgentEnabled || false,
        }));
        setJobs(normalizedJobs);
      })
      .catch(err => {
        console.error('Error fetching jobs:', err);
        setJobs([]);
      })
      .finally(() => setLoading(false));
  }, []);

  // Filter jobs by search query
  const filteredJobs = useMemo(() => {
    if (!searchQuery.trim()) return jobs;
    
    const query = searchQuery.toLowerCase();
    return jobs.filter(job =>
      job.name.toLowerCase().includes(query) ||
      job.job_id.toLowerCase().includes(query) ||
      job.type.toLowerCase().includes(query) ||
      job.status.toLowerCase().includes(query)
    );
  }, [jobs, searchQuery]);

  if (loading) {
    return (
      <MainLayout>
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[#c8ff00]"></div>
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout>
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
        <div>
          <h1 className="text-3xl font-bold text-white">Jobs</h1>
          <p className="text-gray-500 mt-2">
            Manage your Veraset API jobs
          </p>
        </div>
        <div className="flex items-center gap-2">
          {/* View Mode Toggle */}
          <div className="flex items-center border border-[#222] rounded-lg p-1 bg-[#0a0a0a]">
            <Button
              variant="ghost"
              size="sm"
              className={`px-3 ${viewMode === 'modern' ? 'bg-[#1a1a1a] text-white' : 'text-gray-500 hover:text-white'}`}
              onClick={() => setViewMode('modern')}
            >
              <LayoutGrid className="h-4 w-4" />
            </Button>
            <Button
              variant="ghost"
              size="sm"
              className={`px-3 ${viewMode === 'classic' ? 'bg-[#1a1a1a] text-white' : 'text-gray-500 hover:text-white'}`}
              onClick={() => setViewMode('classic')}
            >
              <List className="h-4 w-4" />
            </Button>
          </div>
          <Link href="/jobs/new">
            <Button>
              <Plus className="mr-2 h-4 w-4" />
              New Job
            </Button>
          </Link>
        </div>
      </div>

      {/* Search */}
      <div className="mb-6">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-500" />
          <Input
            placeholder="Search jobs..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10 bg-[#0a0a0a] border-[#222] focus:border-[#333]"
          />
        </div>
      </div>

      {filteredJobs.length === 0 ? (
        <Card className="bg-[#111] border-[#1a1a1a]">
          <CardContent className="py-12 text-center">
            <p className="text-gray-500">
              {searchQuery ? 'No jobs found matching your search' : 'No jobs yet. Create your first job to get started.'}
            </p>
          </CardContent>
        </Card>
      ) : viewMode === 'modern' ? (
        /* Modern View - Grid Cards */
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {filteredJobs.map((job) => (
            <Card key={job.id} className="bg-[#111] border-[#1a1a1a] hover:border-[#333] transition-colors">
              <CardHeader>
                <div className="flex items-start justify-between">
                  <div className="flex-1 min-w-0">
                    <Link href={`/jobs/${job.job_id}`}>
                      <CardTitle className="text-white hover:text-[#c8ff00] transition-colors truncate">
                        {job.name}
                      </CardTitle>
                    </Link>
                    {job.job_id && (
                      <p className="text-xs text-gray-500 font-mono mt-1">
                        {job.job_id.slice(0, 8)}...
                      </p>
                    )}
                  </div>
                  <StatusBadge status={job.status} />
                </div>
                <div className="flex flex-wrap gap-1.5 mt-2">
                  {job.external && (
                    <span className="text-xs px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">
                      External
                    </span>
                  )}
                  {job.audienceAgentEnabled && (
                    <span className="text-xs px-2 py-0.5 rounded-full bg-gmc-primary/10 text-gmc-primary border border-gmc-primary/20 flex items-center gap-1">
                      <Users className="h-3 w-3" />
                      Roamy
                    </span>
                  )}
                </div>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center gap-2 text-sm">
                  <Badge variant="secondary">{job.type}</Badge>
                  <span className="text-gray-400">•</span>
                  <span className="text-gray-400">{job.poi_count.toLocaleString()} POIs</span>
                </div>
                
                {job.dateRange?.from && job.dateRange?.to && (
                  <div className="flex items-center gap-2 text-sm text-gray-400">
                    <Calendar className="h-4 w-4" />
                    <span>{job.dateRange.from} to {job.dateRange.to}</span>
                  </div>
                )}

                {job.summaryMetrics && (
                  <div className="text-sm text-gray-400">
                    {job.summaryMetrics}
                  </div>
                )}

                {job.s3_dest_path && (
                  <div className="flex items-center gap-2 text-sm text-gray-400">
                    <Database className="h-4 w-4" />
                    <span className="truncate">{job.objectCount?.toLocaleString() || 'N/A'} objects</span>
                  </div>
                )}

                <div className="flex items-center gap-2 pt-2 border-t border-[#1a1a1a]">
                  <Link href={`/jobs/${job.job_id}`} className="flex-1">
                    <Button variant="outline" size="sm" className="w-full">
                      View Details
                    </Button>
                  </Link>
                  {job.status === "SUCCESS" && job.s3_dest_path && (
                    <Link href={`/sync?jobId=${job.job_id}`}>
                      <Button variant="ghost" size="sm">
                        <Database className="h-4 w-4" />
                      </Button>
                    </Link>
                  )}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : (
        /* Classic View - Table */
        <Card className="bg-[#111] border-[#1a1a1a]">
          <CardHeader>
            <CardTitle className="text-white">All Jobs</CardTitle>
            <CardDescription className="text-gray-500">View and manage your Veraset jobs</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-[#1a1a1a]">
                  <tr className="border-b border-[#222]">
                    <th className="text-left p-4 font-semibold text-white">Description</th>
                    <th className="text-left p-4 font-semibold text-white">Type</th>
                    <th className="text-left p-4 font-semibold text-white">POIs</th>
                    <th className="text-left p-4 font-semibold text-white">Date Range</th>
                    <th className="text-left p-4 font-semibold text-white">Status</th>
                    <th className="text-left p-4 font-semibold text-white">Summary</th>
                    <th className="text-left p-4 font-semibold text-white">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredJobs.map((job) => (
                    <tr key={job.id} className="border-b border-[#1a1a1a] hover:bg-[#151515]">
                      <td className="p-4">
                        <div className="flex items-center gap-2">
                          <Link
                            href={`/jobs/${job.job_id}`}
                            className="font-semibold text-white hover:text-[#c8ff00] transition-colors"
                          >
                            {job.name}
                          </Link>
                          {job.external && (
                            <span className="text-xs px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">
                              External
                            </span>
                          )}
                          {job.audienceAgentEnabled && (
                            <span className="text-xs px-2 py-0.5 rounded-full bg-gmc-primary/10 text-gmc-primary border border-gmc-primary/20 flex items-center gap-1">
                              <Users className="h-3 w-3" />
                              Audience
                            </span>
                          )}
                        </div>
                        {job.job_id && (
                          <p className="text-xs text-gray-500 font-mono mt-1">
                            {job.job_id.slice(0, 8)}...
                          </p>
                        )}
                      </td>
                      <td className="p-4">
                        <Badge variant="secondary">{job.type}</Badge>
                      </td>
                      <td className="p-4 font-semibold text-white">{job.poi_count.toLocaleString()}</td>
                      <td className="p-4 text-sm text-gray-400">
                        {job.dateRange?.from && job.dateRange?.to ? (
                          <span>{job.dateRange.from} to {job.dateRange.to}</span>
                        ) : (
                          <span className="text-gray-600">N/A</span>
                        )}
                      </td>
                      <td className="p-4">
                        <StatusBadge status={job.status} />
                      </td>
                      <td className="p-4 text-sm text-gray-400">
                        {job.summaryMetrics || (
                          job.s3_dest_path ? (
                            `${job.objectCount?.toLocaleString() || 'N/A'} objects`
                          ) : (
                            new Date(job.created_at).toLocaleDateString()
                          )
                        )}
                      </td>
                      <td className="p-4">
                        <div className="flex items-center space-x-2">
                          <Link href={`/jobs/${job.job_id}`}>
                            <Button variant="ghost" size="sm">
                              View
                            </Button>
                          </Link>
                          {job.status === "SUCCESS" && job.s3_dest_path && (
                            <Link href={`/sync?jobId=${job.job_id}`}>
                              <Button variant="outline" size="sm">
                                Sync
                              </Button>
                            </Link>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </MainLayout>
  )
}
