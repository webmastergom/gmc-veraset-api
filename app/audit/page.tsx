"use client"

import { useState, useEffect } from "react"
import { MainLayout } from "@/components/layout/main-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Loader2, AlertTriangle, CheckCircle, XCircle, RefreshCw } from "lucide-react"

interface POIAuditResult {
  collectionId: string
  collectionName: string
  expectedCount: number
  actualCount: number
  validPoints: number
  invalidFeatures: number
  discrepancy: number
  issues: string[]
}

interface JobAuditResult {
  jobId: string
  jobName: string
  expectedDays: number
  actualPartitions: number
  partitionDates: string[]
  expectedDateRange: { from: string; to: string }
  discrepancy: number
  issues: string[]
}

export default function AuditPage() {
  const [loading, setLoading] = useState(false)
  const [poiResults, setPoiResults] = useState<POIAuditResult[]>([])
  const [jobResults, setJobResults] = useState<JobAuditResult[]>([])
  const [poiSummary, setPoiSummary] = useState<any>(null)
  const [jobSummary, setJobSummary] = useState<any>(null)

  const runAudit = async () => {
    setLoading(true)
    try {
      const response = await fetch('/api/audit?type=all', {
        credentials: 'include',
      })
      const data = await response.json()

      if (data.pois) {
        setPoiResults(data.pois.results || [])
        setPoiSummary(data.pois.summary || {})
      }

      if (data.jobs) {
        setJobResults(data.jobs.results || [])
        setJobSummary(data.jobs.summary || {})
      }
    } catch (error) {
      console.error('Audit error:', error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    runAudit()
  }, [])

  return (
    <MainLayout>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-white">Data Integrity Audit</h1>
          <p className="text-gray-500 mt-2">
            Verify POI collections and job data integrity
          </p>
        </div>
        <Button onClick={runAudit} disabled={loading}>
          {loading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Running...
            </>
          ) : (
            <>
              <RefreshCw className="mr-2 h-4 w-4" />
              Run Audit
            </>
          )}
        </Button>
      </div>

      {(poiSummary || jobSummary) && (
        <div className="grid gap-4 md:grid-cols-2 mb-6">
          {poiSummary && (
            <Card>
              <CardHeader>
                <CardTitle>POI Collections</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Total Collections:</span>
                    <span className="font-semibold">{poiSummary.total}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">With Issues:</span>
                    <Badge variant={poiSummary.withIssues > 0 ? "destructive" : "default"}>
                      {poiSummary.withIssues}
                    </Badge>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Total Discrepancy:</span>
                    <span className="font-semibold">{poiSummary.totalDiscrepancy.toLocaleString()}</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {jobSummary && (
            <Card>
              <CardHeader>
                <CardTitle>Jobs</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Total Jobs:</span>
                    <span className="font-semibold">{jobSummary.total}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">With Issues:</span>
                    <Badge variant={jobSummary.withIssues > 0 ? "destructive" : "default"}>
                      {jobSummary.withIssues}
                    </Badge>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Total Discrepancy:</span>
                    <span className="font-semibold">{jobSummary.totalDiscrepancy.toLocaleString()} days</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      )}

      <Tabs defaultValue="pois">
        <TabsList>
          <TabsTrigger value="pois">POI Collections ({poiResults.length})</TabsTrigger>
          <TabsTrigger value="jobs">Jobs ({jobResults.length})</TabsTrigger>
        </TabsList>

        <TabsContent value="pois">
          <div className="space-y-4">
            {poiResults.map((result) => (
              <Card key={result.collectionId}>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle>{result.collectionName}</CardTitle>
                      <CardDescription className="font-mono text-xs mt-1">
                        {result.collectionId}
                      </CardDescription>
                    </div>
                    {result.issues.length === 0 ? (
                      <CheckCircle className="h-5 w-5 text-green-500" />
                    ) : (
                      <XCircle className="h-5 w-5 text-red-500" />
                    )}
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                    <div>
                      <p className="text-sm text-muted-foreground">Expected</p>
                      <p className="text-lg font-semibold">{result.expectedCount.toLocaleString()}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Valid POIs</p>
                      <p className="text-lg font-semibold">{result.validPoints.toLocaleString()}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Total Features</p>
                      <p className="text-lg font-semibold">{result.actualCount.toLocaleString()}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Invalid</p>
                      <p className="text-lg font-semibold text-red-500">
                        {result.invalidFeatures.toLocaleString()}
                      </p>
                    </div>
                  </div>

                  {result.issues.length > 0 && (
                    <Alert variant="destructive">
                      <AlertTriangle className="h-4 w-4" />
                      <AlertTitle>Issues Found</AlertTitle>
                      <AlertDescription>
                        <ul className="list-disc list-inside mt-2 space-y-1">
                          {result.issues.map((issue, idx) => (
                            <li key={idx} className="text-sm">{issue}</li>
                          ))}
                        </ul>
                      </AlertDescription>
                    </Alert>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>

        <TabsContent value="jobs">
          <div className="space-y-4">
            {jobResults.map((result) => (
              <Card key={result.jobId}>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle>{result.jobName}</CardTitle>
                      <CardDescription className="font-mono text-xs mt-1">
                        {result.jobId}
                      </CardDescription>
                    </div>
                    {result.issues.length === 0 ? (
                      <CheckCircle className="h-5 w-5 text-green-500" />
                    ) : (
                      <XCircle className="h-5 w-5 text-red-500" />
                    )}
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-4">
                    <div>
                      <p className="text-sm text-muted-foreground">Expected Days</p>
                      <p className="text-lg font-semibold">{result.expectedDays}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Actual Partitions</p>
                      <p className="text-lg font-semibold">{result.actualPartitions}</p>
                    </div>
                    <div>
                      <p className="text-sm text-muted-foreground">Discrepancy</p>
                      <p className={`text-lg font-semibold ${result.discrepancy !== 0 ? 'text-red-500' : ''}`}>
                        {result.discrepancy > 0 ? `-${result.discrepancy}` : result.discrepancy}
                      </p>
                    </div>
                  </div>

                  <div className="mb-4">
                    <p className="text-sm text-muted-foreground mb-2">Date Range:</p>
                    <p className="text-sm">
                      {result.expectedDateRange.from} to {result.expectedDateRange.to}
                    </p>
                  </div>

                  {result.partitionDates.length > 0 && (
                    <div className="mb-4">
                      <p className="text-sm text-muted-foreground mb-2">
                        Partitions Found ({result.partitionDates.length}):
                      </p>
                      <div className="flex flex-wrap gap-1">
                        {result.partitionDates.slice(0, 10).map((date) => (
                          <Badge key={date} variant="outline" className="text-xs">
                            {date}
                          </Badge>
                        ))}
                        {result.partitionDates.length > 10 && (
                          <Badge variant="outline" className="text-xs">
                            +{result.partitionDates.length - 10} more
                          </Badge>
                        )}
                      </div>
                    </div>
                  )}

                  {result.issues.length > 0 && (
                    <Alert variant="destructive">
                      <AlertTriangle className="h-4 w-4" />
                      <AlertTitle>Issues Found</AlertTitle>
                      <AlertDescription>
                        <ul className="list-disc list-inside mt-2 space-y-1">
                          {result.issues.map((issue, idx) => (
                            <li key={idx} className="text-sm">{issue}</li>
                          ))}
                        </ul>
                      </AlertDescription>
                    </Alert>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </TabsContent>
      </Tabs>
    </MainLayout>
  )
}
