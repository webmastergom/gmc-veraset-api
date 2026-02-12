"use client"

import { useState, useEffect } from "react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { CheckCircle, XCircle, AlertTriangle, AlertCircle, ShieldCheck, Loader2, FileText, Calendar, Database, Info } from "lucide-react"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

interface JobAuditProps {
  jobId: string
}

interface AuditData {
  jobId: string
  jobName: string
  timestamp: string
  hasAuditTrail: boolean
  verification: {
    overall: 'PASSED' | 'FAILED'
    payloadVerification: {
      passed: boolean
      issues: string[]
    }
    responseVerification: {
      passed: boolean
      issues: string[]
    }
  }
  comparisons: {
    dateRange: {
      userRequested: { from: string; to: string }
      sentToVeraset: { from: string; to: string }
      matches: boolean
    }
    schema: {
      userRequested: string
      sentToVeraset: string
      matches: boolean
    }
    poiCounts: {
      userGeoRadius: number
      verasetGeoRadius: number
      userPlaceKey: number
      verasetPlaceKey: number
      matches: boolean
    }
  }
  poiVerification: {
    samples: Array<{
      index: number
      type: 'geo_radius' | 'place_key'
      userPoi: any
      verasetPoi: any
      matches: boolean
      differences?: string[]
      poiName?: string
    }>
    allSamplesMatch: boolean
  }
  summary: {
    userRequested: {
      dateRange: string
      schema: string
      geoRadiusPois: number
      placeKeyPois: number
    }
    sentToVeraset: {
      dateRange: string
      schema: string
      geoRadiusPois: number
      placeKeyPois: number
    }
    verasetProcessed: {
      processedPois: number | string
      dateRange: string | any
    }
  }
}

export function JobAuditDialog({ jobId }: JobAuditProps) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [auditData, setAuditData] = useState<AuditData | null>(null)
  const [error, setError] = useState<string | null>(null)

  const fetchAudit = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch(`/api/jobs/${jobId}/audit`, {
        credentials: 'include',
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.error || err.details || 'Failed to fetch audit')
      }
      const data = await res.json()
      setAuditData(data)
    } catch (e: any) {
      setError(e.message || 'Failed to load audit data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (open && !auditData) {
      fetchAudit()
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps -- fetchAudit only when open changes
  }, [open])

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <ShieldCheck className="h-4 w-4 mr-2" />
          Verify Payload
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <ShieldCheck className="h-5 w-5" />
            Job Payload Verification
          </DialogTitle>
          <DialogDescription>
            Verify that Veraset received exactly what you requested
          </DialogDescription>
        </DialogHeader>

        {loading && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        )}

        {error && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {auditData && !auditData.hasAuditTrail && (
          <Alert>
            <Info className="h-4 w-4" />
            <AlertTitle>No Audit Trail</AlertTitle>
            <AlertDescription>
              This job was created before the audit system was implemented. 
              Audit trails are available for jobs created after the system update.
            </AlertDescription>
          </Alert>
        )}

        {auditData && auditData.hasAuditTrail && (
          <div className="space-y-4">
            {/* Overall Verification Status */}
            <Card className={auditData.verification.overall === 'PASSED' ? 'border-green-500/50 bg-green-500/5' : 'border-red-500/50 bg-red-500/5'}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  {auditData.verification.overall === 'PASSED' ? (
                    <CheckCircle className="h-5 w-5 text-green-500" />
                  ) : (
                    <XCircle className="h-5 w-5 text-red-500" />
                  )}
                  Overall Verification: {auditData.verification.overall}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium">Payload Verification:</span>
                  {auditData.verification.payloadVerification.passed ? (
                    <Badge variant="default" className="bg-green-500">PASSED</Badge>
                  ) : (
                    <Badge variant="destructive">FAILED</Badge>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium">Response Verification:</span>
                  {auditData.verification.responseVerification.passed ? (
                    <Badge variant="default" className="bg-green-500">PASSED</Badge>
                  ) : (
                    <Badge variant="destructive">FAILED</Badge>
                  )}
                </div>
                {auditData.verification.payloadVerification.issues.length > 0 && (
                  <Alert variant="destructive" className="mt-2">
                    <AlertTitle>Payload Issues</AlertTitle>
                    <AlertDescription className="space-y-1">
                      {auditData.verification.payloadVerification.issues.map((issue, idx) => (
                        <div key={idx} className="text-sm">• {issue}</div>
                      ))}
                    </AlertDescription>
                  </Alert>
                )}
                {auditData.verification.responseVerification.issues.length > 0 && (
                  <Alert variant="destructive" className="mt-2">
                    <AlertTitle>Response Issues</AlertTitle>
                    <AlertDescription className="space-y-1">
                      {auditData.verification.responseVerification.issues.map((issue, idx) => (
                        <div key={idx} className="text-sm">• {issue}</div>
                      ))}
                    </AlertDescription>
                  </Alert>
                )}
              </CardContent>
            </Card>

            {/* Comparison Table */}
            <Card>
              <CardHeader>
                <CardTitle>Detailed Comparison</CardTitle>
                <CardDescription>User input vs. Payload sent to Veraset</CardDescription>
              </CardHeader>
              <CardContent>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Field</TableHead>
                      <TableHead>User Requested</TableHead>
                      <TableHead>Sent to Veraset</TableHead>
                      <TableHead>Status</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    <TableRow>
                      <TableCell className="font-medium">Date Range (From)</TableCell>
                      <TableCell className="font-mono text-sm">{auditData.comparisons.dateRange.userRequested.from}</TableCell>
                      <TableCell className="font-mono text-sm">{auditData.comparisons.dateRange.sentToVeraset.from}</TableCell>
                      <TableCell>
                        {auditData.comparisons.dateRange.matches ? (
                          <CheckCircle className="h-4 w-4 text-green-500" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-500" />
                        )}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-medium">Date Range (To)</TableCell>
                      <TableCell className="font-mono text-sm">{auditData.comparisons.dateRange.userRequested.to}</TableCell>
                      <TableCell className="font-mono text-sm">{auditData.comparisons.dateRange.sentToVeraset.to}</TableCell>
                      <TableCell>
                        {auditData.comparisons.dateRange.matches ? (
                          <CheckCircle className="h-4 w-4 text-green-500" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-500" />
                        )}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-medium">Schema</TableCell>
                      <TableCell>{auditData.comparisons.schema.userRequested}</TableCell>
                      <TableCell>{auditData.comparisons.schema.sentToVeraset}</TableCell>
                      <TableCell>
                        {auditData.comparisons.schema.matches ? (
                          <CheckCircle className="h-4 w-4 text-green-500" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-500" />
                        )}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-medium">Geo Radius POIs</TableCell>
                      <TableCell>{auditData.comparisons.poiCounts.userGeoRadius}</TableCell>
                      <TableCell>{auditData.comparisons.poiCounts.verasetGeoRadius}</TableCell>
                      <TableCell>
                        {auditData.comparisons.poiCounts.userGeoRadius === auditData.comparisons.poiCounts.verasetGeoRadius ? (
                          <CheckCircle className="h-4 w-4 text-green-500" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-500" />
                        )}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="font-medium">Place Key POIs</TableCell>
                      <TableCell>{auditData.comparisons.poiCounts.userPlaceKey}</TableCell>
                      <TableCell>{auditData.comparisons.poiCounts.verasetPlaceKey}</TableCell>
                      <TableCell>
                        {auditData.comparisons.poiCounts.userPlaceKey === auditData.comparisons.poiCounts.verasetPlaceKey ? (
                          <CheckCircle className="h-4 w-4 text-green-500" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-500" />
                        )}
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </CardContent>
            </Card>

            {/* POI Sample Verification */}
            {auditData.poiVerification.samples.length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle>POI Sample Verification</CardTitle>
                  <CardDescription>
                    Verification of first {auditData.poiVerification.samples.length} POIs
                    {auditData.poiVerification.allSamplesMatch ? (
                      <span className="text-green-500 ml-2">✓ All match</span>
                    ) : (
                      <span className="text-red-500 ml-2">✗ Mismatches found</span>
                    )}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {auditData.poiVerification.samples.map((sample, idx) => (
                      <div key={idx} className={`border rounded-lg p-3 ${sample.matches ? 'bg-green-500/5 border-green-500/20' : 'bg-red-500/5 border-red-500/20'}`}>
                        <div className="flex items-center justify-between mb-2">
                          <div className="flex-1">
                            <div className="font-medium text-sm">
                              {sample.poiName ? (
                                <>
                                  <span className="text-base">{sample.poiName}</span>
                                  <span className="text-muted-foreground ml-2 text-xs">
                                    ({sample.type === 'geo_radius' ? 'Geo Radius' : 'Place Key'} POI #{sample.index + 1})
                                  </span>
                                </>
                              ) : (
                                <span>
                                  {sample.type === 'geo_radius' ? 'Geo Radius' : 'Place Key'} POI #{sample.index + 1}
                                </span>
                              )}
                            </div>
                            {sample.poiName && (
                              <div className="text-xs text-muted-foreground font-mono mt-1">
                                ID: {sample.verasetPoi?.poi_id || sample.userPoi?.poi_id}
                              </div>
                            )}
                          </div>
                          {sample.matches ? (
                            <CheckCircle className="h-4 w-4 text-green-500" />
                          ) : (
                            <XCircle className="h-4 w-4 text-red-500" />
                          )}
                        </div>
                        {sample.differences && sample.differences.length > 0 && (
                          <Alert variant="destructive" className="mt-2">
                            <AlertDescription className="text-xs">
                              {sample.differences.map((diff, i) => (
                                <div key={i}>• {diff}</div>
                              ))}
                            </AlertDescription>
                          </Alert>
                        )}
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Summary */}
            <Card className="bg-muted/30">
              <CardHeader>
                <CardTitle className="text-sm">Summary</CardTitle>
              </CardHeader>
              <CardContent className="text-sm space-y-2">
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <p className="font-medium mb-1">User Requested</p>
                    <p className="text-muted-foreground">Date Range: {auditData.summary.userRequested.dateRange}</p>
                    <p className="text-muted-foreground">Schema: {auditData.summary.userRequested.schema}</p>
                    <p className="text-muted-foreground">POIs: {auditData.summary.userRequested.geoRadiusPois} geo_radius + {auditData.summary.userRequested.placeKeyPois} place_key</p>
                  </div>
                  <div>
                    <p className="font-medium mb-1">Sent to Veraset</p>
                    <p className="text-muted-foreground">Date Range: {auditData.summary.sentToVeraset.dateRange}</p>
                    <p className="text-muted-foreground">Schema: {auditData.summary.sentToVeraset.schema}</p>
                    <p className="text-muted-foreground">POIs: {auditData.summary.sentToVeraset.geoRadiusPois} geo_radius + {auditData.summary.sentToVeraset.placeKeyPois} place_key</p>
                  </div>
                  <div>
                    <p className="font-medium mb-1">Veraset Response</p>
                    <p className="text-muted-foreground">Processed POIs: {String(auditData.summary.verasetProcessed.processedPois)}</p>
                    <p className="text-muted-foreground">Date Range: {typeof auditData.summary.verasetProcessed.dateRange === 'object' 
                      ? `${auditData.summary.verasetProcessed.dateRange.from_date} to ${auditData.summary.verasetProcessed.dateRange.to_date}`
                      : String(auditData.summary.verasetProcessed.dateRange)}</p>
                  </div>
                </div>
                <div className="pt-2 border-t text-xs text-muted-foreground">
                  Audit timestamp: {new Date(auditData.timestamp).toLocaleString()}
                </div>
              </CardContent>
            </Card>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}
