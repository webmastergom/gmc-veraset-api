"use client"

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { CheckCircle, AlertCircle, Loader2, Clock, FileText, Calendar } from "lucide-react"
import { useState } from "react"

interface DayProgress {
  date: string
  totalFiles: number
  copiedFiles: number
  failedFiles: number
  totalBytes: number
  copiedBytes: number
  status: 'pending' | 'copying' | 'completed' | 'failed'
  errors?: Array<{ file: string; error: string }>
}

interface SyncProgress {
  currentDay?: string
  currentFile?: number
  totalFilesInCurrentDay?: number
  dayProgress?: Record<string, DayProgress>
  lastUpdated?: string
}

interface ProfessionalLoaderProps {
  syncProgress: SyncProgress | null
  overallProgress: number
  copied: number
  total: number
  copiedBytes: number
  totalBytes: number
}

export function ProfessionalLoader({
  syncProgress,
  overallProgress,
  copied,
  total,
  copiedBytes,
  totalBytes,
}: ProfessionalLoaderProps) {
  const [expandedDays, setExpandedDays] = useState<Set<string>>(new Set())

  if (!syncProgress?.dayProgress) {
    return null
  }

  const days = Object.values(syncProgress.dayProgress).sort((a, b) => 
    a.date.localeCompare(b.date)
  )

  const currentDayData = syncProgress.currentDay 
    ? syncProgress.dayProgress[syncProgress.currentDay]
    : null

  const toggleDay = (date: string) => {
    const newExpanded = new Set(expandedDays)
    if (newExpanded.has(date)) {
      newExpanded.delete(date)
    } else {
      newExpanded.add(date)
    }
    setExpandedDays(newExpanded)
  }

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`
  }

  const getStatusIcon = (status: DayProgress['status']) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-4 w-4 text-green-500" />
      case 'failed':
        return <AlertCircle className="h-4 w-4 text-red-500" />
      case 'copying':
        return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
      default:
        return <Clock className="h-4 w-4 text-gray-400" />
    }
  }

  const getStatusBadge = (status: DayProgress['status']) => {
    switch (status) {
      case 'completed':
        return <Badge variant="default" className="bg-green-500">Completed</Badge>
      case 'failed':
        return <Badge variant="destructive">Failed</Badge>
      case 'copying':
        return <Badge variant="default" className="bg-blue-500">Copying</Badge>
      default:
        return <Badge variant="secondary">Pending</Badge>
    }
  }

  return (
    <div className="space-y-4">
      {/* Current Activity Card */}
      {currentDayData && (
        <Card className="border-blue-500/50 bg-blue-500/5">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              <Loader2 className="h-5 w-5 animate-spin text-blue-500" />
              Current Activity
            </CardTitle>
            <CardDescription>
              Copying files for day {syncProgress.currentDay}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2">
                <Calendar className="h-4 w-4 text-muted-foreground" />
                <span className="font-medium">Day:</span>
                <span className="font-mono">{syncProgress.currentDay}</span>
              </div>
              <div className="flex items-center gap-2">
                <FileText className="h-4 w-4 text-muted-foreground" />
                <span className="font-medium">File:</span>
                <span>
                  {syncProgress.currentFile || 0} / {syncProgress.totalFilesInCurrentDay || currentDayData.totalFiles}
                </span>
              </div>
            </div>
            <Progress 
              value={
                syncProgress.totalFilesInCurrentDay && syncProgress.currentFile
                  ? (syncProgress.currentFile / syncProgress.totalFilesInCurrentDay) * 100
                  : (currentDayData.copiedFiles / currentDayData.totalFiles) * 100
              } 
              className="h-2"
            />
            <div className="text-sm text-muted-foreground">
              {currentDayData.copiedFiles} of {currentDayData.totalFiles} files copied
              {currentDayData.failedFiles > 0 && (
                <span className="text-red-500 ml-2">
                  ({currentDayData.failedFiles} failed)
                </span>
              )}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Overall Progress */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Overall Progress</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          <div className="flex justify-between text-sm">
            <span className="text-muted-foreground">Total Progress</span>
            <span className="font-medium">{overallProgress}%</span>
          </div>
          <Progress value={overallProgress} className="h-3" />
          <div className="grid grid-cols-2 gap-4 text-sm mt-4">
            <div>
              <p className="text-muted-foreground">Files</p>
              <p className="font-medium">
                {copied.toLocaleString()} / {total.toLocaleString()}
              </p>
            </div>
            <div>
              <p className="text-muted-foreground">Data</p>
              <p className="font-medium">
                {formatBytes(copiedBytes)} / {formatBytes(totalBytes)}
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Day-by-Day Progress */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Progress by Day</CardTitle>
          <CardDescription>
            Detailed progress for each day partition ({days.length} days total)
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {days.map((day) => {
            const dayProgress = (day.copiedFiles / day.totalFiles) * 100
            const isExpanded = expandedDays.has(day.date)
            const hasErrors = day.errors && day.errors.length > 0

            return (
              <div key={day.date} className="border rounded-lg p-4 space-y-3">
                <button
                  onClick={() => toggleDay(day.date)}
                  className="w-full flex items-center justify-between hover:bg-muted/50 rounded-md p-2 -m-2 transition-colors cursor-pointer"
                >
                    <div className="flex items-center gap-3 flex-1">
                      {getStatusIcon(day.status)}
                      <div className="flex-1">
                        <div className="flex items-center gap-2">
                          <span className="font-medium">{day.date}</span>
                          {getStatusBadge(day.status)}
                          {hasErrors && (
                            <Badge variant="destructive" className="ml-2">
                              {day.errors!.length} error{day.errors!.length !== 1 ? 's' : ''}
                            </Badge>
                          )}
                        </div>
                        <div className="text-sm text-muted-foreground mt-1">
                          {day.copiedFiles} / {day.totalFiles} files
                          {day.failedFiles > 0 && (
                            <span className="text-red-500 ml-2">
                              ({day.failedFiles} failed)
                            </span>
                          )}
                          {' • '}
                          {formatBytes(day.copiedBytes)} / {formatBytes(day.totalBytes)}
                        </div>
                      </div>
                    </div>
                    <Progress value={dayProgress} className="w-32 h-2 mr-4" />
                    <span className="text-xs text-muted-foreground ml-2">
                      {isExpanded ? '▼' : '▶'}
                    </span>
                  </button>

                  {isExpanded && (
                    <div className="space-y-2 pt-2 border-t">
                    <div className="grid grid-cols-2 gap-4 text-sm">
                      <div>
                        <p className="text-muted-foreground">Files Progress</p>
                        <p className="font-medium">
                          {day.copiedFiles} copied, {day.totalFiles - day.copiedFiles - day.failedFiles} pending
                          {day.failedFiles > 0 && `, ${day.failedFiles} failed`}
                        </p>
                      </div>
                      <div>
                        <p className="text-muted-foreground">Data Progress</p>
                        <p className="font-medium">
                          {formatBytes(day.copiedBytes)} / {formatBytes(day.totalBytes)}
                          {' '}
                          ({dayProgress.toFixed(1)}%)
                        </p>
                      </div>
                    </div>

                    {hasErrors && (
                      <Alert variant="destructive" className="mt-2">
                        <AlertCircle className="h-4 w-4" />
                        <AlertTitle>Errors ({day.errors!.length})</AlertTitle>
                        <AlertDescription className="space-y-1 mt-2">
                          {day.errors!.slice(0, 10).map((err, idx) => (
                            <div key={idx} className="text-xs font-mono bg-red-500/10 p-2 rounded">
                              <div className="font-semibold">{err.file}</div>
                              <div className="text-red-400 mt-1">{err.error}</div>
                            </div>
                          ))}
                          {day.errors!.length > 10 && (
                            <div className="text-xs text-muted-foreground mt-2">
                              ... and {day.errors!.length - 10} more errors
                            </div>
                          )}
                        </AlertDescription>
                      </Alert>
                    )}
                    </div>
                  )}
              </div>
            )
          })}
        </CardContent>
      </Card>

      {/* Audit Information */}
      {syncProgress.lastUpdated && (
        <Card className="bg-muted/30">
          <CardHeader>
            <CardTitle className="text-sm">Audit Information</CardTitle>
          </CardHeader>
          <CardContent className="text-xs text-muted-foreground space-y-1">
            <p>Last updated: {new Date(syncProgress.lastUpdated).toLocaleString()}</p>
            <p>Total days: {days.length}</p>
            <p>
              Status breakdown: {' '}
              {days.filter(d => d.status === 'completed').length} completed, {' '}
              {days.filter(d => d.status === 'copying').length} copying, {' '}
              {days.filter(d => d.status === 'pending').length} pending, {' '}
              {days.filter(d => d.status === 'failed').length} failed
            </p>
            <p>
              Total errors: {days.reduce((sum, d) => sum + (d.errors?.length || 0), 0)}
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
