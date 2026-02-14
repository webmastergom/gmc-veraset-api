"use client"

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Progress } from '@/components/ui/progress'
import { AlertTriangle } from 'lucide-react'
import { cn } from '@/lib/utils'

interface UsageData {
  used: number
  limit: number
  remaining: number
  percentage: number
}

export function UsageCard({ usage }: { usage: UsageData }) {
  const isWarning = usage.remaining < 50 && usage.remaining >= 20
  const isCritical = usage.remaining < 20
  
  return (
    <Card className={cn(
      isCritical && 'border-red-500',
      isWarning && 'border-yellow-500'
    )}>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium">API Usage</CardTitle>
        {isCritical && <AlertTriangle className="h-4 w-4 text-red-500" />}
        {isWarning && !isCritical && <AlertTriangle className="h-4 w-4 text-yellow-500" />}
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{usage.remaining} remaining</div>
        <p className="text-xs text-muted-foreground mt-1">
          {usage.used} of {usage.limit} calls used this month
        </p>
        <Progress
          value={usage.percentage} 
          className={cn(
            "mt-2",
            isCritical && "[&>div]:bg-red-500",
            isWarning && !isCritical && "[&>div]:bg-yellow-500"
          )}
        />
        {isCritical && (
          <p className="text-xs text-red-500 mt-2 font-medium">
            ⚠️ Critical: Only {usage.remaining} calls remaining
          </p>
        )}
        {isWarning && !isCritical && (
          <p className="text-xs text-yellow-600 mt-2">
            ⚠️ Warning: Low remaining calls
          </p>
        )}
      </CardContent>
    </Card>
  )
}
