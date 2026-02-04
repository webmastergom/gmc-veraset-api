"use client"

import { useEffect, useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Tooltip, TooltipContent, TooltipTrigger, TooltipProvider } from '@/components/ui/tooltip'

interface UsageData {
  used: number
  limit: number
  remaining: number
  percentage: number
}

export function UsageBadge() {
  const [usage, setUsage] = useState<UsageData>({ used: 0, limit: 200, remaining: 200, percentage: 0 })
  const [loading, setLoading] = useState(true)
  
  useEffect(() => {
    fetch('/api/usage', {
      credentials: 'include',
    })
      .then(r => r.json())
      .then(data => {
        setUsage({
          used: data.used || 0,
          limit: data.limit || 200,
          remaining: data.remaining || 200,
          percentage: data.percentage || 0,
        })
        setLoading(false)
      })
      .catch(err => {
        console.error('Error fetching usage:', err)
        setLoading(false)
      })
  }, [])

  if (loading) {
    return <Badge variant="secondary">Loading...</Badge>
  }

  const variant = usage.remaining === 0 ? 'destructive' : 
                  usage.remaining < 20 ? 'destructive' : 
                  usage.remaining < 50 ? 'warning' : 'secondary'
  
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Badge variant={variant} className="cursor-help">
            {usage.remaining}/{usage.limit} calls
          </Badge>
        </TooltipTrigger>
        <TooltipContent>
          <div className="space-y-1">
            <p className="font-medium">{usage.used} used this month</p>
            <p className="text-xs text-muted-foreground">Resets on the 1st</p>
            {usage.remaining < 20 && (
              <p className="text-xs text-destructive font-medium">
                ⚠️ Low remaining calls
              </p>
            )}
          </div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}
