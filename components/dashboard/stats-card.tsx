import { LucideIcon } from 'lucide-react'

interface StatsCardProps {
  title: string
  value: string
  subtitle: string
  icon: LucideIcon
  trend?: { value: number; label: string }
  color: 'lime' | 'blue' | 'purple' | 'orange'
}

const colorMap = {
  lime: {
    bg: 'bg-[var(--theme-accent)]/10',
    text: 'text-[var(--theme-accent)]',
    glow: 'shadow-[var(--theme-accent)]/20'
  },
  blue: {
    bg: 'bg-blue-500/10 dark:bg-blue-500/10',
    text: 'text-blue-600 dark:text-blue-400',
    glow: 'shadow-blue-500/20'
  },
  purple: {
    bg: 'bg-purple-500/10 dark:bg-purple-500/10',
    text: 'text-purple-600 dark:text-purple-400',
    glow: 'shadow-purple-500/20'
  },
  orange: {
    bg: 'bg-orange-500/10 dark:bg-orange-500/10',
    text: 'text-orange-600 dark:text-orange-400',
    glow: 'shadow-orange-500/20'
  }
}

export function StatsCard({ title, value, subtitle, icon: Icon, trend, color }: StatsCardProps) {
  const colors = colorMap[color]

  return (
    <div className="relative bg-card border border-border rounded-2xl p-6 hover:border-muted-foreground/30 transition-all group">
      <div className="flex items-start justify-between mb-4">
        <div className={`p-3 rounded-xl ${colors.bg}`}>
          <Icon className={`w-6 h-6 ${colors.text}`} />
        </div>
        {trend && (
          <span className="text-xs text-muted-foreground">
            {trend.value} {trend.label}
          </span>
        )}
      </div>

      <div className="space-y-1">
        <h3 className="text-3xl font-bold text-foreground">{value}</h3>
        <p className="text-sm text-muted-foreground">{subtitle}</p>
      </div>

      {/* Subtle glow on hover */}
      <div className={`absolute inset-0 rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity ${colors.glow} shadow-lg pointer-events-none`} />
    </div>
  )
}
