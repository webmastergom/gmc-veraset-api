"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import { cn } from "@/lib/utils"
import { LayoutDashboard, MapPin, PlayCircle, Database, BarChart3, Download } from "lucide-react"
import { UsageBadge } from "@/components/usage-badge"

const navigation = [
  { name: "Dashboard", href: "/dashboard", icon: LayoutDashboard },
  { name: "POIs", href: "/pois", icon: MapPin },
  { name: "Jobs", href: "/jobs", icon: PlayCircle },
  { name: "Datasets", href: "/datasets", icon: Database },
  { name: "Export", href: "/export", icon: Download },
]

export function Nav() {
  const pathname = usePathname()

  return (
    <nav className="border-b border-border bg-background/80 backdrop-blur-sm">
      <div className="container mx-auto px-4">
        <div className="flex h-16 items-center space-x-8">
          <Link href="/dashboard" className="flex items-center space-x-3">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src="/assets/logos/GMC_blackbck.png"
              alt="GMC Logo"
              className="h-10 w-auto dark:block hidden"
            />
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src="/assets/logos/GMC_whitebck.png"
              alt="GMC Logo"
              className="h-10 w-auto dark:hidden block"
            />
          </Link>
          <div className="flex flex-1 items-center space-x-1">
            {navigation.map((item) => {
              const Icon = item.icon
              const isActive = pathname === item.href || pathname.startsWith(item.href + "/")
              return (
                <Link
                  key={item.name}
                  href={item.href}
                  className={cn(
                    "flex items-center space-x-2 px-3 py-2 text-sm font-medium transition-colors rounded-lg",
                    isActive
                      ? "text-theme-accent bg-theme-accent-light"
                      : "text-muted-foreground hover:text-foreground hover:bg-secondary"
                  )}
                >
                  <Icon className="h-4 w-4" />
                  <span>{item.name}</span>
                </Link>
              )
            })}
          </div>
          <div className="flex items-center">
            <UsageBadge />
          </div>
        </div>
      </div>
    </nav>
  )
}
