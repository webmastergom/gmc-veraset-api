'use client'

import Link from 'next/link'
import { usePathname, useRouter } from 'next/navigation'
import {
  LayoutDashboard,
  MapPin,
  Briefcase,
  Database,
  Download,
  LogOut,
  ChevronRight,
  ShieldCheck,
  Key
} from 'lucide-react'
import { useEffect, useState } from 'react'
import { ThemeToggle } from '@/components/theme-toggle'

const navigation = [
  { name: 'Dashboard', href: '/dashboard', icon: LayoutDashboard },
  { name: 'POIs', href: '/pois', icon: MapPin },
  { name: 'Jobs', href: '/jobs', icon: Briefcase },
  { name: 'Datasets', href: '/datasets', icon: Database },
  { name: 'Audit', href: '/audit', icon: ShieldCheck },
  { name: 'API Keys', href: '/api-keys', icon: Key },
]

export function Sidebar() {
  const pathname = usePathname()
  const router = useRouter()
  const [usage, setUsage] = useState<any>(null)

  useEffect(() => {
    fetch('/api/usage', {
      credentials: 'include',
    })
      .then(res => res.json())
      .then(data => setUsage(data))
      .catch(() => {})
  }, [])

  const handleLogout = async () => {
    await fetch('/api/auth/logout', { 
      method: 'POST',
      credentials: 'include',
    })
    router.push('/login')
    router.refresh()
  }

  const remaining = usage?.remaining || 0
  const limit = usage?.limit || 200
  const percentage = Math.round(((limit - remaining) / limit) * 100)

  return (
    <div className="fixed left-0 top-0 h-screen w-64 bg-background border-r border-border flex flex-col z-50">
      {/* Logo Section */}
      <div className="p-6 border-b border-border">
        <div className="mb-2 flex items-center justify-center">
          <div className="bg-secondary rounded-lg p-2 border border-border">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src="/assets/logos/GMC_blackbck.png"
              alt="Garritz"
              className="h-10 w-auto object-contain dark:block hidden"
              style={{ maxWidth: '160px' }}
            />
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src="/assets/logos/GMC_whitebck.png"
              alt="Garritz"
              className="h-10 w-auto object-contain dark:hidden block"
              style={{ maxWidth: '160px' }}
            />
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-theme-accent bg-theme-accent-light px-2 py-1 rounded">
            MOBILITY
          </span>
          <span className="text-xs text-muted-foreground">API</span>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4 space-y-1 overflow-y-auto">
        {navigation.map((item) => {
          const isActive = pathname.startsWith(item.href)
          return (
            <Link
              key={item.name}
              href={item.href}
              className={`flex items-center gap-3 px-4 py-3 rounded-xl transition-all group ${
                isActive
                  ? 'bg-theme-accent text-primary-foreground dark:text-black'
                  : 'text-muted-foreground hover:text-foreground hover:bg-secondary'
              }`}
            >
              <item.icon className={`w-5 h-5 ${isActive ? 'dark:text-black text-white' : ''}`} />
              <span className="font-medium">{item.name}</span>
              {isActive && (
                <ChevronRight className="w-4 h-4 ml-auto dark:text-black text-white" />
              )}
            </Link>
          )
        })}
      </nav>

      {/* Theme Toggle */}
      <div className="px-4 py-3 border-t border-border">
        <div className="flex items-center justify-between">
          <span className="text-xs text-muted-foreground uppercase tracking-wider">Theme</span>
          <ThemeToggle />
        </div>
      </div>

      {/* API Usage */}
      <div className="p-4 border-t border-border">
        <div className="bg-secondary rounded-xl p-4">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs text-muted-foreground uppercase tracking-wider">API Usage</span>
            <span className="text-sm font-bold text-foreground">{limit - remaining}/{limit}</span>
          </div>
          <div className="w-full bg-muted rounded-full h-2">
            <div
              className="bg-theme-accent h-2 rounded-full transition-all"
              style={{ width: `${percentage}%` }}
            />
          </div>
          <p className="text-xs text-muted-foreground mt-2">
            {remaining} calls remaining in {new Date().toLocaleString('en-US', { month: 'short' })} {new Date().getFullYear()}
          </p>
        </div>
      </div>

      {/* Logout */}
      <div className="p-4 border-t border-border">
        <button
          onClick={handleLogout}
          className="flex items-center gap-3 px-4 py-3 w-full rounded-xl text-muted-foreground hover:text-foreground hover:bg-secondary transition-all"
        >
          <LogOut className="w-5 h-5" />
          <span className="font-medium">Sign Out</span>
        </button>
      </div>
    </div>
  )
}
