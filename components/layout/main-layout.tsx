import { Sidebar } from './sidebar'

export function MainLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-background">
      <Sidebar />
      <main className="ml-64 min-h-screen">
        {/* Top glow effect */}
        <div className="absolute top-0 left-64 right-0 h-96 bg-gradient-to-b from-[var(--theme-accent)]/5 via-transparent to-transparent pointer-events-none" />

        {/* Content */}
        <div className="relative p-8">
          {children}
        </div>
      </main>
    </div>
  )
}
