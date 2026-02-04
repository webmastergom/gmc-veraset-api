"use client"

import * as React from "react"
import { Moon, Sun } from "lucide-react"
import { useTheme } from "next-themes"

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()
  const [mounted, setMounted] = React.useState(false)

  React.useEffect(() => {
    setMounted(true)
  }, [])

  if (!mounted) {
    return (
      <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-white/5">
        <div className="w-4 h-4" />
        <div className="w-8 h-5 rounded-full bg-gray-600" />
        <div className="w-4 h-4" />
      </div>
    )
  }

  const isDark = theme === "dark"

  return (
    <button
      onClick={() => setTheme(isDark ? "light" : "dark")}
      className="flex items-center gap-2 px-3 py-2 rounded-lg bg-white/5 dark:bg-white/5 light:bg-black/5 hover:bg-white/10 dark:hover:bg-white/10 transition-all"
      aria-label="Toggle theme"
    >
      <Sun className={`w-4 h-4 transition-colors ${isDark ? 'text-gray-500' : 'text-amber-500'}`} />
      <div
        className={`relative w-10 h-5 rounded-full transition-colors ${
          isDark ? 'bg-[#c8ff00]/30' : 'bg-[#0066cc]/30'
        }`}
      >
        <div
          className={`absolute top-0.5 w-4 h-4 rounded-full transition-all ${
            isDark
              ? 'right-0.5 bg-[#c8ff00]'
              : 'left-0.5 bg-[#0066cc]'
          }`}
        />
      </div>
      <Moon className={`w-4 h-4 transition-colors ${isDark ? 'text-[#c8ff00]' : 'text-gray-400'}`} />
    </button>
  )
}
