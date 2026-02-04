'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'

export default function LoginPage() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const router = useRouter()
  
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    
    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      })
      
      if (res.ok) {
        router.push('/dashboard')
        router.refresh()
      } else {
        const data = await res.json().catch(() => ({}))
        setError(data.error || 'Invalid credentials')
      }
    } catch (err) {
      setError('Login failed. Please try again.')
    } finally {
      setLoading(false)
    }
  }
  
  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-black to-gray-900 flex items-center justify-center p-4">
      {/* Animated background elements */}
      <div className="absolute inset-0 overflow-hidden">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-[#c8ff00]/10 rounded-full blur-3xl" />
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-[#c8ff00]/5 rounded-full blur-3xl" />
      </div>
      
      <div className="relative w-full max-w-md">
        {/* Logo */}
        <div className="text-center mb-10">
          <div className="mb-6 flex justify-center">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src="/assets/logos/GMC_blackbck.png"
              alt="Garritz Marketing Cloud"
              className="h-28 w-auto object-contain drop-shadow-2xl"
            />
          </div>
          <h1 className="text-3xl font-bold text-white">Garritz Marketing Cloud</h1>
          <p className="text-gray-400 mt-2 text-lg">Mobility API</p>
        </div>
        
        {/* Login Card */}
        <div className="bg-gray-900/50 backdrop-blur-xl border border-gray-800 rounded-2xl p-8 shadow-2xl">
          <form onSubmit={handleSubmit} className="space-y-6">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Username
              </label>
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="w-full px-4 py-3 bg-gray-800/50 border border-gray-700 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-[#c8ff00] focus:border-transparent transition-all"
                placeholder="Enter username"
                required
              />
            </div>
            
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-2">
                Password
              </label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full px-4 py-3 bg-gray-800/50 border border-gray-700 rounded-lg text-white placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-[#c8ff00] focus:border-transparent transition-all"
                placeholder="Enter password"
                required
              />
            </div>
            
            {error && (
              <div className="text-red-400 text-sm text-center bg-red-500/10 py-2 rounded-lg">
                {error}
              </div>
            )}
            
            <button
              type="submit"
              disabled={loading}
              className="w-full py-3 px-4 bg-[#c8ff00] text-black font-semibold rounded-lg hover:bg-[#b8ef00] focus:outline-none focus:ring-2 focus:ring-[#c8ff00] focus:ring-offset-2 focus:ring-offset-gray-900 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? 'Signing in...' : 'Sign In'}
            </button>
          </form>
        </div>
        
        {/* Footer */}
        <p className="text-center text-gray-600 text-sm mt-8">
          © 2026 Garritz Marketing Cloud. All rights reserved.
        </p>
      </div>
    </div>
  )
}
