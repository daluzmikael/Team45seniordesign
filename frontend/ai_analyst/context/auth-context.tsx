"use client"

import { createContext, useContext, useEffect, useState, useCallback, useRef } from "react"
import { useRouter } from "next/navigation"

interface AuthUser {
  uid: string
  email: string
  token: string
  refreshToken: string
  tokenExpiresAt: number
}

interface AuthContextType {
  user: AuthUser | null
  login: (email: string, password: string) => Promise<{ success: boolean; error?: string }>
  signup: (email: string, password: string) => Promise<{ success: boolean; error?: string }>
  logout: () => void
  loading: boolean
  /** Always returns a fresh token — refreshes automatically if near expiry */
  getFreshToken: () => Promise<string | null>
}

const AuthContext = createContext<AuthContextType | null>(null)

const FIREBASE_API_KEY = process.env.NEXT_PUBLIC_FIREBASE_API_KEY
// Refresh 5 minutes before actual expiry to avoid edge cases
const EXPIRY_BUFFER_MS = 5 * 60 * 1000
// Firebase ID tokens last 1 hour
const TOKEN_LIFETIME_MS = 60 * 60 * 1000

function tokenExpiresAt(): number {
  return Date.now() + TOKEN_LIFETIME_MS
}

async function refreshIdToken(refreshToken: string): Promise<{ idToken: string; refreshToken: string } | null> {
  if (!FIREBASE_API_KEY) {
    console.error("NEXT_PUBLIC_FIREBASE_API_KEY is not set")
    return null
  }
  try {
    const res = await fetch(
      `https://securetoken.googleapis.com/v1/token?key=${FIREBASE_API_KEY}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ grant_type: "refresh_token", refresh_token: refreshToken }),
      }
    )
    const data = await res.json()
    if (!res.ok) {
      console.error("Token refresh failed:", data)
      return null
    }
    return { idToken: data.id_token, refreshToken: data.refresh_token }
  } catch (e) {
    console.error("Token refresh error:", e)
    return null
  }
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null)
  const [loading, setLoading] = useState(true)
  const router = useRouter()
  const refreshingRef = useRef(false)

  const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

  useEffect(() => {
    const stored = localStorage.getItem("auth_user")
    if (stored) {
      try {
        const parsed = JSON.parse(stored)
        // If stored user is missing new fields, force re-login
        if (!parsed.refreshToken || !parsed.tokenExpiresAt) {
          localStorage.removeItem("auth_user")
        } else {
          setUser(parsed)
        }
      } catch {
        localStorage.removeItem("auth_user")
      }
    }
    setLoading(false)
  }, [])

  const persistUser = (u: AuthUser) => {
    setUser(u)
    localStorage.setItem("auth_user", JSON.stringify(u))
  }

  /** Returns a valid token, refreshing first if it's expired or close to expiry */
  const getFreshToken = useCallback(async (): Promise<string | null> => {
    if (!user) return null

    const needsRefresh = Date.now() >= user.tokenExpiresAt - EXPIRY_BUFFER_MS
    if (!needsRefresh) return user.token

    // Prevent multiple simultaneous refresh calls
    if (refreshingRef.current) {
      // Wait briefly and return whatever token we have
      await new Promise((r) => setTimeout(r, 500))
      return user.token
    }

    refreshingRef.current = true
    try {
      const result = await refreshIdToken(user.refreshToken)
      if (!result) {
        // Refresh failed — token is dead, log out
        setUser(null)
        localStorage.removeItem("auth_user")
        router.push("/login")
        return null
      }
      const updated: AuthUser = {
        ...user,
        token: result.idToken,
        refreshToken: result.refreshToken,
        tokenExpiresAt: tokenExpiresAt(),
      }
      persistUser(updated)
      return updated.token
    } finally {
      refreshingRef.current = false
    }
  }, [user, router])

  const login = async (email: string, password: string) => {
    try {
      const res = await fetch(`${API_URL}/api/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || "Login failed")

      const authUser: AuthUser = {
        uid: data.uid,
        email,
        token: data.token,
        refreshToken: data.refreshToken,
        tokenExpiresAt: tokenExpiresAt(),
      }
      persistUser(authUser)
      return { success: true }
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Login failed" }
    }
  }

  const signup = async (email: string, password: string) => {
    try {
      const res = await fetch(`${API_URL}/api/signup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || "Signup failed")

      const authUser: AuthUser = {
        uid: data.uid,
        email,
        token: data.token,
        refreshToken: data.refreshToken,
        tokenExpiresAt: tokenExpiresAt(),
      }
      persistUser(authUser)
      return { success: true }
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Signup failed" }
    }
  }

  const logout = () => {
    setUser(null)
    localStorage.removeItem("auth_user")
    router.push("/login")
  }

  return (
    <AuthContext.Provider value={{ user, login, signup, logout, loading, getFreshToken }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error("useAuth must be used within AuthProvider")
  return ctx
}