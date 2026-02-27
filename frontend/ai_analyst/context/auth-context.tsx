"use client"

import { createContext, useContext, useEffect, useState } from "react"
import { useRouter } from "next/navigation"

interface AuthUser {
  uid: string
  email: string
  token: string
}

interface AuthContextType {
  user: AuthUser | null
  login: (email: string, password: string) => Promise<{ success: boolean; error?: string }>
  signup: (email: string, password: string) => Promise<{ success: boolean; error?: string }>
  logout: () => void
  loading: boolean
}

const AuthContext = createContext<AuthContextType | null>(null)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null)
  const [loading, setLoading] = useState(true)
  const router = useRouter()

  useEffect(() => {
    // Check if user is already logged in
    const stored = localStorage.getItem("auth_user")
    if (stored) {
      setUser(JSON.parse(stored))
    }
    setLoading(false)
  }, [])

  const login = async (email: string, password: string) => {
    try {
      const res = await fetch("http://localhost:8000/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || "Login failed")

      const authUser = { uid: data.uid, email, token: data.token }
      setUser(authUser)
      localStorage.setItem("auth_user", JSON.stringify(authUser))
      return { success: true }
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Login failed" }
    }
  }

  const signup = async (email: string, password: string) => {
    try {
      const res = await fetch("http://localhost:8000/api/signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.detail || "Signup failed")

      const authUser = { uid: data.uid, email, token: data.token }
      setUser(authUser)
      localStorage.setItem("auth_user", JSON.stringify(authUser))
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
    <AuthContext.Provider value={{ user, login, signup, logout, loading }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error("useAuth must be used within AuthProvider")
  return ctx
}