"use client"

import { useState } from "react"
import { Search, Loader2, AlertCircle } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

import SinglePlayerStat from "@/components/recharts/SinglePlayerStat"
import CompareStats from "@/components/recharts/CompareStats"
import CategoricalBreakdown from "@/components/recharts/CategoricalBreakdown" 
import CompareCategoricalBreakdown from "@/components/recharts/CompareCategoricalBreakdown"
import Leaderboard from "@/components/recharts/Leaderboard"

interface AnalysisResult {
  success: boolean
  chartType: string
  data: any[]
  config: {
    statKey?: string
    playerNames?: string[] 
    playerName?: string   
    xAxisKey?: string
    timeFrame?: string
    statDisplayName?: string
  }
  error?: string
}

export default function DashboardsPage() {
  const [searchQuery, setSearchQuery] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [result, setResult] = useState<AnalysisResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleSearch = async () => {
    if (!searchQuery.trim()) return

    setIsLoading(true)
    setError(null)
    setResult(null) 

    try {
      const response = await fetch("http://127.0.0.1:8000/api/dashboards", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: searchQuery }),
      })

      const data = await response.json()

      if (!response.ok || !data.success) {
        throw new Error(data.error || "Failed to analyze data")
      }
      
      // makes sure playerNames is always an array for consistency
      if (data.config && data.config.playerName && !data.config.playerNames) {
        data.config.playerNames = [data.config.playerName];
      }

      setResult(data)
    } catch (err: any) {
      console.error(err)
      setError(err.message || "Was not able to connect to serber")
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="flex flex-col gap-6 max-w-6xl mx-auto p-6">
      <div className="space-y-2">
        <h1 className="text-4xl font-bold">Basketball Analyst</h1>
        <p className="text-muted-foreground text-lg">
          Ask about trends, comparisons, or specific game stats.
        </p>
      </div>

      <div className="flex gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") handleSearch()
            }}
            placeholder='Try "Show me Steph Curry skill profile" or "Compare LeBron and KD points 2024"'
            className="pl-10"
            disabled={isLoading}
          />
        </div>
        <Button onClick={handleSearch} disabled={isLoading}>
          {isLoading ? <Loader2 className="animate-spin" /> : "Search"}
        </Button>
      </div>

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {result && (
        <div className="space-y-4 animate-in fade-in slide-in-from-bottom-4 duration-500">
          {/* Single Player Trend (Line Chart) */}
          {result.chartType === "SinglePlayerStat" && (
            <div className="border rounded-xl p-6 bg-card shadow-sm">
              <SinglePlayerStat
                data={result.data}
                statKey={result.config.statKey!}
                playerName={result.config.playerNames?.[0] || "Player"}
                xAxisKey={result.config.xAxisKey}
                timeFrame={result.config.timeFrame}
                statDisplayName={result.config.statDisplayName}
              />
            </div>
          )}

          {/* Comparison (Bar chart) */}
          {result.chartType === "CompareStats" && (
            <div className="border rounded-xl p-6 bg-card shadow-sm">
              <CompareStats
                data={result.data}
                config={{
                  statDisplayName: result.config.statDisplayName || "Comparison",
                  xAxisKey: result.config.xAxisKey
                }}
              />
            </div>
          )}

          {/* Skill Profile (Radar chart) */}
          {result.chartType === "CategoricalBreakdown" && (
            <div className="border rounded-xl p-6 bg-card shadow-sm w-full max-w-lg mx-auto">
              {result.config.playerNames && result.config.playerNames.length > 1 ? (
                <CompareCategoricalBreakdown
                  data={result.data}
                  config={{ statDisplayName: "Skill Comparison" }}
                />
              ) : (
                <CategoricalBreakdown
                  data={result.data}
                  config={{
                    playerName: result.config.playerNames?.[0] || "Player",
                    statDisplayName: "Skill Profile"
                  }}
                />
              )}
            </div>
          )}

          {/* Leaderboard */}
          {result.chartType === "Leaderboard" && (
            <div className="border rounded-xl p-6 bg-card shadow-sm">
              <Leaderboard
                data={result.data}
                config={{
                  statDisplayName: result.config.statDisplayName || "Value",
                  timeFrame: result.config.timeFrame
                }}
              />
            </div>
          )}
        </div>
      )}
    </div>
  )
}