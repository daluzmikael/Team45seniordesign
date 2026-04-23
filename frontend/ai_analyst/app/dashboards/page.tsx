"use client"

import { useState } from "react"
import { Search, Loader2, AlertCircle, ChevronDown } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

import SinglePlayerStat from "@/components/recharts/SinglePlayerStat"
import CompareStats from "@/components/recharts/CompareStats"
import CategoricalBreakdown from "@/components/recharts/CategoricalBreakdown"
import CompareCategoricalBreakdown from "@/components/recharts/CompareCategoricalBreakdown"
import Leaderboard from "@/components/recharts/Leaderboard"

import ShotChart from "@/components/recharts/ShotChart"

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
    mode?: "volume" | "accuracy" | "hotspots" | "coldspots"
  }
  error?: string
}

const EXAMPLE_CATEGORIES = [
  {
    label: "Leaderboards",
    examples: [
      "Show me the top 10 scorers in 2008",
      "Who were the top 10 players by assists per game in 2023?",
      "Show me the top 5 players by rebounds per game in 2009",
      "Who were the top 10 players with the most minutes in 2007?"
    ],
  },
  {
    label: "Player Trends",
    examples: [
      "Show me Kyle Kuzma ppg trend from 2019 to 2024",
      "Show me Jimmy Butler's points trend in the 2023 Playoffs",
      "Show me LeBron James rebounds per game trend from 2010 to 2023",
      "Show me Stephen Curry 3 point percentage trend over his career"
    ],
  },
  {
    label: "Comparisons",
    examples: [
      "Show me Karl-Anthony Towns rebounds per game in 2021 vs Bol Bol",
      "Show me Kyle Kuzma and Kevin Durant points from 2019 to 2024",
      "Compare Stephen Curry and Damian Lillard 3 point shooting percentages in 2023",
      "Compare LeBron James and Trae Young assists in the 2023 season"
    ],
  },
  {
    label: "Skill Profiles",
    examples: [
      "Show me Dirk Nowitzki's skill profile in 2005",
      "Show me Manu Ginobili 2013 skill profile vs Trae Young's 2018 skill profile",
      "Show me Stephen Curry's skill profile",
      "Show me James Harden's skill profile in the 2018 season"
    ],
  },
  {
    label: "Shot Chart Heat Maps",
    examples: [
      "Show me a heat map of Stephen Curry's 3 point shot selection",
      "Show me LeBron's best shooting zones against the Celtics",
      "Show me Steph Curry's shooting percentages heat map",
      "Show me Shaquille O'Neal's worst shooting zone within 15 feet",
    ],
  },
]

export default function DashboardsPage() {
  const [searchQuery, setSearchQuery] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [result, setResult] = useState<AnalysisResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [expandedCategory, setExpandedCategory] = useState<string | null>(null)

  const handleSearch = async () => {
    if (!searchQuery.trim()) return

    setIsLoading(true)
    setError(null)
    setResult(null)

    try {
      const API_URL =
        process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

      const response = await fetch(`${API_URL}/api/dashboards`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: searchQuery }),
      })

      const data = await response.json()

      if (!response.ok || !data.success) {
        throw new Error(data.error || data.detail || "Failed to analyze data")
      }

      if (data.config && data.config.playerName && !data.config.playerNames) {
        data.config.playerNames = [data.config.playerName]
      }

      setResult(data)
    } catch (err: any) {
      console.error(err)
      setError(err.message || "Was not able to connect to server")
    } finally {
      setIsLoading(false)
    }
  }

  const handleExampleClick = (example: string) => {
    setSearchQuery(example)
    setExpandedCategory(null)
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

      {/* Example categories — only show when idle */}
      {!result && !isLoading && !error && (
        <div className="flex flex-col items-center gap-6 pt-4">
          <p className="text-muted-foreground text-sm">
            Not sure what to ask? Pick a category to see examples.
          </p>

          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3 w-full max-w-3xl">
            {EXAMPLE_CATEGORIES.map((category) => (
              <div
                key={category.label}
                className={`rounded-lg border bg-card p-4 cursor-pointer transition-all text-center hover:bg-accent ${
                  expandedCategory === category.label
                    ? "ring-2 ring-primary bg-accent"
                    : ""
                }`}
                onClick={() =>
                  setExpandedCategory(
                    expandedCategory === category.label ? null : category.label
                  )
                }
              >
                <p className="text-sm font-medium">{category.label}</p>
                <ChevronDown
                  className={`h-3 w-3 mx-auto mt-1 text-muted-foreground transition-transform ${
                    expandedCategory === category.label ? "rotate-180" : ""
                  }`}
                />
              </div>
            ))}
          </div>

          {/* Expanded example questions */}
          {expandedCategory && (
            <div className="w-full max-w-3xl animate-in fade-in slide-in-from-top-2 duration-200">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {EXAMPLE_CATEGORIES.find(
                  (c) => c.label === expandedCategory
                )?.examples.map((example) => (
                  <div
                    key={example}
                    className="rounded-lg border bg-card p-4 hover:bg-accent cursor-pointer transition-colors"
                    onClick={() => handleExampleClick(example)}
                  >
                    <p className="text-sm">{example}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      {result && (
        <div className="space-y-4 animate-in fade-in slide-in-from-bottom-4 duration-500">
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

          {result.chartType === "CompareStats" && (
            <div className="border rounded-xl p-6 bg-card shadow-sm">
              <CompareStats
                data={result.data}
                config={{
                  statDisplayName: result.config.statDisplayName || "Comparison",
                  xAxisKey: result.config.xAxisKey,
                }}
              />
            </div>
          )}

          {(result.chartType === "CategoricalBreakdown" || result.chartType === "CompareCategoricalBreakdown") && (
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
                    statDisplayName: "Skill Profile",
                  }}
                />
              )}
            </div>
          )}

          {result.chartType === "Leaderboard" && (
            <div className="border rounded-xl p-6 bg-card shadow-sm">
              <Leaderboard
                data={result.data}
                config={{
                  statDisplayName: result.config.statDisplayName || "Value",
                  timeFrame: result.config.timeFrame,
                }}
              />
            </div>
          )}

          {result.chartType === "ShotChart" && (
            <div className="border rounded-xl p-6 bg-card shadow-sm">
              <ShotChart
                data={result.data}
                config={{
                  playerName: result.config.playerNames?.[0] || "Player",
                  statDisplayName: result.config.statDisplayName || "Shot Chart",
                  timeFrame: result.config.timeFrame,
                  mode: (result.config as any).mode || "volume",
                }}
              />
            </div>
          )}
        </div>
      )}
    </div>
  )
}