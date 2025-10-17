"use client"

import { useState } from "react"
import { Search } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"

export default function DashboardsPage() {
  const [searchQuery, setSearchQuery] = useState("")

  const handleSearch = () => {
    // Placeholder for database search functionality
    console.log("Searching for:", searchQuery)
  }

  return (
    <div className="flex flex-col gap-6 max-w-6xl mx-auto p-6">
      <div className="space-y-2">
        <h1 className="text-4xl font-bold">Basketball Stat Dashboards</h1>
        <p className="text-muted-foreground text-lg">
          Search for any basketball stat to view detailed dashboards and analytics
        </p>
      </div>

      {/* Search bar */}
      <div className="flex gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                handleSearch()
              }
            }}
            placeholder='Try "LeBron 3% percentage the last 5 games" or "Lakers win rate (2022-23)"'
            className="pl-10"
          />
        </div>
        <Button onClick={handleSearch}>Search</Button>
      </div>

      {/* Placeholder for dashboards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mt-8">
        <div className="rounded-lg border bg-card p-6 hover:bg-accent cursor-pointer transition-colors">
          <h3 className="font-semibold mb-2">Player Performance</h3>
          <p className="text-sm text-muted-foreground">View individual player statistics and performance metrics</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:bg-accent cursor-pointer transition-colors">
          <h3 className="font-semibold mb-2">Team Analytics</h3>
          <p className="text-sm text-muted-foreground">Analyze team performance, win rates, and season trends</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:bg-accent cursor-pointer transition-colors">
          <h3 className="font-semibold mb-2">Shooting Stats</h3>
          <p className="text-sm text-muted-foreground">Deep dive into shooting percentages and efficiency</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:bg-accent cursor-pointer transition-colors">
          <h3 className="font-semibold mb-2">Historical Comparisons</h3>
          <p className="text-sm text-muted-foreground">Compare players across different eras and seasons</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:bg-accent cursor-pointer transition-colors">
          <h3 className="font-semibold mb-2">Advanced Metrics</h3>
          <p className="text-sm text-muted-foreground">Explore PER, VORP, BPM, and other advanced statistics</p>
        </div>
        <div className="rounded-lg border bg-card p-6 hover:bg-accent cursor-pointer transition-colors">
          <h3 className="font-semibold mb-2">Playoff Performance</h3>
          <p className="text-sm text-muted-foreground">Track playoff statistics and championship runs</p>
        </div>
      </div>

      <div className="rounded-lg border border-dashed bg-muted/50 p-8 text-center mt-4">
        <p className="text-muted-foreground">
          Still gotta connect database
        </p>
      </div>
    </div>
  )
}
