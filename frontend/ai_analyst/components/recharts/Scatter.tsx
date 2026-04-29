"use client"

import { useMemo, useState, useEffect } from "react"
import { useTheme } from "next-themes"
import {
  CartesianGrid,
  Scatter,
  ScatterChart,
  Tooltip,
  TooltipProps,
  XAxis,
  YAxis,
  ZAxis,
  ResponsiveContainer,
} from "recharts"
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  ChartConfig,
  ChartContainer,
} from "@/components/ui/chart"

interface ScatterProps {
  data: any[]
  config: {
    statDisplayName?: string
    xAxisLabel?: string
    yAxisLabel?: string
    timeFrame?: string
  }
}

// Coerce a value to a finite number, or null if it can't be parsed.
// Postgres can return numerics as strings via JSON, especially when columns
// are stored as text (advanced/tracking tables). NULLIF + CAST in SQL handles
// most cases, but we still drop any straggler nulls or empty strings.
function toNumeric(v: unknown): number | null {
  if (v === null || v === undefined) return null
  if (typeof v === "number") return Number.isFinite(v) ? v : null
  const s = String(v).trim()
  if (s === "") return null
  const n = Number(s)
  return Number.isFinite(n) ? n : null
}

// Deterministic jitter so the same player always lands at the same X position
// across re-renders. Without this, single-axis distribution charts shimmer
// every time React re-renders.
function deterministicJitter(seed: string): number {
  let h = 2166136261 >>> 0
  for (let i = 0; i < seed.length; i++) {
    h ^= seed.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  // Map to [-0.45, 0.45] so dots stay clear of the axis edges
  return ((h >>> 0) / 0xffffffff) * 0.9 - 0.45
}

interface ScatterRow {
  player_name: string
  x_value: number
  y_value: number
}

const HARD_RENDER_CAP = 750

const CustomTooltip = ({
  active,
  payload,
  isSingleAxis,
  xAxisLabel,
  yAxisLabel,
}: TooltipProps<number, string> & {
  isSingleAxis: boolean
  xAxisLabel?: string
  yAxisLabel?: string
}) => {
  if (!active || !payload || payload.length === 0) return null
  const p = payload[0]?.payload as ScatterRow | undefined
  if (!p) return null

  return (
    <div className="rounded-lg border bg-background p-2 shadow-sm text-xs">
      <div className="font-semibold">{p.player_name}</div>
      {!isSingleAxis && xAxisLabel && (
        <div className="text-muted-foreground">
          {xAxisLabel}: <span className="text-foreground font-medium">{formatValue(p.x_value, xAxisLabel)}</span>
        </div>
      )}
      <div className="text-muted-foreground">
        {yAxisLabel || "Value"}: <span className="text-foreground font-medium">{formatValue(p.y_value, yAxisLabel)}</span>
      </div>
    </div>
  )
}

function formatValue(v: number, label?: string): string {
  if (!Number.isFinite(v)) return "—"

  // Check if the label implies it's a percentage stat
  const isPercent = label && (
    label.toLowerCase().includes("%") ||
    label.toLowerCase().includes("pct") ||
    label.toLowerCase().includes("rate")
  )

  // Only apply % formatting if the label suggests it AND it's a decimal <= 1
  if (isPercent && Math.abs(v) > 0 && Math.abs(v) <= 1) {
    return (v * 100).toFixed(1) + "%"
  }

  // Big numbers (salary, totals) get comma-separated; small numbers get one decimal
  if (Math.abs(v) >= 1000) return Math.round(v).toLocaleString()
  if (Number.isInteger(v)) return v.toString()
  return v.toFixed(1)
}

const chartConfig = {
  point: { label: "Player", color: "hsl(var(--chart-1))" },
} satisfies ChartConfig

export default function ScatterComponent({ data, config }: ScatterProps) {
  const { statDisplayName = "Scatter Plot", xAxisLabel = "", yAxisLabel = "", timeFrame } = config || {}

  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  useEffect(() => { setMounted(true) }, [])
  
  const isDark = mounted ? resolvedTheme === "dark" : false
  const dotColor = isDark ? "#ffffff" : "#000000" // White in dark mode, black in light mode

  // Normalize rows: parse numerics, drop nulls, detect single-axis mode.
  // We do this client-side as a safety net even though the backend validator
  // already checks. This keeps the chart resilient to JSON quirks.
  const { rows, isSingleAxis, capped } = useMemo(() => {
    if (!Array.isArray(data) || data.length === 0) {
      return { rows: [] as ScatterRow[], isSingleAxis: false, capped: false }
    }

    const hasXValue = data.some(
      (r) => r && Object.prototype.hasOwnProperty.call(r, "x_value") && r.x_value !== null && r.x_value !== undefined && r.x_value !== ""
    )
    const isSingleAxis = !hasXValue

    const cleaned: ScatterRow[] = []
    for (const r of data) {
      if (!r) continue
      const player = (r.player_name ?? r.full_name ?? "").toString().trim()
      if (!player) continue

      const yNum = toNumeric(r.y_value)
      if (yNum === null) continue

      let xNum: number | null
      if (isSingleAxis) {
        // Spread points across [-0.45, 0.45] using a stable hash of the player name
        xNum = deterministicJitter(player)
      } else {
        xNum = toNumeric(r.x_value)
        if (xNum === null) continue
      }

      cleaned.push({ player_name: player, x_value: xNum, y_value: yNum })
    }

    let capped = false
    let final = cleaned
    if (cleaned.length > HARD_RENDER_CAP) {
      capped = true
      final = cleaned.slice(0, HARD_RENDER_CAP)
    }
    return { rows: final, isSingleAxis, capped }
  }, [data])

  if (!data || data.length === 0) {
    return <div>No data available</div>
  }

  if (rows.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>{statDisplayName}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-sm text-muted-foreground">
            No numeric data points to display. The query returned {data.length} rows but none contained valid x/y values.
          </div>
        </CardContent>
      </Card>
    )
  }

  // Compute axis domains. For single-axis we hide the X domain so the jitter
  // doesn't appear as meaningful values; for two-axis, give a small padding.
  const yValues = rows.map((r) => r.y_value)
  const yMin = Math.min(...yValues)
  const yMax = Math.max(...yValues)
  const yPadding = (yMax - yMin) * 0.05 || 1
  const yDomain: [number, number] = [yMin - yPadding, yMax + yPadding]

  let xDomain: [number, number] = [-0.5, 0.5]
  if (!isSingleAxis) {
    const xValues = rows.map((r) => r.x_value)
    const xMin = Math.min(...xValues)
    const xMax = Math.max(...xValues)
    const xPadding = (xMax - xMin) * 0.05 || 1
    xDomain = [xMin - xPadding, xMax + xPadding]
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>{statDisplayName}</CardTitle>
        <CardDescription>
          {timeFrame ? `${timeFrame} • ` : ""}
          {rows.length} {rows.length === 1 ? "player" : "players"}
          {capped ? ` (showing first ${HARD_RENDER_CAP})` : ""}
          {isSingleAxis ? " — distribution view" : ""}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="min-h-[450px] w-full">
          <ResponsiveContainer width="100%" height={450}>
            <ScatterChart margin={{ top: 20, right: 30, bottom: 40, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                type="number"
                dataKey="x_value"
                name={xAxisLabel || "X"}
                domain={xDomain}
                hide={isSingleAxis}
                tick={{ fontSize: 12 }}
                label={
                  !isSingleAxis && xAxisLabel
                    ? { value: xAxisLabel, position: "insideBottom", offset: -10, style: { fontSize: 13 } }
                    : undefined
                }
                tickFormatter={(v) => formatValue(v, xAxisLabel)}
              />
              <YAxis
                type="number"
                dataKey="y_value"
                name={yAxisLabel || "Y"}
                domain={yDomain}
                tick={{ fontSize: 12 }}
                label={
                  yAxisLabel
                    ? { value: yAxisLabel, angle: -90, position: "insideLeft", style: { fontSize: 13, textAnchor: "middle" } }
                    : undefined
                }
                tickFormatter={(v) => formatValue(v, yAxisLabel)}
                width={70}
              />
              {/* ZAxis controls dot size — keep it modest so 500 dots don't overlap */}
              <ZAxis range={[40, 40]} />
              <Tooltip
                content={
                  <CustomTooltip
                    isSingleAxis={isSingleAxis}
                    xAxisLabel={xAxisLabel}
                    yAxisLabel={yAxisLabel}
                  />
                }
                cursor={{ strokeDasharray: "3 3" }}
              />
            <Scatter
                name={statDisplayName}
                data={rows}
                fill={dotColor}
                fillOpacity={0.4}
                stroke={dotColor}
                strokeOpacity={0.4}
                strokeWidth={0.1}
            />
            </ScatterChart>
          </ResponsiveContainer>
        </ChartContainer>
      </CardContent>
      <CardFooter className="flex-col items-start gap-1 text-xs text-muted-foreground">
        <div>Tap or hover any point to see the player.</div>
        {isSingleAxis && (
          <div>Horizontal positions are randomized to spread overlapping points; only the vertical axis is meaningful.</div>
        )}
      </CardFooter>
    </Card>
  )
}