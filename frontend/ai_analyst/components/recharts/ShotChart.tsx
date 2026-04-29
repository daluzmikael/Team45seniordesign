"use client"

import { useMemo, useState, useEffect } from "react"
import { useTheme } from "next-themes"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"

// ─── NBA Court Geometry ───
// NBA API: x = -250 to 250, y = -52 to ~420 (units = 1/10th foot)
// Basket at (0,0). y increases toward half court.

const PADDING = 20
const COURT_LEFT = -250
const COURT_RIGHT = 250
const COURT_BOTTOM = -52
const COURT_TOP = 422

const SVG_W = (COURT_RIGHT - COURT_LEFT) + PADDING * 2
const SVG_H = (COURT_TOP - COURT_BOTTOM) + PADDING * 2

function toSvg(apiX: number, apiY: number): [number, number] {
  return [
    apiX - COURT_LEFT + PADDING,
    COURT_TOP - apiY + PADDING,
  ]
}

const HEX_RADIUS = 8

interface ShotChartProps {
  data: any[]
  config: {
    playerName?: string
    statDisplayName?: string
    timeFrame?: string
    mode?: "volume" | "accuracy" | "hotspots" | "coldspots"
  }
}

// ─── Color Scales ───
function getColor(value: number, mode: string): string {
  if (mode === "accuracy") {
    // Diverging: red (low FG%) → white/neutral (avg) → green (high FG%)
    // value is 0-1 where 0.5 = league average
    if (value < 0.5) {
      const t = value / 0.5 // 0 to 1
      const r = Math.round(200 - t * 60)
      const g = Math.round(40 + t * 80)
      const b = Math.round(40 + t * 60)
      return `rgb(${r},${g},${b})`
    }
    const t = (value - 0.5) / 0.5 // 0 to 1
    const r = Math.round(140 - t * 110)
    const g = Math.round(120 + t * 120)
    const b = Math.round(100 - t * 50)
    return `rgb(${r},${g},${b})`
  }
  if (mode === "hotspots") {
    // Pure green scale for best zones
    const r = Math.round(15 + value * 60)
    const g = Math.round(60 + value * 195)
    const b = Math.round(40 + value * 40)
    return `rgb(${r},${g},${b})`
  }
  if (mode === "coldspots") {
    // Pure red scale for worst zones
    const r = Math.round(100 + value * 155)
    const g = Math.round(20 + value * 25)
    const b = Math.round(25 + value * 15)
    return `rgb(${r},${g},${b})`
  }
  // Volume: indigo → purple → magenta → orange → yellow
  if (value < 0.2) {
    const t = value / 0.2
    return `rgb(${Math.round(60 + t * 20)}, ${Math.round(30 + t * 10)}, ${Math.round(90 + t * 30)})`
  }
  if (value < 0.4) {
    const t = (value - 0.2) / 0.2
    return `rgb(${Math.round(50 + t * 70)}, ${Math.round(15 + t * 10)}, ${Math.round(100 + t * 30)})`
  }
  if (value < 0.6) {
    const t = (value - 0.4) / 0.2
    return `rgb(${Math.round(120 + t * 60)}, ${Math.round(25 + t * 20)}, ${Math.round(130 - t * 30)})`
  }
  if (value < 0.8) {
    const t = (value - 0.6) / 0.2
    return `rgb(${Math.round(180 + t * 50)}, ${Math.round(45 + t * 80)}, ${Math.round(100 - t * 70)})`
  }
  const t = (value - 0.8) / 0.2
  return `rgb(${Math.round(230 + t * 25)}, ${Math.round(125 + t * 120)}, ${Math.round(30 + t * 20)})`
}

function hexPoints(cx: number, cy: number, r: number): string {
  const points = []
  for (let i = 0; i < 6; i++) {
    const angle = (Math.PI / 3) * i - Math.PI / 6
    points.push(`${cx + r * Math.cos(angle)},${cy + r * Math.sin(angle)}`)
  }
  return points.join(" ")
}

// ─── Hexbin Grouping ───
function buildHexBins(shots: any[], radius: number, mode: string) {
  const bins: Record<string, { x: number; y: number; made: number; total: number }> = {}
  const hexW = radius * 2
  const hexH = Math.sqrt(3) * radius

  for (const shot of shots) {
    const apiX = Number(shot.loc_x)
    const apiY = Number(shot.loc_y)
    if (apiY > 420 || apiY < -52) continue

    const [px, py] = toSvg(apiX, apiY)
    const col = Math.round(px / (hexW * 0.75))
    const row = Math.round((py - (col % 2 === 0 ? 0 : hexH / 2)) / hexH)
    const key = `${col}_${row}`

    if (!bins[key]) {
      const cx = col * hexW * 0.75
      const cy = row * hexH + (col % 2 === 0 ? 0 : hexH / 2)
      bins[key] = { x: cx, y: cy, made: 0, total: 0 }
    }
    bins[key].total += 1
    if (shot.shot_made_flag === 1 || shot.shot_made_flag === "1" || shot.shot_made_flag === true) {
      bins[key].made += 1
    }
  }

  let binList = Object.values(bins).filter(b => b.total >= 1)

  if (mode === "hotspots") {
    // Only show zones with 5+ attempts, sorted by FG%, keep top 35%
    const qualified = binList.filter(b => b.total >= 5)
    if (qualified.length === 0) return binList.map(b => ({ ...b, pct: b.made / b.total }))
    qualified.sort((a, b) => (a.made / a.total) - (b.made / b.total))
    const cutoff = Math.max(1, Math.floor(qualified.length * 0.35))
    return qualified.slice(-cutoff).map(b => ({ ...b, pct: b.made / b.total }))
  }

  if (mode === "coldspots") {
    // Only show zones with 5+ attempts, sorted by FG%, keep bottom 35%
    const qualified = binList.filter(b => b.total >= 5)
    if (qualified.length === 0) return binList.map(b => ({ ...b, pct: b.made / b.total }))
    qualified.sort((a, b) => (a.made / a.total) - (b.made / b.total))
    const cutoff = Math.max(1, Math.floor(qualified.length * 0.35))
    return qualified.slice(0, cutoff).map(b => ({ ...b, pct: b.made / b.total }))
  }

  // For accuracy mode, filter to 3+ attempts so stray 1/1 zones don't dominate
  if (mode === "accuracy") {
    binList = binList.filter(b => b.total >= 3)
  }

  return binList.map(b => ({ ...b, pct: b.made / b.total }))
}

// ─── NBA Half Court Drawing ───
function pointsOnArc(
  cx: number, cy: number,
  radius: number,
  startAngleDeg: number,
  endAngleDeg: number,
  steps: number
): string {
  const points: string[] = []
  for (let i = 0; i <= steps; i++) {
    const t = i / steps
    const angleDeg = startAngleDeg + t * (endAngleDeg - startAngleDeg)
    const angleRad = (angleDeg * Math.PI) / 180
    const apiX = cx + radius * Math.cos(angleRad)
    const apiY = cy + radius * Math.sin(angleRad)
    const [sx, sy] = toSvg(apiX, apiY)
    points.push(`${sx},${sy}`)
  }
  return points.join(" ")
}

function CourtLines({ isDark }: { isDark: boolean }) {
  const lineColor = isDark ? "rgba(255,255,255,0.6)" : "rgba(0,0,0,0.65)"
  const softLineColor = isDark ? "rgba(255,255,255,0.45)" : "rgba(0,0,0,0.5)"
  const rimColor = isDark ? "rgba(255,140,0,0.7)" : "rgba(220,90,0,0.85)"

  const [bx, by] = toSvg(0, 0)
  const [courtL, courtBot] = toSvg(-250, -47)
  const [courtR] = toSvg(250, -47)
  const [, courtTop] = toSvg(0, 422)
  const [paintL] = toSvg(-80, 0)
  const [paintR] = toSvg(80, 0)
  const [, paintTop] = toSvg(0, 143)

  const THREE_R = 237.5
  const cornerAngle = Math.acos(220 / THREE_R) * (180 / Math.PI)
  const arcStartY = THREE_R * Math.sin((cornerAngle * Math.PI) / 180)

  const [cornerLX, cornerLBot] = toSvg(-220, -47)
  const [cornerRX, cornerRBot] = toSvg(220, -47)
  const [, cornerLTop] = toSvg(-220, arcStartY)
  const [, cornerRTop] = toSvg(220, arcStartY)

  const threeArcPoints = pointsOnArc(0, 0, THREE_R, cornerAngle, 180 - cornerAngle, 60)
  const raArcPoints = pointsOnArc(0, 0, 40, 0, 180, 30)
  const [raLX, raLY] = toSvg(-40, 0)
  const [raRX, raRY] = toSvg(40, 0)
  const ftTopPoints = pointsOnArc(0, 143, 60, 0, 180, 30)
  const ftBotPoints = pointsOnArc(0, 143, 60, 180, 360, 30)
  const ccPoints = pointsOnArc(0, 422, 60, 180, 360, 30)

  return (
    <g stroke={lineColor} strokeWidth={2.5} fill="none">
      <rect x={courtL} y={courtTop} width={courtR - courtL} height={courtBot - courtTop} />
      <rect x={paintL} y={paintTop} width={paintR - paintL} height={courtBot - paintTop} />
      <polyline points={ftTopPoints} />
      <polyline points={ftBotPoints} strokeDasharray="8 6" />
      <line x1={bx - 30} y1={by + 15} x2={bx + 30} y2={by + 15} strokeWidth={2} stroke={softLineColor} />
      <circle cx={bx} cy={by} r={7.5} strokeWidth={1.8} stroke={rimColor} />
      <polyline points={raArcPoints} />
      <line x1={raLX} y1={raLY} x2={raLX} y2={courtBot} />
      <line x1={raRX} y1={raRY} x2={raRX} y2={courtBot} />
      <line x1={cornerLX} y1={cornerLBot} x2={cornerLX} y2={cornerLTop} />
      <line x1={cornerRX} y1={cornerRBot} x2={cornerRX} y2={cornerRTop} />
      <polyline points={threeArcPoints} />
      <polyline points={ccPoints} />
    </g>
  )
}

// ─── Main Component ───
export default function ShotChart({ data, config }: ShotChartProps) {
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  useEffect(() => { setMounted(true) }, [])

  // Default to dark on first paint to match the previous look and avoid flicker
  const isDark = mounted ? resolvedTheme === "dark" : true

  const mode = config.mode || "volume"
  const bins = useMemo(() => buildHexBins(data || [], HEX_RADIUS, mode), [data, mode])

  const maxVal = useMemo(() => {
    if (mode === "volume") return Math.max(...bins.map(b => b.total), 1)
    return 1
  }, [bins, mode])

  if (!data || data.length === 0) return <div>No shot data found</div>

  const modeLabel = mode === "hotspots" ? "Best Shooting Zones"
    : mode === "coldspots" ? "Worst Shooting Zones"
    : mode === "accuracy" ? "Shooting Accuracy (Red = Cold, Green = Hot)"
    : "Shot Frequency"

  const legendColors = [0, 0.25, 0.5, 0.75, 1].map(v => getColor(v, mode))
  const legendLabelLeft = mode === "volume" ? "Few"
    : mode === "accuracy" ? "Cold"
    : mode === "coldspots" ? "Bad"
    : "Low"
  const legendLabelRight = mode === "volume" ? "Many"
    : mode === "accuracy" ? "Hot"
    : mode === "coldspots" ? "Worse"
    : "High"

  const courtBg = isDark ? "#000000" : "#f5f5f0"
  const legendTextColor = isDark ? "rgba(255,255,255,0.45)" : "rgba(0,0,0,0.55)"

  return (
    <Card>
      <CardHeader className="items-center pb-4">
        <CardTitle>{config.playerName || "Player"}: Shot Chart</CardTitle>
        <CardDescription>
          {modeLabel} {config.timeFrame ? `— ${config.timeFrame}` : "— Career"}
          {` • ${data.length.toLocaleString()} shots`}
        </CardDescription>
      </CardHeader>
      <CardContent className="pb-6 px-2">
        <svg
          viewBox={`0 0 ${SVG_W} ${SVG_H}`}
          className="w-full"
          style={{ background: courtBg, borderRadius: "12px" }}
        >
          {(() => {
            const [lx] = toSvg(-250, 0)
            const [rx] = toSvg(250, 0)
            const [, ty] = toSvg(0, 422)
            const [, bly] = toSvg(0, -47)
            return <rect x={lx} y={ty} width={rx - lx} height={bly - ty} fill="rgba(255,255,255,0.0)" />
          })()}

          <CourtLines isDark={isDark} />

          {bins.map((bin, i) => {
            let colorVal: number
            let size: number

            if (mode === "volume") {
              colorVal = bin.total / maxVal
              size = HEX_RADIUS * (0.5 + 0.5 * (bin.total / maxVal))
            } else if (mode === "accuracy") {
              // Normalize FG% to 0-1 scale where 0.5 = ~45% (league avg FG%)
              const pct = (bin as any).pct ?? 0
              colorVal = Math.min(1, Math.max(0, pct / 0.9)) // 0% → 0, 45% → 0.5, 90%+ → 1
              // Size by volume so you can still see where they shoot most
              size = HEX_RADIUS * (0.5 + 0.5 * Math.min(1, bin.total / Math.max(maxVal * 0.3, 1)))
            } else {
              // hotspots / coldspots — uniform size, color by FG%
              colorVal = (bin as any).pct ?? 0
              size = HEX_RADIUS * 0.9
            }

            return (
              <g key={i}>
                <polygon
                  points={hexPoints(bin.x, bin.y, size)}
                  fill={getColor(colorVal, mode)}
                  opacity={0.9}
                  stroke={getColor(colorVal, mode)}
                  strokeWidth={0.3}
                />
                <title>
                  {`${bin.made}/${bin.total} (${(((bin as any).pct ?? 0) * 100).toFixed(1)}%)`}
                </title>
              </g>
            )
          })}

          <g transform={`translate(${SVG_W - 170}, ${SVG_H - 28})`}>
            <text fill={legendTextColor} fontSize={9} fontFamily="sans-serif" y={-2}>
              {legendLabelLeft}
            </text>
            <defs>
              <linearGradient id="shotLegendGrad">
                {legendColors.map((c, i) => (
                  <stop key={i} offset={`${i * 25}%`} stopColor={c} />
                ))}
              </linearGradient>
            </defs>
            <rect x={28} y={-10} width={90} height={8} rx={2} fill="url(#shotLegendGrad)" />
            <text fill={legendTextColor} fontSize={9} fontFamily="sans-serif" x={123} y={-2}>
              {legendLabelRight}
            </text>
          </g>
        </svg>
      </CardContent>
    </Card>
  )
}