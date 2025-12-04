"use client"

import { PolarAngleAxis, PolarGrid, PolarRadiusAxis, Radar, RadarChart } from "recharts"
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
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart"

const chartConfig = {
  value: {
    label: "Stat Value",
    color: "hsl(var(--chart-1))",
  },
} satisfies ChartConfig

interface CategoricalBreakdownProps {
  data: any[]
  config: {
    statDisplayName?: string
    playerName?: string
  }
}

export default function CategoricalBreakdown({ data, config }: CategoricalBreakdownProps) {
  if (!data || data.length === 0) return <div>No data found</div>

  return (
    <Card>
      <CardHeader className="items-center pb-4">
        <CardTitle>{config.playerName || "Player"} Profile</CardTitle>
        <CardDescription>
          Skill Breakdown (Averages)
        </CardDescription>
      </CardHeader>
      <CardContent className="pb-0">
        <ChartContainer
          config={chartConfig}
          className="mx-auto aspect-square max-h-[350px]"
        >
          <RadarChart data={data} outerRadius={140}>
            <ChartTooltip cursor={false} content={<ChartTooltipContent />} />
            <PolarGrid className="fill-[--color-desktop] opacity-100" />
            <PolarAngleAxis dataKey="category" tick={{fontSize: 14 }} />
            <PolarRadiusAxis angle={30} domain={[0, 'auto']} tick={false} axisLine={false} />
            
            <Radar
              dataKey="value"
              fill="var(--chart-1)"
              fillOpacity={0.7}
              stroke="var(--color-value)"
              strokeWidth={2}
            />
          </RadarChart>
        </ChartContainer>
      </CardContent>
      <CardFooter className="flex-col gap-2 text-sm">
        <div className="flex items-center gap-2 font-medium leading-none">
          Benchmark - PTS: 35, AST: 12, REB: 15, STL: 3, BLK: 3
        </div>
      </CardFooter>
    </Card>
  )
}