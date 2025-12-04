"use client"

import { Bar, BarChart, XAxis, YAxis, CartesianGrid, LabelList } from "recharts"
import {
  Card,
  CardContent,
  CardDescription,
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
    label: "Value",
    color: "hsl(var(--chart-1))",
  },
} satisfies ChartConfig

interface LeaderboardProps {
  data: any[]
  config: {
    statDisplayName: string
    timeFrame?: string
  }
}

export default function Leaderboard({ data, config }: LeaderboardProps) {
  if (!data || data.length === 0) return <div>No data available</div>

  const statKey = Object.keys(data[0]).find(
    k => !['rank', 'player_name', 'team_abbreviation', 'full_name', 'season'].includes(k)
  ) || 'stat';

  // 2. Names for Y-Axis, if too long truncate/shorten
  const formattedData = data.map(d => ({
    ...d,
    name: d.player_name || d.full_name || "Unknown",
    [statKey]: Number(d[statKey])
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle>League Leaders: {config.statDisplayName}</CardTitle>
        <CardDescription>{config.timeFrame || "Top Performers"}</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="min-h-[400px] w-full">
          <BarChart
            accessibilityLayer
            data={formattedData}
            layout="vertical"
            margin={{ left: 0, right: 40 }}
          >
            <CartesianGrid horizontal={false} />
            
            <YAxis
              dataKey="name"
              type="category"
              tickLine={false}
              tickMargin={10}
              axisLine={false}
              width={120}
              tick={{ fontSize: 12 }}
            />
            
            <XAxis dataKey={statKey} type="number" hide />
            
            <ChartTooltip
              cursor={false}
              content={<ChartTooltipContent hideLabel />}
            />
            
            <Bar dataKey={statKey} fill="var(--chart-1)" radius={5}>
              <LabelList 
                dataKey={statKey} 
                position="right" 
                formatter={(val: number) => val.toLocaleString()}
                className="fill-foreground text-xs"
              />
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  )
}