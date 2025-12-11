"use client"

import { useMemo } from "react"
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { ChartConfig, ChartContainer, ChartLegend, ChartLegendContent, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart"

const HEX_COLORS = ["#3b82f6", "#ef4444", "#10b981", "#f59e0b", "#8b5cf6"]

interface CompareStatsProps {
  data: any[]
  config: {
    statDisplayName: string
    xAxisKey?: string 
    playerNames?: string[]
  }
}

export default function CompareStats({ data, config }: CompareStatsProps) {
  if (!data || data.length === 0) return <div>No data available</div>

  const xKey = config.xAxisKey || (data[0].game_date ? 'game_date' : 'season');

  const players = useMemo(() => {
    const allKeys = new Set<string>();
    data.forEach(row => {
      Object.keys(row).forEach(key => {
        if (key !== xKey && key !== "game_date" && key !== "season" && key !== "gameNumber") {
            allKeys.add(key);
        }
      });
    });
    return Array.from(allKeys);
  }, [data, xKey]);

  const chartConfig = useMemo(() => {
    const configObj: ChartConfig = {};
    players.forEach((player, index) => {
      configObj[player] = { label: player, color: HEX_COLORS[index % HEX_COLORS.length] };
    });
    return configObj;
  }, [players]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Comparison: {config.statDisplayName}</CardTitle>
        <CardDescription>By {xKey === 'game_date' ? 'Date' : 'Season'}</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="min-h-[400px] w-full">
          <BarChart accessibilityLayer data={data}>
            <CartesianGrid vertical={false} />
            <XAxis 
              dataKey={xKey} 
              tickLine={false} 
              tickMargin={10} 
              axisLine={false} 
              tickFormatter={(val) => {
                 if (String(val).includes('-') && String(val).length === 10) return val.slice(5); 
                 return val;
              }}
            />
            <YAxis tickLine={false} axisLine={false} />
            <ChartTooltip content={<ChartTooltipContent />} />
            <ChartLegend content={<ChartLegendContent />} />
            {players.map((player, index) => (
              <Bar
                key={player}
                dataKey={player}
                fill={HEX_COLORS[index % HEX_COLORS.length]} 
                radius={[4, 4, 0, 0]}
              />
            ))}
          </BarChart>
        </ChartContainer>
      </CardContent>
      <CardFooter className="flex-col items-start gap-2 text-sm">
        <div className="text-muted-foreground leading-none">
          Showing {config.statDisplayName}
        </div>
      </CardFooter>
    </Card>
  )
}