"use client"

import { useMemo } from "react"
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
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart"

// We have our stat benchmarks below, that we used to normalize a players skills to 0-100
// PTS: 35, AST: 12, REB: 15, STL: 3, BLK: 3
//^^ all per game. So lets say a the question is "What is Player X's skill breakdown for his 2022 season", and he averaged 30ppg, his points will be high, but then lets say he also averaged 2 rebounds a game, that stat will be very low
const MAX_DOMAIN = 100

const HEX_COLORS = [
  "#3b82f6", // Blue
  "#ef4444", // Red
  "#10b981", // Green
  "#f59e0b", // Orange
  "#8b5cf6", // Purple
]

interface CompareCategoricalBreakdownProps {
  data: any[]
  config: {
    statDisplayName?: string
    playerNames?: string[]
  }
}

export default function CompareCategoricalBreakdown({ data, config }: CompareCategoricalBreakdownProps) {
  if (!data || data.length === 0) return <div>No data found</div>

  const players = useMemo(() => {
    const allKeys = new Set<string>();
    data.forEach(row => {
      Object.keys(row).forEach(key => {
        if (key !== "category") allKeys.add(key);
      });
    });
    return Array.from(allKeys);
  }, [data]);

  const chartConfig = useMemo(() => {
    const configObj: ChartConfig = {};
    players.forEach((player, index) => {
      configObj[player] = {
        label: player,
        color: HEX_COLORS[index % HEX_COLORS.length],
      };
    });
    return configObj;
  }, [players]);

  return (
    <Card>
      <CardHeader className="items-center pb-4">
        <CardTitle>Skill Comparison</CardTitle>
        <CardDescription>
          Comparing {players.join(" vs ")}
        </CardDescription>
      </CardHeader>
      <CardContent className="pb-0">
        <ChartContainer
          config={chartConfig}
          className="mx-auto aspect-square max-h-[350px] w-full"
        >
          <RadarChart data={data} outerRadius={140}>
            {/* tooltip that shows values when highlighted for ease of access*/}
            <ChartTooltip cursor={false} content={<ChartTooltipContent indicator="line" />} />
            
            <PolarGrid className="fill-[--color-desktop] opacity-100" />
            
            <PolarAngleAxis dataKey="category" tick={{fontSize: 14 }} />
            
            <PolarRadiusAxis angle={30} domain={[0, 100]} tick={false} axisLine={false} />
            
            <ChartLegend content={<ChartLegendContent />} />

            {/* render a Radar for each player */}
            {players.map((player, index) => (
              <Radar
                key={player}
                name={player}
                dataKey={player}
                stroke={HEX_COLORS[index % HEX_COLORS.length]}
                fill={HEX_COLORS[index % HEX_COLORS.length]}
                fillOpacity={0.3} //transparent so you can see overlaps between players
              />
            ))}
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