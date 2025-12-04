"use client"

import React from 'react';
import { Area, AreaChart, CartesianGrid, XAxis, YAxis, ResponsiveContainer } from 'recharts';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart";

interface SinglePlayerStatProps {
  data: any[];
  statKey: string;
  playerName: string;
  xAxisKey?: string;
  timeFrame?: string;
  statDisplayName?: string;
}

export default function SinglePlayerStat({
  data,
  statKey,
  playerName,
  xAxisKey = 'season',
  timeFrame,
  statDisplayName = "Stat Value",
}: SinglePlayerStatProps) {

  const chartConfig = {
    [statKey]: {
      label: statDisplayName,
      color: "hsl(var(--chart-1))",
    },
  } satisfies ChartConfig;

  const formatXAxis = (tickItem: string) => {
    if (typeof tickItem === 'string' && tickItem.includes('-') && tickItem.length >= 10) {
      try {
        const date = new Date(tickItem);
        return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      } catch (e) { return tickItem; }
    }
    return tickItem;
  };

  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle>{playerName}: {statDisplayName}</CardTitle>
        <CardDescription>{timeFrame || "Trend Analysis"}</CardDescription>
      </CardHeader>
      
      <CardContent>
        <ChartContainer config={chartConfig} className="min-h-[350px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart 
              data={data}
              margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              
              <XAxis 
                dataKey={xAxisKey} 
                tickLine={false} 
                axisLine={false} 
                tickMargin={10} 
                tickFormatter={formatXAxis} 
              />
              
              <YAxis 
                tickLine={false} 
                axisLine={false} 
                tickMargin={10}
                domain={['auto', 'auto']} 
              />
              
              <ChartTooltip 
                content={
                  <ChartTooltipContent 
                    indicator="line"
                    labelFormatter={(label) => formatXAxis(String(label))}
                  />
                } 
              />
              
              <Area
                type="monotone"
                dataKey={statKey}
                fill="var(--chart-1)"
                fillOpacity={0.2}
                stroke="var(--chart-1)"
                strokeWidth={3}
              />
            </AreaChart>
          </ResponsiveContainer>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}