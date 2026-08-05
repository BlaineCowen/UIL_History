"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  AXIS_PROPS,
  ChartFrame,
  GRID_PROPS,
  Legend,
  ratingAxis,
  SERIES,
  TooltipShell,
} from "./ChartKit";

type Row = { year: number; performances: number; concert: number; sight: number };

/**
 * Deliberately two charts rather than one with two y-axes. Performance count
 * and average rating are different scales; overlaying them on a shared plot
 * invents crossings that carry no meaning.
 */
export function SongYearly({ data }: { data: Row[] }) {
  if (!data.length) return null;

  // Both series share one axis, so fit to whichever reaches further.
  const axis = ratingAxis(data.flatMap((d) => [d.concert, d.sight]));

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <ChartFrame
        title="Performances per year"
        hint="How often this piece was programmed."
        heightClass="h-[190px] sm:h-[220px]"
      >
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 6, right: 8, bottom: 0, left: -20 }}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis dataKey="year" {...AXIS_PROPS} minTickGap={18} />
            <YAxis {...AXIS_PROPS} width={40} allowDecimals={false} />
            <Tooltip
              cursor={{ fill: "color-mix(in srgb, var(--muted) 12%, transparent)" }}
              content={({ active, payload, label }) => {
                if (!active || !payload?.length) return null;
                const row = payload[0].payload as Row;
                return (
                  <TooltipShell
                    title={String(label)}
                    rows={[
                      {
                        label: "Performances",
                        value: row.performances.toLocaleString(),
                        color: SERIES[0],
                      },
                    ]}
                  />
                );
              }}
            />
            <Bar
              dataKey="performances"
              fill={SERIES[0]}
              radius={[4, 4, 0, 0]}
              isAnimationActive={false}
            />
          </BarChart>
        </ResponsiveContainer>
      </ChartFrame>

      <ChartFrame
        title="Average rating per year"
        hint="1 is the best possible score, so lower is better."
        legend={
          <Legend
            items={[
              { label: "Concert", color: SERIES[0], shape: "line" },
              { label: "Sight-reading", color: SERIES[1], shape: "line" },
            ]}
          />
        }
        heightClass="h-[190px] sm:h-[220px]"
      >
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 6, right: 8, bottom: 0, left: -20 }}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis dataKey="year" {...AXIS_PROPS} minTickGap={18} />
            <YAxis
              {...AXIS_PROPS}
              reversed
              domain={axis.domain}
              ticks={axis.ticks}
              width={40}
            />
            <Tooltip
              cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
              content={({ active, payload, label }) => {
                if (!active || !payload?.length) return null;
                const row = payload[0].payload as Row;
                return (
                  <TooltipShell
                    title={String(label)}
                    rows={[
                      {
                        label: "Concert",
                        value: row.concert?.toFixed(2) ?? "—",
                        color: SERIES[0],
                      },
                      {
                        label: "Sight-reading",
                        value: row.sight?.toFixed(2) ?? "—",
                        color: SERIES[1],
                      },
                    ]}
                    footer={`${row.performances.toLocaleString()} performances`}
                  />
                );
              }}
            />
            <Line
              type="monotone"
              dataKey="concert"
              stroke={SERIES[0]}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }}
              isAnimationActive={false}
              connectNulls
            />
            <Line
              type="monotone"
              dataKey="sight"
              stroke={SERIES[1]}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }}
              isAnimationActive={false}
              connectNulls
            />
          </LineChart>
        </ResponsiveContainer>
      </ChartFrame>
    </div>
  );
}
