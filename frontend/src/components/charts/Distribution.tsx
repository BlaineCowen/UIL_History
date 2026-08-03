"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { AXIS_PROPS, ChartFrame, GRID_PROPS, Legend, SERIES, TooltipShell } from "./ChartKit";
import { SCORE_LABELS } from "@/lib/format";
import type { Distribution as Row } from "@/lib/db";

export function Distribution({ data, total }: { data: Row[]; total: number }) {
  const legendItems = [
    { label: "Concert", color: SERIES[0], shape: "square" as const },
    { label: "Sight-reading", color: SERIES[1], shape: "square" as const },
  ];

  return (
    <ChartFrame
      title="Rating distribution"
      hint="How often each rating was awarded. 1 is the best possible score."
      legend={<Legend items={legendItems} />}
      height={260}
    >
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={data}
          margin={{ top: 6, right: 8, bottom: 0, left: -18 }}
          barGap={2}
        >
          <CartesianGrid {...GRID_PROPS} />
          <XAxis
            dataKey="score"
            {...AXIS_PROPS}
            tickFormatter={(v) => `${v} · ${SCORE_LABELS[v as number] ?? ""}`}
            interval={0}
            tickMargin={6}
          />
          <YAxis
            {...AXIS_PROPS}
            width={52}
            tickFormatter={(v) =>
              v >= 1000 ? `${Math.round((v as number) / 1000)}k` : String(v)
            }
          />
          <Tooltip
            cursor={{ fill: "color-mix(in srgb, var(--muted) 12%, transparent)" }}
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null;
              const row = payload[0].payload as Row;
              const share = (n: number) =>
                total ? ` (${((n / total) * 100).toFixed(1)}%)` : "";
              return (
                <TooltipShell
                  title={`${label} — ${SCORE_LABELS[Number(label)] ?? ""}`}
                  rows={[
                    {
                      label: "Concert",
                      value: row.concert.toLocaleString() + share(row.concert),
                      color: SERIES[0],
                    },
                    {
                      label: "Sight-reading",
                      value: row.sight.toLocaleString() + share(row.sight),
                      color: SERIES[1],
                    },
                  ]}
                />
              );
            }}
          />
          <Bar dataKey="concert" radius={[4, 4, 0, 0]} isAnimationActive={false}>
            {data.map((d) => (
              <Cell key={`c${d.score}`} fill={SERIES[0]} />
            ))}
          </Bar>
          <Bar dataKey="sight" radius={[4, 4, 0, 0]} isAnimationActive={false}>
            {data.map((d) => (
              <Cell key={`s${d.score}`} fill={SERIES[1]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
