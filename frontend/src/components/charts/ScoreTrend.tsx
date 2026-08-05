"use client";

import {
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
import type { YearPoint } from "@/lib/db";

type Props = {
  title: string;
  hint?: string;
  metric: "concert" | "sight";
  selected: YearPoint[];
  baseline: YearPoint[];
  baselineLabel: string;
  /** When no filter is narrowing the set, the two lines would be identical. */
  showBaseline: boolean;
};

export function ScoreTrend({
  title,
  hint,
  metric,
  selected,
  baseline,
  baselineLabel,
  showBaseline,
}: Props) {
  const byYear = new Map<number, { year: number; selected?: number; all?: number; n?: number }>();
  for (const p of baseline) {
    byYear.set(p.year, { year: p.year, all: p[metric] });
  }
  for (const p of selected) {
    const row = byYear.get(p.year) ?? { year: p.year };
    row.selected = p[metric];
    row.n = p.n;
    byYear.set(p.year, row);
  }
  const data = [...byYear.values()].sort((a, b) => a.year - b.year);

  // Fit the axis to both plotted series, so the two charts on the page do not
  // silently use different scales when only one is narrow.
  const axis = ratingAxis(data.flatMap((d) => [d.selected, showBaseline ? d.all : null]));

  const legendItems = [
    { label: "Selected", color: SERIES[0], shape: "line" as const },
    ...(showBaseline
      ? [{ label: baselineLabel, color: SERIES[1], shape: "line" as const }]
      : []),
  ];

  if (!data.length) {
    return (
      <ChartFrame title={title} hint={hint}>
        <div
          className="h-full grid place-items-center text-sm"
          style={{ color: "var(--muted)" }}
        >
          No data for this selection.
        </div>
      </ChartFrame>
    );
  }

  return (
    <ChartFrame
      title={title}
      hint={hint}
      legend={legendItems.length > 1 ? <Legend items={legendItems} /> : undefined}
    >
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 6, right: 8, bottom: 0, left: -18 }}>
          <CartesianGrid {...GRID_PROPS} />
          <XAxis dataKey="year" {...AXIS_PROPS} minTickGap={18} />
          {/* 1 is the best possible rating, so the axis runs 1 at the top. */}
          <YAxis
            {...AXIS_PROPS}
            reversed
            domain={axis.domain}
            ticks={axis.ticks}
            width={44}
          />
          <Tooltip
            cursor={{ stroke: "var(--axis)", strokeWidth: 1 }}
            content={({ active, payload, label }) => {
              if (!active || !payload?.length) return null;
              const row = payload[0].payload as {
                selected?: number;
                all?: number;
                n?: number;
              };
              const rows = [];
              if (row.selected != null)
                rows.push({
                  label: "Selected",
                  value: row.selected.toFixed(2),
                  color: SERIES[0],
                });
              if (showBaseline && row.all != null)
                rows.push({
                  label: baselineLabel,
                  value: row.all.toFixed(2),
                  color: SERIES[1],
                });
              return (
                <TooltipShell
                  title={String(label)}
                  rows={rows}
                  footer={row.n ? `${row.n.toLocaleString()} entries` : undefined}
                />
              );
            }}
          />
          {showBaseline && (
            <Line
              type="monotone"
              dataKey="all"
              stroke={SERIES[1]}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }}
              isAnimationActive={false}
              connectNulls
            />
          )}
          <Line
            type="monotone"
            dataKey="selected"
            stroke={SERIES[0]}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, strokeWidth: 2, stroke: "var(--surface)" }}
            isAnimationActive={false}
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
