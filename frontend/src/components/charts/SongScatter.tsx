"use client";

import { useRouter } from "next/navigation";
import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import { AXIS_PROPS, ChartFrame, GRID_PROPS, Legend, SERIES, TooltipShell } from "./ChartKit";

export type ScatterPoint = {
  code: string;
  title: string;
  composer: string;
  event_name: string;
  grade: number;
  performance_count: number;
  average_concert_score: number;
  average_sight_reading_score: number;
};

/** Three groups only -- the validated all-pairs palette caps at three slots. */
const GROUPS = ["Band", "Chorus", "Orchestra"] as const;
type Group = (typeof GROUPS)[number];

function groupOf(eventName: string): Group {
  const e = eventName.toLowerCase();
  if (e.includes("orchestra")) return "Orchestra";
  if (e.includes("chorus") || e.includes("madrigal")) return "Chorus";
  return "Band";
}

export function SongScatter({ points }: { points: ScatterPoint[] }) {
  const router = useRouter();

  const byGroup = new Map<Group, ScatterPoint[]>(GROUPS.map((g) => [g, []]));
  for (const p of points) byGroup.get(groupOf(p.event_name))!.push(p);
  const present = GROUPS.filter((g) => byGroup.get(g)!.length > 0);

  if (!points.length) {
    return (
      <ChartFrame title="Concert vs sight-reading" height={320}>
        <div
          className="h-full grid place-items-center text-sm"
          style={{ color: "var(--muted)" }}
        >
          No songs with enough performances to plot.
        </div>
      </ChartFrame>
    );
  }

  return (
    <ChartFrame
      title="Concert vs sight-reading average"
      hint="Each bubble is a song; size is how often it has been performed. Best scores sit top-left. Select a row in the table below for detail."
      legend={
        <Legend
          items={present.map((g, i) => ({ label: g, color: SERIES[i], shape: "dot" }))}
        />
      }
      height={340}
    >
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 8, right: 12, bottom: 4, left: -14 }}>
          <CartesianGrid {...GRID_PROPS} vertical />
          {/* Both axes reversed: 1 is best, so the strongest songs land top-left. */}
          <XAxis
            type="number"
            dataKey="average_concert_score"
            name="Concert"
            domain={[1, "dataMax"]}
            reversed
            {...AXIS_PROPS}
            tickFormatter={(v) => Number(v).toFixed(1)}
            label={{
              value: "Concert average",
              position: "insideBottom",
              offset: -2,
              fill: "var(--muted)",
              fontSize: 11,
            }}
          />
          <YAxis
            type="number"
            dataKey="average_sight_reading_score"
            name="Sight-reading"
            domain={[1, "dataMax"]}
            reversed
            width={44}
            {...AXIS_PROPS}
            tickFormatter={(v) => Number(v).toFixed(1)}
          />
          <ZAxis
            type="number"
            dataKey="performance_count"
            range={[16, 520]}
            name="Performances"
          />
          <Tooltip
            cursor={{ strokeDasharray: "3 3", stroke: "var(--axis)" }}
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const p = payload[0].payload as ScatterPoint;
              return (
                <TooltipShell
                  title={p.title}
                  rows={[
                    { label: "Composer", value: p.composer || "—" },
                    { label: "Concert", value: p.average_concert_score.toFixed(2) },
                    {
                      label: "Sight-reading",
                      value: p.average_sight_reading_score.toFixed(2),
                    },
                    {
                      label: "Performances",
                      value: p.performance_count.toLocaleString(),
                    },
                  ]}
                  footer={`${p.event_name} · Grade ${p.grade}`}
                />
              );
            }}
          />
          {present.map((g, i) => (
            <Scatter
              key={g}
              name={g}
              data={byGroup.get(g)}
              fill={SERIES[i]}
              fillOpacity={0.55}
              stroke="var(--surface)"
              strokeWidth={2}
              isAnimationActive={false}
              onClick={(p: unknown) => {
                const point = p as { code?: string };
                if (point?.code) router.push(`/pml/${encodeURIComponent(point.code)}`);
              }}
              className="cursor-pointer"
            />
          ))}
        </ScatterChart>
      </ResponsiveContainer>
    </ChartFrame>
  );
}
