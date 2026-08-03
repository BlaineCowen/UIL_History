"use client";

import type { ReactNode } from "react";

/**
 * Shared chart chrome. Colours come from CSS custom properties so light and
 * dark are one source of truth (see globals.css for the validation record).
 */

export const SERIES = ["var(--series-1)", "var(--series-2)", "var(--series-3)"];

export const AXIS_PROPS = {
  stroke: "var(--axis)",
  tick: { fill: "var(--muted)", fontSize: 11 },
  tickLine: false,
} as const;

export const GRID_PROPS = {
  stroke: "var(--grid)",
  strokeDasharray: "0",
  vertical: false,
} as const;

type TooltipRow = { label: string; value: string; color?: string };

export function TooltipShell({
  title,
  rows,
  footer,
}: {
  title: ReactNode;
  rows: TooltipRow[];
  footer?: ReactNode;
}) {
  return (
    <div
      className="rounded-lg border px-3 py-2 shadow-lg text-[12px]"
      style={{
        background: "var(--surface)",
        borderColor: "var(--border-strong)",
        color: "var(--ink)",
      }}
    >
      <div className="font-semibold mb-1.5">{title}</div>
      <div className="flex flex-col gap-1">
        {rows.map((row) => (
          <div key={row.label} className="flex items-center gap-2">
            {row.color && (
              <span
                aria-hidden
                className="h-2 w-2 rounded-full shrink-0"
                style={{ background: row.color }}
              />
            )}
            <span style={{ color: "var(--ink-2)" }}>{row.label}</span>
            <span className="tnum ml-auto font-medium">{row.value}</span>
          </div>
        ))}
      </div>
      {footer && (
        <div
          className="mt-1.5 pt-1.5 border-t text-[11px]"
          style={{ color: "var(--muted)" }}
        >
          {footer}
        </div>
      )}
    </div>
  );
}

/** Legend rendered as markup rather than Recharts' default, for consistent ink. */
export function Legend({
  items,
}: {
  items: { label: string; color: string; shape?: "line" | "dot" | "square" }[];
}) {
  return (
    <ul className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-[12px]">
      {items.map((item) => (
        <li key={item.label} className="flex items-center gap-1.5">
          <span
            aria-hidden
            className="shrink-0"
            style={{
              background: item.color,
              width: item.shape === "line" ? 14 : 9,
              height: item.shape === "line" ? 2 : 9,
              borderRadius: item.shape === "square" ? 2 : 999,
            }}
          />
          <span style={{ color: "var(--ink-2)" }}>{item.label}</span>
        </li>
      ))}
    </ul>
  );
}

export function ChartFrame({
  title,
  hint,
  legend,
  children,
  height = 260,
}: {
  title: string;
  hint?: string;
  legend?: ReactNode;
  children: ReactNode;
  height?: number;
}) {
  return (
    <div
      className="rounded-xl border p-4 sm:p-5"
      style={{ background: "var(--surface)" }}
    >
      <div className="flex flex-wrap items-start justify-between gap-3 mb-3">
        <div className="min-w-0">
          <h3 className="text-[15px] font-semibold tracking-tight">{title}</h3>
          {hint && (
            <p className="text-xs mt-0.5" style={{ color: "var(--muted)" }}>
              {hint}
            </p>
          )}
        </div>
        {legend}
      </div>
      <div style={{ height }}>{children}</div>
    </div>
  );
}
