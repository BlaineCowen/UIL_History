"use client";

/**
 * One labelled slider. Range filters pair two of these; on a phone they stack
 * so each gets the full width, because a half-width slider with a 24px thumb
 * leaves too little travel to land on a specific value.
 */
export function RangeRow({
  label,
  value,
  min,
  max,
  step,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step?: number;
  onChange: (value: number) => void;
}) {
  return (
    <span className="flex items-center gap-2">
      <span
        className="tnum w-9 shrink-0 text-[11px] tabular-nums"
        style={{ color: "var(--muted)" }}
      >
        {label}
      </span>
      <input
        type="range"
        aria-label={`${label} — ${value}`}
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
      />
    </span>
  );
}
