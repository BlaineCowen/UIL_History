"use client";

import { useEffect, useRef, useState } from "react";

/**
 * Range filters that move instantly and only touch the URL once the drag
 * settles.
 *
 * Driving the thumb's `value` straight from searchParams meant every tick of a
 * drag fired a router.replace and the handle could not move until the server
 * answered -- so the slider felt stuck. Local state owns the handle; the URL
 * is written once the user pauses.
 */

const DELAY = 180;

/**
 * Debounce `value` upward, and accept changes that arrive from outside.
 *
 * The subtlety is telling those two apart. Each commit rewrites the URL, which
 * comes back as a prop a render or two later. If the user has kept dragging in
 * the meantime, that late echo carries a value they have already moved past --
 * adopting it would visibly yank the handle backwards. So every committed
 * value is remembered, and an incoming value matching one of them is treated
 * as our own echo and ignored. Anything else is a genuine outside change
 * (Reset, browser back) and is adopted.
 */
function useDebouncedCommit<T>(
  value: T,
  external: T,
  onCommit: (value: T) => void,
  eq: (a: T, b: T) => boolean,
  reset: (v: T) => void,
) {
  const committed = useRef(external);
  const previous = useRef(external);
  /** Values we have sent whose echo has not come back yet. */
  const sent = useRef<T[]>([]);
  // Held in a ref so a fresh inline callback each render doesn't restart the
  // timer. Written in an effect, never during render.
  const commit = useRef(onCommit);
  useEffect(() => {
    commit.current = onCommit;
  });

  useEffect(() => {
    if (eq(external, previous.current)) return;
    previous.current = external;

    const i = sent.current.findIndex((v) => eq(v, external));
    if (i >= 0) {
      // Our own echo. Drop it and anything older that got batched with it.
      sent.current = sent.current.slice(i + 1);
      return;
    }

    committed.current = external;
    sent.current = [];
    reset(external);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [external]);

  useEffect(() => {
    if (eq(value, committed.current)) return;
    const t = setTimeout(() => {
      committed.current = value;
      sent.current = [...sent.current, value];
      commit.current(value);
    }, DELAY);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);
}

function Slider({
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
        className="w-9 shrink-0 text-[11px]"
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

type Pair = { from: number; to: number };

const pairEq = (a: Pair, b: Pair) => a.from === b.from && a.to === b.to;

/** Two bounds of one range. Stacks on phones so each slider gets full width. */
export function RangePair({
  label,
  from,
  to,
  min,
  max,
  step,
  fromLabel = "From",
  toLabel = "To",
  onCommit,
}: {
  label: string;
  from: number;
  to: number;
  min: number;
  max: number;
  step?: number;
  fromLabel?: string;
  toLabel?: string;
  onCommit: (range: Pair) => void;
}) {
  const [range, setRange] = useState<Pair>({ from, to });
  const external = { from, to };

  useDebouncedCommit(range, external, onCommit, pairEq, setRange);

  return (
    <div className="grid gap-1.5">
      {/* Reads from local state, so the caption updates as the thumb moves. */}
      <span className="text-xs font-medium" style={{ color: "var(--ink-2)" }}>
        {label} — {range.from} to {range.to}
      </span>
      <div className="grid gap-1 sm:grid-cols-2 sm:gap-3 sm:items-center">
        <Slider
          label={fromLabel}
          value={range.from}
          min={min}
          max={max}
          step={step}
          onChange={(v) =>
            setRange((r) => ({ ...r, from: Math.min(v, r.to) }))
          }
        />
        <Slider
          label={toLabel}
          value={range.to}
          min={min}
          max={max}
          step={step}
          onChange={(v) => setRange((r) => ({ ...r, to: Math.max(v, r.from) }))}
        />
      </div>
    </div>
  );
}

const numEq = (a: number, b: number) => a === b;

/** One bound, same debounce behaviour. */
export function RangeSingle({
  label,
  value,
  min,
  max,
  step,
  onCommit,
  format = String,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step?: number;
  onCommit: (value: number) => void;
  format?: (value: number) => string;
}) {
  const [local, setLocal] = useState(value);

  useDebouncedCommit(local, value, onCommit, numEq, setLocal);

  return (
    <div className="grid gap-1.5">
      <span className="text-xs font-medium" style={{ color: "var(--ink-2)" }}>
        {label} — {format(local)}
      </span>
      <input
        type="range"
        aria-label={`${label} — ${format(local)}`}
        min={min}
        max={max}
        step={step}
        value={local}
        onChange={(e) => setLocal(Number(e.target.value))}
      />
    </div>
  );
}
