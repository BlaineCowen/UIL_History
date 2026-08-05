"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { RotateCcw } from "lucide-react";
import { buildQuery } from "@/lib/params";
import { RangePair, RangeSingle } from "@/components/RangeFilter";

export function SongFilters({
  events,
  gradeBounds,
}: {
  events: { value: string; label: string }[];
  gradeBounds: { min: number; max: number };
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [pending, startTransition] = useTransition();

  const gmin = Number(searchParams.get("gmin") ?? gradeBounds.min);
  const gmax = Number(searchParams.get("gmax") ?? gradeBounds.max);
  const sevent = searchParams.get("sevent") ?? "";
  const acc = searchParams.get("acc") ?? "";
  const minp = Number(searchParams.get("minp") ?? 0);
  const showDelisted = searchParams.get("delisted") === "1";
  const isChorus = sevent.toLowerCase().includes("chorus") || sevent.toLowerCase().includes("madrigal");

  function push(updates: Record<string, string | number | undefined>) {
    startTransition(() =>
      router.replace(`${pathname}${buildQuery(searchParams, updates)}`, { scroll: false }),
    );
  }

  const dirty = ["gmin", "gmax", "sevent", "acc", "minp", "q", "delisted"].some(
    (k) => searchParams.get(k),
  );

  return (
    <div
      className="rounded-xl border p-4 sm:p-5 grid gap-4"
      style={{ background: "var(--surface)", opacity: pending ? 0.72 : 1 }}
    >
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <label className="grid gap-1.5">
          <span className="text-xs font-medium" style={{ color: "var(--ink-2)" }}>
            Search title or composer
          </span>
          <DebouncedInput
            placeholder="e.g. Whitacre"
            value={searchParams.get("q") ?? ""}
            onCommit={(v) => push({ q: v || undefined })}
          />
        </label>

        <label className="grid gap-1.5">
          <span className="text-xs font-medium" style={{ color: "var(--ink-2)" }}>
            Event
          </span>
          <select
            className="field"
            value={sevent}
            onChange={(e) => push({ sevent: e.target.value || undefined, acc: undefined })}
          >
            <option value="">All events</option>
            {events.map((e) => (
              <option key={e.value} value={e.value}>
                {e.label}
              </option>
            ))}
          </select>
        </label>

        <RangePair
          label="Grade"
          from={gmin}
          to={gmax}
          min={gradeBounds.min}
          max={gradeBounds.max}
          fromLabel="Min"
          toLabel="Max"
          onCommit={(r) => push({ gmin: r.from, gmax: r.to })}
        />

        <RangeSingle
          label="Minimum performances"
          value={minp}
          min={0}
          max={200}
          step={5}
          onCommit={(v) => push({ minp: v || undefined })}
        />
      </div>

      <div className="flex flex-wrap items-center gap-3">
        {isChorus && (
          <div
            role="group"
            aria-label="Accompaniment"
            className="flex w-full sm:inline-flex sm:w-auto rounded-lg border p-0.5"
            style={{ background: "var(--surface-2)" }}
          >
            {[
              { value: "", label: "Both" },
              { value: "acappella", label: "A cappella" },
              { value: "accompanied", label: "Accompanied" },
            ].map((o) => {
              const active = acc === o.value;
              return (
                <button
                  key={o.value || "both"}
                  type="button"
                  onClick={() => push({ acc: o.value || undefined })}
                  aria-pressed={active}
                  className="tap-sm flex-1 sm:flex-none rounded-[7px] px-3 py-1.5 text-[13px] font-medium transition"
                  style={{
                    background: active ? "var(--series-1)" : "transparent",
                    color: active ? "#fff" : "var(--ink-2)",
                  }}
                >
                  {o.label}
                </button>
              );
            })}
          </div>
        )}

        <button
          type="button"
          onClick={() => push({ delisted: showDelisted ? undefined : "1" })}
          aria-pressed={showDelisted}
          className="tap-sm inline-flex items-center gap-1.5 rounded-full border px-3 text-[13px] transition"
          style={{
            background: showDelisted ? "var(--series-1)" : "var(--surface)",
            color: showDelisted ? "#fff" : "var(--ink-2)",
            borderColor: showDelisted ? "var(--series-1)" : "var(--border-strong)",
          }}
          title="Pieces removed from the PML keep their contest history"
        >
          Include delisted
        </button>

        {dirty && (
          <button
            type="button"
            onClick={() => startTransition(() => router.replace(pathname, { scroll: false }))}
            className="tap inline-flex items-center gap-1.5 px-1 text-sm transition hover:opacity-70"
            style={{ color: "var(--ink-2)" }}
          >
            <RotateCcw size={14} aria-hidden />
            Reset
          </button>
        )}
      </div>
    </div>
  );
}

function DebouncedInput({
  value,
  onCommit,
  placeholder,
  delay = 350,
}: {
  value: string;
  onCommit: (value: string) => void;
  placeholder?: string;
  delay?: number;
}) {
  const [local, setLocal] = useState(value);
  const committed = useRef(value);

  useEffect(() => {
    if (value !== committed.current) {
      committed.current = value;
      setLocal(value);
    }
  }, [value]);

  useEffect(() => {
    if (local === committed.current) return;
    const t = setTimeout(() => {
      committed.current = local;
      onCommit(local);
    }, delay);
    return () => clearTimeout(t);
  }, [local, delay, onCommit]);

  return (
    <input
      className="field"
      type="search"
      placeholder={placeholder}
      value={local}
      onChange={(e) => setLocal(e.target.value)}
    />
  );
}
