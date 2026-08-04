"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { RotateCcw, SlidersHorizontal, X } from "lucide-react";
import { buildQuery } from "@/lib/params";
import { RangePair } from "@/components/RangeFilter";

type Option = { value: string; label: string; count?: number };

export type FiltersProps = {
  genEvents: string[];
  events: Option[];
  levels: Option[];
  conferences: Option[];
  classifications: Option[];
  yearBounds: { min: number; max: number };
  /** Only the unlisted /blaine route passes this. */
  showDirector?: boolean;
};

export function Filters(props: FiltersProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [pending, startTransition] = useTransition();
  const [open, setOpen] = useState(false);

  const genEvent = searchParams.get("event") ?? "";
  const subs = (searchParams.get("sub") ?? "").split(",").filter(Boolean);
  const confs = (searchParams.get("conf") ?? "").split(",").filter(Boolean);
  const level = searchParams.get("level") ?? "";
  const klass = searchParams.get("class") ?? "";
  const from = Number(searchParams.get("from") ?? props.yearBounds.min);
  const to = Number(searchParams.get("to") ?? props.yearBounds.max);

  function push(updates: Record<string, string | string[] | number | undefined | null>) {
    const qs = buildQuery(searchParams, updates);
    startTransition(() => router.replace(`${pathname}${qs}`, { scroll: false }));
  }

  const activeCount =
    (genEvent ? 1 : 0) +
    subs.length +
    confs.length +
    (level ? 1 : 0) +
    (klass ? 1 : 0) +
    (searchParams.get("school") ? 1 : 0) +
    (searchParams.get("song") ? 1 : 0) +
    (searchParams.get("composer") ? 1 : 0) +
    (props.showDirector && searchParams.get("director") ? 1 : 0) +
    (from !== props.yearBounds.min || to !== props.yearBounds.max ? 1 : 0);

  return (
    <div
      className="rounded-xl border"
      style={{ background: "var(--surface)", opacity: pending ? 0.72 : 1 }}
    >
      {/* Ensemble picker is always visible -- it is the entry point. */}
      <div className="p-4 sm:p-5 flex flex-wrap items-center gap-3">
        <div
          role="group"
          aria-label="Ensemble"
          className="flex w-full sm:inline-flex sm:w-auto rounded-lg border p-0.5"
          style={{ background: "var(--surface-2)" }}
        >
          {props.genEvents.map((g) => {
            const active = genEvent === g;
            return (
              <button
                key={g}
                type="button"
                onClick={() => push({ event: active ? undefined : g, sub: undefined })}
                aria-pressed={active}
                className="tap-sm flex-1 sm:flex-none rounded-[7px] px-3 sm:px-4 py-1.5 text-sm font-medium transition"
                style={{
                  background: active ? "var(--series-1)" : "transparent",
                  color: active ? "#fff" : "var(--ink-2)",
                }}
              >
                {g}
              </button>
            );
          })}
        </div>

        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          className="tap inline-flex items-center gap-2 rounded-lg border px-3.5 py-1.5 text-sm font-medium transition hover:border-[var(--muted)]"
          aria-expanded={open}
        >
          <SlidersHorizontal size={15} aria-hidden />
          Filters
          {activeCount > 0 && (
            <span
              className="tnum grid h-5 min-w-5 place-items-center rounded-full px-1 text-[11px] font-semibold text-white"
              style={{ background: "var(--series-1)" }}
            >
              {activeCount}
            </span>
          )}
        </button>

        {activeCount > 0 && (
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

      {open && (
        <div className="border-t p-4 sm:p-5 grid gap-5">
          {props.events.length > 1 && (
            <Field label="Event">
              <ChipGroup
                options={props.events}
                selected={subs}
                onToggle={(value) => {
                  const next = subs.includes(value)
                    ? subs.filter((s) => s !== value)
                    : [...subs, value];
                  push({ sub: next });
                }}
              />
            </Field>
          )}

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <Field label="School">
              <DebouncedInput
                placeholder="e.g. Berkner"
                value={searchParams.get("school") ?? ""}
                onCommit={(v) => push({ school: v || undefined })}
              />
            </Field>

            <Field label="Song title">
              <DebouncedInput
                placeholder="e.g. Magnificat"
                value={searchParams.get("song") ?? ""}
                onCommit={(v) => push({ song: v || undefined })}
              />
            </Field>

            <Field label="Composer">
              <DebouncedInput
                placeholder="e.g. Whitacre"
                value={searchParams.get("composer") ?? ""}
                onCommit={(v) => push({ composer: v || undefined })}
              />
            </Field>

            {props.showDirector && (
              <Field label="Director">
                <DebouncedInput
                  placeholder="e.g. Ledford"
                  value={searchParams.get("director") ?? ""}
                  onCommit={(v) => push({ director: v || undefined })}
                />
              </Field>
            )}

            <Field label="School level">
              <select
                className="field"
                value={level}
                onChange={(e) => push({ level: e.target.value || undefined, conf: undefined })}
              >
                <option value="">All levels</option>
                {props.levels.map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
            </Field>

            <Field label="Classification">
              <select
                className="field"
                value={klass}
                onChange={(e) => push({ class: e.target.value || undefined })}
              >
                <option value="">All classifications</option>
                {props.classifications.map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
            </Field>

            <RangePair
              label="Years"
              from={from}
              to={to}
              min={props.yearBounds.min}
              max={props.yearBounds.max}
              onCommit={(r) => push({ from: r.from, to: r.to })}
            />
          </div>

          {props.conferences.length > 0 && (
            <Field label="Conference">
              <ChipGroup
                options={props.conferences}
                selected={confs}
                onToggle={(value) => {
                  const next = confs.includes(value)
                    ? confs.filter((c) => c !== value)
                    : [...confs, value];
                  push({ conf: next });
                }}
              />
            </Field>
          )}
        </div>
      )}
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="grid gap-1.5">
      <span className="text-xs font-medium" style={{ color: "var(--ink-2)" }}>
        {label}
      </span>
      {children}
    </label>
  );
}

function ChipGroup({
  options,
  selected,
  onToggle,
}: {
  options: Option[];
  selected: string[];
  onToggle: (value: string) => void;
}) {
  return (
    <div className="flex flex-wrap gap-2">
      {options.map((o) => {
        const active = selected.includes(o.value);
        return (
          <button
            key={o.value}
            type="button"
            onClick={() => onToggle(o.value)}
            aria-pressed={active}
            className="tap-sm inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-[13px] transition"
            style={{
              background: active ? "var(--series-1)" : "var(--surface)",
              color: active ? "#fff" : "var(--ink-2)",
              borderColor: active ? "var(--series-1)" : "var(--border-strong)",
            }}
          >
            {o.label}
            {active && <X size={12} aria-hidden />}
          </button>
        );
      })}
    </div>
  );
}

/** Text input that only touches the URL once typing pauses. */
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

  // Adopt external changes (reset button, back/forward) without clobbering typing.
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
