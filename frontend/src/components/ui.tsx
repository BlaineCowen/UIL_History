import Link from "next/link";
import type { ReactNode } from "react";

export function Card({
  children,
  className = "",
  padded = true,
}: {
  children: ReactNode;
  className?: string;
  padded?: boolean;
}) {
  return (
    <section
      className={`rounded-xl border ${padded ? "p-4 sm:p-5" : ""} ${className}`}
      style={{ background: "var(--surface)" }}
    >
      {children}
    </section>
  );
}

export function SectionTitle({
  title,
  hint,
  right,
}: {
  title: string;
  hint?: string;
  right?: ReactNode;
}) {
  return (
    <div className="flex items-start justify-between gap-4 mb-4">
      <div className="min-w-0">
        <h2 className="text-[15px] font-semibold tracking-tight">{title}</h2>
        {hint && (
          <p className="text-xs mt-0.5" style={{ color: "var(--muted)" }}>
            {hint}
          </p>
        )}
      </div>
      {right && <div className="shrink-0">{right}</div>}
    </div>
  );
}

/** A headline number. Values use proportional figures; only tables get tabular. */
export function StatTile({
  label,
  value,
  sub,
  accent,
}: {
  label: string;
  value: string;
  sub?: string;
  accent?: string;
}) {
  return (
    <div
      className="rounded-xl border p-4 flex flex-col gap-1"
      style={{ background: "var(--surface)" }}
    >
      <span
        className="text-[11px] font-medium uppercase tracking-wide"
        style={{ color: "var(--muted)" }}
      >
        {label}
      </span>
      <span
        className="text-2xl sm:text-[28px] font-semibold leading-none"
        style={{ color: accent ?? "var(--ink)" }}
      >
        {value}
      </span>
      {sub && (
        <span className="text-xs" style={{ color: "var(--ink-2)" }}>
          {sub}
        </span>
      )}
    </div>
  );
}

export function EmptyState({
  title,
  hint,
}: {
  title: string;
  hint?: string;
}) {
  return (
    <div
      className="rounded-xl border border-dashed p-10 text-center"
      style={{ background: "var(--surface)" }}
    >
      <p className="font-medium">{title}</p>
      {hint && (
        <p className="text-sm mt-1" style={{ color: "var(--ink-2)" }}>
          {hint}
        </p>
      )}
    </div>
  );
}

/** A 1-5 UIL rating. Colour is never the only cue -- the number is always shown. */
export function ScoreBadge({ score }: { score: number | null | undefined }) {
  if (score == null) return <span style={{ color: "var(--muted)" }}>—</span>;
  const rounded = Math.round(score);
  const tone: Record<number, string> = {
    1: "var(--seq-550)",
    2: "var(--seq-400)",
    3: "var(--seq-250)",
    4: "var(--seq-100)",
    5: "var(--surface-2)",
  };
  const ink = rounded <= 2 ? "#ffffff" : "var(--ink)";
  return (
    <span
      className="tnum inline-grid h-6 w-6 place-items-center rounded-md text-[12px] font-semibold"
      style={{ background: tone[rounded] ?? "var(--surface-2)", color: ink }}
    >
      {rounded}
    </span>
  );
}

export function Pagination({
  page,
  pageCount,
  hrefFor,
}: {
  page: number;
  pageCount: number;
  hrefFor: (page: number) => string;
}) {
  if (pageCount <= 1) return null;
  const prev = Math.max(1, page - 1);
  const next = Math.min(pageCount, page + 1);

  return (
    <div className="flex items-center justify-between gap-3 pt-3">
      <Link
        href={hrefFor(prev)}
        aria-disabled={page === 1}
        className="rounded-lg border px-3 py-1.5 text-sm transition hover:border-[var(--muted)]"
        style={{
          background: "var(--surface)",
          opacity: page === 1 ? 0.45 : 1,
          pointerEvents: page === 1 ? "none" : undefined,
        }}
      >
        ← Previous
      </Link>
      <span className="tnum text-sm" style={{ color: "var(--ink-2)" }}>
        Page {page.toLocaleString()} of {pageCount.toLocaleString()}
      </span>
      <Link
        href={hrefFor(next)}
        aria-disabled={page === pageCount}
        className="rounded-lg border px-3 py-1.5 text-sm transition hover:border-[var(--muted)]"
        style={{
          background: "var(--surface)",
          opacity: page === pageCount ? 0.45 : 1,
          pointerEvents: page === pageCount ? "none" : undefined,
        }}
      >
        Next →
      </Link>
    </div>
  );
}

/** Wide tables scroll inside this, so the page body never scrolls sideways. */
export function TableScroll({ children }: { children: ReactNode }) {
  return (
    <div className="-mx-4 sm:mx-0 overflow-x-auto">
      <div className="min-w-full px-4 sm:px-0">{children}</div>
    </div>
  );
}
