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

  const button =
    "tap inline-flex items-center justify-center rounded-lg border px-4 py-2 text-sm font-medium transition hover:border-[var(--muted)]";

  return (
    <div className="flex items-center justify-between gap-3 pt-4">
      <Link
        href={hrefFor(prev)}
        aria-disabled={page === 1}
        className={button}
        style={{
          background: "var(--surface)",
          opacity: page === 1 ? 0.45 : 1,
          pointerEvents: page === 1 ? "none" : undefined,
        }}
      >
        <span aria-hidden>←</span>
        <span className="ml-1.5">Prev</span>
      </Link>
      <span
        className="tnum text-[13px] sm:text-sm text-center"
        style={{ color: "var(--ink-2)" }}
      >
        Page {page.toLocaleString()} of {pageCount.toLocaleString()}
      </span>
      <Link
        href={hrefFor(next)}
        aria-disabled={page === pageCount}
        className={button}
        style={{
          background: "var(--surface)",
          opacity: page === pageCount ? 0.45 : 1,
          pointerEvents: page === pageCount ? "none" : undefined,
        }}
      >
        <span className="mr-1.5">Next</span>
        <span aria-hidden>→</span>
      </Link>
    </div>
  );
}

/**
 * Wide tables scroll inside this, so the page body never scrolls sideways.
 * Hidden below `sm` -- narrow screens get the DataList card view instead, so
 * nothing here has to be read through a 390px-wide window.
 */
export function TableScroll({ children }: { children: ReactNode }) {
  return (
    <div className="hscroll hidden sm:block -mx-4 sm:mx-0 overflow-x-auto">
      <div className="min-w-full px-4 sm:px-0">{children}</div>
    </div>
  );
}

/**
 * Sort control for the card views, which have no column headers to tap.
 * Hidden from `sm` up, where the sortable table headers take over.
 */
export function SortChips<K extends string>({
  columns,
  active,
  dir,
  hrefFor,
}: {
  columns: { key: K; label: string }[];
  active: K;
  dir: "asc" | "desc";
  hrefFor: (key: K) => string;
}) {
  return (
    <div className="sm:hidden -mt-1 mb-3 flex flex-wrap gap-1.5">
      <span
        className="tap-sm inline-flex items-center text-[11px] uppercase tracking-wide"
        style={{ color: "var(--muted)" }}
      >
        Sort
      </span>
      {columns.map((c) => {
        const on = c.key === active;
        return (
          <Link
            key={c.key}
            href={hrefFor(c.key)}
            aria-current={on ? "true" : undefined}
            className="tap-sm inline-flex items-center gap-1 rounded-full border px-3 text-[13px] transition"
            style={{
              background: on ? "var(--series-1)" : "var(--surface)",
              color: on ? "#fff" : "var(--ink-2)",
              borderColor: on ? "var(--series-1)" : "var(--border-strong)",
            }}
          >
            {c.label}
            {on && <span aria-hidden>{dir === "asc" ? "↑" : "↓"}</span>}
          </Link>
        );
      })}
    </div>
  );
}

/** The phone-sized counterpart to a table: one card per row, stacked. */
export function DataList({ children }: { children: ReactNode }) {
  return <ul className="grid gap-2 sm:hidden">{children}</ul>;
}

export function DataCard({
  children,
  href,
}: {
  children: ReactNode;
  href?: string;
}) {
  const body = (
    <div
      className="rounded-xl border p-3.5 grid gap-2"
      style={{ background: "var(--surface-2)" }}
    >
      {children}
    </div>
  );
  return (
    <li>
      {href ? (
        <Link href={href} className="block transition active:opacity-60">
          {body}
        </Link>
      ) : (
        body
      )}
    </li>
  );
}

/**
 * A labelled rating for the card views. Two bare badges side by side would be
 * ambiguous; the table doesn't need this because it has column headers.
 */
export function RatingCell({
  label,
  score,
}: {
  label: string;
  score: number | null | undefined;
}) {
  return (
    <span className="grid justify-items-center gap-1">
      <span
        className="text-[10px] font-medium uppercase tracking-wide"
        style={{ color: "var(--muted)" }}
      >
        {label}
      </span>
      <ScoreBadge score={score} />
    </span>
  );
}

/**
 * The three individual panel scores followed by the awarded final, e.g.
 * "1 1 2 → 1". The final is the stored value, not the median of the three:
 * they agree on all but 25 of 159,451 rows, and those 25 are data anomalies
 * rather than a rule worth reimplementing.
 */
export function JudgeScores({
  scores,
  final,
}: {
  scores: (number | null | undefined)[];
  final: number | null | undefined;
}) {
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
      <span className="tnum inline-flex gap-1 text-[12px]">
        {scores.map((s, i) => (
          <span
            key={i}
            className="w-3 text-center"
            style={{ color: "var(--ink-2)" }}
          >
            {s ?? "—"}
          </span>
        ))}
      </span>
      <span aria-hidden style={{ color: "var(--muted)" }}>
        →
      </span>
      <ScoreBadge score={final} />
    </span>
  );
}

/** A label/value line inside a DataCard. */
export function CardRow({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="flex items-baseline justify-between gap-3 text-[13px]">
      <span className="shrink-0" style={{ color: "var(--muted)" }}>
        {label}
      </span>
      <span className="text-right min-w-0">{children}</span>
    </div>
  );
}
