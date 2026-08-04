import Link from "next/link";
import type { Metadata } from "next";
import {
  countSongs,
  getSongEvents,
  getSongScatter,
  getSongs,
  type SongSort,
} from "@/lib/db";
import {
  buildQuery,
  parsePage,
  parseSongFilters,
  parseSongSort,
  type SearchParams,
} from "@/lib/params";
import { formatNumber, formatScore } from "@/lib/format";
import { SongFilters } from "@/components/SongFilters";
import { SongScatter } from "@/components/charts/SongScatter";
import {
  Card,
  DataCard,
  DataList,
  EmptyState,
  Pagination,
  SectionTitle,
  TableScroll,
} from "@/components/ui";

export const metadata: Metadata = {
  title: "Prescribed Music List",
  description:
    "Browse the Texas UIL Prescribed Music List with performance counts and contest averages for every piece.",
};

const PAGE_SIZE = 50;
const GRADE_BOUNDS = { min: 1, max: 6 };

const COLUMNS: { key: SongSort; label: string; numeric?: boolean }[] = [
  { key: "title", label: "Title" },
  { key: "grade", label: "Grade", numeric: true },
  { key: "performance_count", label: "Performances", numeric: true },
  { key: "song_score", label: "Song score", numeric: true },
];

export default async function PmlPage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>;
}) {
  const sp = await searchParams;
  const filters = parseSongFilters(sp);
  const { sort, dir } = parseSongSort(sp);
  const page = parsePage(sp);

  const events = getSongEvents();
  const total = countSongs(filters);
  const rows = getSongs(filters, sort, dir, PAGE_SIZE, (page - 1) * PAGE_SIZE);
  const scatter = getSongScatter(filters, Math.max(10, filters.minPerformances ?? 0));
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));

  function sortHref(key: SongSort) {
    const nextDir = sort === key && dir === "desc" ? "asc" : "desc";
    return `/pml${buildQuery(sp, { sort: key, dir: nextDir, page: undefined })}`;
  }

  return (
    <div className="grid gap-6">
      <header className="grid gap-2">
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          Prescribed Music List
        </h1>
        <p className="text-sm max-w-2xl" style={{ color: "var(--ink-2)" }}>
          Every ensemble piece on the UIL PML, with how often it has been performed
          and how those performances scored. Select a title for its full history.
        </p>
      </header>

      <SongFilters
        events={events.map((e) => ({ value: e.event_name, label: e.event_name }))}
        gradeBounds={GRADE_BOUNDS}
      />

      {total === 0 ? (
        <EmptyState
          title="No songs match these filters"
          hint="Try clearing the search box or lowering the minimum performance count."
        />
      ) : (
        <>
          <SongScatter points={scatter} />

          <Card padded={false}>
            <div className="p-4 sm:p-5 pb-0">
              <SectionTitle
                title="Songs"
                hint={`${formatNumber(total)} matching · sorted by ${
                  COLUMNS.find((c) => c.key === sort)?.label ?? sort
                } (${dir === "desc" ? "high to low" : "low to high"})`}
              />
              {/* The card view has no column headers to click, so sorting
                  gets its own control below sm. */}
              <div className="sm:hidden -mt-1 mb-3 flex flex-wrap gap-1.5">
                {COLUMNS.map((c) => {
                  const active = sort === c.key;
                  return (
                    <Link
                      key={c.key}
                      href={sortHref(c.key)}
                      aria-current={active ? "true" : undefined}
                      className="tap-sm inline-flex items-center gap-1 rounded-full border px-3 text-[13px] transition"
                      style={{
                        background: active ? "var(--series-1)" : "var(--surface)",
                        color: active ? "#fff" : "var(--ink-2)",
                        borderColor: active ? "var(--series-1)" : "var(--border-strong)",
                      }}
                    >
                      {c.label}
                      {active && <span aria-hidden>{dir === "asc" ? "↑" : "↓"}</span>}
                    </Link>
                  );
                })}
              </div>
            </div>
            <div className="px-4 sm:px-5 pb-4 sm:pb-5">
              <TableScroll>
                <table className="w-full text-sm border-separate border-spacing-0">
                  <thead>
                    <tr className="text-left">
                      {COLUMNS.map((c) => {
                        const active = sort === c.key;
                        return (
                          <th
                            key={c.key}
                            className="whitespace-nowrap border-b py-2 pr-4 text-[11px] font-semibold uppercase tracking-wide"
                            style={{ color: active ? "var(--ink)" : "var(--muted)" }}
                          >
                            <Link
                              href={sortHref(c.key)}
                              className="inline-flex items-center gap-1 hover:opacity-70"
                            >
                              {c.label}
                              <span aria-hidden style={{ opacity: active ? 1 : 0.25 }}>
                                {active && dir === "asc" ? "↑" : "↓"}
                              </span>
                            </Link>
                          </th>
                        );
                      })}
                      {["Event", "Concert avg", "SR avg"].map((h) => (
                        <th
                          key={h}
                          className="whitespace-nowrap border-b py-2 pr-4 text-[11px] font-semibold uppercase tracking-wide"
                          style={{ color: "var(--muted)" }}
                        >
                          {h}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((s) => (
                      <tr key={s.code}>
                        <td className="border-b py-2.5 pr-4 min-w-[240px]">
                          <Link
                            href={`/pml/${encodeURIComponent(s.code)}`}
                            className="font-medium underline decoration-transparent hover:decoration-inherit underline-offset-2 transition"
                          >
                            {s.title}
                          </Link>
                          <div className="text-[12px]" style={{ color: "var(--muted)" }}>
                            {s.composer}
                            {s.arranger ? ` · arr. ${s.arranger}` : ""}
                          </div>
                        </td>
                        <td className="tnum border-b py-2.5 pr-4">{s.grade}</td>
                        <td className="tnum border-b py-2.5 pr-4">
                          {formatNumber(s.performance_count)}
                        </td>
                        <td className="tnum border-b py-2.5 pr-4">
                          <SongScoreBar value={s.song_score} />
                        </td>
                        <td
                          className="whitespace-nowrap border-b py-2.5 pr-4"
                          style={{ color: "var(--ink-2)" }}
                        >
                          {s.event_name}
                        </td>
                        <td className="tnum border-b py-2.5 pr-4">
                          {formatScore(s.average_concert_score)}
                        </td>
                        <td className="tnum border-b py-2.5 pr-4">
                          {formatScore(s.average_sight_reading_score)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </TableScroll>

              <DataList>
                {rows.map((s) => (
                  <DataCard key={s.code} href={`/pml/${encodeURIComponent(s.code)}`}>
                    <div>
                      <p className="font-medium leading-snug">{s.title}</p>
                      <p
                        className="text-[12px] mt-0.5"
                        style={{ color: "var(--muted)" }}
                      >
                        {s.composer}
                        {s.arranger ? ` · arr. ${s.arranger}` : ""}
                      </p>
                    </div>
                    <p className="text-[12px]" style={{ color: "var(--ink-2)" }}>
                      {s.event_name} · Grade {s.grade}
                    </p>
                    <div
                      className="grid grid-cols-3 gap-2 pt-1.5 border-t text-[13px]"
                      style={{ borderColor: "var(--border)" }}
                    >
                      <MiniStat
                        label="Performances"
                        value={formatNumber(s.performance_count)}
                      />
                      <MiniStat
                        label="Concert"
                        value={formatScore(s.average_concert_score)}
                      />
                      <MiniStat
                        label="Song score"
                        value={s.song_score == null ? "—" : s.song_score.toFixed(1)}
                      />
                    </div>
                  </DataCard>
                ))}
              </DataList>

              <Pagination
                page={page}
                pageCount={pageCount}
                hrefFor={(p) => `/pml${buildQuery(sp, { page: p === 1 ? undefined : p })}`}
              />
            </div>
          </Card>
        </>
      )}
    </div>
  );
}

/** A labelled figure in the phone card view's footer strip. */
function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <span className="grid gap-0.5">
      <span className="text-[10px] uppercase tracking-wide" style={{ color: "var(--muted)" }}>
        {label}
      </span>
      <span className="tnum font-medium">{value}</span>
    </span>
  );
}

/** Song score is 0-100; the bar is a secondary encoding beside the number. */
function SongScoreBar({ value }: { value: number | null }) {
  if (value == null) return <span style={{ color: "var(--muted)" }}>—</span>;
  const width = Math.max(2, Math.min(100, value));
  return (
    <span className="inline-flex items-center gap-2">
      <span className="w-10 text-right">{value.toFixed(1)}</span>
      <span
        aria-hidden
        className="hidden sm:block h-1.5 w-16 rounded-full overflow-hidden"
        style={{ background: "var(--surface-2)" }}
      >
        <span
          className="block h-full rounded-full"
          style={{ width: `${width}%`, background: "var(--series-1)" }}
        />
      </span>
    </span>
  );
}
