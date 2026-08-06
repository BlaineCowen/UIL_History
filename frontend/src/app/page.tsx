import Link from "next/link";
import {
  countEntries,
  getConferences,
  getDistribution,
  getEntries,
  getFilterOptions,
  getScoresByYear,
  getSummary,
  getYearBounds,
} from "@/lib/db";
import {
  parseEntryFilters,
  parseEntrySort,
  parsePage,
  buildQuery,
  ENTRY_SORT_COLUMNS,
  ENTRY_SORT_DEFAULT_DIR,
  ENTRY_SORT_DESCRIPTIONS,
  ENTRY_SORT_LABELS,
  type SearchParams,
} from "@/lib/params";
import type { EntrySort } from "@/lib/db";
import { choice, formatNumber, formatScore, pct } from "@/lib/format";
import { Filters } from "@/components/Filters";
import { ScoreTrend } from "@/components/charts/ScoreTrend";
import { Distribution } from "@/components/charts/Distribution";
import {
  Card,
  DataCard,
  DataList,
  EmptyState,
  Pagination,
  RatingCell,
  ScoreBadge,
  SectionTitle,
  SortChips,
  StatTile,
  TableScroll,
} from "@/components/ui";

const PAGE_SIZE = 50;

/** Column header -> sort key. Headers absent here are not sortable. */
const SORTABLE = Object.fromEntries(
  ENTRY_SORT_COLUMNS.map((c) => [c.label, c.key]),
) as Partial<Record<string, EntrySort>>;

const COLUMNS = [
  "Year",
  "Event",
  "School",
  "Director",
  "Class",
  "Selections",
  "Concert",
  "SR",
];

export default async function ResultsPage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>;
}) {
  const sp = await searchParams;
  const filters = parseEntryFilters(sp);
  const { sort, dir } = parseEntrySort(sp);
  const page = parsePage(sp);
  const bounds = await getYearBounds();

  /** Re-selecting the active column flips it; a new column starts at its own natural direction. */
  function sortHref(key: EntrySort) {
    const nextDir =
      sort === key
        ? dir === "asc"
          ? "desc"
          : "asc"
        : ENTRY_SORT_DEFAULT_DIR[key];
    return `/${buildQuery(sp, { sort: key, dir: nextDir, page: undefined })}`;
  }

  const options = await getFilterOptions(filters.genEvent);
  const conferences = await getConferences(filters.genEvent, filters.schoolLevel);

  const total = await countEntries(filters);
  const summary = await getSummary(filters);
  const rows = await getEntries(filters, sort, dir, PAGE_SIZE, (page - 1) * PAGE_SIZE);
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));

  // The table and the phone card list render the same three selections, so
  // they are unpacked once here rather than in each view.
  const entries = rows.map((r) => ({
    row: r,
    selections: [
      { t: r.title_1, c: r.composer_1, code: r.code_1 },
      { t: r.title_2, c: r.composer_2, code: r.code_2 },
      { t: r.title_3, c: r.composer_3, code: r.code_3 },
    ].filter((s) => (s.t ?? "").trim()),
  }));

  const selected = await getScoresByYear(filters);
  // The comparison line is the same ensemble with every other filter removed.
  const baseline = await getScoresByYear({ genEvent: filters.genEvent });
  const distribution = await getDistribution(filters);

  const narrowed = Object.entries(filters).some(
    ([key, value]) =>
      key !== "genEvent" &&
      value !== undefined &&
      !(Array.isArray(value) && value.length === 0) &&
      !(key === "yearFrom" && value === bounds.min) &&
      !(key === "yearTo" && value === bounds.max),
  );

  const baselineLabel = filters.genEvent ? `All ${filters.genEvent}` : "All entries";

  return (
    <div className="grid gap-6">
      <header className="grid gap-2">
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          Concert &amp; Sight-Reading results
        </h1>
        <p className="text-sm max-w-2xl" style={{ color: "var(--ink-2)" }}>
          Every Texas UIL concert and sight-reading entry from {bounds.min} to{" "}
          {bounds.max}. 
        </p>
      </header>

      <Filters
        genEvents={["Band", "Chorus", "Orchestra"]}
        events={options.events.map((e) => ({
          value: e.event,
          label: e.event,
          count: e.n,
        }))}
        levels={options.levels.map((l) => ({
          value: l.school_level,
          label: l.school_level,
          count: l.n,
        }))}
        conferences={conferences.map((c) => ({
          value: c.conference,
          label: c.conference,
          count: c.n,
        }))}
        classifications={options.classifications.map((c) => ({
          value: c.classification,
          label: c.classification,
          count: c.n,
        }))}
        yearBounds={bounds}
      />

      {total === 0 ? (
        <EmptyState
          title="No entries match these filters"
          hint="Try widening the year range or clearing the school and song filters."
        />
      ) : (
        <>
          <div className="grid gap-3 grid-cols-2 lg:grid-cols-4">
            <StatTile label="Entries" value={formatNumber(total)} />
            <StatTile label="Schools" value={formatNumber(summary.schools)} />
            <StatTile
              label="Avg concert"
              value={formatScore(summary.avgConcert)}
              sub="1 is best"
            />
            <StatTile
              label="Sweepstakes"
              value={formatNumber(summary.sweepstakes)}
              sub={`${pct(summary.sweepstakes, total)} of entries · 1 in both`}
            />
          </div>

          <div className="grid gap-4 lg:grid-cols-2">
            <ScoreTrend
              title="Concert rating over time"
              hint="Average concert rating by contest year."
              metric="concert"
              selected={selected}
              baseline={baseline}
              baselineLabel={baselineLabel}
              showBaseline={narrowed}
            />
            <ScoreTrend
              title="Sight-reading rating over time"
              hint="Average sight-reading rating by contest year."
              metric="sight"
              selected={selected}
              baseline={baseline}
              baselineLabel={baselineLabel}
              showBaseline={narrowed}
            />
          </div>

          <Distribution data={distribution} total={total} />

          <Card padded={false}>
            <div className="p-4 sm:p-5 pb-0">
              <SectionTitle
                title="Entries"
                hint={`${formatNumber(total)} matching · sorted by ${
                  ENTRY_SORT_LABELS[sort]
                } (${ENTRY_SORT_DESCRIPTIONS[sort][dir]})`}
              />
              <SortChips
                columns={ENTRY_SORT_COLUMNS}
                active={sort}
                dir={dir}
                hrefFor={sortHref}
              />
            </div>
            <div className="px-4 sm:px-5 pb-4 sm:pb-5">
              <TableScroll>
                <table className="w-full text-sm border-separate border-spacing-0">
                  <thead>
                    <tr className="text-left">
                      {COLUMNS.map((h) => {
                        const key = SORTABLE[h];
                        const active = key && sort === key;
                        return (
                          <th
                            key={h}
                            aria-sort={
                              active
                                ? dir === "asc"
                                  ? "ascending"
                                  : "descending"
                                : undefined
                            }
                            className="whitespace-nowrap border-b py-2 pr-4 text-[11px] font-semibold uppercase tracking-wide"
                            style={{ color: active ? "var(--ink)" : "var(--muted)" }}
                          >
                            {key ? (
                              <Link
                                href={sortHref(key)}
                                className="inline-flex items-center gap-1 hover:opacity-70"
                              >
                                {h}
                                <span aria-hidden style={{ opacity: active ? 1 : 0.25 }}>
                                  {active && dir === "asc" ? "↑" : "↓"}
                                </span>
                              </Link>
                            ) : (
                              h
                            )}
                          </th>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {entries.map(({ row: r, selections }) => {
                      return (
                        <tr key={r.entry_number} className="align-top">
                          <td className="tnum whitespace-nowrap border-b py-2.5 pr-4">
                            {r.year}
                          </td>
                          <td className="whitespace-nowrap border-b py-2.5 pr-4">
                            {r.event}
                          </td>
                          <td className="border-b py-2.5 pr-4 min-w-[160px]">
                            <span className="font-medium">{r.school}</span>
                            {r.conference && (
                              <span
                                className="ml-1.5 text-[11px]"
                                style={{ color: "var(--muted)" }}
                              >
                                {r.conference}
                              </span>
                            )}
                          </td>
                          <td
                            className="border-b py-2.5 pr-4 min-w-[130px]"
                            style={{ color: "var(--ink-2)" }}
                          >
                            {r.director || "—"}
                          </td>
                          <td
                            className="whitespace-nowrap border-b py-2.5 pr-4"
                            style={{ color: "var(--ink-2)" }}
                          >
                            {r.classification || "—"}
                          </td>
                          <td className="border-b py-2.5 pr-4 min-w-[260px]">
                            <ul className="grid gap-0.5">
                              {selections.map((s, i) => (
                                <li key={i} className="text-[13px]">
                                  {s.code ? (
                                    <Link
                                      href={`/pml/${encodeURIComponent(s.code)}`}
                                      className="underline decoration-transparent hover:decoration-inherit underline-offset-2 transition"
                                    >
                                      {choice(s.t, s.c)}
                                    </Link>
                                  ) : (
                                    choice(s.t, s.c)
                                  )}
                                </li>
                              ))}
                              {!selections.length && (
                                <li style={{ color: "var(--muted)" }}>—</li>
                              )}
                            </ul>
                          </td>
                          <td className="border-b py-2.5 pr-4">
                            <ScoreBadge score={r.concert_final_score} />
                          </td>
                          <td className="border-b py-2.5 pr-2">
                            <ScoreBadge score={r.sight_reading_final_score} />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </TableScroll>

              <DataList>
                {entries.map(({ row: r, selections }) => (
                  <DataCard key={r.entry_number}>
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="font-medium leading-snug">{r.school}</p>
                        <p
                          className="text-[12px] mt-0.5"
                          style={{ color: "var(--muted)" }}
                        >
                          {[r.year, r.event, r.conference, r.classification]
                            .filter(Boolean)
                            .join(" · ")}
                        </p>
                      </div>
                      <div className="flex shrink-0 gap-2">
                        <RatingCell label="Concert" score={r.concert_final_score} />
                        <RatingCell label="SR" score={r.sight_reading_final_score} />
                      </div>
                    </div>

                    {selections.length > 0 && (
                      <ul className="grid gap-1 pt-0.5">
                        {selections.map((s, i) => (
                          <li key={i} className="text-[13px] leading-snug">
                            {s.code ? (
                              <Link
                                href={`/pml/${encodeURIComponent(s.code)}`}
                                className="underline decoration-[var(--border-strong)] underline-offset-2"
                              >
                                {choice(s.t, s.c)}
                              </Link>
                            ) : (
                              choice(s.t, s.c)
                            )}
                          </li>
                        ))}
                      </ul>
                    )}

                    {r.director && (
                      <p className="text-[12px]" style={{ color: "var(--muted)" }}>
                        {r.director}
                      </p>
                    )}
                  </DataCard>
                ))}
              </DataList>

              <Pagination
                page={page}
                pageCount={pageCount}
                hrefFor={(p) => `/${buildQuery(sp, { page: p === 1 ? undefined : p })}`}
              />
            </div>
          </Card>
        </>
      )}
    </div>
  );
}
