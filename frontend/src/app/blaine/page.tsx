import Link from "next/link";
import type { Metadata } from "next";
import {
  countDirectorRoles,
  countEntries,
  getConferences,
  getEntriesWithJudges,
  getFilterOptions,
  getSummary,
  getYearBounds,
  type EntrySort,
  type EntryWithJudges,
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
import { choice, formatNumber, formatScore, pct } from "@/lib/format";
import { Filters } from "@/components/Filters";
import {
  Card,
  CardRow,
  DataCard,
  DataList,
  EmptyState,
  JudgeScores,
  Pagination,
  SectionTitle,
  SortChips,
  StatTile,
  TableScroll,
} from "@/components/ui";

/**
 * Unlisted companion to the public results page: adds a director filter and
 * shows each panel's three individual scores instead of only the final.
 *
 * "Unlisted" is the whole of the protection -- there is no nav link and no
 * password, so anyone who types the path can read it. That is deliberate, and
 * fine behind the tailnet; revisit it before this goes on a public host.
 */
export const metadata: Metadata = {
  title: "Results with judge detail",
  robots: { index: false, follow: false },
};

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

export default async function BlainePage({
  searchParams,
}: {
  searchParams: Promise<SearchParams>;
}) {
  const sp = await searchParams;
  const filters = parseEntryFilters(sp, { director: true });
  const { sort, dir } = parseEntrySort(sp);
  const page = parsePage(sp);
  const bounds = getYearBounds();

  function sortHref(key: EntrySort) {
    const nextDir =
      sort === key
        ? dir === "asc"
          ? "desc"
          : "asc"
        : ENTRY_SORT_DEFAULT_DIR[key];
    return `/blaine${buildQuery(sp, { sort: key, dir: nextDir, page: undefined })}`;
  }

  const options = getFilterOptions(filters.genEvent);
  const conferences = getConferences(filters.genEvent, filters.schoolLevel);

  const total = countEntries(filters);
  const summary = getSummary(filters);
  const roles = countDirectorRoles(filters);
  const rows = getEntriesWithJudges(
    filters,
    sort,
    dir,
    PAGE_SIZE,
    (page - 1) * PAGE_SIZE,
  );

  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE));

  const entries = rows.map((r) => ({
    row: r,
    selections: [
      { t: r.title_1, c: r.composer_1, code: r.code_1 },
      { t: r.title_2, c: r.composer_2, code: r.code_2 },
      { t: r.title_3, c: r.composer_3, code: r.code_3 },
    ].filter((s) => (s.t ?? "").trim()),
    concert: [r.concert_score_1, r.concert_score_2, r.concert_score_3],
    sight: [
      r.sight_reading_score_1,
      r.sight_reading_score_2,
      r.sight_reading_score_3,
    ],
  }));

  return (
    <div className="grid gap-6">
      <header className="grid gap-2">
        <div className="flex flex-wrap items-center gap-2">
          <span
            className="rounded-full border px-2.5 py-0.5 text-[11px] font-medium uppercase tracking-wide"
            style={{ color: "var(--muted)" }}
          >
            Unlisted
          </span>
        </div>
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          Results with judge detail
        </h1>
        <p className="text-sm max-w-2xl" style={{ color: "var(--ink-2)" }}>
          The full results explorer, plus a director filter and every panel&apos;s
          three individual scores. Ratings run 1 (superior) to 5 — lower is
          better throughout.
        </p>
        {roles && (
          <p className="text-sm" style={{ color: "var(--ink-2)" }}>
            Matching <strong>{formatNumber(total)}</strong>{" "}
            {total === 1 ? "entry" : "entries"} —{" "}
            <RoleDot main /> {formatNumber(roles.main)} as main director,{" "}
            <RoleDot /> {formatNumber(roles.additional)} as additional
            {roles.both > 0 && ` (${formatNumber(roles.both)} both)`}.
          </p>
        )}
      </header>

      <Filters
        showDirector
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
          hint="Try a shorter director name, or widen the year range."
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

          <Card padded={false}>
            <div className="p-4 sm:p-5 pb-0">
              <SectionTitle
                title="Entries"
                hint={`${formatNumber(total)} matching · sorted by ${
                  ENTRY_SORT_LABELS[sort]
                } (${ENTRY_SORT_DESCRIPTIONS[sort][dir]}) · each panel shows its
                  three judges then the awarded rating`}
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
                    {entries.map(({ row: r, selections, concert, sight }) => (
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
                          <DirectorCell entry={r} />
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
                          <JudgeScores
                            scores={concert}
                            final={r.concert_final_score}
                          />
                        </td>
                        <td className="border-b py-2.5 pr-2">
                          <JudgeScores
                            scores={sight}
                            final={r.sight_reading_final_score}
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </TableScroll>

              <DataList>
                {entries.map(({ row: r, selections, concert, sight }) => (
                  <DataCard key={r.entry_number}>
                    <div>
                      <p className="font-medium leading-snug">{r.school}</p>
                      <p className="text-[12px] mt-0.5" style={{ color: "var(--muted)" }}>
                        {[r.year, r.event, r.conference, r.classification]
                          .filter(Boolean)
                          .join(" · ")}
                      </p>
                    </div>

                    {/* Judges get their own rows here -- three numbers plus a
                        badge is too wide to sit beside the school name. */}
                    <div
                      className="grid gap-1 pt-1.5 border-t"
                      style={{ borderColor: "var(--border)" }}
                    >
                      <CardRow label="Concert">
                        <JudgeScores scores={concert} final={r.concert_final_score} />
                      </CardRow>
                      <CardRow label="Sight-reading">
                        <JudgeScores scores={sight} final={r.sight_reading_final_score} />
                      </CardRow>
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

                    {(r.director || r.additional_director) && (
                      <div className="text-[12px]" style={{ color: "var(--muted)" }}>
                        <DirectorCell entry={r} />
                      </div>
                    )}
                  </DataCard>
                ))}
              </DataList>

              <Pagination
                page={page}
                pageCount={pageCount}
                hrefFor={(p) =>
                  `/blaine${buildQuery(sp, { page: p === 1 ? undefined : p })}`
                }
              />
            </div>
          </Card>
        </>
      )}
    </div>
  );
}

/** Colour is never the only cue -- every tag carries its word too. */
const MAIN_COLOR = "var(--series-1)";
const ADDITIONAL_COLOR = "var(--series-2)";

function RoleDot({ main }: { main?: boolean }) {
  return (
    <span
      aria-hidden
      className="inline-block h-2 w-2 rounded-full align-middle"
      style={{ background: main ? MAIN_COLOR : ADDITIONAL_COLOR }}
    />
  );
}

function RoleTag({ main }: { main?: boolean }) {
  return (
    <span
      className="ml-1.5 inline-flex items-center rounded-full px-1.5 py-px text-[10px] font-semibold uppercase tracking-wide align-middle text-white"
      style={{ background: main ? MAIN_COLOR : ADDITIONAL_COLOR }}
    >
      {main ? "Main" : "Additional"}
    </span>
  );
}

/**
 * Both names for the entry, with a tag on whichever one the director search
 * actually hit. The flags come from SQL, so a tag appears if and only if that
 * field is why the row is here. With no director filter active they are
 * undefined and this renders exactly as before.
 */
function DirectorCell({ entry }: { entry: EntryWithJudges }) {
  const hitMain = Boolean(entry.matched_director);
  const hitAdditional = Boolean(entry.matched_additional);

  return (
    <>
      <span style={hitMain ? { color: "var(--ink)", fontWeight: 600 } : undefined}>
        {entry.director || "—"}
      </span>
      {hitMain && <RoleTag main />}
      {entry.additional_director && (
        <div
          className="text-[11px]"
          style={
            hitAdditional
              ? { color: "var(--ink)", fontWeight: 600 }
              : { color: "var(--muted)" }
          }
        >
          + {entry.additional_director}
          {hitAdditional && <RoleTag />}
        </div>
      )}
    </>
  );
}
