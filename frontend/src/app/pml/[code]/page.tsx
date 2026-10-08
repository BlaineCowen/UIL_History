import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import { ArrowLeft } from "lucide-react";
import {
  getSong,
  getSongPairings,
  getSongPerformances,
  getSongShare,
  getSongSummary,
  getSongTopSchools,
  getSongYearly,
} from "@/lib/db";
import { choice, formatNumber, formatScore, pct } from "@/lib/format";
import { SongYearly } from "@/components/charts/SongYearly";
import { SongStructuredData } from "@/components/StructuredData";
import {
  Card,
  DataCard,
  DataList,
  PmlStatus,
  RatingCell,
  ScoreBadge,
  SectionTitle,
  StatTile,
  TableScroll,
} from "@/components/ui";

/**
 * Without these two exports this route is fully dynamic: every request renders
 * the page again, and the CDN stores nothing. The runtime logs showed it
 * plainly -- `cache=MISS` on every hit, never once a HIT. That is affordable
 * for a page or two and not for 8,395, which is how many song URLs the sitemap
 * hands to crawlers.
 *
 * `generateStaticParams` is what flips the route from dynamic to cached, and
 * returning an *empty* list is enough to do it. This is the documented
 * behaviour, not a trick -- "you must always return an array from
 * generateStaticParams, even if it's empty. Otherwise, the route will be
 * dynamically rendered." Verified against a production build too: without the
 * export, repeat requests report no cache header at all; with it returning [],
 * the first request to a code is a MISS and every one after it is a HIT.
 *
 * If Cache Components is ever enabled app-wide, this becomes a build error --
 * that mode requires at least one param. See the note in db.ts about that
 * migration; it would want revisiting here at the same time.
 *
 * That is why nothing is prerendered here. Listing codes would mean querying
 * the database during the build, and this project has already had a deploy
 * broken exactly that way -- see the comment in sitemap.ts. Since an unlisted
 * code is cached the moment it is first rendered, prerendering would only buy
 * a faster first visit, and stale entries revalidate in the background rather
 * than making anyone wait. Not worth reintroducing a build-time dependency on
 * a reachable database for.
 */
export async function generateStaticParams() {
  return [];
}

/**
 * A week, not the data cache's day: this controls how often a *rendered page*
 * is thrown away, and re-rendering 8,395 of them daily is the cost this change
 * exists to remove. As with the data cache, the real invalidation is
 * `revalidateTag(CONTEST_DATA_TAG)` from /api/revalidate after a load -- every
 * query this page makes is tagged, so dropping the tag drops these pages too.
 * This is only the backstop for a load that forgets to call it.
 */
export const revalidate = 604800;

type Props = { params: Promise<{ code: string }> };

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { code } = await params;
  const song = await getSong(decodeURIComponent(code));
  if (!song) return { title: "Song not found", robots: { index: false } };

  const by = song.composer ? ` by ${song.composer}` : "";
  // The long tail is people searching one piece by name, so the title leads
  // with it and the description carries the facts they came for -- grade,
  // event, how often it is programmed, how it scores.
  const title = `${song.title}${by} — UIL Grade ${song.grade} ${song.event_name}`;
  const parts = [
    `${song.title}${by} is a grade ${song.grade} ${song.event_name.toLowerCase()} piece on the Texas UIL Prescribed Music List.`,
  ];
  if (song.performance_count > 0) {
    parts.push(
      `Performed ${song.performance_count.toLocaleString()} times at UIL contest` +
        (song.average_concert_score
          ? `, averaging ${song.average_concert_score.toFixed(2)} in concert.`
          : "."),
    );
  } else {
    parts.push("No recorded UIL contest performances yet.");
  }
  if (song.on_current_pml === 0) {
    parts.push("No longer on the current PML.");
  }

  return {
    title,
    description: parts.join(" "),
    alternates: { canonical: `/pml/${encodeURIComponent(song.code)}` },
    openGraph: {
      title,
      description: parts.join(" "),
      url: `/pml/${encodeURIComponent(song.code)}`,
    },
  };
}

export default async function SongPage({ params }: Props) {
  const { code } = await params;
  const song = await getSong(decodeURIComponent(code));
  if (!song) notFound();

  // In parallel: six independent queries that were previously awaited one
  // after another, so the page waited for the sum of them. Only `song` has to
  // come first -- getSongShare needs its event and grade. A cached render is
  // now the one MISS a code ever costs, so it is worth it being short.
  const [summary, yearly, performances, share, topSchools, pairings] =
    await Promise.all([
      getSongSummary(song.code),
      getSongYearly(song.code),
      getSongPerformances(song.code),
      getSongShare(song.code, song.event_name, song.grade),
      getSongTopSchools(song.code),
      getSongPairings(song.code),
    ]);

  const neverPerformed = summary.performances === 0;

  // The rest of each program -- what directors put alongside this piece. The
  // row matched on this song's code, so dropping that code removes exactly
  // this piece and leaves the other selections, matched or not.
  const programs = performances.map((p) => ({
    p,
    pairedWith: [
      { t: p.title_1, c: p.composer_1, code: p.code_1 },
      { t: p.title_2, c: p.composer_2, code: p.code_2 },
      { t: p.title_3, c: p.composer_3, code: p.code_3 },
    ].filter((s) => (s.t ?? "").trim() && s.code !== song.code),
  }));

  return (
    <div className="grid gap-6">
      <SongStructuredData
        title={song.title}
        composer={song.composer}
        eventName={song.event_name}
        grade={song.grade}
        code={song.code}
        performances={summary.performances}
        averageConcert={summary.avgConcert}
      />
      <div>
        <Link
          href="/pml"
          className="inline-flex items-center gap-1.5 text-sm transition hover:opacity-70"
          style={{ color: "var(--ink-2)" }}
        >
          <ArrowLeft size={15} aria-hidden />
          All songs
        </Link>
      </div>

      <header className="grid gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <Badge>{song.event_name}</Badge>
          <Badge>Grade {song.grade}</Badge>
          {song.specification && <Badge subtle>{song.specification}</Badge>}
          <Badge subtle>Code {song.code}</Badge>
        </div>
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          {song.title}
        </h1>
        <p className="text-sm" style={{ color: "var(--ink-2)" }}>
          {song.composer || "Composer unknown"}
          {song.arranger ? ` · arranged by ${song.arranger}` : ""}
          {song.publisher ? ` · ${song.publisher}` : ""}
        </p>
        <PmlStatus
          onCurrent={song.on_current_pml}
          previousGrade={song.previous_grade}
          grade={song.grade}
          changedFrom={song.grade_changed_from}
          changedTo={song.grade_changed_to}
        />
      </header>

      {neverPerformed ? (
        <Card>
          <p className="text-sm" style={{ color: "var(--ink-2)" }}>
            This piece is on the Prescribed Music List but has no recorded
            performances in the contest data.
          </p>
        </Card>
      ) : (
        <>
          <div className="grid gap-3 grid-cols-2 lg:grid-cols-4">
            <StatTile
              label="Performances"
              value={formatNumber(summary.performances)}
              sub={`${summary.firstYear}–${summary.lastYear}`}
            />
            <StatTile label="Schools" value={formatNumber(summary.schools)} />
            <StatTile
              label="Avg concert"
              value={formatScore(summary.avgConcert)}
              sub="1 is best"
            />
            <StatTile
              label="Straight ones"
              value={pct(summary.ones, summary.performances)}
              sub={`${formatNumber(summary.ones)} superior ratings`}
            />
          </div>

          <SongYearly data={yearly} />

          <div className="grid gap-4 lg:grid-cols-2">
            <Card>
              <SectionTitle
                title="Share of its category"
                hint={`Against every grade ${song.grade} ${song.event_name} piece performed since ${share.since}.`}
              />
              <div className="grid gap-3">
                <div className="flex items-baseline gap-2">
                  <span className="text-3xl font-semibold">
                    {pct(share.mine, share.peers)}
                  </span>
                  <span className="text-sm" style={{ color: "var(--ink-2)" }}>
                    of {formatNumber(share.peers)} performances
                  </span>
                </div>
                <div
                  className="h-2.5 w-full rounded-full overflow-hidden"
                  style={{ background: "var(--surface-2)" }}
                >
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${Math.max(1, Math.min(100, (share.mine / Math.max(1, share.peers)) * 100))}%`,
                      background: "var(--series-1)",
                    }}
                  />
                </div>
                <p className="text-xs" style={{ color: "var(--muted)" }}>
                  {formatNumber(share.mine)} of this piece vs{" "}
                  {formatNumber(share.peers - share.mine)} of everything else in
                  the same grade and event.
                </p>
              </div>
            </Card>

            <Card>
              <SectionTitle
                title="Programmed most often by"
                hint="Schools that have returned to this piece."
              />
              {topSchools.length ? (
                <ul className="grid gap-2">
                  {topSchools.map((s) => (
                    <li key={s.school} className="flex items-center gap-3 text-sm">
                      <span className="truncate flex-1">{s.school}</span>
                      <span
                        className="tnum text-xs"
                        style={{ color: "var(--muted)" }}
                      >
                        avg {formatScore(s.avgConcert, 1)}
                      </span>
                      <span className="tnum font-medium">{s.n}×</span>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm" style={{ color: "var(--muted)" }}>
                  No repeat performances.
                </p>
              )}
            </Card>
          </div>

          <Card>
            <SectionTitle
              title="Most often paired with"
              hint="Pieces programmed alongside this one, across every recorded performance."
            />
            {pairings.length ? (
              <ul className="grid gap-x-8 gap-y-2 md:grid-cols-2">
                {pairings.map((s) => (
                  <li key={s.code} className="flex items-center gap-3 text-sm">
                    <Link
                      href={`/pml/${encodeURIComponent(s.code)}`}
                      className="min-w-0 flex-1 truncate underline decoration-[var(--border-strong)] underline-offset-2 hover:opacity-70 transition"
                      title={choice(s.title, s.composer)}
                    >
                      {choice(s.title, s.composer)}
                    </Link>
                    <span
                      className="tnum text-xs whitespace-nowrap"
                      style={{ color: "var(--muted)" }}
                    >
                      {s.event_name === song.event_name
                        ? `Gr ${s.grade}`
                        : `${s.event_name} · Gr ${s.grade}`}{" "}
                      · avg {formatScore(s.avgConcert, 1)}
                    </span>
                    <span className="tnum font-medium">{s.n}×</span>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-sm" style={{ color: "var(--muted)" }}>
                No matched pieces have been programmed with this one.
              </p>
            )}
          </Card>

          <Card padded={false}>
            <div className="p-4 sm:p-5 pb-0">
              <SectionTitle
                title="Performances"
                hint={
                  performances.length < summary.performances
                    ? `Showing the ${formatNumber(performances.length)} most recent of ${formatNumber(summary.performances)}.`
                    : `All ${formatNumber(performances.length)} recorded performances.`
                }
              />
            </div>
            <div className="px-4 sm:px-5 pb-4 sm:pb-5">
              <TableScroll>
                <table className="w-full text-sm border-separate border-spacing-0">
                  <thead>
                    <tr className="text-left">
                      {["Year", "School", "Event", "Performed with", "Director", "Class", "Concert", "SR"].map(
                        (h) => (
                          <th
                            key={h}
                            className="whitespace-nowrap border-b py-2 pr-4 text-[11px] font-semibold uppercase tracking-wide"
                            style={{ color: "var(--muted)" }}
                          >
                            {h}
                          </th>
                        ),
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {programs.map(({ p, pairedWith }) => (
                      <tr key={p.entry_number}>
                        <td className="tnum whitespace-nowrap border-b py-2.5 pr-4">
                          {p.year}
                        </td>
                        <td className="border-b py-2.5 pr-4 min-w-[160px] font-medium">
                          {p.school}
                          {p.conference && (
                            <span
                              className="ml-1.5 text-[11px] font-normal"
                              style={{ color: "var(--muted)" }}
                            >
                              {p.conference}
                            </span>
                          )}
                        </td>
                        <td
                          className="whitespace-nowrap border-b py-2.5 pr-4"
                          style={{ color: "var(--ink-2)" }}
                        >
                          {p.event}
                        </td>
                        <td className="border-b py-2.5 pr-4 min-w-[240px]">
                          <PairedWith songs={pairedWith} />
                        </td>
                        <td
                          className="border-b py-2.5 pr-4 min-w-[130px]"
                          style={{ color: "var(--ink-2)" }}
                        >
                          {p.director || "—"}
                        </td>
                        <td
                          className="whitespace-nowrap border-b py-2.5 pr-4"
                          style={{ color: "var(--ink-2)" }}
                        >
                          {p.classification || "—"}
                        </td>
                        <td className="border-b py-2.5 pr-4">
                          <ScoreBadge score={p.concert_final_score} />
                        </td>
                        <td className="border-b py-2.5 pr-2">
                          <ScoreBadge score={p.sight_reading_final_score} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </TableScroll>

              <DataList>
                {programs.map(({ p, pairedWith }) => (
                  <DataCard key={p.entry_number}>
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="font-medium leading-snug">{p.school}</p>
                        <p
                          className="text-[12px] mt-0.5"
                          style={{ color: "var(--muted)" }}
                        >
                          {[p.year, p.event, p.conference, p.classification]
                            .filter(Boolean)
                            .join(" · ")}
                        </p>
                        {p.director && (
                          <p
                            className="text-[12px] mt-0.5"
                            style={{ color: "var(--muted)" }}
                          >
                            {p.director}
                          </p>
                        )}
                      </div>
                      <div className="flex shrink-0 gap-2">
                        <RatingCell label="Concert" score={p.concert_final_score} />
                        <RatingCell label="SR" score={p.sight_reading_final_score} />
                      </div>
                    </div>
                    {pairedWith.length > 0 && (
                      <div className="grid gap-1 pt-0.5">
                        <p
                          className="text-[11px] font-semibold uppercase tracking-wide"
                          style={{ color: "var(--muted)" }}
                        >
                          Performed with
                        </p>
                        <PairedWith songs={pairedWith} />
                      </div>
                    )}
                  </DataCard>
                ))}
              </DataList>
            </div>
          </Card>
        </>
      )}
    </div>
  );
}

function PairedWith({
  songs,
}: {
  songs: { t: string; c: string; code: string }[];
}) {
  if (!songs.length) {
    return <span style={{ color: "var(--muted)" }}>—</span>;
  }
  return (
    <ul className="grid gap-0.5">
      {songs.map((s, i) => (
        <li key={i} className="text-[13px] leading-snug">
          {s.code ? (
            <Link
              href={`/pml/${encodeURIComponent(s.code)}`}
              className="underline decoration-[var(--border-strong)] underline-offset-2 hover:opacity-70 transition"
            >
              {choice(s.t, s.c)}
            </Link>
          ) : (
            choice(s.t, s.c)
          )}
        </li>
      ))}
    </ul>
  );
}

function Badge({
  children,
  subtle,
}: {
  children: React.ReactNode;
  subtle?: boolean;
}) {
  return (
    <span
      className="rounded-full border px-2.5 py-0.5 text-[12px] font-medium"
      style={{
        background: subtle ? "transparent" : "var(--surface-2)",
        color: subtle ? "var(--muted)" : "var(--ink-2)",
      }}
    >
      {children}
    </span>
  );
}
