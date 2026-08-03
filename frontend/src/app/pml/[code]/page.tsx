import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import { ArrowLeft } from "lucide-react";
import {
  getSong,
  getSongPerformances,
  getSongShare,
  getSongSummary,
  getSongTopSchools,
  getSongYearly,
} from "@/lib/db";
import { formatNumber, formatScore, pct } from "@/lib/format";
import { SongYearly } from "@/components/charts/SongYearly";
import { Card, ScoreBadge, SectionTitle, StatTile, TableScroll } from "@/components/ui";

type Props = { params: Promise<{ code: string }> };

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { code } = await params;
  const song = getSong(decodeURIComponent(code));
  if (!song) return { title: "Song not found" };
  return {
    title: song.title,
    description: `UIL performance history for ${song.title} by ${song.composer}.`,
  };
}

export default async function SongPage({ params }: Props) {
  const { code } = await params;
  const song = getSong(decodeURIComponent(code));
  if (!song) notFound();

  const summary = getSongSummary(song.code);
  const yearly = getSongYearly(song.code);
  const performances = getSongPerformances(song.code);
  const share = getSongShare(song.code, song.event_name, song.grade);
  const topSchools = getSongTopSchools(song.code);

  const neverPerformed = summary.performances === 0;

  return (
    <div className="grid gap-6">
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
                      {["Year", "School", "Event", "Director", "Class", "Concert", "SR"].map(
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
                    {performances.map((p) => (
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
            </div>
          </Card>
        </>
      )}
    </div>
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
