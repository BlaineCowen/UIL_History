import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import { getSongCategories, getSongs } from "@/lib/db";
import { formatNumber, formatScore } from "@/lib/format";
import { eventFromSlug, listHref, parseGradeSlug } from "@/lib/lists";
import { OG_IMAGE } from "@/lib/site";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import {
  Card,
  DataCard,
  DataList,
  PmlStatus,
  SectionTitle,
  TableScroll,
} from "@/components/ui";

type Props = { params: Promise<{ event: string; grade: string }> };

// Cached on first request and never prerendered -- see the comment on the
// same two exports in pml/[code]/page.tsx for why.
export async function generateStaticParams() {
  return [];
}
export const revalidate = 604800;

/** Comfortably above the largest single event x grade list. */
const MAX_ROWS = 2000;

async function load(eventSlug: string, gradeSlug: string) {
  const grade = parseGradeSlug(gradeSlug);
  if (grade === undefined) return undefined;
  const categories = await getSongCategories();
  const eventName = eventFromSlug(eventSlug, categories);
  if (!eventName) return undefined;
  const siblings = categories.filter((c) => c.event_name === eventName);
  const category = siblings.find((c) => c.grade === grade);
  if (!category || category.current === 0) return undefined;
  return { eventName, grade, category, siblings };
}

/** What people type: "uil grade 3 concert band list", "uil grade 2 treble chorus music". */
function titleFor(eventName: string, grade: number) {
  return `UIL Grade ${grade} ${eventName} Music List`;
}

/** "X by Y" for prose; `choice()`'s "X — Y" reads badly mid-sentence after a dash. */
function titleBy(s: { title: string; composer: string | null }) {
  return s.composer ? `${s.title} by ${s.composer}` : s.title;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { event, grade: gradeParam } = await params;
  const data = await load(event, gradeParam);
  if (!data) return { title: "List not found", robots: { index: false } };

  const { eventName, grade, category } = data;
  const [top] = await getSongs(
    { eventName, gradeFrom: grade, gradeTo: grade, hideDelisted: true },
    "performance_count",
    "desc",
    1,
    0,
  );
  const title = titleFor(eventName, grade);
  const description =
    `All ${formatNumber(category.current)} grade ${grade} ` +
    `${eventName.toLowerCase()} pieces on the Texas UIL Prescribed Music List, ` +
    `ranked by how often they are performed at Concert & Sight-Reading` +
    (top && top.performance_count > 0
      ? ` — led by ${titleBy(top)}.`
      : ".") +
    " See each piece's average concert and sight-reading ratings.";
  const url = listHref(eventName, grade);
  return {
    title,
    description,
    alternates: { canonical: url },
    openGraph: { title, description, url, images: OG_IMAGE },
  };
}

export default async function GradeListPage({ params }: Props) {
  const { event, grade: gradeParam } = await params;
  const data = await load(event, gradeParam);
  if (!data) notFound();
  const { eventName, grade, category, siblings } = data;

  const songs = await getSongs(
    { eventName, gradeFrom: grade, gradeTo: grade, hideDelisted: true },
    "performance_count",
    "desc",
    MAX_ROWS,
    0,
  );
  const top = songs[0];
  const lower = eventName.toLowerCase();
  const explorerHref = `/pml?sevent=${encodeURIComponent(eventName)}&gmin=${grade}&gmax=${grade}`;

  return (
    <div className="grid gap-6">
      <Breadcrumbs
        crumbs={[
          { name: "Home", path: "/" },
          { name: "Lists", path: "/lists" },
          { name: eventName, path: listHref(eventName) },
          { name: `Grade ${grade}`, path: listHref(eventName, grade) },
        ]}
      />
      <header className="grid gap-2">
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          {titleFor(eventName, grade)}
        </h1>
        <p className="text-sm max-w-2xl" style={{ color: "var(--ink-2)" }}>
          There are {formatNumber(category.current)} grade {grade} {lower} pieces on
          the current Texas UIL Prescribed Music List.{" "}
          {category.performed > 0 && (
            <>
              {formatNumber(category.performed)} of them have been performed at UIL
              Concert &amp; Sight-Reading
              {top && top.performance_count > 0 && (
                <>
                  , and the most programmed is{" "}
                  <Link
                    href={`/pml/${encodeURIComponent(top.code)}`}
                    className="underline underline-offset-2 hover:no-underline"
                  >
                    {titleBy(top)}
                  </Link>{" "}
                  with {formatNumber(top.performance_count)} performances
                </>
              )}
              .{" "}
            </>
          )}
          Ratings run 1 (best) to 5.
        </p>
      </header>

      <Card padded={false}>
        <div className="p-4 sm:p-5 pb-0">
          <SectionTitle
            title={`Grade ${grade} ${eventName}`}
            hint={`${formatNumber(songs.length)} pieces · most performed first`}
            right={
              <Link
                href={explorerHref}
                className="text-sm font-medium underline underline-offset-2 hover:no-underline"
              >
                Filter &amp; sort
              </Link>
            }
          />
        </div>
        <div className="px-4 sm:px-5 pb-4 sm:pb-5">
          <TableScroll>
            <table className="w-full text-sm border-separate border-spacing-0">
              <thead>
                <tr className="text-left">
                  {["Title", "Performances", "Concert avg", "SR avg"].map((h) => (
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
                {songs.map((s) => (
                  <tr key={s.code}>
                    <td className="border-b py-2.5 pr-4 min-w-[240px]">
                      <Link
                        href={`/pml/${encodeURIComponent(s.code)}`}
                        className="font-medium underline decoration-transparent hover:decoration-inherit underline-offset-2 transition"
                      >
                        {s.title}
                      </Link>{" "}
                      <PmlStatus
                        compact
                        onCurrent={s.on_current_pml}
                        previousGrade={s.previous_grade}
                        grade={s.grade}
                      />
                      <div className="text-[12px]" style={{ color: "var(--muted)" }}>
                        {s.composer}
                        {s.arranger ? ` · arr. ${s.arranger}` : ""}
                      </div>
                    </td>
                    <td className="tnum border-b py-2.5 pr-4">
                      {formatNumber(s.performance_count)}
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
            {songs.map((s) => (
              <DataCard key={s.code} href={`/pml/${encodeURIComponent(s.code)}`}>
                <div>
                  <p className="font-medium leading-snug">
                    {s.title}{" "}
                    <PmlStatus
                      compact
                      onCurrent={s.on_current_pml}
                      previousGrade={s.previous_grade}
                      grade={s.grade}
                    />
                  </p>
                  <p className="text-[12px] mt-0.5" style={{ color: "var(--muted)" }}>
                    {s.composer}
                    {s.arranger ? ` · arr. ${s.arranger}` : ""}
                  </p>
                </div>
                <p className="tnum text-[12px]" style={{ color: "var(--ink-2)" }}>
                  {formatNumber(s.performance_count)} performances · concert{" "}
                  {formatScore(s.average_concert_score)} · SR{" "}
                  {formatScore(s.average_sight_reading_score)}
                </p>
              </DataCard>
            ))}
          </DataList>
        </div>
      </Card>

      <Card>
        <SectionTitle title={`Other ${lower} grades`} />
        <ul className="flex flex-wrap gap-2 text-sm">
          {siblings.map((g) => (
            <li key={g.grade}>
              {g.grade === grade ? (
                <span
                  aria-current="page"
                  className="inline-flex rounded-lg border px-3 py-1.5 font-medium"
                  style={{ background: "var(--surface-2)" }}
                >
                  Grade {g.grade}
                </span>
              ) : (
                <Link
                  href={listHref(eventName, g.grade)}
                  className="inline-flex rounded-lg border px-3 py-1.5 transition hover:border-[var(--muted)]"
                >
                  Grade {g.grade}
                </Link>
              )}
            </li>
          ))}
        </ul>
        {category.delisted > 0 && (
          <p className="text-xs mt-3" style={{ color: "var(--muted)" }}>
            {formatNumber(category.delisted)} former grade {grade} {lower} pieces
            have since left the list; they are searchable in the{" "}
            <Link href={explorerHref} className="underline underline-offset-2">
              full PML
            </Link>
            .
          </p>
        )}
      </Card>
    </div>
  );
}
