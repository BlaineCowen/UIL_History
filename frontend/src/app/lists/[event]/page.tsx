import Link from "next/link";
import { notFound } from "next/navigation";
import type { Metadata } from "next";
import { getSongCategories, getSongs } from "@/lib/db";
import { formatNumber } from "@/lib/format";
import { eventFromSlug, listHref } from "@/lib/lists";
import { OG_IMAGE } from "@/lib/site";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { Card, SectionTitle } from "@/components/ui";

type Props = { params: Promise<{ event: string }> };

// Cached on first request and never prerendered -- see the comment on the
// same two exports in pml/[code]/page.tsx for why.
export async function generateStaticParams() {
  return [];
}
export const revalidate = 604800;

const TOP_N = 5;

async function load(slug: string) {
  const categories = await getSongCategories();
  const eventName = eventFromSlug(slug, categories);
  if (!eventName) return undefined;
  return { eventName, grades: categories.filter((c) => c.event_name === eventName) };
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { event } = await params;
  const data = await load(event);
  if (!data) return { title: "List not found", robots: { index: false } };

  const { eventName, grades } = data;
  const total = grades.reduce((n, g) => n + g.current, 0);
  const range = `${grades[0].grade}–${grades[grades.length - 1].grade}`;
  const title = `UIL ${eventName} Music List — Grades ${range}`;
  const description =
    `All ${formatNumber(total)} ${eventName.toLowerCase()} pieces on the Texas UIL ` +
    `Prescribed Music List, grades ${range}, with the most-performed pieces at ` +
    `each grade and how they score at Concert & Sight-Reading.`;
  const url = listHref(eventName);
  return {
    title,
    description,
    alternates: { canonical: url },
    openGraph: { title, description, url, images: OG_IMAGE },
  };
}

export default async function EventListPage({ params }: Props) {
  const { event } = await params;
  const data = await load(event);
  if (!data) notFound();
  const { eventName, grades } = data;

  const tops = await Promise.all(
    grades.map((g) =>
      getSongs(
        { eventName, gradeFrom: g.grade, gradeTo: g.grade, hideDelisted: true },
        "performance_count",
        "desc",
        TOP_N,
        0,
      ),
    ),
  );

  return (
    <div className="grid gap-6">
      <Breadcrumbs
        crumbs={[
          { name: "Home", path: "/" },
          { name: "Lists", path: "/lists" },
          { name: eventName, path: listHref(eventName) },
        ]}
      />
      <header className="grid gap-2">
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          UIL {eventName} music list
        </h1>
        <p className="text-sm max-w-2xl" style={{ color: "var(--ink-2)" }}>
          The {eventName.toLowerCase()} pieces on the current Prescribed Music List,
          grade by grade, with the ones schools perform most often at Concert &amp;
          Sight-Reading. Open a grade for its complete list.
        </p>
      </header>

      <div className="grid gap-4 lg:grid-cols-2">
        {grades.map((g, i) => (
          <Card key={g.grade}>
            <SectionTitle
              title={`Grade ${g.grade}`}
              hint={`${formatNumber(g.current)} pieces · ${formatNumber(g.performed)} performed at contest`}
              right={
                <Link
                  href={listHref(eventName, g.grade)}
                  className="text-sm font-medium underline underline-offset-2 hover:no-underline"
                >
                  Full list
                </Link>
              }
            />
            {tops[i].length > 0 && (
              <ol className="grid gap-1.5 text-sm">
                {tops[i].map((s) => (
                  <li key={s.code} className="flex items-baseline justify-between gap-3">
                    <Link
                      href={`/pml/${encodeURIComponent(s.code)}`}
                      className="min-w-0 truncate hover:opacity-70 transition"
                    >
                      {s.title}
                      {s.composer && (
                        <span style={{ color: "var(--muted)" }}> · {s.composer}</span>
                      )}
                    </Link>
                    <span className="tnum shrink-0 text-xs" style={{ color: "var(--muted)" }}>
                      {formatNumber(s.performance_count)}×
                    </span>
                  </li>
                ))}
              </ol>
            )}
          </Card>
        ))}
      </div>
    </div>
  );
}
