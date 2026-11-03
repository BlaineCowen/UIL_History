import Link from "next/link";
import type { Metadata } from "next";
import { getSongCategories } from "@/lib/db";
import { formatNumber } from "@/lib/format";
import { groupByEvent, listHref } from "@/lib/lists";
import { OG_IMAGE } from "@/lib/site";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { Card } from "@/components/ui";

const TITLE = "UIL Prescribed Music List by Event and Grade";
const DESCRIPTION =
  "Browse the Texas UIL Prescribed Music List one event and grade at a time — " +
  "concert band, full and string orchestra, mixed, treble and tenor-bass " +
  "chorus — with how often each piece is played at contest and how it scores.";

export const metadata: Metadata = {
  title: TITLE,
  description: DESCRIPTION,
  alternates: { canonical: "/lists" },
  openGraph: { title: TITLE, description: DESCRIPTION, url: "/lists", images: OG_IMAGE },
};

// Rendered per request rather than at build time, for the same reason as
// sitemap.ts: prerendering would make every deploy depend on a reachable
// database. The query itself is cached, so a request costs a render, not a scan.
export const dynamic = "force-dynamic";

export default async function ListsPage() {
  const events = groupByEvent(await getSongCategories());

  return (
    <div className="grid gap-6">
      <Breadcrumbs
        crumbs={[
          { name: "Home", path: "/" },
          { name: "Lists", path: "/lists" },
        ]}
      />
      <header className="grid gap-2">
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          UIL music lists by event and grade
        </h1>
        <p className="text-sm max-w-2xl" style={{ color: "var(--ink-2)" }}>
          Every grade of every large-ensemble event on the Prescribed Music List,
          ranked by how often schools actually perform each piece at Concert &amp;
          Sight-Reading. To search across all of them at once, use the{" "}
          <Link href="/pml" className="underline underline-offset-2 hover:no-underline">
            full PML
          </Link>
          .
        </p>
      </header>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {events.map(({ eventName, grades }) => (
          <Card key={eventName}>
            <h2 className="text-[15px] font-semibold tracking-tight mb-3">
              <Link href={listHref(eventName)} className="hover:opacity-70 transition">
                {eventName}
              </Link>
            </h2>
            <ul className="grid gap-1.5 text-sm">
              {grades.map((g) => (
                <li key={g.grade}>
                  <Link
                    href={listHref(eventName, g.grade)}
                    className="flex items-baseline justify-between gap-3 rounded-md px-2 py-1 -mx-2 transition hover:bg-[var(--surface-2)]"
                  >
                    <span>Grade {g.grade}</span>
                    <span className="tnum text-xs" style={{ color: "var(--muted)" }}>
                      {formatNumber(g.current)} pieces
                    </span>
                  </Link>
                </li>
              ))}
            </ul>
          </Card>
        ))}
      </div>
    </div>
  );
}
