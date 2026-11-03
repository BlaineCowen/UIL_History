import type { MetadataRoute } from "next";
import { getSongCategories, getSongs } from "@/lib/db";
import { groupByEvent, listHref } from "@/lib/lists";
import { SITE_URL } from "@/lib/site";

/**
 * Without this, the 8,392 song pages are only reachable by crawling 168 pages
 * of pagination -- which crawlers do slowly and incompletely. Listing them
 * directly is the difference between a handful being indexed and all of them.
 *
 * 8,392 URLs is well inside the 50,000-per-file limit, so one file suffices.
 * /blaine is deliberately absent: it is unlisted and carries noindex.
 */
// Generated on request, not at build time. Next prerenders sitemap.xml by
// default, which made the whole build depend on a reachable database -- and a
// build without one failed outright ("Export encountered an error on
// /sitemap.xml"). A sitemap is crawled rarely, so paying for it per request
// (cached for a day) is the right trade against blocking every deploy.
export const dynamic = "force-dynamic";
export const revalidate = 86400;

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const staticRoutes: MetadataRoute.Sitemap = [
    { url: SITE_URL, changeFrequency: "monthly", priority: 1 },
    { url: `${SITE_URL}/pml`, changeFrequency: "monthly", priority: 0.9 },
    { url: `${SITE_URL}/about`, changeFrequency: "yearly", priority: 0.3 },
  ];

  // Never let a database problem take the sitemap -- or a deploy -- down.
  // An incomplete sitemap is a minor SEO setback; a failed build is total.
  let songs: Awaited<ReturnType<typeof getSongs>> = [];
  let categories: Awaited<ReturnType<typeof getSongCategories>> = [];
  try {
    [songs, categories] = await Promise.all([
      getSongs({}, "performance_count", "desc", 20000, 0),
      getSongCategories(),
    ]);
  } catch (err) {
    console.error("[sitemap] song query failed, serving static routes only", err);
  }

  // The per-grade lists are the pages built to rank for "UIL grade N <event>"
  // searches, so they sit just below /pml rather than with the song pages.
  const listRoutes: MetadataRoute.Sitemap = groupByEvent(
    categories.filter((c) => c.current > 0),
  ).flatMap(({ eventName, grades }) => [
    { url: `${SITE_URL}${listHref(eventName)}`, changeFrequency: "yearly" as const, priority: 0.8 },
    ...grades.map((g) => ({
      url: `${SITE_URL}${listHref(eventName, g.grade)}`,
      changeFrequency: "yearly" as const,
      priority: 0.8,
    })),
  ]);

  return [
    ...staticRoutes,
    ...(listRoutes.length
      ? [{ url: `${SITE_URL}/lists`, changeFrequency: "yearly" as const, priority: 0.8 }]
      : []),
    ...listRoutes,
    ...songs.map((s) => ({
      url: `${SITE_URL}/pml/${encodeURIComponent(s.code)}`,
      changeFrequency: "yearly" as const,
      // Pieces with contest history are the ones worth crawling first.
      priority: s.performance_count > 0 ? 0.7 : 0.4,
    })),
  ];
}
