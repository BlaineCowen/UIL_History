import type { MetadataRoute } from "next";
import { getSongs } from "@/lib/db";
import { SITE_URL } from "@/lib/site";

/**
 * Without this, the 8,392 song pages are only reachable by crawling 168 pages
 * of pagination -- which crawlers do slowly and incompletely. Listing them
 * directly is the difference between a handful being indexed and all of them.
 *
 * 8,392 URLs is well inside the 50,000-per-file limit, so one file suffices.
 * /blaine is deliberately absent: it is unlisted and carries noindex.
 */
export const revalidate = 86400;

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const staticRoutes: MetadataRoute.Sitemap = [
    { url: SITE_URL, changeFrequency: "monthly", priority: 1 },
    { url: `${SITE_URL}/pml`, changeFrequency: "monthly", priority: 0.9 },
    { url: `${SITE_URL}/about`, changeFrequency: "yearly", priority: 0.3 },
  ];

  // Ask for every song in one page rather than paginating.
  const songs = await getSongs({ includeDelisted: true }, "performance_count", "desc", 20000, 0);

  return [
    ...staticRoutes,
    ...songs.map((s) => ({
      url: `${SITE_URL}/pml/${encodeURIComponent(s.code)}`,
      changeFrequency: "yearly" as const,
      // Pieces with contest history are the ones worth crawling first.
      priority: s.performance_count > 0 ? 0.7 : 0.4,
    })),
  ];
}
