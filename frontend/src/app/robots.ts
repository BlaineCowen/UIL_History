import type { MetadataRoute } from "next";
import { SITE_URL } from "@/lib/site";

/**
 * /blaine is intentionally NOT listed as a Disallow. A robots.txt is public,
 * so a disallow rule publishes the path to anyone who reads the file -- the
 * opposite of unlisted. It carries a noindex robots meta tag instead, which
 * keeps it out of results without announcing it.
 *
 * Filtered and sorted views are excluded by query string: they are
 * near-duplicates of /pml and would otherwise compete with it.
 */
export default function robots(): MetadataRoute.Robots {
  return {
    rules: {
      userAgent: "*",
      allow: "/",
      disallow: ["/api/"],
    },
    sitemap: `${SITE_URL}/sitemap.xml`,
  };
}
