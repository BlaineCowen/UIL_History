/**
 * Canonical origin for metadata, sitemap and structured data.
 *
 * Set NEXT_PUBLIC_SITE_URL when a custom domain is attached -- otherwise
 * Vercel's own production URL is used, and failing that the deploy URL. Getting
 * this wrong is quietly expensive: canonical tags and sitemap entries pointing
 * at a preview host tell Google the preview is the real site.
 */
export const SITE_URL = (
  process.env.NEXT_PUBLIC_SITE_URL ||
  (process.env.VERCEL_PROJECT_PRODUCTION_URL
    ? `https://${process.env.VERCEL_PROJECT_PRODUCTION_URL}`
    : "https://uil-history.vercel.app")
).replace(/\/$/, "");

export const SITE_NAME = "UIL History";

/**
 * What people actually type. The searches worth ranking for are "UIL PML",
 * "Texas UIL prescribed music list", and lookups of a specific piece, so those
 * phrases belong in the titles rather than a house style like "Songs".
 */
export const SITE_DESCRIPTION =
  "Search the Texas UIL Prescribed Music List (PML) and two decades of " +
  "Concert & Sight-Reading contest results — every piece, its grade, how " +
  "often it has been performed, and how those performances scored.";

export function canonical(path: string): string {
  return `${SITE_URL}${path.startsWith("/") ? path : `/${path}`}`;
}
