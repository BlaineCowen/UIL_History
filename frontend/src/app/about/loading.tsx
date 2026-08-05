/**
 * Deliberately empty.
 *
 * `app/loading.tsx` covers every nested segment that lacks its own, so without
 * this file /about briefly rendered the results page's skeleton -- stat tiles
 * and charts it does not have. /about is statically prerendered and arrives
 * immediately, so the honest loading state is nothing at all.
 */
export default function Loading() {
  return null;
}
