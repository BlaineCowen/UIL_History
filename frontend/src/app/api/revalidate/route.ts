import { revalidateTag } from "next/cache";
import { NextResponse } from "next/server";
import { CONTEST_DATA_TAG } from "@/lib/db";

/**
 * Drop the cached contest data after a load.
 *
 * Every read is cached under CONTEST_DATA_TAG for a day, which is what keeps
 * the database from being scanned on every request. The cost is that a fresh
 * load is invisible until the cache is dropped -- and restarting the server
 * does *not* do it, because Next's data cache lives in `.next/cache` and
 * survives restarts. That is how a load of 173,600 entries sat behind a
 * homepage still reporting 159,451.
 *
 *   curl -X POST https://<host>/api/revalidate -H "x-revalidate-secret: ..."
 *
 * Set REVALIDATE_SECRET in the environment. Without it the route refuses to
 * run rather than defaulting to open: this is a public site, and an
 * unauthenticated cache-drop is a free way to make every request hit Postgres.
 */
export async function POST(request: Request) {
  const secret = process.env.REVALIDATE_SECRET;
  if (!secret) {
    return NextResponse.json(
      { error: "REVALIDATE_SECRET is not configured" },
      { status: 503 },
    );
  }

  const provided = request.headers.get("x-revalidate-secret");
  // Length check first so the comparison below is on equal-length strings.
  if (!provided || provided.length !== secret.length || provided !== secret) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  // `{ expire: 0 }` rather than the usually-recommended profile "max".
  // "max" is stale-while-revalidate: the next visitor still sees the previous
  // season while fresh data loads behind them. This endpoint is only ever
  // called because the data just changed, so serving the old numbers once more
  // defeats the point. The cost is one blocking revalidate on the next
  // request, which is fine for a once-a-year load.
  revalidateTag(CONTEST_DATA_TAG, { expire: 0 });
  return NextResponse.json({
    revalidated: CONTEST_DATA_TAG,
    at: new Date().toISOString(),
  });
}

/** GET exists only to make a misconfigured deploy obvious. */
export async function GET() {
  return NextResponse.json(
    { error: "Use POST with the x-revalidate-secret header" },
    { status: 405 },
  );
}
