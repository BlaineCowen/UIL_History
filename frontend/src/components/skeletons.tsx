/**
 * Loading placeholders.
 *
 * Every data route is dynamically rendered, so navigation blocks on the server
 * with nothing on screen -- clicking a song from /pml looked like nothing had
 * happened. A `loading.tsx` per segment lets Next paint these instantly while
 * the page renders behind them.
 *
 * They deliberately mirror the real layouts (same tile grid, same table
 * shape, same heights) so the content does not jump when it swaps in.
 */

export function Line({
  w = "100%",
  h = 14,
  className = "",
}: {
  w?: string | number;
  h?: number;
  className?: string;
}) {
  return (
    <span
      className={`skeleton block ${className}`}
      style={{ width: typeof w === "number" ? `${w}px` : w, height: h }}
      aria-hidden
    />
  );
}

/** Matches the four StatTiles on the results and song pages. */
export function StatTilesSkeleton({ count = 4 }: { count?: number }) {
  return (
    <div className="grid gap-3 grid-cols-2 lg:grid-cols-4">
      {Array.from({ length: count }, (_, i) => (
        <div
          key={i}
          className="rounded-xl border p-4 grid gap-2"
          style={{ background: "var(--surface)" }}
        >
          <Line w={72} h={10} />
          <Line w={96} h={26} />
          <Line w={110} h={10} />
        </div>
      ))}
    </div>
  );
}

export function ChartSkeleton({ heightClass = "h-[210px] sm:h-[260px]" }) {
  return (
    <div
      className="rounded-xl border p-4 sm:p-5"
      style={{ background: "var(--surface)" }}
    >
      <div className="grid gap-2 mb-3">
        <Line w={180} h={15} />
        <Line w={240} h={11} />
      </div>
      <div className={`${heightClass} skeleton`} />
    </div>
  );
}

/**
 * Table on sm and up, stacked cards below -- the same split TableScroll and
 * DataList use, so the skeleton matches whichever the viewport will show.
 */
export function TableSkeleton({ rows = 8, cols = 6 }: { rows?: number; cols?: number }) {
  return (
    <div
      className="rounded-xl border p-4 sm:p-5 grid gap-4"
      style={{ background: "var(--surface)" }}
    >
      <div className="grid gap-2">
        <Line w={120} h={15} />
        <Line w={260} h={11} />
      </div>

      <div className="hidden sm:grid gap-2.5">
        <div className="flex gap-4">
          {Array.from({ length: cols }, (_, i) => (
            <Line key={i} w={i === 2 ? "22%" : "12%"} h={10} />
          ))}
        </div>
        {Array.from({ length: rows }, (_, r) => (
          <div key={r} className="flex gap-4 items-center">
            {Array.from({ length: cols }, (_, i) => (
              <Line key={i} w={i === 2 ? "22%" : "12%"} h={13} />
            ))}
          </div>
        ))}
      </div>

      <ul className="grid gap-2 sm:hidden">
        {Array.from({ length: Math.min(rows, 5) }, (_, r) => (
          <li
            key={r}
            className="rounded-xl border p-3.5 grid gap-2"
            style={{ background: "var(--surface-2)" }}
          >
            <Line w="70%" h={14} />
            <Line w="45%" h={11} />
            <Line w="60%" h={11} />
          </li>
        ))}
      </ul>
    </div>
  );
}

export function FiltersSkeleton() {
  return (
    <div
      className="rounded-xl border p-4 sm:p-5 flex flex-wrap items-center gap-3"
      style={{ background: "var(--surface)" }}
    >
      <Line w={220} h={38} className="rounded-lg" />
      <Line w={110} h={38} className="rounded-lg" />
    </div>
  );
}

export function PageHeaderSkeleton({ lines = 2 }: { lines?: number }) {
  return (
    <div className="grid gap-2">
      <Line w="55%" h={30} />
      {Array.from({ length: lines - 1 }, (_, i) => (
        <Line key={i} w="80%" h={13} />
      ))}
    </div>
  );
}
