import {
  ChartSkeleton,
  Line,
  PageHeaderSkeleton,
  StatTilesSkeleton,
  TableSkeleton,
} from "@/components/skeletons";

/**
 * Shown the instant a song link is clicked. This route runs six queries --
 * summary, yearly, performances, share, top schools, and the song itself --
 * so without this the browser sat on the old page with no feedback at all.
 */
export default function Loading() {
  return (
    <div className="grid gap-6">
      <Line w={90} h={13} />
      <div className="grid gap-3">
        <div className="flex flex-wrap gap-2">
          <Line w={110} h={22} className="rounded-full" />
          <Line w={78} h={22} className="rounded-full" />
          <Line w={96} h={22} className="rounded-full" />
        </div>
        <PageHeaderSkeleton lines={2} />
      </div>

      <StatTilesSkeleton />

      <div className="grid gap-4 lg:grid-cols-2">
        <ChartSkeleton heightClass="h-[190px] sm:h-[220px]" />
        <ChartSkeleton heightClass="h-[190px] sm:h-[220px]" />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        {[0, 1].map((i) => (
          <div
            key={i}
            className="rounded-xl border p-4 sm:p-5 grid gap-3"
            style={{ background: "var(--surface)" }}
          >
            <Line w={170} h={15} />
            <Line w={240} h={11} />
            <Line w="100%" h={10} className="rounded-full" />
          </div>
        ))}
      </div>

      <TableSkeleton rows={6} cols={7} />
    </div>
  );
}
