import {
  ChartSkeleton,
  FiltersSkeleton,
  PageHeaderSkeleton,
  StatTilesSkeleton,
  TableSkeleton,
} from "@/components/skeletons";

export default function Loading() {
  return (
    <div className="grid gap-6">
      <PageHeaderSkeleton />
      <FiltersSkeleton />
      <StatTilesSkeleton />
      <div className="grid gap-4 lg:grid-cols-2">
        <ChartSkeleton />
        <ChartSkeleton />
      </div>
      <ChartSkeleton heightClass="h-[220px] sm:h-[260px]" />
      <TableSkeleton rows={8} cols={8} />
    </div>
  );
}
