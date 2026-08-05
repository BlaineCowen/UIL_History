import {
  FiltersSkeleton,
  PageHeaderSkeleton,
  StatTilesSkeleton,
  TableSkeleton,
} from "@/components/skeletons";

export default function Loading() {
  return (
    <div className="grid gap-6">
      <PageHeaderSkeleton lines={3} />
      <FiltersSkeleton />
      <StatTilesSkeleton />
      <TableSkeleton rows={8} cols={8} />
    </div>
  );
}
