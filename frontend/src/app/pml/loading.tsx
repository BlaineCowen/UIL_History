import {
  ChartSkeleton,
  FiltersSkeleton,
  PageHeaderSkeleton,
  TableSkeleton,
} from "@/components/skeletons";

export default function Loading() {
  return (
    <div className="grid gap-6">
      <PageHeaderSkeleton />
      <FiltersSkeleton />
      <ChartSkeleton heightClass="h-[300px] sm:h-[340px]" />
      <TableSkeleton rows={8} cols={7} />
    </div>
  );
}
