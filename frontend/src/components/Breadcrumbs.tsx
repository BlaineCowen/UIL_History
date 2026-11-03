import Link from "next/link";
import { ChevronRight } from "lucide-react";
import { BreadcrumbStructuredData } from "@/components/StructuredData";

/**
 * Visible trail plus its JSON-LD twin, from one list so the two cannot drift.
 * The last crumb is the current page and is not a link.
 */
export function Breadcrumbs({ crumbs }: { crumbs: { name: string; path: string }[] }) {
  return (
    <nav aria-label="Breadcrumb">
      <BreadcrumbStructuredData crumbs={crumbs} />
      <ol className="flex flex-wrap items-center gap-1 text-sm" style={{ color: "var(--ink-2)" }}>
        {crumbs.map((c, i) => {
          const last = i === crumbs.length - 1;
          return (
            <li key={c.path} className="inline-flex items-center gap-1">
              {last ? (
                <span aria-current="page" style={{ color: "var(--ink)" }}>
                  {c.name}
                </span>
              ) : (
                <>
                  <Link href={c.path} className="transition hover:opacity-70">
                    {c.name}
                  </Link>
                  <ChevronRight size={14} aria-hidden style={{ color: "var(--muted)" }} />
                </>
              )}
            </li>
          );
        })}
      </ol>
    </nav>
  );
}
