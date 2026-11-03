/**
 * URL scheme for the per-event, per-grade list pages:
 *
 *   /lists                          every event and grade
 *   /lists/concert-band             one event, all grades
 *   /lists/concert-band/grade-3     one event, one grade
 *
 * They exist for search. /pml's filtered views all canonicalize to bare /pml,
 * so a query like "UIL grade 3 band list" had nothing on this site that could
 * rank for it. Not under /pml/ because /pml/[code] already owns that segment.
 *
 * Slugs are derived from the event names in the data rather than a hand-kept
 * table, so a new event gets pages without a code change.
 */

export function eventSlug(eventName: string): string {
  return eventName
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

export function gradeSlug(grade: number): string {
  return `grade-${grade}`;
}

/** `grade-3` -> 3; anything else -> undefined, so the page can 404. */
export function parseGradeSlug(slug: string): number | undefined {
  const m = /^grade-([1-6])$/.exec(slug);
  return m ? Number(m[1]) : undefined;
}

export function listHref(eventName: string, grade?: number): string {
  const base = `/lists/${eventSlug(eventName)}`;
  return grade === undefined ? base : `${base}/${gradeSlug(grade)}`;
}

/** Find the event a slug names, among the events actually in the data. */
export function eventFromSlug<T extends { event_name: string }>(
  slug: string,
  categories: T[],
): string | undefined {
  return categories.find((c) => eventSlug(c.event_name) === slug)?.event_name;
}

/** Group categories under their event, preserving the query's ordering. */
export function groupByEvent<T extends { event_name: string }>(
  categories: T[],
): { eventName: string; grades: T[] }[] {
  const map = new Map<string, T[]>();
  for (const c of categories) {
    const list = map.get(c.event_name) ?? [];
    list.push(c);
    map.set(c.event_name, list);
  }
  return [...map].map(([eventName, grades]) => ({ eventName, grades }));
}
