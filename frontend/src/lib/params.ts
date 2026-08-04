import type { EntryFilters, EntrySort, SongFilters, SongSort } from "./db";

/**
 * Filter state lives in the URL, so any view is linkable and shareable --
 * the main thing the Streamlit version could not do.
 */

export type SearchParams = { [key: string]: string | string[] | undefined };

function one(v: string | string[] | undefined): string | undefined {
  if (Array.isArray(v)) return v[0];
  return v || undefined;
}

function many(v: string | string[] | undefined): string[] | undefined {
  if (v === undefined) return undefined;
  const list = Array.isArray(v) ? v : v.split(",");
  const cleaned = list.map((s) => s.trim()).filter(Boolean);
  return cleaned.length ? cleaned : undefined;
}

function num(v: string | string[] | undefined): number | undefined {
  const s = one(v);
  if (s === undefined) return undefined;
  const n = Number(s);
  return Number.isFinite(n) ? n : undefined;
}

/**
 * `director` is opt-in: the public route must ignore `?director=` entirely, so
 * filtering by a person stays on the unlisted /blaine page. Reading it is a
 * deliberate act by the caller, not a default.
 */
export function parseEntryFilters(
  sp: SearchParams,
  opts: { director?: boolean } = {},
): EntryFilters {
  return {
    genEvent: one(sp.event),
    events: many(sp.sub),
    school: one(sp.school),
    schoolLevel: one(sp.level),
    conferences: many(sp.conf),
    classification: one(sp.class),
    song: one(sp.song),
    composer: one(sp.composer),
    yearFrom: num(sp.from),
    yearTo: num(sp.to),
    director: opts.director ? one(sp.director) : undefined,
  };
}

export function parsePage(sp: SearchParams): number {
  const n = num(sp.page) ?? 1;
  return n < 1 ? 1 : Math.floor(n);
}

export function parseSongFilters(sp: SearchParams): SongFilters {
  const acc = one(sp.acc);
  return {
    gradeFrom: num(sp.gmin),
    gradeTo: num(sp.gmax),
    eventName: one(sp.sevent),
    search: one(sp.q),
    minPerformances: num(sp.minp),
    accompaniment:
      acc === "acappella" || acc === "accompanied" ? acc : undefined,
  };
}

const ENTRY_SORTS: EntrySort[] = [
  "year",
  "school",
  "concert_final_score",
  "sight_reading_final_score",
];

/**
 * Ratings are ranks, so "best first" is ascending while "newest first" is
 * descending. Each column therefore carries its own natural direction rather
 * than defaulting every column to desc.
 */
export const ENTRY_SORT_DEFAULT_DIR: Record<EntrySort, "asc" | "desc"> = {
  year: "desc",
  school: "asc",
  concert_final_score: "asc",
  sight_reading_final_score: "asc",
};

/** Shared by the public results page and the unlisted /blaine view. */
export const ENTRY_SORT_COLUMNS: { key: EntrySort; label: string }[] = [
  { key: "year", label: "Year" },
  { key: "school", label: "School" },
  { key: "concert_final_score", label: "Concert" },
  { key: "sight_reading_final_score", label: "SR" },
];

export const ENTRY_SORT_LABELS: Record<EntrySort, string> = {
  year: "year",
  school: "school",
  concert_final_score: "concert rating",
  sight_reading_final_score: "sight-reading rating",
};

/** Ratings are ranks, so ascending is "best first" -- worth spelling out. */
export const ENTRY_SORT_DESCRIPTIONS: Record<
  EntrySort,
  Record<"asc" | "desc", string>
> = {
  year: { asc: "oldest first", desc: "newest first" },
  school: { asc: "A to Z", desc: "Z to A" },
  concert_final_score: { asc: "best first", desc: "worst first" },
  sight_reading_final_score: { asc: "best first", desc: "worst first" },
};

export function parseEntrySort(sp: SearchParams): {
  sort: EntrySort;
  dir: "asc" | "desc";
} {
  const raw = one(sp.sort);
  const sort = ENTRY_SORTS.includes(raw as EntrySort)
    ? (raw as EntrySort)
    : "year";
  const rawDir = one(sp.dir);
  const dir =
    rawDir === "asc" || rawDir === "desc"
      ? rawDir
      : ENTRY_SORT_DEFAULT_DIR[sort];
  return { sort, dir };
}

const SONG_SORTS: SongSort[] = ["song_score", "performance_count", "title", "grade"];

export function parseSongSort(sp: SearchParams): {
  sort: SongSort;
  dir: "asc" | "desc";
} {
  const raw = one(sp.sort);
  const sort = SONG_SORTS.includes(raw as SongSort)
    ? (raw as SongSort)
    : "song_score";
  const dir = one(sp.dir) === "asc" ? "asc" : "desc";
  return { sort, dir };
}

/** Merge updates into the current query string, dropping empty values. */
export function buildQuery(
  current: SearchParams | URLSearchParams,
  updates: Record<string, string | string[] | number | undefined | null>,
): string {
  const params = new URLSearchParams();

  if (current instanceof URLSearchParams) {
    current.forEach((value, key) => params.append(key, value));
  } else {
    for (const [key, value] of Object.entries(current)) {
      if (value === undefined) continue;
      if (Array.isArray(value)) value.forEach((v) => params.append(key, v));
      else params.set(key, value);
    }
  }

  for (const [key, value] of Object.entries(updates)) {
    params.delete(key);
    if (value === undefined || value === null || value === "") continue;
    if (Array.isArray(value)) {
      if (value.length) params.set(key, value.join(","));
    } else {
      params.set(key, String(value));
    }
  }

  // Any filter change resets pagination.
  if (!("page" in updates)) params.delete("page");

  const s = params.toString();
  return s ? `?${s}` : "";
}
