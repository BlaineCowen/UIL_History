import type { EntryFilters, SongFilters, SongSort } from "./db";

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

export function parseEntryFilters(sp: SearchParams): EntryFilters {
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
