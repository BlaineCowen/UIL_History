import "server-only";

import postgres from "postgres";
import { unstable_cache } from "next/cache";

/**
 * All database access lives here. Pages call the exported functions and never
 * write SQL, which is what made the move off SQLite a single-file change.
 *
 * Ported from better-sqlite3. Several differences are behavioural rather than
 * syntactic and are called out at their call sites:
 *   - SQLite's LIKE is case-insensitive for ASCII; Postgres' is not (ILIKE is).
 *   - Postgres folds unquoted identifiers to lower case, so camelCase aliases
 *     must be double-quoted or they come back as `avgconcert`.
 *   - COUNT/SUM return int8 and AVG returns numeric, both of which postgres.js
 *     hands back as *strings* to avoid precision loss. Every aggregate is cast.
 *   - Postgres rejects bare columns that are not in GROUP BY, and does not
 *     allow output aliases in HAVING.
 */

declare global {
  var __uilSql: ReturnType<typeof postgres> | undefined;
}

function connect() {
  /**
   * Vercel's Supabase integration injects POSTGRES_URL rather than
   * DATABASE_URL, so accept either and save anyone the duplicate variable.
   *
   * Order matters: POSTGRES_URL is the *pooled* string (port 6543), which is
   * the one serverless wants. POSTGRES_URL_NON_POOLING (5432) is deliberately
   * not consulted -- it is for migrations and bulk loads, and using it here
   * would exhaust connections. DATABASE_URL wins when set explicitly.
   */
  const url = process.env.DATABASE_URL || process.env.POSTGRES_URL;
  if (!url) {
    throw new Error(
      "No database URL. Set DATABASE_URL (or POSTGRES_URL) in the Vercel " +
        "project's Environment Variables. Use Supabase's pooled string, " +
        "port 6543 -- the direct 5432 connection will exhaust connections " +
        "under serverless.",
    );
  }
  if (/:5432\//.test(url)) {
    // Loud, because the failure mode is intermittent connection exhaustion
    // under load rather than an obvious error in development.
    console.warn(
      "[db] Connected on port 5432 (direct). Serverless should use the " +
        "pooled connection on 6543.",
    );
  }
  return postgres(url, {
    /**
     * Small on purpose. This pool is per serverless instance, and Vercel runs
     * many of them, so the ceiling that matters is instances x max against the
     * pooler's client limit -- not throughput within one instance. Reads are
     * cached anyway, so most invocations never open a connection at all.
     */
    max: Number(process.env.DATABASE_POOL_MAX ?? 3),
    idle_timeout: 20,
    // Required for transaction-mode poolers (Supabase's port 6543). Named
    // prepared statements do not survive a pooler handing you a new session.
    prepare: false,
    onnotice: () => {},
  });
}

// Next.js dev reloads modules on every edit; without a global the process
// accumulates connection pools.
export function getSql() {
  if (!global.__uilSql) global.__uilSql = connect();
  return global.__uilSql;
}

/**
 * The queries below build WHERE clauses with `?` placeholders, which Postgres
 * does not accept. No query contains a literal `?`, so a positional rewrite is
 * safe and keeps the clause builders readable.
 */
function toPositional(text: string): string {
  let i = 0;
  return text.replace(/\?/g, () => `$${++i}`);
}

async function rows<T>(text: string, params: unknown[] = []): Promise<T[]> {
  const result = await getSql().unsafe(toPositional(text), params as never[]);
  return result as unknown as T[];
}

async function first<T>(text: string, params: unknown[] = []): Promise<T> {
  const result = await rows<T>(text, params);
  return result[0];
}

/* ----------------------------------------------------------------- cache */

/**
 * The contest data is immutable between seasons, so every read below is
 * cacheable until the loader next runs.
 *
 * This is not an optimisation, it is a capacity requirement: an uncached
 * unfiltered "/" runs eight queries that each scan the whole 159k-row table,
 * measured at ~139ms of pure database CPU -- about 7 views/sec on one core.
 * Under public traffic that saturates the database long before anything else
 * gives.
 *
 * Only aggregates and option lists are wrapped. Paginated row fetches are
 * index-served and cheap, and caching them would multiply entries across every
 * page x filter combination for little gain.
 *
 * `unstable_cache` is deprecated in Next 16 in favour of the `use cache`
 * directive, but that requires enabling Cache Components app-wide, which
 * changes rendering semantics for every route that reads searchParams. Worth
 * migrating deliberately, not as a side effect of this.
 */
export const CONTEST_DATA_TAG = "contest-data";

/** A day is arbitrary; `revalidateTag(CONTEST_DATA_TAG)` after a load is the real invalidation. */
const CACHE_SECONDS = 86_400;

function cached<A extends unknown[], R>(
  name: string,
  fn: (...args: A) => Promise<R>,
): (...args: A) => Promise<R> {
  return unstable_cache(fn, [name], {
    tags: [CONTEST_DATA_TAG],
    revalidate: CACHE_SECONDS,
  });
}

/* ------------------------------------------------------------------ types */

export type EntryFilters = {
  genEvent?: string;
  events?: string[];
  school?: string;
  schoolLevel?: string;
  conferences?: string[];
  classification?: string;
  song?: string;
  composer?: string;
  yearFrom?: number;
  yearTo?: number;
  /**
   * Only ever set by the unlisted /blaine route -- parseEntryFilters ignores
   * the URL parameter unless explicitly asked for it.
   */
  director?: string;
};

export type Entry = {
  entry_number: string;
  year: number;
  contest_date: string;
  event: string;
  gen_event: string;
  school: string;
  city: string;
  conference: string;
  classification: string;
  school_level: string;
  director: string;
  additional_director: string;
  concert_final_score: number;
  sight_reading_final_score: number;
  title_1: string;
  title_2: string;
  title_3: string;
  composer_1: string;
  composer_2: string;
  composer_3: string;
  code_1: string;
  code_2: string;
  code_3: string;
};

/**
 * An entry plus the three individual panel scores per discipline. The stored
 * final is the median of its three judges on all but 25 rows, so keep showing
 * the stored value rather than recomputing it.
 */
export type EntryWithJudges = Entry & {
  concert_score_1: number;
  concert_score_2: number;
  concert_score_3: number;
  sight_reading_score_1: number;
  sight_reading_score_2: number;
  sight_reading_score_3: number;
  /**
   * Which field the director search hit, present only when one is active.
   * Computed by the same ILIKE the WHERE clause uses, so the label can never
   * contradict why the row was returned. Postgres yields a real boolean here
   * where SQLite yielded 0/1.
   */
  matched_director?: boolean | null;
  matched_additional?: boolean | null;
};

/** How a filtered director appears across the matching entries. */
export type DirectorRoles = { main: number; additional: number; both: number };

export type Song = {
  code: string;
  event_name: string;
  title: string;
  composer: string;
  arranger: string;
  publisher: string;
  grade: number;
  specification: string;
  performance_count: number;
  average_concert_score: number;
  average_sight_reading_score: number;
  song_score: number;
  earliest_year: string;
  /** 0 when the piece has left the PML -- it keeps its history either way. */
  on_current_pml: number;
  /** Its grade in the previous edition, when that differs from now. */
  previous_grade: number | null;
  /** Which edition it changed from, so the note can say when. */
  grade_changed_from: string | null;
};

/* --------------------------------------------------------------- helpers */

/** Normalize user input the same way the search columns were normalized. */
export function normalizeSearch(input: string): string {
  return input.toLowerCase().replace(/[^a-z0-9]/g, "");
}

type Where = { sql: string; params: unknown[] };

function buildWhere(f: EntryFilters): Where {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (f.genEvent) {
    clauses.push("gen_event = ?");
    params.push(f.genEvent);
  }
  if (f.events?.length) {
    clauses.push(`event IN (${f.events.map(() => "?").join(",")})`);
    params.push(...f.events);
  }
  if (f.school) {
    // school_search is pre-normalized to lowercase alphanumerics, so plain
    // LIKE is correct here and lets the trigram index do the work.
    clauses.push("school_search LIKE ?");
    params.push(`%${normalizeSearch(f.school)}%`);
  }
  if (f.schoolLevel) {
    clauses.push("school_level = ?");
    params.push(f.schoolLevel);
  }
  if (f.conferences?.length) {
    clauses.push(`conference IN (${f.conferences.map(() => "?").join(",")})`);
    params.push(...f.conferences);
  }
  if (f.classification) {
    clauses.push("classification = ?");
    params.push(f.classification);
  }
  if (f.song) {
    clauses.push("song_concat LIKE ?");
    params.push(`%${normalizeSearch(f.song)}%`);
  }
  if (f.composer) {
    clauses.push("composer_concat LIKE ?");
    params.push(`%${normalizeSearch(f.composer)}%`);
  }
  if (f.yearFrom !== undefined) {
    clauses.push("year >= ?");
    params.push(f.yearFrom);
  }
  if (f.yearTo !== undefined) {
    clauses.push("year <= ?");
    params.push(f.yearTo);
  }
  if (f.director) {
    // Both fields: 55,730 rows name an additional director, and someone listed
    // there was still on the podium. ILIKE, not LIKE -- these are raw names,
    // and SQLite's LIKE was case-insensitive where Postgres' is not.
    clauses.push("(director ILIKE ? OR additional_director ILIKE ?)");
    const term = `%${f.director.trim()}%`;
    params.push(term, term);
  }

  return {
    sql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    params,
  };
}

/* --------------------------------------------------------------- queries */

async function _getYearBounds(): Promise<{ min: number; max: number }> {
  return first<{ min: number; max: number }>(
    "SELECT MIN(year) AS min, MAX(year) AS max FROM entries",
  );
}

async function _getFilterOptions(genEvent?: string) {
  const where = genEvent ? "WHERE gen_event = ?" : "";
  const args = genEvent ? [genEvent] : [];

  const [events, levels, classifications] = await Promise.all([
    rows<{ event: string; n: number }>(
      `SELECT event, COUNT(*)::int AS n FROM entries ${where} GROUP BY event ORDER BY event`,
      args,
    ),
    rows<{ school_level: string; n: number }>(
      `SELECT school_level, COUNT(*)::int AS n FROM entries ${where}
       ${where ? "AND" : "WHERE"} school_level != ''
       GROUP BY school_level ORDER BY school_level`,
      args,
    ),
    rows<{ classification: string; n: number }>(
      `SELECT classification, COUNT(*)::int AS n FROM entries ${where}
       ${where ? "AND" : "WHERE"} classification != ''
       GROUP BY classification ORDER BY classification`,
      args,
    ),
  ]);

  return { events, levels, classifications };
}

/** Conferences available for a level -- the list differs wildly between HS and MS. */
async function _getConferences(genEvent?: string, schoolLevel?: string) {
  const clauses: string[] = ["conference != ''"];
  const params: unknown[] = [];
  if (genEvent) {
    clauses.push("gen_event = ?");
    params.push(genEvent);
  }
  if (schoolLevel) {
    clauses.push("school_level = ?");
    params.push(schoolLevel);
  }
  // HAVING repeats the aggregate: SQLite accepted the output alias `n` here,
  // Postgres does not.
  return rows<{ conference: string; n: number }>(
    `SELECT conference, COUNT(*)::int AS n FROM entries
     WHERE ${clauses.join(" AND ")}
     GROUP BY conference HAVING COUNT(*) > 20
     ORDER BY length(conference), conference`,
    params,
  );
}

async function _countEntries(f: EntryFilters): Promise<number> {
  const { sql, params } = buildWhere(f);
  const row = await first<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM entries ${sql}`,
    params,
  );
  return row.n;
}

export type EntrySort =
  | "year"
  | "school"
  | "concert_final_score"
  | "sight_reading_final_score";

/**
 * Sort expressions, not bare columns. `school` needs both halves because the
 * scraped values are neither trimmed nor consistently cased: without btrim the
 * handful of " Sam Houston Middle School" rows sort ahead of everything, and
 * without lower() "west Brook High School" lands after every capitalised name.
 * There is a matching expression index, so this still uses an index.
 */
const ENTRY_ORDER: Record<EntrySort, string> = {
  year: "year",
  school: "lower(btrim(school))",
  concert_final_score: "concert_final_score",
  sight_reading_final_score: "sight_reading_final_score",
};

const ENTRY_SELECT = `entry_number, year, contest_date, event, gen_event, school, city,
       conference, classification, school_level, director, additional_director,
       concert_final_score, sight_reading_final_score,
       title_1, title_2, title_3, composer_1, composer_2, composer_3,
       code_1, code_2, code_3`;

/** The individual panel scores. Only the unlisted /blaine route selects these. */
const JUDGE_SELECT = `concert_score_1, concert_score_2, concert_score_3,
       sight_reading_score_1, sight_reading_score_2, sight_reading_score_3`;

async function entriesQuery<T>(
  columns: string,
  f: EntryFilters,
  sort: EntrySort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
  /** Bound before the WHERE params -- placeholders inside `columns`. */
  selectParams: unknown[] = [],
): Promise<T[]> {
  const { sql, params } = buildWhere(f);
  // Both sides come from the whitelisting EntrySort type, not user input.
  const order = `${ENTRY_ORDER[sort]} ${dir === "asc" ? "ASC" : "DESC"}`;
  return rows<T>(
    `SELECT ${columns}
     FROM entries ${sql}
     ORDER BY ${order} NULLS LAST, year DESC, ${ENTRY_ORDER.school} ASC,
              entry_number ASC
     LIMIT ? OFFSET ?`,
    [...selectParams, ...params, limit, offset],
  );
}

/** The `%term%` the director filter matches with, or null when it is off. */
function directorTerm(f: EntryFilters): string | null {
  return f.director ? `%${f.director.trim()}%` : null;
}

async function _getEntries(
  f: EntryFilters,
  sort: EntrySort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
): Promise<Entry[]> {
  return entriesQuery<Entry>(ENTRY_SELECT, f, sort, dir, limit, offset);
}

async function _getEntriesWithJudges(
  f: EntryFilters,
  sort: EntrySort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
): Promise<EntryWithJudges[]> {
  const term = directorTerm(f);
  const matchSelect = term
    ? `, (director ILIKE ?) AS matched_director,
         (additional_director ILIKE ?) AS matched_additional`
    : "";
  return entriesQuery<EntryWithJudges>(
    `${ENTRY_SELECT}, ${JUDGE_SELECT}${matchSelect}`,
    f,
    sort,
    dir,
    limit,
    offset,
    term ? [term, term] : [],
  );
}

/**
 * Split the matching entries by the role the searched director held. `both`
 * counts entries naming them in each field, so it is a subset of the other
 * two rather than a third bucket.
 */
async function _countDirectorRoles(
  f: EntryFilters,
): Promise<DirectorRoles | null> {
  const term = directorTerm(f);
  if (!term) return null;
  const { sql, params } = buildWhere(f);
  const row = await first<{
    main: number | null;
    additional: number | null;
    both: number | null;
  }>(
    `SELECT
       SUM(CASE WHEN director ILIKE ? THEN 1 ELSE 0 END)::int AS main,
       SUM(CASE WHEN additional_director ILIKE ? THEN 1 ELSE 0 END)::int AS additional,
       SUM(CASE WHEN director ILIKE ?
                 AND additional_director ILIKE ? THEN 1 ELSE 0 END)::int AS both
     FROM entries ${sql}`,
    [term, term, term, term, ...params],
  );
  return {
    main: row.main ?? 0,
    additional: row.additional ?? 0,
    both: row.both ?? 0,
  };
}

export type Summary = {
  total: number;
  avgConcert: number | null;
  avgSight: number | null;
  schools: number;
  sweepstakes: number;
};

/** Headline numbers for the stat tiles. */
async function _getSummary(f: EntryFilters): Promise<Summary> {
  const { sql, params } = buildWhere(f);
  // Aliases are double-quoted: Postgres would otherwise fold them to
  // `avgconcert` and every caller would read undefined.
  return first<Summary>(
    `SELECT COUNT(*)::int AS total,
            AVG(concert_final_score)::float8 AS "avgConcert",
            AVG(sight_reading_final_score)::float8 AS "avgSight",
            COUNT(DISTINCT school_search)::int AS schools,
            SUM(CASE WHEN concert_final_score = 1 AND sight_reading_final_score = 1
                     THEN 1 ELSE 0 END)::int AS sweepstakes
     FROM entries ${sql}`,
    params,
  );
}

export type YearPoint = { year: number; concert: number; sight: number; n: number };

async function _getScoresByYear(f: EntryFilters): Promise<YearPoint[]> {
  const { sql, params } = buildWhere(f);
  return rows<YearPoint>(
    `SELECT year,
            ROUND(AVG(concert_final_score), 3)::float8 AS concert,
            ROUND(AVG(sight_reading_final_score), 3)::float8 AS sight,
            COUNT(*)::int AS n
     FROM entries ${sql}
     GROUP BY year ORDER BY year`,
    params,
  );
}

export type Distribution = { score: number; concert: number; sight: number };

/** Counts of each rating 1-5 for both score types, in one pass. */
async function _getDistribution(f: EntryFilters): Promise<Distribution[]> {
  const { sql, params } = buildWhere(f);
  const [concert, sight] = await Promise.all([
    rows<{ score: number; n: number }>(
      `SELECT concert_final_score AS score, COUNT(*)::int AS n FROM entries ${sql}
       GROUP BY score ORDER BY score`,
      params,
    ),
    rows<{ score: number; n: number }>(
      `SELECT sight_reading_final_score AS score, COUNT(*)::int AS n FROM entries ${sql}
       GROUP BY score ORDER BY score`,
      params,
    ),
  ]);

  const byScore = new Map<number, Distribution>();
  for (let s = 1; s <= 5; s++) byScore.set(s, { score: s, concert: 0, sight: 0 });
  for (const r of concert) {
    const row = byScore.get(r.score);
    if (row) row.concert = r.n;
  }
  for (const r of sight) {
    const row = byScore.get(r.score);
    if (row) row.sight = r.n;
  }
  return [...byScore.values()];
}

/* ----------------------------------------------------------------- songs */

export type SongFilters = {
  gradeFrom?: number;
  gradeTo?: number;
  eventName?: string;
  search?: string;
  minPerformances?: number;
  accompaniment?: "accompanied" | "acappella";
  /**
   * Delisted pieces are excluded by default: a director browsing for
   * repertoire cannot programme them. They stay reachable by URL, and by
   * setting this, because their contest history is still worth reading.
   */
  includeDelisted?: boolean;
};

function buildSongWhere(f: SongFilters): Where {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (f.gradeFrom !== undefined) {
    clauses.push("grade >= ?");
    params.push(f.gradeFrom);
  }
  if (f.gradeTo !== undefined) {
    clauses.push("grade <= ?");
    params.push(f.gradeTo);
  }
  if (f.eventName) {
    clauses.push("event_name = ?");
    params.push(f.eventName);
  }
  if (f.search) {
    // total_search is pre-normalized lowercase, so LIKE is correct.
    clauses.push("total_search LIKE ?");
    params.push(`%${normalizeSearch(f.search)}%`);
  }
  if (f.minPerformances) {
    clauses.push("performance_count >= ?");
    params.push(f.minPerformances);
  }
  // ILIKE, not LIKE: `specification` is raw source text of mixed case, and
  // SQLite's LIKE matched it case-insensitively. Plain LIKE here silently
  // returned nothing.
  if (!f.includeDelisted) {
    clauses.push("on_current_pml = 1");
  }
  if (f.accompaniment === "acappella") {
    clauses.push("specification ILIKE '%a cappella%'");
  } else if (f.accompaniment === "accompanied") {
    clauses.push("specification ILIKE '%accomp%'");
    clauses.push("specification NOT ILIKE '%a cappella%'");
  }

  return { sql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "", params };
}

async function _getSongEvents(): Promise<{ event_name: string; n: number }[]> {
  return rows<{ event_name: string; n: number }>(
    `SELECT event_name, COUNT(*)::int AS n FROM songs
     WHERE event_name != '' GROUP BY event_name ORDER BY event_name`,
  );
}

async function _countSongs(f: SongFilters): Promise<number> {
  const { sql, params } = buildSongWhere(f);
  const row = await first<{ n: number }>(
    `SELECT COUNT(*)::int AS n FROM songs ${sql}`,
    params,
  );
  return row.n;
}

export type SongSort = "song_score" | "performance_count" | "title" | "grade";

async function _getSongs(
  f: SongFilters,
  sort: SongSort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
): Promise<Song[]> {
  const { sql, params } = buildSongWhere(f);
  // Whitelisted above by the SongSort type; interpolation is safe here.
  // Titles sort case-insensitively for the same reason school names do.
  const column = sort === "title" ? "lower(title)" : sort;
  const order = `${column} ${dir === "asc" ? "ASC" : "DESC"}`;
  return rows<Song>(
    `SELECT code, event_name, title, composer, arranger, publisher, grade,
            specification, performance_count, average_concert_score,
            average_sight_reading_score, song_score, earliest_year,
            on_current_pml, previous_grade, grade_changed_from
     FROM songs ${sql}
     ORDER BY ${order} NULLS LAST, lower(title) ASC
     LIMIT ? OFFSET ?`,
    [...params, limit, offset],
  );
}

/** Songs plotted on the score scatter -- only those with enough data to mean anything. */
async function _getSongScatter(f: SongFilters, minCount = 10) {
  const { sql, params } = buildSongWhere(f);
  const extra = sql ? `${sql} AND` : "WHERE";
  return rows<{
    code: string;
    title: string;
    composer: string;
    event_name: string;
    grade: number;
    performance_count: number;
    average_concert_score: number;
    average_sight_reading_score: number;
  }>(
    `SELECT code, title, composer, event_name, grade, performance_count,
            average_concert_score, average_sight_reading_score
     FROM songs ${extra} performance_count >= ?
       AND average_concert_score > 0 AND average_sight_reading_score > 0
     ORDER BY performance_count DESC
     LIMIT 1200`,
    [...params, minCount],
  );
}

async function _getSong(code: string): Promise<Song | undefined> {
  return first<Song | undefined>(
    `SELECT code, event_name, title, composer, arranger, publisher, grade,
            specification, performance_count, average_concert_score,
            average_sight_reading_score, song_score, earliest_year,
            on_current_pml, previous_grade, grade_changed_from
     FROM songs WHERE code = ?`,
    [code],
  );
}

/** Any entry that programmed this song in any of its three slots. */
const SONG_MATCH = "(code_1 = ? OR code_2 = ? OR code_3 = ?)";

async function _getSongYearly(code: string) {
  return rows<{
    year: number;
    performances: number;
    concert: number;
    sight: number;
  }>(
    `SELECT year, COUNT(*)::int AS performances,
            ROUND(AVG(concert_final_score), 3)::float8 AS concert,
            ROUND(AVG(sight_reading_final_score), 3)::float8 AS sight
     FROM entries WHERE ${SONG_MATCH}
     GROUP BY year ORDER BY year`,
    [code, code, code],
  );
}

async function _getSongPerformances(code: string, limit = 250) {
  return rows<Entry>(
    `SELECT entry_number, year, event, school, city, conference, classification,
            director, concert_final_score, sight_reading_final_score,
            title_1, title_2, title_3, composer_1, composer_2, composer_3
     FROM entries WHERE ${SONG_MATCH}
     ORDER BY year DESC, lower(btrim(school)) ASC LIMIT ?`,
    [code, code, code, limit],
  );
}

async function _getSongSummary(code: string) {
  return first<{
    performances: number;
    avgConcert: number | null;
    avgSight: number | null;
    schools: number;
    firstYear: number | null;
    lastYear: number | null;
    ones: number;
  }>(
    `SELECT COUNT(*)::int AS performances,
            AVG(concert_final_score)::float8 AS "avgConcert",
            AVG(sight_reading_final_score)::float8 AS "avgSight",
            COUNT(DISTINCT school_search)::int AS schools,
            MIN(year) AS "firstYear", MAX(year) AS "lastYear",
            SUM(CASE WHEN concert_final_score = 1 THEN 1 ELSE 0 END)::int AS ones
     FROM entries WHERE ${SONG_MATCH}`,
    [code, code, code],
  );
}

/**
 * How this song's usage compares to every other song of the same event and
 * grade, counted from the year the song first appeared.
 */
async function _getSongShare(code: string, eventName: string, grade: number) {
  const yearRow = await first<{ y: number | null }>(
    `SELECT MIN(year) AS y FROM entries WHERE ${SONG_MATCH}`,
    [code, code, code],
  );
  const since = yearRow.y ?? 2005;

  const [peers, mine] = await Promise.all([
    first<{ n: number }>(
      `SELECT COUNT(*)::int AS n FROM entries
       WHERE event = ? AND year >= ?
         AND (code_1 IN (SELECT code FROM songs WHERE grade = ? AND event_name = ?)
           OR code_2 IN (SELECT code FROM songs WHERE grade = ? AND event_name = ?)
           OR code_3 IN (SELECT code FROM songs WHERE grade = ? AND event_name = ?))`,
      [eventName, since, grade, eventName, grade, eventName, grade, eventName],
    ),
    first<{ n: number }>(
      `SELECT COUNT(*)::int AS n FROM entries WHERE ${SONG_MATCH} AND year >= ?`,
      [code, code, code, since],
    ),
  ]);

  return { since, mine: mine.n, peers: peers.n };
}

/** Top schools by number of performances of a song. */
async function _getSongTopSchools(code: string, limit = 8) {
  // Grouped by school_search but displaying `school`: SQLite allowed the bare
  // column and picked one arbitrarily, Postgres rejects it, so pick explicitly.
  return rows<{ school: string; n: number; avgConcert: number }>(
    `SELECT MIN(school) AS school, COUNT(*)::int AS n,
            AVG(concert_final_score)::float8 AS "avgConcert"
     FROM entries WHERE ${SONG_MATCH}
     GROUP BY school_search ORDER BY n DESC, school ASC LIMIT ?`,
    [code, code, code, limit],
  );
}

/* --- cached exports ------------------------------------------------ */

// Each wraps the implementation above; signatures are inferred, so callers
// see no difference.
export const getYearBounds = cached("getYearBounds", _getYearBounds);
export const getFilterOptions = cached("getFilterOptions", _getFilterOptions);
export const getConferences = cached("getConferences", _getConferences);
export const countEntries = cached("countEntries", _countEntries);
export const countDirectorRoles = cached("countDirectorRoles", _countDirectorRoles);
export const getSummary = cached("getSummary", _getSummary);
export const getScoresByYear = cached("getScoresByYear", _getScoresByYear);
export const getDistribution = cached("getDistribution", _getDistribution);
export const getSongEvents = cached("getSongEvents", _getSongEvents);
export const countSongs = cached("countSongs", _countSongs);
export const getSongSummary = cached("getSongSummary", _getSongSummary);
export const getSongYearly = cached("getSongYearly", _getSongYearly);
export const getSongShare = cached("getSongShare", _getSongShare);
export const getSongTopSchools = cached("getSongTopSchools", _getSongTopSchools);
export const getSong = cached("getSong", _getSong);

// Row fetches too: the residual scans after caching the aggregates were all
// from these, and repeat views should touch the database not at all.
export const getEntries = cached("getEntries", _getEntries);
export const getEntriesWithJudges = cached("getEntriesWithJudges", _getEntriesWithJudges);
export const getSongs = cached("getSongs", _getSongs);
export const getSongScatter = cached("getSongScatter", _getSongScatter);
export const getSongPerformances = cached("getSongPerformances", _getSongPerformances);
