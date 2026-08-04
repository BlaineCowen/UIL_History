import "server-only";

import Database from "better-sqlite3";
import path from "node:path";

/**
 * The database is built by `scripts/build_web_db.py` at the repo root and is
 * read-only at runtime. Every filter below is an indexed SQL predicate --
 * the Streamlit app did the equivalent work with pandas `str.contains` over
 * all ~160k rows held in memory.
 */

declare global {
  var __uilDb: Database.Database | undefined;
}

function connect(): Database.Database {
  const file = path.join(process.cwd(), "data", "uil_web.db");
  const db = new Database(file, { readonly: true, fileMustExist: true });
  // Note: journal_mode cannot be set here -- it writes to the file, which a
  // readonly connection rejects. cache_size is per-connection and safe.
  db.pragma("cache_size = -64000");
  return db;
}

// Next.js dev reloads modules on every edit; without a global the process
// accumulates file handles.
export function getDb(): Database.Database {
  if (!global.__uilDb) global.__uilDb = connect();
  return global.__uilDb;
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
};

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
    // there was still on the podium. There is no normalized director_search
    // column and no index, but an unindexed scan of this table costs ~18ms.
    clauses.push(
      "(director LIKE ? COLLATE NOCASE OR additional_director LIKE ? COLLATE NOCASE)",
    );
    const term = `%${f.director.trim()}%`;
    params.push(term, term);
  }

  return {
    sql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    params,
  };
}

/* --------------------------------------------------------------- queries */

export function getYearBounds(): { min: number; max: number } {
  const row = getDb()
    .prepare("SELECT MIN(year) AS min, MAX(year) AS max FROM entries")
    .get() as { min: number; max: number };
  return row;
}

export function getFilterOptions(genEvent?: string) {
  const db = getDb();
  const where = genEvent ? "WHERE gen_event = ?" : "";
  const args = genEvent ? [genEvent] : [];

  const events = db
    .prepare(
      `SELECT event, COUNT(*) AS n FROM entries ${where} GROUP BY event ORDER BY event`,
    )
    .all(...args) as { event: string; n: number }[];

  const levels = db
    .prepare(
      `SELECT school_level, COUNT(*) AS n FROM entries ${where}
       ${where ? "AND" : "WHERE"} school_level != ''
       GROUP BY school_level ORDER BY school_level`,
    )
    .all(...args) as { school_level: string; n: number }[];

  const classifications = db
    .prepare(
      `SELECT classification, COUNT(*) AS n FROM entries ${where}
       ${where ? "AND" : "WHERE"} classification != ''
       GROUP BY classification ORDER BY classification`,
    )
    .all(...args) as { classification: string; n: number }[];

  return { events, levels, classifications };
}

/** Conferences available for a level -- the list differs wildly between HS and MS. */
export function getConferences(genEvent?: string, schoolLevel?: string) {
  const db = getDb();
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
  return db
    .prepare(
      `SELECT conference, COUNT(*) AS n FROM entries
       WHERE ${clauses.join(" AND ")}
       GROUP BY conference HAVING n > 20 ORDER BY LENGTH(conference), conference`,
    )
    .all(...params) as { conference: string; n: number }[];
}

export function countEntries(f: EntryFilters): number {
  const { sql, params } = buildWhere(f);
  const row = getDb()
    .prepare(`SELECT COUNT(*) AS n FROM entries ${sql}`)
    .get(...params) as { n: number };
  return row.n;
}

export type EntrySort =
  | "year"
  | "school"
  | "concert_final_score"
  | "sight_reading_final_score";

/**
 * Sort expressions, not bare columns. `school` needs both because the scraped
 * values are neither trimmed nor consistently cased: without TRIM the handful
 * of " Sam Houston Middle School" rows sort ahead of everything, and without
 * COLLATE NOCASE binary collation puts "west Brook High School" after every
 * capitalised name.
 */
const ENTRY_ORDER: Record<EntrySort, string> = {
  year: "year",
  school: "TRIM(school) COLLATE NOCASE",
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

function entriesQuery(
  columns: string,
  f: EntryFilters,
  sort: EntrySort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
) {
  const { sql, params } = buildWhere(f);
  // Both sides come from the whitelisting EntrySort type, not user input.
  const order = `${ENTRY_ORDER[sort]} ${dir === "asc" ? "ASC" : "DESC"}`;
  return getDb()
    .prepare(
      `SELECT ${columns}
       FROM entries ${sql}
       ORDER BY ${order} NULLS LAST, year DESC, ${ENTRY_ORDER.school} ASC,
                entry_number ASC
       LIMIT ? OFFSET ?`,
    )
    .all(...params, limit, offset);
}

export function getEntries(
  f: EntryFilters,
  sort: EntrySort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
): Entry[] {
  return entriesQuery(ENTRY_SELECT, f, sort, dir, limit, offset) as Entry[];
}

export function getEntriesWithJudges(
  f: EntryFilters,
  sort: EntrySort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
): EntryWithJudges[] {
  return entriesQuery(
    `${ENTRY_SELECT}, ${JUDGE_SELECT}`,
    f,
    sort,
    dir,
    limit,
    offset,
  ) as EntryWithJudges[];
}

export type Summary = {
  total: number;
  avgConcert: number | null;
  avgSight: number | null;
  schools: number;
  sweepstakes: number;
};

/** Headline numbers for the stat tiles. */
export function getSummary(f: EntryFilters): Summary {
  const { sql, params } = buildWhere(f);
  const row = getDb()
    .prepare(
      `SELECT COUNT(*) AS total,
              AVG(concert_final_score) AS avgConcert,
              AVG(sight_reading_final_score) AS avgSight,
              COUNT(DISTINCT school_search) AS schools,
              SUM(CASE WHEN concert_final_score = 1 AND sight_reading_final_score = 1
                       THEN 1 ELSE 0 END) AS sweepstakes
       FROM entries ${sql}`,
    )
    .get(...params) as Summary;
  return row;
}

export type YearPoint = { year: number; concert: number; sight: number; n: number };

export function getScoresByYear(f: EntryFilters): YearPoint[] {
  const { sql, params } = buildWhere(f);
  return getDb()
    .prepare(
      `SELECT year,
              ROUND(AVG(concert_final_score), 3) AS concert,
              ROUND(AVG(sight_reading_final_score), 3) AS sight,
              COUNT(*) AS n
       FROM entries ${sql}
       GROUP BY year ORDER BY year`,
    )
    .all(...params) as YearPoint[];
}

export type Distribution = { score: number; concert: number; sight: number };

/** Counts of each rating 1-5 for both score types, in one pass. */
export function getDistribution(f: EntryFilters): Distribution[] {
  const { sql, params } = buildWhere(f);
  const db = getDb();
  const concert = db
    .prepare(
      `SELECT concert_final_score AS score, COUNT(*) AS n FROM entries ${sql}
       GROUP BY score ORDER BY score`,
    )
    .all(...params) as { score: number; n: number }[];
  const sight = db
    .prepare(
      `SELECT sight_reading_final_score AS score, COUNT(*) AS n FROM entries ${sql}
       GROUP BY score ORDER BY score`,
    )
    .all(...params) as { score: number; n: number }[];

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
    clauses.push("total_search LIKE ?");
    params.push(`%${normalizeSearch(f.search)}%`);
  }
  if (f.minPerformances) {
    clauses.push("performance_count >= ?");
    params.push(f.minPerformances);
  }
  if (f.accompaniment === "acappella") {
    clauses.push("specification LIKE '%a cappella%'");
  } else if (f.accompaniment === "accompanied") {
    clauses.push("specification LIKE '%accomp%'");
    clauses.push("specification NOT LIKE '%a cappella%'");
  }

  return { sql: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "", params };
}

export function getSongEvents(): { event_name: string; n: number }[] {
  return getDb()
    .prepare(
      `SELECT event_name, COUNT(*) AS n FROM songs
       WHERE event_name != '' GROUP BY event_name ORDER BY event_name`,
    )
    .all() as { event_name: string; n: number }[];
}

export function countSongs(f: SongFilters): number {
  const { sql, params } = buildSongWhere(f);
  const row = getDb()
    .prepare(`SELECT COUNT(*) AS n FROM songs ${sql}`)
    .get(...params) as { n: number };
  return row.n;
}

export type SongSort = "song_score" | "performance_count" | "title" | "grade";

export function getSongs(
  f: SongFilters,
  sort: SongSort,
  dir: "asc" | "desc",
  limit: number,
  offset: number,
): Song[] {
  const { sql, params } = buildSongWhere(f);
  // Whitelisted above by the SongSort type; interpolation is safe here.
  // Titles sort case-insensitively for the same reason school names do.
  const column = sort === "title" ? "title COLLATE NOCASE" : sort;
  const order = `${column} ${dir === "asc" ? "ASC" : "DESC"}`;
  return getDb()
    .prepare(
      `SELECT code, event_name, title, composer, arranger, publisher, grade,
              specification, performance_count, average_concert_score,
              average_sight_reading_score, song_score, earliest_year
       FROM songs ${sql}
       ORDER BY ${order} NULLS LAST, title COLLATE NOCASE ASC
       LIMIT ? OFFSET ?`,
    )
    .all(...params, limit, offset) as Song[];
}

/** Songs plotted on the score scatter -- only those with enough data to mean anything. */
export function getSongScatter(f: SongFilters, minCount = 10) {
  const { sql, params } = buildSongWhere(f);
  const extra = sql ? `${sql} AND` : "WHERE";
  return getDb()
    .prepare(
      `SELECT code, title, composer, event_name, grade, performance_count,
              average_concert_score, average_sight_reading_score
       FROM songs ${extra} performance_count >= ?
         AND average_concert_score > 0 AND average_sight_reading_score > 0
       ORDER BY performance_count DESC
       LIMIT 1200`,
    )
    .all(...params, minCount) as {
    code: string;
    title: string;
    composer: string;
    event_name: string;
    grade: number;
    performance_count: number;
    average_concert_score: number;
    average_sight_reading_score: number;
  }[];
}

export function getSong(code: string): Song | undefined {
  return getDb()
    .prepare(
      `SELECT code, event_name, title, composer, arranger, publisher, grade,
              specification, performance_count, average_concert_score,
              average_sight_reading_score, song_score, earliest_year
       FROM songs WHERE code = ?`,
    )
    .get(code) as Song | undefined;
}

/** Any entry that programmed this song in any of its three slots. */
const SONG_MATCH = "(code_1 = ? OR code_2 = ? OR code_3 = ?)";

export function getSongYearly(code: string) {
  return getDb()
    .prepare(
      `SELECT year, COUNT(*) AS performances,
              ROUND(AVG(concert_final_score), 3) AS concert,
              ROUND(AVG(sight_reading_final_score), 3) AS sight
       FROM entries WHERE ${SONG_MATCH}
       GROUP BY year ORDER BY year`,
    )
    .all(code, code, code) as {
    year: number;
    performances: number;
    concert: number;
    sight: number;
  }[];
}

export function getSongPerformances(code: string, limit = 250) {
  return getDb()
    .prepare(
      `SELECT entry_number, year, event, school, city, conference, classification,
              director, concert_final_score, sight_reading_final_score,
              title_1, title_2, title_3, composer_1, composer_2, composer_3
       FROM entries WHERE ${SONG_MATCH}
       ORDER BY year DESC, school ASC LIMIT ?`,
    )
    .all(code, code, code, limit) as Entry[];
}

export function getSongSummary(code: string) {
  const row = getDb()
    .prepare(
      `SELECT COUNT(*) AS performances,
              AVG(concert_final_score) AS avgConcert,
              AVG(sight_reading_final_score) AS avgSight,
              COUNT(DISTINCT school_search) AS schools,
              MIN(year) AS firstYear, MAX(year) AS lastYear,
              SUM(CASE WHEN concert_final_score = 1 THEN 1 ELSE 0 END) AS ones
       FROM entries WHERE ${SONG_MATCH}`,
    )
    .get(code, code, code) as {
    performances: number;
    avgConcert: number | null;
    avgSight: number | null;
    schools: number;
    firstYear: number | null;
    lastYear: number | null;
    ones: number;
  };
  return row;
}

/**
 * How this song's usage compares to every other song of the same event and
 * grade, counted from the year the song first appeared.
 */
export function getSongShare(code: string, eventName: string, grade: number) {
  const db = getDb();
  const yearRow = db
    .prepare(`SELECT MIN(year) AS y FROM entries WHERE ${SONG_MATCH}`)
    .get(code, code, code) as { y: number | null };
  const since = yearRow.y ?? 2005;

  const peers = db
    .prepare(
      `SELECT COUNT(*) AS n FROM entries
       WHERE event = ? AND year >= ?
         AND (code_1 IN (SELECT code FROM songs WHERE grade = ? AND event_name = ?)
           OR code_2 IN (SELECT code FROM songs WHERE grade = ? AND event_name = ?)
           OR code_3 IN (SELECT code FROM songs WHERE grade = ? AND event_name = ?))`,
    )
    .get(eventName, since, grade, eventName, grade, eventName, grade, eventName) as {
    n: number;
  };

  const mine = db
    .prepare(`SELECT COUNT(*) AS n FROM entries WHERE ${SONG_MATCH} AND year >= ?`)
    .get(code, code, code, since) as { n: number };

  return { since, mine: mine.n, peers: peers.n };
}

/** Top schools by number of performances of a song. */
export function getSongTopSchools(code: string, limit = 8) {
  return getDb()
    .prepare(
      `SELECT school, COUNT(*) AS n, AVG(concert_final_score) AS avgConcert
       FROM entries WHERE ${SONG_MATCH}
       GROUP BY school_search ORDER BY n DESC, school ASC LIMIT ?`,
    )
    .all(code, code, code, limit) as {
    school: string;
    n: number;
    avgConcert: number;
  }[];
}
