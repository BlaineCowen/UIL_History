/**
 * Load the built SQLite database into Postgres.
 *
 * Reads `frontend/data/uil_web.db` -- the *already cleaned* output of
 * `build_web_db.py` -- rather than re-deriving the cleaning against the raw
 * `uil.db`. That makes cleaning drift between the two databases structurally
 * impossible instead of merely something to be careful about: there is one
 * cleaning implementation and Postgres is downstream of its output.
 *
 * Usage, from the frontend/ directory:
 *   DATABASE_URL=postgres://uil:uil@localhost:55432/uil \
 *     node scripts/load_postgres.mjs
 *
 * Safe to re-run: each table is dropped and rebuilt. It only touches `entries`
 * and `songs`, so user tables (profiles, song_ratings) are never harmed -- the
 * same property the seasonal refresh depends on.
 */

import Database from "better-sqlite3";
import postgres from "postgres";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const SQLITE_FILE = path.join(HERE, "..", "data", "uil_web.db");
const TABLES = ["entries", "songs"];
const PRIMARY_KEY = { entries: "entry_number", songs: "code" };
/**
 * Postgres binds at most 65535 parameters per statement, and a multi-row
 * INSERT spends columns x rows of them. Derive the batch from the column count
 * rather than hardcoding it -- 34 columns x 2000 rows silently overshoots.
 */
const MAX_BIND_PARAMS = 60000;
const batchSize = (columnCount) => Math.max(1, Math.floor(MAX_BIND_PARAMS / columnCount));

/** SQLite is dynamically typed; these are the declared types we map from. */
const TYPE_MAP = { INTEGER: "integer", REAL: "double precision", TEXT: "text" };

const INDEXES = {
  entries: [
    "CREATE INDEX idx_entries_gen_event_year ON entries (gen_event, year)",
    "CREATE INDEX idx_entries_event ON entries (event)",
    "CREATE INDEX idx_entries_year ON entries (year)",
    "CREATE INDEX idx_entries_level ON entries (school_level, conference)",
    "CREATE INDEX idx_entries_classification ON entries (classification)",
    "CREATE INDEX idx_entries_code_1 ON entries (code_1)",
    "CREATE INDEX idx_entries_code_2 ON entries (code_2)",
    "CREATE INDEX idx_entries_code_3 ON entries (code_3)",
    // Sorting by school orders on this expression, so index the expression.
    "CREATE INDEX idx_entries_school_sort ON entries (lower(btrim(school)))",
    // Trigram indexes turn the ILIKE '%...%' filters from scans into lookups,
    // which a plain btree cannot do for a leading wildcard.
    "CREATE INDEX idx_entries_school_search ON entries USING gin (school_search gin_trgm_ops)",
    "CREATE INDEX idx_entries_song_concat ON entries USING gin (song_concat gin_trgm_ops)",
    "CREATE INDEX idx_entries_composer_concat ON entries USING gin (composer_concat gin_trgm_ops)",
    "CREATE INDEX idx_entries_director ON entries USING gin (director gin_trgm_ops)",
    "CREATE INDEX idx_entries_additional_director ON entries USING gin (additional_director gin_trgm_ops)",
  ],
  songs: [
    "CREATE INDEX idx_songs_grade_event ON songs (grade, event_name)",
    "CREATE INDEX idx_songs_perf_count ON songs (performance_count)",
    "CREATE INDEX idx_songs_title_sort ON songs (lower(title))",
    "CREATE INDEX idx_songs_total_search ON songs USING gin (total_search gin_trgm_ops)",
  ],
};

const url = process.env.DATABASE_URL;
if (!url) {
  console.error("DATABASE_URL is required.");
  process.exit(1);
}

const lite = new Database(SQLITE_FILE, { readonly: true, fileMustExist: true });
const sql = postgres(url, { onnotice: () => {} });

const started = Date.now();

try {
  await sql`CREATE EXTENSION IF NOT EXISTS pg_trgm`;
  console.log("pg_trgm ready");

  for (const table of TABLES) {
    const columns = lite.prepare(`PRAGMA table_info(${table})`).all();
    const defs = columns.map((c) => {
      const type = TYPE_MAP[c.type.toUpperCase()] ?? "text";
      return `${c.name} ${type}`;
    });

    await sql.unsafe(`DROP TABLE IF EXISTS ${table} CASCADE`);
    await sql.unsafe(
      `CREATE TABLE ${table} (${defs.join(", ")},
       PRIMARY KEY (${PRIMARY_KEY[table]}))`,
    );

    const names = columns.map((c) => c.name);
    const total = lite.prepare(`SELECT COUNT(*) n FROM ${table}`).get().n;
    let done = 0;

    // Stream rather than materialising 159k rows, and insert in batches --
    // postgres.js builds one multi-row INSERT per call.
    const rows = lite.prepare(`SELECT * FROM ${table}`).iterate();
    const limit = batchSize(names.length);
    let batch = [];
    for (const row of rows) {
      batch.push(row);
      if (batch.length >= limit) {
        await sql`INSERT INTO ${sql(table)} ${sql(batch, names)}`;
        done += batch.length;
        batch = [];
        process.stdout.write(`\r  ${table}: ${done}/${total}`);
      }
    }
    if (batch.length) {
      await sql`INSERT INTO ${sql(table)} ${sql(batch, names)}`;
      done += batch.length;
    }
    process.stdout.write(`\r  ${table}: ${done}/${total}\n`);

    // Indexes after the load: building them once beats maintaining them
    // across 159k inserts.
    for (const stmt of INDEXES[table]) await sql.unsafe(stmt);
    console.log(`  ${table}: ${INDEXES[table].length} indexes`);

    await sql.unsafe(`ANALYZE ${table}`);
  }

  console.log("\nverifying:");
  for (const table of TABLES) {
    const [{ count }] = await sql.unsafe(`SELECT COUNT(*)::int AS count FROM ${table}`);
    const expected = lite.prepare(`SELECT COUNT(*) n FROM ${table}`).get().n;
    const ok = count === expected ? "ok" : "MISMATCH";
    console.log(`  ${table}: ${count.toLocaleString()} (sqlite ${expected.toLocaleString()}) ${ok}`);
    if (count !== expected) process.exitCode = 1;
  }

  console.log(`\ndone in ${((Date.now() - started) / 1000).toFixed(1)}s`);
  console.log(
    "\nNOTE: the app caches every read under the 'contest-data' tag for a day,\n" +
      "so this load is invisible until that cache is dropped. Restarting the\n" +
      "server does NOT do it -- Next's data cache lives in .next/cache and\n" +
      "survives restarts. Drop it with:\n" +
      "\n" +
      "  curl -X POST <site>/api/revalidate -H \"x-revalidate-secret: $REVALIDATE_SECRET\"\n" +
      "\n" +
      "or locally, rm -rf .next/cache and restart.",
  );
} finally {
  await sql.end();
  lite.close();
}
