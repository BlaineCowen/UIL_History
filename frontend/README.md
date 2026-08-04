# UIL History — web frontend

A Next.js recreation of the Streamlit dashboard (`UIL_dashboard.py`).

## Why it exists

The Streamlit app loads the entire database into pandas on boot (~160k rows,
110 MB) and filters in memory. This version queries a pre-cleaned, indexed
SQLite file, so a filtered page renders in single-digit milliseconds and filter
state lives in the URL — every view is linkable and shareable.

## Setup

The app reads **Postgres**, via `DATABASE_URL`. Getting there is two steps,
because the cleaning still happens in Python against SQLite.

**1. Build the cleaned SQLite file** (from the repo root):

```bash
.venv/bin/python scripts/build_web_db.py
```

That reads `uil.db`, applies the dashboard's own `get_db()` / `clean_pml()`
cleaning functions (so the two apps agree), and writes a slim indexed database
of ~160k entries and ~7.8k songs to `frontend/data/uil_web.db`.

**2. Load it into Postgres** (from this directory):

```bash
DATABASE_URL=postgres://... node scripts/load_postgres.mjs
```

The loader reads the *already cleaned* SQLite output rather than re-deriving
anything, which makes cleaning drift between the two structurally impossible.
It only touches `entries` and `songs`, so user tables are never harmed.

For local development, a throwaway Postgres:

```bash
docker run -d --name uil-pg -e POSTGRES_PASSWORD=uil -e POSTGRES_USER=uil \
  -e POSTGRES_DB=uil -p 55432:5432 postgres:17
```

Then:

```bash
npm install
DATABASE_URL=postgres://uil:uil@localhost:55432/uil npm run dev
DATABASE_URL=... npm run build && DATABASE_URL=... npm run start
```

## Deploying to Vercel

The repository holds **two** applications: the original Streamlit app at the
root, and this one in `frontend/`. They do not conflict, and neither needs to
move.

1. **Root Directory: `frontend`** in the Vercel project settings. This is the
   whole trick — Vercel builds only this directory and ignores the Python at
   the root.
2. **Environment variable `DATABASE_URL`** — Supabase's *pooler* string, port
   `6543`. The direct `5432` connection will exhaust connections under
   serverless. See `.env.example`.
3. **Production branch**: whichever branch carries `frontend/`. Note that
   `master` is the real trunk here; `main` is an unrelated single commit from
   2022 and is not used.

The build does not need a database — every data route is dynamic, so nothing
queries Postgres at build time. A missing `DATABASE_URL` therefore fails at
request time rather than build time.

`better-sqlite3` is a devDependency (the loader uses it) and Vercel installs
devDependencies during builds. It ships prebuilds, so it should install without
compiling; if a build ever fails on it, that is the thing to look at.

## Routes

| Route | What it does |
|---|---|
| `/` | Results explorer — ensemble, event, school, level, conference, classification, song, composer, year range |
| `/pml` | Prescribed Music List, sortable, with a concert-vs-sight-reading scatter |
| `/pml/[code]` | One song: yearly usage and ratings, category share, top schools, full performance list |
| `/about` | Methodology |

## Notes for whoever works on this next

- **Scores are ranks: 1 is best, 5 is worst.** Every score axis is inverted and
  every "better" comparison is a *lower* number.
- The chart palette in `globals.css` was validated with the dataviz validator in
  both light and dark on the all-pairs list. Adding a fourth categorical series
  to the scatter would break the colourblind-separation floor — fold extra
  categories into "Other" or facet instead.
- Song detail deliberately uses **two charts** rather than one with two y-axes.
  Performance count and average rating are different scales.
- `src/lib/db.ts` is the only file that touches the database, which is what made
  the SQLite→Postgres move a single-file change. Keep it that way.
- **Every read is cached** under the `contest-data` tag. This is a capacity
  requirement, not an optimisation: uncached, one unfiltered `/` runs eight
  queries that each scan the whole 159k-row table, ~139ms of database CPU, about
  7 views/sec per core. Cached, a repeat view issues **zero** queries.
  **After loading new data you must invalidate** — redeploy, restart, or call
  `revalidateTag("contest-data")` — or the site serves the previous season for
  up to a day.
- Four SQLite behaviours do not carry over, and three are silent rather than
  errors: SQLite's `LIKE` is case-insensitive (Postgres needs `ILIKE`);
  Postgres folds unquoted identifiers, so camelCase aliases must be quoted or
  `avgConcert` arrives as `avgconcert`; and `COUNT`/`SUM`/`AVG` come back as
  *strings* unless cast. The two that are hard errors — output aliases in
  `HAVING`, and bare non-grouped columns — at least fail loudly.
- **Sort order changed on purpose.** SQLite sorted by raw bytes, so
  `"Emperor" Variations` was song #1 of 7,832; Postgres's collation ignores
  leading punctuation and files it at #2134, under E. That is the better
  behaviour, but it is a visible difference.
- **Every table renders twice**: `TableScroll` for `sm` and up, `DataList` /
  `DataCard` below it. The rows are derived once and both views consume them,
  so a new column needs adding in two places. This costs ~15KB gzipped per
  page and buys not reading an 8-column table through a 390px window.
- Anything a finger touches needs 44px (`.tap`) or 38px (`.tap-sm`), and any
  focusable control needs 16px text on coarse pointers — under that, iOS
  Safari zooms the viewport on focus and never zooms back out.
- Charts can't size their axes in CSS because Recharts lays out in JS. Use
  `useIsNarrow()` from `ChartKit` for tick density and bubble scale; use the
  `heightClass` prop for height.
- The connection is opened **readonly**, so no pragma that writes to the file
  (`journal_mode`) can be set on it.

## Data fixes applied during the build

These correct issues visible in the Streamlit version:

1. ~10k rows carry a UIL form number on the event (`100-Concert Band`,
   `931-Mixed Chorus`). Streamlit lists these as separate events; they are folded
   into their plain equivalents (12 distinct → 6).
2. `pml.event_name` contains literal `concert concert concert band` and
   `steel concert concert concert band`. Streamlit's steel-band filter looks for
   `steelband` and misses the latter, so 113 steel band pieces leaked into the
   Band lists. Both are repaired.
3. Streamlit runs `.str.title()` over titles that are already correctly cased,
   producing `At The Round Earth'S Imagined Corners`. Casing is now applied only
   to values that arrive all-lowercase.
