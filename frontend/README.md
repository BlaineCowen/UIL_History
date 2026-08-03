# UIL History — web frontend

A Next.js recreation of the Streamlit dashboard (`UIL_dashboard.py`).

## Why it exists

The Streamlit app loads the entire database into pandas on boot (~160k rows,
110 MB) and filters in memory. This version queries a pre-cleaned, indexed
SQLite file, so a filtered page renders in single-digit milliseconds and filter
state lives in the URL — every view is linkable and shareable.

## Setup

The app reads `frontend/data/uil_web.db`, which is **generated**, not committed.
Build it from the repo root:

```bash
.venv/bin/python scripts/build_web_db.py
```

That reads `uil.db`, applies the dashboard's own `get_db()` / `clean_pml()`
cleaning functions (so the two apps agree), and writes a slim indexed database
of ~160k entries and ~7.8k songs.

Then, from this directory:

```bash
npm install
npm run dev      # http://localhost:3000
npm run build && npm run start
```

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
- `src/lib/db.ts` is the only file that touches the database. When the Postgres
  migration happens, this is the file that changes; the pages call functions,
  not SQL.
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
