"""Build a slim, indexed SQLite database for the Next.js frontend.

The Streamlit app cleans data at *read* time (``get_db`` / ``clean_pml`` in
``UIL_dashboard.py``), which is why it has to hold all ~160k rows in RAM. The
web app instead queries a pre-cleaned database, so every filter becomes an
indexed SQL predicate.

This script reuses the dashboard's own cleaning functions rather than
reimplementing them, then applies a few display-quality fixes on top.

Run from the repo root:

    .venv/bin/python scripts/build_web_db.py

Reads ``uil.db``, writes ``frontend/data/uil_web.db``. Read-only with respect
to ``uil.db`` -- it never writes back to the source.

Normalization beyond what Streamlit does
----------------------------------------
1. ~10k rows carry a UIL form number on the event ("100-Concert Band",
   "931-Mixed Chorus"). Streamlit lists these as *separate* events in its
   dropdown. They are folded into their plain equivalents (12 -> 6 events).
2. ``pml.event_name`` contains literal "concert concert concert band" and
   "steel concert concert concert band" values. Streamlit's steel-band filter
   looks for "steelband" and therefore misses the latter entirely.
3. Streamlit runs ``.str.title()`` over titles that are *already* correctly
   cased, producing "At The Round Earth'S Imagined Corners". Casing here is
   only applied to values that arrive all-lowercase.
"""

import os
import re
import sqlite3
import sys

import pandas as pd

# UIL_dashboard.py lives at the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from UIL_dashboard import clean_pml, get_db  # noqa: E402

SOURCE_DB = "uil.db"
TARGET_DB = os.path.join("frontend", "data", "uil_web.db")

# Columns the frontend actually reads. `results` has ~70 columns, but the rest
# is fuzzy-matching exhaust (potential_match_N, composer_no_hyphen_N, ...) that
# only the pipeline needs. choice_N is deliberately excluded -- it is just
# title_N + composer_N, and the frontend composes it for display.
ENTRY_COLUMNS = [
    "entry_number",
    "year",
    "contest_date",
    "event",
    "event_search",
    "gen_event",
    "school",
    "school_search",
    "city",
    "region",
    "conference",
    "classification",
    "school_level",
    "director",
    "additional_director",
    "concert_score_1",
    "concert_score_2",
    "concert_score_3",
    "concert_final_score",
    "sight_reading_score_1",
    "sight_reading_score_2",
    "sight_reading_score_3",
    "sight_reading_final_score",
    "title_1",
    "title_2",
    "title_3",
    "composer_1",
    "composer_2",
    "composer_3",
    "code_1",
    "code_2",
    "code_3",
    "song_concat",
    "composer_concat",
]

SONG_COLUMNS = [
    "code",
    "event_name",
    "title",
    "composer",
    "arranger",
    "publisher",
    "grade",
    "specification",
    "performance_count",
    "average_concert_score",
    "average_sight_reading_score",
    "song_score",
    "delta_score",
    "earliest_year",
    "song_search",
    "composer_search",
    "total_search",
    # Derived from the stored PML editions -- see attach_pml_status.
    "on_current_pml",
    "previous_grade",
    "grade_changed_from",
]

def attach_pml_status(pml_df):
    """Mark each song against the stored PML editions.

    Adds three columns:
      on_current_pml     1 if present in the newest edition, else 0
      previous_grade     its grade in the preceding edition, when it differs
      grade_changed_from that edition's label, so the UI can say when

    A song absent from the newest edition has been delisted -- it keeps all its
    contest history and stays reachable, it simply cannot be programmed now.
    If no editions have been fetched yet, everything is treated as current so
    the site never invents a delisting from missing data.
    """
    pml_df["on_current_pml"] = 1
    pml_df["previous_grade"] = None
    pml_df["grade_changed_from"] = None

    # Own connection: the caller closes its handle before this runs, and
    # depending on that ordering is how this broke the first time.
    src = sqlite3.connect(f"file:{SOURCE_DB}?mode=ro", uri=True)
    try:
        # Ordered by label, not id: editions can be ingested out of order --
        # the 2019 backfill was loaded after the current list, and ordering by
        # insertion would have made 2019 "newest" and inverted every change.
        # Labels are year-first ("2019-03", "2025-2026") so they sort right.
        editions = src.execute(
            "SELECT id, label FROM pml_editions ORDER BY label"
        ).fetchall()
    except sqlite3.OperationalError:
        print("  no pml_editions table -- skipping delisting/grade history")
        src.close()
        return pml_df
    if not editions:
        src.close()
        print("  no PML editions stored -- skipping delisting/grade history")
        return pml_df

    newest_id, newest_label = editions[-1]
    current = {
        str(r[0]): r[1]
        for r in src.execute(
            "SELECT song_id, MIN(grade) FROM pml_entries WHERE edition_id = ?"
            " GROUP BY song_id",
            (newest_id,),
        )
    }
    codes = pml_df["code"].astype(str).str.strip()
    pml_df["on_current_pml"] = codes.isin(current).astype(int)

    if len(editions) > 1:
        prev_id, prev_label = editions[-2]
        previous = {
            str(r[0]): r[1]
            for r in src.execute(
                "SELECT song_id, MIN(grade) FROM pml_entries WHERE edition_id = ?"
                " GROUP BY song_id",
                (prev_id,),
            )
        }
        # Coerce both sides: pandas reads pml.grade as float (NaNs elsewhere in
        # the column), while the edition stores int. Comparing them directly
        # reported 90 "re-grades" that were really 2.0 vs 2.
        prev_series = pd.to_numeric(codes.map(previous), errors="coerce")
        now_series = pd.to_numeric(codes.map(current), errors="coerce")
        changed = prev_series.notna() & now_series.notna() & (prev_series != now_series)
        pml_df.loc[changed, "previous_grade"] = prev_series[changed].astype(int)
        pml_df.loc[changed, "grade_changed_from"] = prev_label
        print(f"  PML: {int(changed.sum())} songs re-graded since {prev_label}")

    delisted = int((pml_df["on_current_pml"] == 0).sum())
    print(f"  PML: {len(current):,} on the {newest_label} list, {delisted:,} delisted")
    src.close()
    return pml_df


EVENT_PREFIX = re.compile(r"^\s*\d+\s*-\s*")

# Words kept lowercase inside a title (never first or last word).
SMALL_WORDS = {
    "a", "an", "the", "and", "or", "nor", "but", "for", "of", "in", "on",
    "to", "from", "with", "at", "by", "as", "into", "onto", "upon", "over",
    "under", "de", "du", "des", "la", "le", "les", "el", "von", "van", "der",
    "den", "di", "da", "dos", "das", "e", "y",
}

# Capitalize the letter after a start-of-string, whitespace, or an opening
# bracket/quote/slash -- but never after an apostrophe, so "earth's" stays
# "Earth's" rather than Python's str.title() "Earth'S".
WORD_START = re.compile(r"(^|[\s\(\[\{\"/–—-])([a-z])")


def smart_title(value):
    """Title-case a string, but only if it arrived all-lowercase.

    Values that already contain an uppercase letter are assumed to be
    intentionally cased and are returned untouched. This is what preserves
    "At the Round Earth's Imagined Corners" exactly as scraped.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped or any(ch.isupper() for ch in stripped):
        return value

    titled = WORD_START.sub(lambda m: m.group(1) + m.group(2).upper(), stripped)

    # Push small words back down, except the first and last.
    tokens = titled.split(" ")
    for i, token in enumerate(tokens):
        if 0 < i < len(tokens) - 1 and token.lower().strip(".,;:") in SMALL_WORDS:
            tokens[i] = token.lower()
    return " ".join(tokens)


def normalize_events(df: pd.DataFrame) -> pd.DataFrame:
    """Fold form-number-prefixed events into their plain equivalents."""
    before = df["event"].nunique()

    df["event"] = df["event"].astype(str).str.replace(EVENT_PREFIX, "", regex=True)
    df["event"] = df["event"].str.replace("Tenor/Bass", "Tenor-Bass", regex=False)
    df["event"] = df["event"].str.strip().str.title()

    # event_search is the lowercased, letters-only form the filters match on.
    df["event_search"] = df["event"].str.lower().str.replace(r"[^a-z]", "", regex=True)

    print(f"  events normalized: {before} distinct -> {df['event'].nunique()} distinct")
    return df


def normalize_song_events(df: pd.DataFrame) -> pd.DataFrame:
    """Repair the mangled band event names in the PML table."""
    before = len(df)

    # "steel concert concert concert band" -- clean_pml looks for "steelband"
    # and misses this, so steel band songs leak into the Band lists.
    df = df[~df["event_name"].str.contains("Steel", case=False, na=False)].copy()
    dropped = before - len(df)

    df["event_name"] = df["event_name"].str.replace(
        r"(?:Concert\s+)+Band", "Concert Band", regex=True
    )

    if dropped:
        print(f"  dropped {dropped} steel band songs")
    return df


def build() -> None:
    if not os.path.exists(SOURCE_DB):
        raise SystemExit(f"{SOURCE_DB} not found -- run from the repo root.")

    print(f"Reading {SOURCE_DB} ...")
    src = sqlite3.connect(SOURCE_DB)
    results_df = pd.read_sql_query("SELECT * FROM results", src)
    pml_df = pd.read_sql_query("SELECT * FROM pml", src)
    src.close()
    print(f"  results: {len(results_df):,} rows")
    print(f"  pml:     {len(pml_df):,} rows")

    print("Applying the dashboard's cleaning functions ...")
    results_df = get_db(results_df)
    pml_df = clean_pml(pml_df)
    pml_df = attach_pml_status(pml_df)

    # Mirror get_data(): drop partial-score entries, clamp sight-reading.
    score_cols = [
        "concert_score_1",
        "concert_score_2",
        "concert_score_3",
        "concert_final_score",
        "sight_reading_score_1",
        "sight_reading_score_2",
        "sight_reading_score_3",
        "sight_reading_final_score",
    ]
    results_df = results_df.fillna(0)
    before = len(results_df)
    for col in score_cols:
        results_df = results_df[results_df[col] != 0]
    print(f"  dropped {before - len(results_df):,} rows with partial scores")

    results_df.loc[
        results_df["sight_reading_final_score"] > 5, "sight_reading_final_score"
    ] = 5
    for col in score_cols:
        results_df[col] = results_df[col].astype(float).astype(int)

    results_df["school_level"] = ""
    results_df.loc[
        results_df["conference"].str.contains("A", na=False), "school_level"
    ] = "High School"
    results_df.loc[
        results_df["conference"].str.contains("C", na=False), "school_level"
    ] = "Middle School/JH"

    results_df["classification"] = (
        results_df["classification"].str.replace("-", " ").str.title()
    )
    results_df.loc[
        results_df["classification"].str.contains("Nv", na=False), "classification"
    ] = "Non Varsity"
    results_df.loc[
        results_df["classification"].str.contains(r"^V", na=False), "classification"
    ] = "Varsity"

    results_df = normalize_events(results_df)
    results_df["conference"] = (
        results_df["conference"].astype(str).str.strip().str.upper()
    )

    print("Fixing display casing ...")
    for n in (1, 2, 3):
        results_df[f"title_{n}"] = results_df[f"title_{n}"].map(smart_title)
        results_df[f"composer_{n}"] = results_df[f"composer_{n}"].map(smart_title)
    for col in ("school", "director", "additional_director", "city"):
        if col in results_df.columns:
            results_df[col] = results_df[col].map(smart_title)

    pml_df = normalize_song_events(pml_df)
    for col in ("title", "composer", "arranger", "publisher"):
        if col in pml_df.columns:
            pml_df[col] = pml_df[col].map(smart_title)

    results_df["contest_date"] = pd.to_datetime(
        results_df["contest_date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    entries = results_df[[c for c in ENTRY_COLUMNS if c in results_df.columns]].copy()
    songs = pml_df[[c for c in SONG_COLUMNS if c in pml_df.columns]].copy()

    missing_entry = set(ENTRY_COLUMNS) - set(entries.columns)
    missing_song = set(SONG_COLUMNS) - set(songs.columns)
    if missing_entry:
        print(f"  WARNING: entries missing columns: {sorted(missing_entry)}")
    if missing_song:
        print(f"  WARNING: songs missing columns: {sorted(missing_song)}")

    # The frontend treats code as a lookup key. If duplicates survive upstream,
    # keep the row with the most performances -- the one the derived statistics
    # actually describe.
    if "code" in songs.columns:
        dupes = len(songs) - songs["code"].nunique()
        if dupes:
            songs = (
                songs.sort_values("performance_count", ascending=False)
                .drop_duplicates(subset=["code"], keep="first")
                .reset_index(drop=True)
            )
            print(f"  collapsed {dupes} duplicate pml code rows")

    os.makedirs(os.path.dirname(TARGET_DB), exist_ok=True)
    if os.path.exists(TARGET_DB):
        os.remove(TARGET_DB)

    print(f"Writing {TARGET_DB} ...")
    dst = sqlite3.connect(TARGET_DB)
    entries.to_sql("entries", dst, if_exists="replace", index=False)
    songs.to_sql("songs", dst, if_exists="replace", index=False)

    print("Creating indexes ...")
    cur = dst.cursor()
    for stmt in [
        "CREATE INDEX idx_entries_gen_event_year ON entries(gen_event, year)",
        "CREATE INDEX idx_entries_event ON entries(event)",
        "CREATE INDEX idx_entries_year ON entries(year)",
        "CREATE INDEX idx_entries_school_search ON entries(school_search)",
        "CREATE INDEX idx_entries_level ON entries(school_level, conference)",
        "CREATE INDEX idx_entries_classification ON entries(classification)",
        "CREATE INDEX idx_entries_code_1 ON entries(code_1)",
        "CREATE INDEX idx_entries_code_2 ON entries(code_2)",
        "CREATE INDEX idx_entries_code_3 ON entries(code_3)",
        "CREATE INDEX idx_songs_code ON songs(code)",
        "CREATE INDEX idx_songs_grade_event ON songs(grade, event_name)",
        "CREATE INDEX idx_songs_perf_count ON songs(performance_count)",
        "CREATE INDEX idx_songs_total_search ON songs(total_search)",
    ]:
        cur.execute(stmt)
    dst.commit()
    cur.execute("ANALYZE")
    dst.commit()
    dst.close()

    # VACUUM has to run outside any open transaction.
    vac = sqlite3.connect(TARGET_DB)
    vac.isolation_level = None
    vac.execute("VACUUM")
    vac.close()

    size_mb = os.path.getsize(TARGET_DB) / 1_048_576
    print(f"\nDone. entries={len(entries):,}  songs={len(songs):,}  ({size_mb:.1f} MB)")


if __name__ == "__main__":
    build()
