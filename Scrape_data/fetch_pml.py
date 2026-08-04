"""Fetch a UIL Prescribed Music List edition and store it as a snapshot.

The PML page at https://www.uiltexas.org/pml/ renders a DataTables grid backed
by `pml.php`, which returns the *entire* list as JSON -- ~25,800 rows in about a
second. The "CSV" button on that page is a client-side export of whatever rows
are currently on screen, so there is no manual download step here.

Each run stores one edition verbatim. Current state is "present in the newest
edition", delisting is absence from it, and a re-grade is a change in a piece's
grades between editions. Keeping whole editions rather than flags on `songs`
means the list can be reconstructed for any year we have captured.

Run from the repo root, like the rest of the pipeline:

    python Scrape_data/fetch_pml.py --label 2025-2026
    python Scrape_data/fetch_pml.py --label 2019-03 --url <wayback snapshot>
    python Scrape_data/fetch_pml.py --label test --db /tmp/scratch.db --dry-run

This script is purely additive: it creates and writes `pml_editions` and
`pml_entries` and never touches `results` or `pml`.
"""

import argparse
import json
import os
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone

LIVE_URL = "https://www.uiltexas.org/pml/pml.php"

# Column order is positional -- pml.php returns arrays, not objects. Verified
# against the page's own DataTables columnDefs.
COLUMNS = [
    "code",
    "event",
    "title",
    "composer",
    "arranger",
    "publisher",
    "grade",
    "specification",
    "status",
]

# Codes are <form>-<grade>-<id>. These seven forms are the full-ensemble events
# this site covers; everything else is solo and small ensemble.
ENSEMBLE_FORMS = {
    "100": "Concert Band",
    "179": "Madrigal",
    "921": "Full Orchestra",
    "926": "String Orchestra",
    "931": "Mixed Chorus",
    "933": "Tenor-Bass Chorus",
    "935": "Treble Chorus",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS pml_editions (
    id             INTEGER PRIMARY KEY,
    label          TEXT NOT NULL UNIQUE,
    source_url     TEXT,
    fetched_at     TEXT NOT NULL,
    source_rows    INTEGER NOT NULL,
    ensemble_rows  INTEGER NOT NULL
);

-- Surrogate key, deliberately. Neither the id nor the full code is unique
-- within an edition:
--   * 19 ids appear twice as the same piece at two grades -- 921-1-18218 and
--     921-2-18218 are both "Intrada and Elegy".
--   * 3 *full codes* repeat, two of them differing only in specification
--     (' ' vs '(play all)'), one an exact duplicate row.
-- This is a snapshot table, so it records what the source said rather than
-- imposing a uniqueness the source does not have. Collapse when building the
-- flattened current-state view, not on the way in.
CREATE TABLE IF NOT EXISTS pml_entries (
    id             INTEGER PRIMARY KEY,
    edition_id     INTEGER NOT NULL REFERENCES pml_editions(id),
    code           TEXT NOT NULL,
    song_id        TEXT NOT NULL,
    form           TEXT NOT NULL,
    grade          INTEGER,
    event          TEXT,
    title          TEXT,
    composer       TEXT,
    arranger       TEXT,
    publisher      TEXT,
    specification  TEXT,
    status         TEXT
);

CREATE INDEX IF NOT EXISTS idx_pml_entries_song ON pml_entries (edition_id, song_id);
CREATE INDEX IF NOT EXISTS idx_pml_entries_code ON pml_entries (edition_id, code);
"""


class ShapeError(RuntimeError):
    """The feed is not the shape we expect -- fail loudly rather than import garbage."""


def fetch(url: str) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "uil-history-pml-refresh/1.0"}
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        return resp.read()


def parse(raw: bytes, source: str):
    """Validate and normalise one edition. Returns (all_rows, ensemble_rows)."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ShapeError(f"{source} did not return JSON: {exc}") from exc

    if "pml" not in payload:
        raise ShapeError(f"{source}: no 'pml' key (got {list(payload)[:5]})")

    rows = payload["pml"]
    if not rows:
        raise ShapeError(f"{source}: 'pml' is empty")

    width = len(rows[0])
    if width != len(COLUMNS):
        raise ShapeError(
            f"{source}: expected {len(COLUMNS)} columns, got {width}. "
            "The feed's shape changed -- check the page before trusting this."
        )

    ensemble = []
    malformed = 0
    for row in rows:
        rec = dict(zip(COLUMNS, row))
        code = (rec["code"] or "").strip()
        parts = code.split("-")
        if len(parts) != 3:
            malformed += 1
            continue
        form, grade, song_id = parts
        if form not in ENSEMBLE_FORMS:
            continue
        # The middle segment is the grade on every row we have ever seen; if
        # that stops being true the code can no longer be trusted as a key.
        if grade != str(rec["grade"]).strip():
            raise ShapeError(
                f"{source}: code {code} disagrees with its grade column "
                f"({rec['grade']!r}). The code format changed."
            )
        rec.update(code=code, form=form, song_id=song_id, grade=int(grade))
        for key in ("title", "composer", "arranger", "publisher", "specification", "event"):
            rec[key] = (rec[key] or "").strip()
        rec["status"] = str(rec["status"]).strip()
        ensemble.append(rec)

    if malformed > len(rows) * 0.05:
        raise ShapeError(
            f"{source}: {malformed} of {len(rows)} codes are not <form>-<grade>-<id>"
        )
    return rows, ensemble


def store(db_path: str, label: str, url: str, total: int, ensemble: list) -> int:
    db = sqlite3.connect(db_path)
    try:
        db.executescript(SCHEMA)
        existing = db.execute(
            "SELECT id FROM pml_editions WHERE label = ?", (label,)
        ).fetchone()
        if existing:
            # Re-running a label replaces it, so a partial run can be repeated.
            db.execute("DELETE FROM pml_entries WHERE edition_id = ?", (existing[0],))
            db.execute("DELETE FROM pml_editions WHERE id = ?", (existing[0],))

        cur = db.execute(
            "INSERT INTO pml_editions (label, source_url, fetched_at, source_rows,"
            " ensemble_rows) VALUES (?,?,?,?,?)",
            (
                label,
                url,
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                total,
                len(ensemble),
            ),
        )
        edition_id = cur.lastrowid
        db.executemany(
            "INSERT INTO pml_entries (edition_id, code, song_id, form, grade, event,"
            " title, composer, arranger, publisher, specification, status)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    edition_id,
                    r["code"],
                    r["song_id"],
                    r["form"],
                    r["grade"],
                    r["event"],
                    r["title"],
                    r["composer"],
                    r["arranger"],
                    r["publisher"],
                    r["specification"],
                    r["status"],
                )
                for r in ensemble
            ],
        )
        db.commit()
        return edition_id
    finally:
        db.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--label", required=True, help='Edition name, e.g. "2025-2026"')
    ap.add_argument("--url", default=LIVE_URL, help="Source URL (or a Wayback snapshot)")
    ap.add_argument("--file", help="Read from a local JSON file instead of fetching")
    ap.add_argument("--db", default="uil.db", help="SQLite database (default: uil.db)")
    ap.add_argument("--cache", help="Write the raw JSON here, so re-runs need no refetch")
    ap.add_argument("--dry-run", action="store_true", help="Parse and report, write nothing")
    args = ap.parse_args()

    if args.file:
        source = args.file
        raw = open(args.file, "rb").read()
    else:
        source = args.url
        print(f"fetching {source}")
        raw = fetch(args.url)
    print(f"  {len(raw):,} bytes")

    total_rows, ensemble = parse(raw, source)
    print(f"  {len(total_rows):,} rows in source, {len(ensemble):,} ensemble")

    by_form = {}
    for r in ensemble:
        by_form[r["form"]] = by_form.get(r["form"], 0) + 1
    for form, name in ENSEMBLE_FORMS.items():
        print(f"    {form}  {name:<20} {by_form.get(form, 0):>6,}")

    # Surfaced rather than silently collapsed -- if these counts jump, the
    # feed has changed in a way worth looking at before trusting a diff.
    codes, songs = {}, {}
    for r in ensemble:
        codes[r["code"]] = codes.get(r["code"], 0) + 1
        songs[r["song_id"]] = songs.get(r["song_id"], 0) + 1
    dup_codes = sum(1 for n in codes.values() if n > 1)
    dup_songs = sum(1 for n in songs.values() if n > 1)
    print(
        f"  distinct: {len(codes):,} codes, {len(songs):,} song ids"
        f"   (repeated: {dup_codes} codes, {dup_songs} ids at multiple grades)"
    )

    if args.cache and not args.file:
        os.makedirs(os.path.dirname(args.cache) or ".", exist_ok=True)
        with open(args.cache, "wb") as fh:
            fh.write(raw)
        print(f"  cached raw JSON -> {args.cache}")

    if args.dry_run:
        print("dry run: nothing written")
        return 0

    edition_id = store(args.db, args.label, source, len(total_rows), ensemble)
    print(f"stored edition {edition_id} ({args.label!r}) in {args.db}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
