"""Insert newly-listed PML songs into the `pml` table the matcher reads.

`fetch_pml.py` stores editions in `pml_editions`/`pml_entries`, but
`add_new_song_data.py` matches against the `pml` table. Without this step the
new songs are recorded but invisible to matching, and clearing `not_found`
markers finds nothing extra.

Only inserts. Existing rows are never modified, so the derived columns the
pipeline computes (`song_score`, percentiles, `performance_count`, ...) are
untouched; new rows get them on the next `add_new_performance_data.py` run.

    python Scrape_data/sync_pml.py --edition 2025-2026            # report only
    python Scrape_data/sync_pml.py --edition 2025-2026 --apply
"""

import argparse
import sqlite3
import sys

# The pml table's own spelling of each ensemble, taken from what is already
# there. `adjust_pml` rewrites "band" to "concert band" on every matcher run,
# so inserting the plain form here keeps that step a no-op.
FORM_TO_EVENT_NAME = {
    "100": "concert band",
    "179": "madrigal",
    "921": "full orchestra",
    "926": "string orchestra",
    "931": "mixed chorus",
    "933": "tenor-bass chorus",
    "935": "treble chorus",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="uil.db")
    ap.add_argument("--edition", required=True, help="Edition label to sync from")
    ap.add_argument("--apply", action="store_true", help="Write. Otherwise report only.")
    ap.add_argument(
        "--include-placeholders",
        action="store_true",
        help="Also insert grade-0 category rows (see the note in the source)",
    )
    args = ap.parse_args()

    db = sqlite3.connect(args.db)
    row = db.execute(
        "SELECT id FROM pml_editions WHERE label = ?", (args.edition,)
    ).fetchone()
    if not row:
        have = [r[0] for r in db.execute("SELECT label FROM pml_editions ORDER BY id")]
        raise SystemExit(f"No edition {args.edition!r}. Stored: {', '.join(have) or '(none)'}")
    edition_id = row[0]

    existing = {str(r[0]).strip() for r in db.execute("SELECT code FROM pml")}
    print(f"  pml currently holds {len(existing):,} codes")

    # One row per song_id. A piece listed at two grades yields two entries;
    # take the lowest grade so the matcher's "sort by grade descending, take
    # first" tie-break still has both options available once the second is
    # added by a later edition.
    entries = db.execute(
        "SELECT song_id, form, grade, title, composer, arranger, publisher,"
        " specification FROM pml_entries WHERE edition_id = ? ORDER BY song_id, grade",
        (edition_id,),
    ).fetchall()

    seen, to_insert, placeholders = set(), [], []
    for song_id, form, grade, title, composer, arranger, publisher, spec in entries:
        key = str(song_id).strip()
        if key in existing or key in seen:
            continue
        seen.add(key)
        # Grade 0 is not a difficulty -- it marks "any work of this kind by this
        # composer", e.g. "Overture, tone poem, or one mvt of a symphony" by
        # Beethoven. Those are real PML entries but terrible match targets: every
        # Beethoven overture would collapse onto one code, and this site's whole
        # point is per-piece statistics. The existing pml contains none of them
        # and no result references one, so the default follows that convention.
        if grade == 0 and not args.include_placeholders:
            placeholders.append((key, title, composer))
            continue
        to_insert.append(
            (
                key,
                FORM_TO_EVENT_NAME[form],
                # The pml table stores these lower-cased; adjust_pml would do it
                # anyway, and build_web_db re-cases for display.
                (title or "").strip().lower(),
                (composer or "").strip().lower(),
                (arranger or "").strip().lower(),
                (publisher or "").strip(),
                int(grade) if grade is not None else None,
                (spec or "").strip(),
            )
        )

    if placeholders:
        print(
            f"  skipped {len(placeholders):,} grade-0 category rows"
            " (--include-placeholders to keep them), e.g.:"
        )
        for key, title, composer in placeholders[:3]:
            print(f"    {key:<8} {str(title)[:46]!r} / {composer}")

    print(f"  edition {args.edition!r} adds {len(to_insert):,} songs not in pml")
    if to_insert:
        by_event = {}
        for r in to_insert:
            by_event[r[1]] = by_event.get(r[1], 0) + 1
        for ev, n in sorted(by_event.items(), key=lambda x: -x[1]):
            print(f"    {ev:<20} {n:>5,}")
        print("\n  examples:")
        for r in to_insert[:5]:
            print(f"    {r[0]:<8} g{r[6]}  {r[1]:<18} {r[2][:40]}")

    if not args.apply:
        print("\n  report only -- pass --apply to insert")
        return 0

    cols = [
        "code",
        "event_name",
        "title",
        "composer",
        "arranger",
        "publisher",
        "grade",
        "specification",
    ]
    db.executemany(
        f"INSERT INTO pml ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
        to_insert,
    )
    db.commit()
    after = db.execute("SELECT COUNT(*) FROM pml").fetchone()[0]
    print(f"\n  inserted {len(to_insert):,}; pml now holds {after:,} rows")
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
