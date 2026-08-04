"""Diff two stored PML editions, or an edition against the current song data.

Answers the three questions an annual refresh needs:

  * what was **added** -- these need the fuzzy matcher run against all results,
    and that will match nothing unless `not_found` markers are cleared first
    (see the note at the bottom of the report);
  * what **left the list** -- but most apparent removals are re-codes, not
    delistings, so they are reconciled rather than taken at face value;
  * what was **re-graded** -- compared as a set, because 19 pieces are
    legitimately listed at two grades at once.

Run from the repo root:

    python Scrape_data/diff_pml.py --from 2019-03 --to 2025-2026
    python Scrape_data/diff_pml.py --to 2025-2026 --against-songs
    python Scrape_data/diff_pml.py --to 2025-2026 --against-songs --csv out/
"""

import argparse
import csv
import os
import re
import sqlite3
import sys
from collections import defaultdict

DEFAULT_SONGS_DB = os.path.join("frontend", "data", "uil_web.db")

# Same normalisation the search columns use: lowercase, alphanumerics only.
_STRIP = re.compile(r"[^a-z0-9]")


def norm(value) -> str:
    return _STRIP.sub("", str(value or "").lower())


def load_edition(db, label):
    row = db.execute("SELECT id FROM pml_editions WHERE label = ?", (label,)).fetchone()
    if not row:
        have = [r[0] for r in db.execute("SELECT label FROM pml_editions ORDER BY id")]
        raise SystemExit(f"No edition {label!r}. Stored: {', '.join(have) or '(none)'}")
    entries = defaultdict(list)
    for r in db.execute(
        "SELECT song_id, code, grade, event, title, composer, arranger FROM pml_entries"
        " WHERE edition_id = ?",
        (row[0],),
    ):
        entries[r[0]].append(
            {
                "code": r[1],
                "grade": r[2],
                "event": r[3],
                "title": r[4],
                "composer": r[5],
                "arranger": r[6],
            }
        )
    return entries


def load_songs(path):
    if not os.path.exists(path):
        raise SystemExit(
            f"{path} not found. Build it first:\n"
            "    .venv/bin/python scripts/build_web_db.py"
        )
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    out = {}
    for r in db.execute(
        "SELECT code, title, composer, grade, event_name, performance_count, arranger"
        " FROM songs"
    ):
        out[str(r[0])] = {
            "code": str(r[0]),
            "title": r[1],
            "composer": r[2],
            "grade": r[3],
            "event": r[4],
            "performances": r[5] or 0,
            "arranger": r[6],
        }
    db.close()
    return out


def grades_of(rows):
    """A piece's grade(s) as a set -- 19 ids are listed at two grades at once."""
    return frozenset(r["grade"] for r in rows)


def reconcile(missing, old_side, new_side):
    """Classify pieces absent from the new edition.

    UIL reissues pieces under new ids, so absence is not delisting. Title alone
    is not enough to tell them apart -- it paired Lux Aeterna with the wrong
    entry -- so a re-code requires title *and* composer to agree.
    """
    by_title_composer = defaultdict(list)
    by_title_arranger = defaultdict(list)
    by_title = defaultdict(list)
    for song_id, rows in new_side.items():
        r = rows[0]
        by_title_composer[(norm(r["title"]), norm(r["composer"]))].append((song_id, r))
        by_title[norm(r["title"])].append((song_id, r))
        if norm(r.get("arranger")):
            by_title_arranger[
                (norm(r["title"]), norm(r.get("arranger")), r["grade"])
            ].append((song_id, r))

    recoded, review, delisted = [], [], []
    for song_id in missing:
        old = old_side[song_id][0] if isinstance(old_side[song_id], list) else old_side[song_id]

        strong = by_title_composer.get((norm(old["title"]), norm(old["composer"])), [])
        if strong:
            recoded.append((song_id, old, strong[0]))
            continue

        # Composer attribution drifts on traditional material -- "Dawson"
        # becomes composer "Traditional Spiritual" with Dawson as *arranger*,
        # and "Traditional" becomes "Anon. or Trad.". Title plus arranger plus
        # grade is a tighter key than title plus composer in those cases, and
        # it resolved both pieces that previously needed a human: grade alone
        # separates the grade-4 "Soon-Ah Will Be Done" from the grade-5
        # Tenor-Bass setting sharing its title. Event is deliberately NOT in
        # the key -- the live feed says "Band" where songs says "Concert
        # Band", so including it would silently exclude every band piece.
        # Requiring exactly one candidate keeps a collision going to review.
        # Try our arranger, then our *composer*, against their arranger. The
        # second covers the commonest form of this drift: the name is demoted
        # from composer to arranger when UIL re-credits a traditional work to
        # "Traditional Spiritual". "Soon-Ah Will Be Done" is exactly that --
        # ours has Dawson as composer with no arranger, theirs has Dawson as
        # arranger -- so an arranger-to-arranger key alone cannot see it.
        for candidate_name in (norm(old.get("arranger")), norm(old.get("composer"))):
            if not candidate_name:
                continue
            byarr = by_title_arranger.get(
                (norm(old["title"]), candidate_name, old.get("grade")), []
            )
            if len(byarr) == 1:
                recoded.append((song_id, old, byarr[0]))
                break
        else:
            weak = by_title.get(norm(old["title"]), [])
            if weak:
                review.append((song_id, old, weak))
            else:
                delisted.append((song_id, old))
        continue
    return recoded, review, delisted


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="uil.db", help="Database holding the editions")
    ap.add_argument("--from", dest="frm", help="Baseline edition label")
    ap.add_argument("--to", required=True, help="Newer edition label")
    ap.add_argument(
        "--against-songs",
        action="store_true",
        help="Compare the edition to the built songs table instead of another edition",
    )
    ap.add_argument("--songs-db", default=DEFAULT_SONGS_DB)
    ap.add_argument("--csv", help="Directory to write added/recoded/review/delisted CSVs")
    args = ap.parse_args()

    if not args.frm and not args.against_songs:
        raise SystemExit("Give --from <label> or --against-songs")

    db = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    new = load_edition(db, args.to)

    if args.against_songs:
        songs = load_songs(args.songs_db)
        old = {k: [v] for k, v in songs.items()}
        baseline = f"songs table ({args.songs_db})"
    else:
        old = load_edition(db, args.frm)
        baseline = f"edition {args.frm}"
    db.close()

    print(f"\n  {baseline}  ->  edition {args.to}")
    print(f"  {len(old):,} pieces  ->  {len(new):,} pieces\n")

    added = sorted(set(new) - set(old))
    missing = sorted(set(old) - set(new))
    common = set(old) & set(new)

    regraded = []
    for song_id in sorted(common):
        before, after = grades_of(old[song_id]), grades_of(new[song_id])
        if before != after:
            regraded.append((song_id, sorted(before), sorted(after), new[song_id][0]))

    recoded, review, delisted = reconcile(missing, old, new)

    print(f"  ADDED      {len(added):>6,}   need the fuzzy matcher run against all results")
    print(f"  RE-GRADED  {len(regraded):>6,}")
    print(f"  ABSENT     {len(missing):>6,}   of which:")
    print(f"    re-coded {len(recoded):>6,}   title AND composer match -> carry history to the new id")
    print(f"    review   {len(review):>6,}   title matches, composer differs -> decide by hand")
    print(f"    delisted {len(delisted):>6,}   genuinely off the list")

    if regraded:
        # A piece can gain or lose a grade as well as move between them, so
        # "harder/easier" only means something when the set is the same size.
        # Calling "2 -> 1,2" easier would be wrong: it is still offered at 2.
        gained = [x for x in regraded if set(x[1]) < set(x[2])]
        lost = [x for x in regraded if set(x[2]) < set(x[1])]
        moved = [x for x in regraded if x not in gained and x not in lost]
        harder = sum(1 for _, b, a, _ in moved if max(a) > max(b))
        print(
            f"\n  re-grades: {len(moved)} moved ({harder} harder,"
            f" {len(moved)-harder} easier), {len(gained)} gained a grade,"
            f" {len(lost)} lost one"
        )
        for song_id, b, a, r in (moved + gained + lost)[:10]:
            kind = "moved " if (song_id, b, a, r) in moved else (
                "gained" if (song_id, b, a, r) in gained else "lost  ")
            print(
                f"    {kind} {song_id:<8} {','.join(map(str,b))} ->"
                f" {','.join(map(str,a))}   {r['title'][:44]}"
            )
        if len(regraded) > 10:
            print(f"    ... and {len(regraded)-10} more")

    if recoded:
        print("\n  re-coded (performance history must follow the new id):")
        for song_id, o, (new_id, n) in recoded[:10]:
            perf = f"{o.get('performances', 0):,} perfs" if "performances" in o else ""
            print(f"    {song_id} -> {new_id}  {n['code']:<14} {o['title'][:34]:<34} {perf}")
        if len(recoded) > 10:
            print(f"    ... and {len(recoded)-10} more")

    if review:
        print("\n  NEEDS REVIEW -- same title, different composer:")
        for song_id, o, cands in review:
            names = ", ".join(sorted({c[1]["composer"][:18] for c in cands})[:3])
            print(f"    {song_id}  {o['title'][:34]:<34} yours={str(o['composer'])[:16]:<16} theirs={names}")

    if delisted:
        print("\n  delisted (keep, mark as off the list):")
        ranked = sorted(delisted, key=lambda x: -(x[1].get("performances", 0) or 0))
        for song_id, o in ranked[:10]:
            perf = o.get("performances", 0) or 0
            print(f"    {song_id}  {perf:>5,} perfs   {o['title'][:44]}")

    if args.csv:
        os.makedirs(args.csv, exist_ok=True)
        write = lambda name, header, rows: _csv(os.path.join(args.csv, name), header, rows)
        write("added.csv", ["song_id", "code", "grade", "event", "title", "composer"],
              [[i, new[i][0]["code"], new[i][0]["grade"], new[i][0]["event"],
                new[i][0]["title"], new[i][0]["composer"]] for i in added])
        write("regraded.csv", ["song_id", "from", "to", "title"],
              [[i, "/".join(map(str, b)), "/".join(map(str, a)), r["title"]] for i, b, a, r in regraded])
        write("recoded.csv", ["old_song_id", "new_song_id", "new_code", "title", "composer"],
              [[i, ni, n["code"], n["title"], n["composer"]] for i, o, (ni, n) in recoded])
        write("review.csv", ["song_id", "title", "our_composer", "their_composers"],
              [[i, o["title"], o["composer"], "; ".join(sorted({c[1]["composer"] for c in cs}))]
               for i, o, cs in review])
        write("delisted.csv", ["song_id", "title", "composer", "performances"],
              [[i, o["title"], o["composer"], o.get("performances", 0)] for i, o in delisted])
        print(f"\n  wrote CSVs to {args.csv}/")

    if added:
        print(
            "\n  NOTE: add_new_song_data.py only retries slots that are NULL/'none'."
            "\n  Entries that failed before are stamped 'not_found' and will never be"
            "\n  retried, so re-running the matcher for these additions is a no-op"
            "\n  until those markers are cleared."
        )
    return 0


def _csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


if __name__ == "__main__":
    sys.exit(main())
