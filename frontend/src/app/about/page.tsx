import type { Metadata } from "next";
import { Card } from "@/components/ui";

export const metadata: Metadata = {
  title: "About",
  description:
    "How the UIL History data is collected, matched to the Prescribed Music List, and scored.",
};

export default function AboutPage() {
  return (
    <div className="grid gap-6 max-w-3xl">
      <header className="grid gap-2">
        <h1 className="text-2xl sm:text-3xl font-semibold tracking-tight">
          About this site
        </h1>
        <p className="text-sm" style={{ color: "var(--ink-2)" }}>
          Where the numbers come from, and what they do and don&apos;t mean.
        </p>
      </header>

      <Card>
        <h2 className="text-[15px] font-semibold mb-2">Methodology</h2>
        <p className="text-sm leading-relaxed" style={{ color: "var(--ink-2)" }}>
          Results are scraped from the public Texas UIL contest forms and stored
          in a database. Directors type their repertoire by hand, so the same
          piece appears under many spellings. Each entry&apos;s title and composer
          text is matched — exactly where possible, fuzzily otherwise — against
          an official Prescribed Music List code. Grades change and pieces are
          retired over time, so the matching is good but not perfect.
        </p>
      </Card>

      <Card>
        <h2 className="text-[15px] font-semibold mb-2">Reading the ratings</h2>
        <p className="text-sm leading-relaxed" style={{ color: "var(--ink-2)" }}>
          UIL ratings are ranks, not points: <strong>1 is the best</strong> and 5
          is the worst. Every chart on this site puts 1 at the top for that
          reason. A &quot;sweepstakes&quot; is a 1 in both concert and
          sight-reading.
        </p>
      </Card>

      <Card>
        <h2 className="text-[15px] font-semibold mb-2">Song score</h2>
        <p className="text-sm leading-relaxed" style={{ color: "var(--ink-2)" }}>
          Song score (0–100) grades a piece on how it has performed at contest.
          It blends two things: how much better or worse the piece scored than
          the average entry at the same contests, and how many times it has been
          performed. A piece that beats its field consistently across hundreds of
          performances scores high; one that did well twice does not.
        </p>
        <p className="text-sm leading-relaxed mt-3" style={{ color: "var(--ink-2)" }}>
          For example, if the average rating at a 2006 contest was 1.6 and a
          piece earned straight 1s there, it outperformed the field by 0.6. The
          more performances behind that difference, the more it counts.
        </p>
      </Card>

      <Card>
        <h2 className="text-[15px] font-semibold mb-2">Known gaps</h2>
        <ul
          className="text-sm leading-relaxed grid gap-2 list-disc pl-4"
          style={{ color: "var(--ink-2)" }}
        >
          <li>
            Entries missing any of the eight individual judge scores are excluded
            entirely, so a handful of contests are under-counted.
          </li>
          <li>
            Pieces the matcher could not tie to a PML code still appear in the
            results table, but do not contribute to any song&apos;s statistics.
          </li>
          <li>
            Solo and small-ensemble events are out of scope — this covers full
            ensembles only.
          </li>
        </ul>
      </Card>

      <Card>
        <h2 className="text-[15px] font-semibold mb-2">Support</h2>
        <p className="text-sm leading-relaxed" style={{ color: "var(--ink-2)" }}>
          This is a personal project, maintained one season at a time. Support
          helps improve the matching, add features, and keep it running.
        </p>
      </Card>
    </div>
  );
}
