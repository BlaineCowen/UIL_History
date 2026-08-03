/** Display helpers shared by server and client components. */

export const SCORE_LABELS: Record<number, string> = {
  1: "Superior",
  2: "Excellent",
  3: "Good",
  4: "Fair",
  5: "Poor",
};

/** Scores are ranks: 1 is best, 5 is worst. */
export function scoreLabel(score: number | null | undefined): string {
  if (score == null) return "—";
  return SCORE_LABELS[Math.round(score)] ?? String(score);
}

export function formatScore(value: number | null | undefined, digits = 2): string {
  if (value == null || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) return "—";
  return value.toLocaleString("en-US");
}

/** "Title — Composer", skipping either half when it is missing. */
export function choice(title?: string, composer?: string): string {
  const t = (title ?? "").trim();
  const c = (composer ?? "").trim();
  if (!t && !c) return "";
  if (!c) return t;
  if (!t) return c;
  return `${t} — ${c}`;
}

export function titleCase(value: string): string {
  return value.replace(/\b([a-z])/g, (m) => m.toUpperCase());
}

/** A UIL "sweepstakes" award is a 1 in both concert and sight-reading. */
export function isSweepstakes(concert: number, sight: number): boolean {
  return concert === 1 && sight === 1;
}

export function pct(part: number, whole: number): string {
  if (!whole) return "0%";
  const v = (part / whole) * 100;
  return v < 1 && v > 0 ? "<1%" : `${v.toFixed(v < 10 ? 1 : 0)}%`;
}
