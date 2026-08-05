/**
 * Warm the read cache for the views people actually land on.
 *
 * Every read is cached per filter combination, so the *first* visitor to each
 * combination pays the full cost -- 8 queries over 173k rows. Measured cold
 * against a remote database that is 1.4-4.0s for an ensemble switch, against
 * ~20ms once warm. Clicking Band / Chorus / Orchestra for the first time is
 * exactly that penalty, and it lands on a real user unless something else
 * pays it first.
 *
 * Run after a data load or a deploy, both of which start from an empty cache:
 *
 *   node scripts/warm_cache.mjs https://uilpml.com
 *
 * Only warms the handful of entry points worth pre-paying for. The long tail of
 * filter combinations is unbounded and not worth chasing.
 */

const base = (process.argv[2] || process.env.SITE_URL || "http://localhost:3114").replace(/\/$/, "");

const paths = [
  "/",
  "/pml",
  "/about",
  // The ensemble switcher -- the three most-clicked cold keys.
  "/?event=Band",
  "/?event=Chorus",
  "/?event=Orchestra",
  // The PML list's default sort, and its most likely first re-sort.
  "/pml?sort=song_score&dir=desc",
  "/pml?sort=performance_count&dir=desc",
];

const results = [];
for (const path of paths) {
  const url = `${base}${path}`;
  const started = Date.now();
  try {
    const res = await fetch(url, { headers: { "user-agent": "uil-cache-warmer" } });
    // Read the body: the response is streamed, and the work is not finished
    // until it has all arrived.
    await res.text();
    results.push({ path, ms: Date.now() - started, status: res.status });
  } catch (err) {
    results.push({ path, ms: Date.now() - started, status: `failed: ${err.message}` });
  }
}

console.log(`warmed ${base}`);
let slow = 0;
for (const r of results) {
  if (typeof r.status === "number" && r.status !== 200) slow++;
  console.log(`  ${String(r.ms).padStart(6)}ms  ${String(r.status).padEnd(6)} ${r.path}`);
}
console.log(
  "\nRe-run to confirm: a warm pass should be an order of magnitude faster.",
);
process.exit(slow ? 1 : 0);
