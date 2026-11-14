/**
 * Child process for predicate-hook-fallback.test.ts. Spawned without the test preload and with
 * only PATH, HOME and the SCRUB_* variables under test. It loads only predicate-hook.ts and
 * ts-capability.ts (never the seam, so the hook is not installed and the fallback answers),
 * optionally declares a TS grant, and prints one JSON line.
 *
 *   bun predicate-hook-child.ts <grant|none>
 */
const [, , grant = "none"] = process.argv;
const base = new URL("../../../../packages/pipeline/src/lib/scrub-engine/", import.meta.url);
// A secret-shaped token built at run time (nothing secret-shaped is stored in the repo).
const SECRET = `ghp_${"A1b2C3d4E5".repeat(4).slice(0, 36)}`;

const cap = await import(new URL("ts-capability.ts", base).href);
const hook = await import(new URL("predicate-hook.ts", base).href);
if (grant !== "none") cap.declareScrubTsCapability(grant);
const value: string | undefined = hook.predicateScrubText(`token=${SECRET}`);
const seamLoaded = Object.keys(require.cache).some((k) => k.endsWith("/scrub-engine/index.ts"));
console.log(JSON.stringify({
  grants: cap.scrubTsCapabilities(),
  seamLoaded,
  result: value === undefined ? "undefined" : value.includes(SECRET) ? "leaked" : "scrubbed",
}));
