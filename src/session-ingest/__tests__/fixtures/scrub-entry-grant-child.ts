/**
 * Child process for scrub-default-flip.test.ts, "module-load grant" (docs/scrub-engine.md,
 * "Default flip"). Spawned from a directory without bunfig.toml (no test preload) with only PATH
 * and HOME set. It has no static import, so nothing evaluates before the module under test except
 * the side-effect-free grant registry: it imports ONE entry module, never calls its handler, uses
 * the seam's log scrub, and prints one JSON line.
 *
 *   bun scrub-entry-grant-child.ts <repo-relative module> <hosted|self-host>
 */
const [, , target = "", mode = ""] = process.argv;
const root = new URL("../../../../", import.meta.url);
const at = (p: string) => new URL(p, root).href;
// A secret-shaped token built at run time (nothing secret-shaped is stored in the repo).
const SECRET = `ghp_${"A1b2C3d4E5".repeat(4).slice(0, 36)}`;

const cap = await import(at("packages/pipeline/src/lib/scrub-engine/ts-capability.ts"));
const out: Record<string, unknown> = { target, mode, grantsAtStart: cap.scrubTsCapabilities() };
await import(at(target));
out.grantsAfterImport = cap.scrubTsCapabilities();
const seam = await import(at("packages/pipeline/src/lib/scrub-engine/index.ts"));
// The seam's first use, before any handler: the lazy selection of the unset default happens here.
const log: string = seam.scrubTextForLog(`token=${SECRET}`);
out.log = log === "[scrub-unavailable]" ? log : log.includes(SECRET) ? "leaked" : "scrubbed";
try { out.kind = seam.activeScrubEngine().kind; } catch (e) { out.kind = `throws:${(e as { reason?: string }).reason ?? (e as Error).name}`; }
if (mode === "self-host") {
  try { out.init = (await seam.initScrubEngine()).kind; } catch (e) { out.init = `throws:${(e as { reason?: string }).reason ?? (e as Error).name}`; }
}
out.grantsAtEnd = cap.scrubTsCapabilities();
console.log(JSON.stringify(out));
