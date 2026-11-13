// The production-identifier guard as gate code: the digests and the scanner come from the gate's own checkout
// (scripts/check-engine-identifiers.ts beside these flows), the text scanned from the tree under review. The
// tree's own copy of that script is never run as the guard, so a change cannot disarm it for itself.
//
//   node flows/lib/publish-guard.mjs engine <repo root>             the engine roots (src, packages, scripts), as ci.yml
//   node flows/lib/publish-guard.mjs added <repo root> <base> <sha> every line <sha> adds over <base>, any file:
//                                                                   what a snapshot pushed to the public remote,
//                                                                   or uploaded to a sandbox, would newly publish
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const guard = await import(new URL("../../scripts/check-engine-identifiers.ts", import.meta.url).href);

/** Hits on the lines a diff adds (unified diff text, as `git diff -U0`). Pending-move paths are skipped, as in the engine scan. */
export function scanAddedLines(diff) {
  const hits = [];
  let path = null;
  let line = 0;
  for (const raw of diff.split("\n")) {
    if (raw.startsWith("+++ ")) { path = raw === "+++ /dev/null" ? null : raw.replace(/^\+\+\+ b\//, ""); continue; }
    const hunk = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(raw);
    if (hunk) { line = Number(hunk[1]); continue; }
    if (!path || guard.pendingMove(path)) continue;
    if (raw.startsWith("+")) {
      for (const h of guard.scanText(path, raw.slice(1))) hits.push({ ...h, line });
      line++;
    } else if (!raw.startsWith("-") && !raw.startsWith("\\")) line++;
  }
  return hits;
}

export function addedHits(root, base, sha) {
  const diff = execFileSync("git", ["-C", root, "diff", "--no-ext-diff", "--no-renames", "-U0", "--text", base, sha], { encoding: "utf8", maxBuffer: 256 << 20 });
  return scanAddedLines(diff);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [mode, root = ".", base, sha] = process.argv.slice(2);
  try {
    const hits = mode === "engine" ? guard.scanRepository(root) : mode === "added" ? addedHits(root, base, sha) : null;
    if (!hits) throw new Error("usage: publish-guard.mjs engine <root> | added <root> <base> <sha>");
    for (const h of hits) console.error(`${h.path}:${h.line}: production ${h.kind} identifier; supply it as operator configuration`);
    if (hits.length) process.exitCode = 1;
    else console.log(JSON.stringify({ identifierGuard: "clean", mode }));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
