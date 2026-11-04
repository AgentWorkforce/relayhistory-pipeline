/** Explicit source checkout only; never fetches, reads environment or credentials. */
import { readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { resolve, join } from "node:path";
const args = process.argv.slice(2),
  at = args.indexOf("--source");
if (at < 0 || !args[at + 1]) throw Error("--source RHC worktree required");
const root = resolve(args[at + 1]),
  path = "packages/relayhistory/src/lib/search-feature-journal.ts";
const text = await readFile(join(root, path), "utf8"),
  sha = createHash("sha256").update(text).digest("hex");
const commit = execFileSync(
  "git",
  ["-C", root, "log", "-1", "--format=%H", "--", path],
  {
    encoding: "utf8",
  },
).trim();
if (
  execFileSync("git", ["-C", root, "status", "--porcelain", "--", path], {
    encoding: "utf8",
  }).trim()
)
  throw Error("source API must be committed");
const expected =
  "// GENERATED from relayhistory-cloud " +
  path +
  "\n// Source commit: " +
  commit +
  "; SHA256: " +
  sha +
  "\n// Regenerate/check: bun scripts/sync-search-feature-source.ts --source <RHC-worktree> [--check]\n" +
  text;
const target = new URL("../src/search-features/source.ts", import.meta.url);
if (args.includes("--check")) {
  if ((await readFile(target, "utf8")) !== expected)
    throw Error("generated feature API drift");
} else await writeFile(target, expected);
console.log(
  JSON.stringify({
    sourceCommit: commit,
    sourceSha256: sha,
    checked: args.includes("--check"),
  }),
);
