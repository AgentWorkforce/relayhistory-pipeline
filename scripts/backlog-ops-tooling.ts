/**
 * The files that decide what a backlog-ops run does, so a run may bind earlier runs at an older main commit when
 * nothing between them touched any of them (scripts/backlog-ops-github.ts, sameTooling).
 *
 * The set is computed, never hand-listed: the transitive static import closure of the backlog-ops entry points, read
 * from this checkout, plus the fixed files that change how that code is installed, compiled or dispatched. Why this
 * checkout's closure is enough for an older commit too: a file can only enter or leave the closure through an edit to
 * a file already in it (its import list), and that edit is itself in the diff and refuses.
 *
 * Fails closed: an import it cannot resolve to a file in this checkout throws, so the gate refuses rather than
 * silently shrinking the set.
 */
import { existsSync, readFileSync, statSync } from "node:fs";
import { dirname, join, normalize, relative } from "node:path";

export const BACKLOG_OPS_ENTRY_POINTS = [
  "scripts/backlog-ops.ts",
  "scripts/backlog-ops-github.ts",
  "scripts/buffered-drain.ts",
  "scripts/upload-fence-backfill.ts",
  "scripts/accepted-stage-export.ts",
] as const;

/** Exact files outside the import graph that change what a run executes. */
export const BACKLOG_OPS_FIXED_FILES = [
  ".github/workflows/backlog-ops.yml",
  "package.json",
  "bun.lock",
  "bunfig.toml",
  "tsconfig.json",
  "versions.lock.json",
] as const;

/** Any file with one of these basenames, anywhere (workspace package manifests and tsconfigs). */
const FIXED_BASENAMES = new Set(["package.json", "tsconfig.json"]);

/** tsconfig.json "paths" (kept in step by a test). */
const ALIASES: Array<[RegExp, (m: RegExpMatchArray) => string]> = [
  [/^@rhc-object-store$/u, () => "packages/object-store/src/index.ts"],
  [/^@rhc\/(.+)$/u, (m) => "packages/pipeline/src/" + m[1]],
];

const IMPORT = /(?:^|[\s;])(?:import|export)\s(?:[^"';]*?\sfrom\s)?["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)/gu;

function isFile(p: string) {
  return existsSync(p) && statSync(p).isFile();
}

function resolveFile(root: string, rel: string): string {
  for (const c of [rel, rel.replace(/\.js$/u, ".ts"), rel.replace(/\.mjs$/u, ".mts"), rel + ".ts", rel + "/index.ts"])
    if (isFile(join(root, c))) return normalize(c);
  throw Object.assign(new Error("backlog_ops_tooling_unresolved"), { code: "backlog_ops_tooling_unresolved" });
}

/** Repository-relative paths (forward slashes) of every file in the closure, sorted. */
export function backlogOpsToolingFiles(root = process.cwd()): string[] {
  const seen = new Set<string>();
  const stack: string[] = [...BACKLOG_OPS_ENTRY_POINTS];
  while (stack.length > 0) {
    const file = normalize(stack.pop()!);
    if (seen.has(file)) continue;
    seen.add(file);
    const src = readFileSync(join(root, file), "utf8");
    for (const m of src.matchAll(IMPORT)) {
      const spec = m[1] ?? m[2]!;
      if (spec.startsWith(".")) { stack.push(resolveFile(root, relative(root, join(root, dirname(file), spec)))); continue; }
      const alias = ALIASES.find(([re]) => re.test(spec));
      if (alias) stack.push(resolveFile(root, alias[1](spec.match(alias[0])!)));
      // Bare package specifiers are pinned by package.json and bun.lock (fixed files).
    }
  }
  for (const f of BACKLOG_OPS_FIXED_FILES) seen.add(f);
  return [...seen].map((p) => p.split("\\").join("/")).sort();
}

/** True when a changed path (a compare entry's filename or previous_filename) is backlog-ops tooling. */
export function touchesTooling(tooling: ReadonlySet<string>, path: string): boolean {
  const base = path.slice(path.lastIndexOf("/") + 1);
  return tooling.has(path) || FIXED_BASENAMES.has(base);
}
