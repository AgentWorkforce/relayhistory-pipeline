/**
 * The offline workspace-synthesis CLI (docs/session-intel-workspace-synthesis.md). No network, no database, no
 * credentials, no environment and no clock: `--now` is required. Outputs name sessions, so --out must be outside
 * this repository unless --allow-repo-out is given (synthetic runs only), as the replay CLI's convention.
 *
 *   bun src/session-intel/synthesis/cli.ts --in <cohort.json> --out <dir> --now <ISO> [--allow-repo-out]
 *
 * Writes <dir>/synthesis.json (workspace-synthesis/1, the bytes the golden pins) and <dir>/synthesis.md.
 */
import { existsSync, readFileSync, realpathSync, statSync } from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { writeAtomic } from "../replay/util.ts";
import { validateWorkspaceSynthesis } from "./schema.ts";
import { synthesisJson, synthesize, type WorkspaceSynthesis } from "./synthesize.ts";

const REPO_ROOT = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
/** A cohort file larger than this is refused before it is read. */
export const COHORT_FILE_MAX_BYTES = 256 * 1024 * 1024;

export interface CliIo { out(line: string): void; err(line: string): void }
const defaultIo: CliIo = { out: (l) => console.log(l), err: (l) => console.error(l) };

/** The path with symlinks resolved (nearest existing ancestor), as replay/cli.ts realPathOf. */
function realPathOf(path: string): string {
  let head = resolve(path);
  const tail: string[] = [];
  while (!existsSync(head)) {
    const up = dirname(head);
    if (up === head) break;
    tail.unshift(basename(head));
    head = up;
  }
  return join(realpathSync(head), ...tail);
}
export function isInside(root: string, path: string): boolean {
  const rel = relative(realPathOf(root), realPathOf(path));
  return !(rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel));
}
export function outDir(value: string | undefined, allowRepo: boolean, repoRoot = REPO_ROOT): string {
  if (!value) throw new Error("--out is required");
  const dir = resolve(value);
  if (!allowRepo && isInside(repoRoot, dir))
    throw new Error("--out is inside the repository; synthesis outputs name sessions and belong outside it (--allow-repo-out for synthetic runs)");
  return dir;
}

export function synthesisMarkdown(s: WorkspaceSynthesis): string {
  const skipped = Object.entries(s.inputs.skipped).filter(([, n]) => n > 0).map(([r, n]) => `${r} ${n}`).join(", ") || "none";
  const lines = [
    "# Workspace synthesis", "",
    `Contract ${s.contract}, workspace ${s.workspace}, window ${s.window.since} to ${s.window.until}, generated ${s.generatedAt}.`, "",
    `Considered ${s.inputs.considered} (eligible ${s.inputs.eligible ?? "unknown"}), analyzed ${s.inputs.analyzed}, skipped: ${skipped}.${s.inputs.truncated ? " Some caps truncated (see inputs.caps)." : ""}`, "",
    "## Categories", "", "| Session type | Outcome status | Sessions |", "|---|---|---|",
    ...s.categories.map((c) => `| ${c.sessionType} | ${c.outcomeStatus} | ${c.count} |`), "",
    "## Usage coverage", "", "| Source | Analyzed | Observed rate | Null cost rate |", "|---|---|---|---|",
    ...Object.entries(s.usageCoverage).map(([src, u]) => `| ${src} | ${u.analyzed} | ${u.observedRate ?? "n/a"} | ${u.nullCostRate ?? "n/a"} |`), "",
    "## Issue candidates", "",
    ...(s.issues.length ? ["| Kind | Key | Sessions | Roots |", "|---|---|---|---|", ...s.issues.map((i) => `| ${i.kind} | \`${i.key.replace(/[|`]/gu, "?")}\` | ${i.sessionCount} | ${i.roots} |`)] : ["None."]), "",
    `Freshness: analyzed ${s.freshness.analyzedAtMin ?? "n/a"} to ${s.freshness.analyzedAtMax ?? "n/a"}, ${s.freshness.staleCount} stale, indexedThroughMs ${s.freshness.indexedThroughMs ?? "n/a"} (${s.freshness.indexedThroughMissing} missing).`,
  ];
  return `${lines.join("\n")}\n`;
}

export async function main(argv: string[], io: CliIo = defaultIo): Promise<number> {
  const { values } = parseArgs({
    args: argv, strict: true, allowPositionals: false,
    options: { in: { type: "string" }, out: { type: "string" }, now: { type: "string" }, "allow-repo-out": { type: "boolean" } },
  });
  if (!values.in) throw new Error("--in is required");
  if (!values.now) throw new Error("--now is required (the CLI reads no clock)");
  const out = outDir(values.out, values["allow-repo-out"] === true);
  const input = resolve(values.in);
  if (statSync(input).size > COHORT_FILE_MAX_BYTES) throw new Error(`--in is larger than ${COHORT_FILE_MAX_BYTES} bytes`);
  const synthesis = validateWorkspaceSynthesis(synthesize(JSON.parse(readFileSync(input, "utf8")), { now: values.now }));
  await writeAtomic(join(out, "synthesis.json"), synthesisJson(synthesis));
  await writeAtomic(join(out, "synthesis.md"), synthesisMarkdown(synthesis));
  io.out(`synthesis: ${synthesis.inputs.analyzed} of ${synthesis.inputs.considered} sessions analyzed, ${synthesis.issues.length} issue candidates -> ${out}`);
  return 0;
}

if (import.meta.main) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (err) {
    console.error(`session-intel synthesis: ${(err as Error).message}`);
    process.exitCode = 1;
  }
}
