// What a review run looks at, and what it leaves for the next one.
//
//   node flows/lib/review-state.mjs prepare <flow> <base> <full 0|1> [include...]
//   node flows/lib/review-state.mjs save <flow> <snapshot> <merge-base>
//   node flows/lib/review-state.mjs open <flow> <merge-base>     how many blocker/major findings the last round left open
//
// The first review of a change reads all of it: the diff from the merge base
// to a snapshot of the working tree (tracked edits, staged files, and the
// untracked paths in `include`). After that, each run reads only what changed
// since the snapshot the last run reviewed, so a fix round reviews the fix,
// not the whole change again. The last run's open blocker and major findings
// carry over to the verifier, which reports each one again until the code no
// longer has it; minor ones are listed in the report, not re-raised.
//
// prepare resets the flow's state dir (.relayflow/<flow>/, or under
// RELAYFLOW_STATE_DIR: lib/state-dir.mjs) with change.patch, files.txt (the
// files the patch touches), scope-files.txt (those plus the files earlier
// open findings cite) and previous.json, and prints {"dir","stateRoot",
// "snapshot","mergeBase","from","incremental","files","previous","unseen"}.
// save records the reviewed snapshot (refs/relayflow/<flow>/reviewed keeps it from git gc)
// and report.json's open findings in <state root>/<flow>.state.json, keyed by
// branch and merge base: another change never inherits this one's state.
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { MACHINE_LOCAL } from "./changes.ts";
import { snapshot } from "./remote-ci.mjs";
import { checkRoot, stateDir, stateRoot } from "./state-dir.mjs";

const git = (args) => execFileSync("git", args, { encoding: "utf8", maxBuffer: 256 * 1024 * 1024 }).trim();
// Lockfiles stay in files.txt but out of the patch: thousands of generated
// lines that no reviewer reads, and that would crowd out the real change.
export const NOT_LOCKFILES = [".", ":(exclude,glob)**/*.lock", ":(exclude,glob)**/*-lock.json", ":(exclude,glob)**/Package.resolved"];
const BLOCKING = new Set(["blocker", "major"]);

/**
 * The paths among `paths` that a .gitattributes `-diff` line hides: git shows
 * them as binary, so their text has to be asked for on its own.
 */
export function hiddenByAttribute(paths) {
  if (!paths.length) return [];
  // `-diff` reads as unset; `binary` is a macro for `-diff -merge -text` and reads the same way.
  // Over stdin: a change with thousands of binaries must not overrun the command line.
  const out = execFileSync("git", ["check-attr", "-z", "--stdin", "diff"], { input: paths.join("\0") + "\0", encoding: "utf8", maxBuffer: 64 * 1024 * 1024 }).split("\0");
  const hidden = [];
  for (let i = 0; i + 2 < out.length; i += 3) if (out[i + 2] === "unset") hidden.push(out[i]);
  return hidden;
}

/** Whether the blob at `rev:path` holds a NUL in its first 8000 bytes, as git's own binary test does; a missing blob is not binary. */
export function binaryBlob(rev, path) {
  // Only a missing blob reads as text; the head alone is read, so size does not matter.
  let exists = true;
  try {
    execFileSync("git", ["cat-file", "-e", `${rev}:${path}`], { stdio: "ignore" });
  } catch {
    exists = false;
  }
  if (!exists) return false;
  const quoted = `'${`${rev}:${path}`.replaceAll("'", "'\\''")}'`;
  return execFileSync("/bin/sh", ["-c", `git cat-file -p ${quoted} | head -c 8000`], { maxBuffer: 64 * 1024 }).includes(0);
}

/**
 * The diff from `from` to `to` over `pathspec`, with any text file a `-diff`
 * or `binary` attribute hides shown as text after it: a change cannot hide a
 * source file from a review by marking it binary. A blob that really is
 * binary on either side stays "Binary files differ", and the second diff is
 * asked for each hidden path literally, inside the same exclusions.
 */
export function diffShowingHidden(from, to, pathspec, extra = []) {
  const uncounted = git(["diff", "--numstat", "--no-renames", "-z", from, to, "--", ...pathspec])
    .split("\0")
    .filter((entry) => entry.startsWith("-\t-\t"))
    .map((entry) => entry.slice(4));
  const hidden = hiddenByAttribute(uncounted).filter((path) => !binaryBlob(from, path) && !binaryBlob(to, path));
  if (!hidden.length) return git(["diff", "--no-renames", "--no-ext-diff", ...extra, from, to, "--", ...pathspec]);
  // Each hidden file once: out of the main diff, then as text.
  const diff = git(["diff", "--no-renames", "--no-ext-diff", ...extra, from, to, "--", ...pathspec, ...hidden.map((path) => `:(exclude,literal)${path}`)]);
  const excludes = pathspec.filter((spec) => spec.startsWith(":(exclude"));
  const literal = hidden.map((path) => `:(literal)${path}`);
  return `${diff}\n${git(["diff", "--no-renames", "--no-ext-diff", "--text", ...extra, from, to, "--", ...literal, ...excludes])}`;
}

export const statePath = (flow) => `${stateRoot()}/${flow}.state.json`;
const branch = () => git(["rev-parse", "--abbrev-ref", "HEAD"]);
const readState = (flow) => {
  try {
    return JSON.parse(readFileSync(statePath(flow), "utf8"));
  } catch {
    return undefined;
  }
};
const exists = (sha) => {
  try {
    git(["cat-file", "-e", `${sha}^{commit}`]);
    return true;
  } catch {
    return false;
  }
};

export function prepare(flow, base, full, include) {
  const root = stateRoot();
  checkRoot(root);
  const dir = stateDir(flow);
  const mergeBase = git(["merge-base", base, "HEAD"]);
  const snap = snapshot(include);
  const state = readState(flow);
  const same = state && state.branch === branch() && state.mergeBase === mergeBase && exists(state.snapshot);
  const incremental = !full && !!same;
  const from = incremental ? state.snapshot : mergeBase;
  const previous = incremental ? state.open ?? [] : [];

  rmSync(dir, { recursive: true, force: true });
  mkdirSync(dir, { recursive: true });
  writeFileSync(`${dir}/change.patch`, diffShowingHidden(from, snap, NOT_LOCKFILES) + "\n");
  const files = git(["diff", "--no-renames", "--name-only", from, snap]).split("\n").filter(Boolean);
  writeFileSync(`${dir}/files.txt`, files.map((file) => `${file}\n`).join(""));
  const scope = [...new Set([...files, ...previous.map((finding) => finding.file)])];
  writeFileSync(`${dir}/scope-files.txt`, scope.map((file) => `${file}\n`).join(""));
  writeFileSync(`${dir}/previous.json`, `${JSON.stringify(previous, null, 2)}\n`);
  // New files neither staged nor passed in `include` are invisible to the
  // review; the flow refuses to call such a tree reviewed, as CI refuses to
  // call it green.
  const unseen = git(["ls-files", "--others", "--exclude-standard", "--", ".", ...[...MACHINE_LOCAL, ...include].map((p) => `:!:${p}`)])
    .split("\n")
    .filter(Boolean);
  return { dir, stateRoot: root, snapshot: snap, mergeBase, from, incremental, files: files.length, previous: previous.length, unseen };
}

/** The last round's open blocker and major findings for this branch and merge base, else 0. */
export function openCount(flow, mergeBase) {
  const state = readState(flow);
  return state && state.branch === branch() && state.mergeBase === mergeBase ? (state.open ?? []).length : 0;
}

export function save(flow, snap, mergeBase) {
  const report = JSON.parse(readFileSync(`${stateDir(flow)}/report.json`, "utf8"));
  const open = report.findings.filter((finding) => BLOCKING.has(finding.severity));
  git(["update-ref", `refs/relayflow/${flow}/reviewed`, snap]);
  mkdirSync(stateRoot(), { recursive: true });
  writeFileSync(statePath(flow), `${JSON.stringify({ branch: branch(), mergeBase, snapshot: snap, open }, null, 2)}\n`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [command, flow, ...rest] = process.argv.slice(2);
  try {
    if (command === "prepare") {
      const [base, full, ...include] = rest;
      console.log(JSON.stringify(prepare(flow, base, full === "1", include)));
    } else if (command === "save") save(flow, rest[0], rest[1]);
    else if (command === "open") console.log(openCount(flow, rest[0]));
    else throw new Error("usage: review-state.mjs prepare <flow> <base> <full 0|1> [include...] | save <flow> <snapshot> <merge-base> | open <flow> <merge-base>");
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
