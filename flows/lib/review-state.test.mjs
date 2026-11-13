import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { hiddenByAttribute, openCount, prepare, save, statePath } from "./review-state.mjs";

// The default layout, whatever the shell running the tests has set; one test sets it.
delete process.env.RELAYFLOW_STATE_DIR;

/** A repo on `work`, one commit past `main`, with a tracked edit not yet committed; cwd moves into it. */
function repo(t) {
  const dir = mkdtempSync(join(tmpdir(), "review-state-test-"));
  const git = (...args) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8" }).trim();
  git("init", "-q", "-b", "main");
  git("config", "user.email", "t@example.com");
  git("config", "user.name", "t");
  mkdirSync(join(dir, "src"));
  writeFileSync(join(dir, "src/a.txt"), "one\n");
  writeFileSync(join(dir, ".gitignore"), ".relayflow/\n");
  git("add", ".");
  git("commit", "-qm", "base");
  git("checkout", "-qb", "work");
  writeFileSync(join(dir, "src/b.txt"), "committed\n");
  writeFileSync(join(dir, "Cargo.lock"), "lock\n".repeat(50));
  git("add", ".");
  git("commit", "-qm", "work");
  writeFileSync(join(dir, "src/a.txt"), "two\n");
  const cwd = process.cwd();
  process.chdir(dir);
  t.after(() => process.chdir(cwd));
  return { dir, git };
}
const read = (path) => readFileSync(path, "utf8");
const lines = (path) => read(path).trim().split("\n").filter(Boolean).sort();
const report = (flow, findings) => {
  mkdirSync(`.relayflow/${flow}`, { recursive: true });
  writeFileSync(`.relayflow/${flow}/report.json`, JSON.stringify({ reviewer: "verifier", findings }));
};
const major = (file) => ({ severity: "major", file, line: 1, title: "t", failure_scenario: "s" });

test("the first round reviews the branch, tracked edits and included new files, without lockfiles", (t) => {
  const { dir } = repo(t);
  mkdirSync(join(dir, "fresh"));
  writeFileSync(join(dir, "fresh/c.txt"), "new file\n");
  writeFileSync(join(dir, "stray.txt"), "not included\n");
  const p = prepare("r", "main", false, ["fresh"]);
  assert.equal(p.incremental, false);
  const patch = read(".relayflow/r/change.patch");
  assert.match(patch, /\+committed/, "the branch's commits");
  assert.match(patch, /\+two/, "the uncommitted tracked edit");
  assert.match(patch, /\+new file/, "the included untracked file");
  assert.doesNotMatch(patch, /stray|lock/);
  assert.deepEqual(lines(".relayflow/r/files.txt"), ["Cargo.lock", "fresh/c.txt", "src/a.txt", "src/b.txt"]);
});

test("a later round reviews only what changed since, and re-checks only the open blockers and majors", (t) => {
  const { dir } = repo(t);
  const first = prepare("r", "main", false, []);
  report("r", [major("src/b.txt"), { ...major("src/a.txt"), severity: "minor" }]);
  save("r", first.snapshot, first.mergeBase);

  writeFileSync(join(dir, "src/a.txt"), "three\n");
  const second = prepare("r", "main", false, []);
  assert.equal(second.incremental, true);
  assert.equal(second.from, first.snapshot);
  const patch = read(".relayflow/r/change.patch");
  assert.match(patch, /-two\n\+three/);
  assert.doesNotMatch(patch, /committed/, "already reviewed");
  assert.deepEqual(lines(".relayflow/r/files.txt"), ["src/a.txt"]);
  assert.deepEqual(JSON.parse(read(".relayflow/r/previous.json")).map((f) => f.file), ["src/b.txt"], "minor ones are not carried");
  assert.deepEqual(lines(".relayflow/r/scope-files.txt"), ["src/a.txt", "src/b.txt"]);
});

test("a .gitattributes -diff line cannot hide a text file from the patch; a real binary stays binary even when marked, and a file named * cannot widen the pass", (t) => {
  const { dir, git } = repo(t);
  writeFileSync(join(dir, ".gitattributes"), "src/hidden.txt -diff\n*.png binary\nsrc/marked.bin -diff\n[*] -diff\n");
  writeFileSync(join(dir, "src/hidden.txt"), "secret logic\n".repeat(5));
  writeFileSync(join(dir, "src/blob.bin"), Buffer.concat([Buffer.from([0, 1, 2, 0]), Buffer.from("BLOBPAYLOAD".repeat(50))]));
  writeFileSync(join(dir, "src/shot.png"), Buffer.concat([Buffer.from([0x89, 0x50, 0, 0]), Buffer.from("PNGPAYLOAD".repeat(50))]));
  writeFileSync(join(dir, "src/marked.bin"), Buffer.concat([Buffer.from([0, 7]), Buffer.from("MARKEDPAYLOAD".repeat(50))]));
  writeFileSync(join(dir, "*"), "star file\n");
  git("add", ".");
  prepare("r", "main", false, []);
  const patch = read(".relayflow/r/change.patch");
  assert.ok(patch.includes("+secret logic"), "the hidden file's text is in the patch");
  assert.ok(patch.includes("+star file"), "so is the file named *");
  for (const bytes of ["BLOBPAYLOAD", "PNGPAYLOAD", "MARKEDPAYLOAD"]) assert.ok(!patch.includes(bytes), `${bytes} stays out`);
  assert.ok(!patch.includes("lock\n+lock"), "the lockfile stays out even though * was hidden");
  assert.equal(patch.split("src/hidden.txt b/src/hidden.txt").length, 2, "the hidden file appears once");
});

test("the open count is the last round's blockers and majors, for this branch and merge base only", (t) => {
  const { git } = repo(t);
  assert.equal(openCount("r", "x"), 0, "no state yet");
  const first = prepare("r", "main", false, []);
  report("r", [major("src/b.txt"), { ...major("src/a.txt"), severity: "blocker" }, { ...major("src/a.txt"), severity: "minor" }]);
  save("r", first.snapshot, first.mergeBase);
  assert.equal(openCount("r", first.mergeBase), 2);
  assert.equal(openCount("other", first.mergeBase), 0, "another flow");
  assert.equal(openCount("r", "0".repeat(40)), 0, "another merge base");
  const cli = execFileSync(process.execPath, [fileURLToPath(new URL("./review-state.mjs", import.meta.url)), "open", "r", first.mergeBase], { encoding: "utf8" });
  assert.equal(cli.trim(), "2");
  git("checkout", "-qb", "elsewhere");
  assert.equal(openCount("r", first.mergeBase), 0, "another branch");
});

test("nothing changed since the last round leaves an empty patch to review", (t) => {
  repo(t);
  const first = prepare("r", "main", false, []);
  report("r", []);
  save("r", first.snapshot, first.mergeBase);
  const again = prepare("r", "main", false, []);
  assert.equal(again.incremental, true);
  assert.equal(again.files, 0);
  assert.equal(again.previous, 0);
});

test("full, another branch or a moved merge base start over with a whole review", (t) => {
  const { dir, git } = repo(t);
  const first = prepare("r", "main", false, []);
  report("r", [major("src/b.txt")]);
  save("r", first.snapshot, first.mergeBase);
  assert.equal(prepare("r", "main", true, []).incremental, false, "full");

  report("r", [major("src/b.txt")]);
  save("r", first.snapshot, first.mergeBase);
  git("stash", "-q");
  git("checkout", "-qb", "other");
  assert.equal(prepare("r", "main", false, []).incremental, false, "another branch");

  git("checkout", "-q", "work");
  report("r", [major("src/b.txt")]);
  save("r", first.snapshot, first.mergeBase);
  git("checkout", "-q", "main");
  writeFileSync(join(dir, "src/z.txt"), "main moved\n");
  git("add", ".");
  git("commit", "-qm", "main moved");
  git("checkout", "-q", "work");
  git("rebase", "-q", "main");
  const rebased = prepare("r", "main", false, []);
  assert.equal(rebased.incremental, false, "a moved merge base");
  assert.equal(rebased.previous, 0);
});

test("new files neither staged nor included are reported as unseen; machine-local state is not", (t) => {
  const { dir } = repo(t);
  mkdirSync(join(dir, "fresh"));
  writeFileSync(join(dir, "fresh/c.txt"), "included\n");
  writeFileSync(join(dir, "forgotten.ts"), "not included\n");
  mkdirSync(join(dir, ".agentworkforce"));
  writeFileSync(join(dir, ".agentworkforce/state.json"), "{}\n");
  assert.deepEqual(prepare("r", "main", false, ["fresh"]).unseen, ["forgotten.ts"]);
});

test("with RELAYFLOW_STATE_DIR, the work dir and the saved state go there and nothing lands in the checkout", (t) => {
  const { dir } = repo(t);
  const root = mkdtempSync(join(tmpdir(), "review-state-root-"));
  process.env.RELAYFLOW_STATE_DIR = root;
  t.after(() => delete process.env.RELAYFLOW_STATE_DIR);
  const first = prepare("r", "main", false, []);
  assert.equal(first.dir, join(root, "r"));
  assert.equal(first.stateRoot, root);
  assert.match(read(join(root, "r/change.patch")), /\+committed/);
  mkdirSync(join(root, "r"), { recursive: true });
  writeFileSync(join(root, "r/report.json"), JSON.stringify({ reviewer: "verifier", findings: [major("src/b.txt")] }));
  save("r", first.snapshot, first.mergeBase);
  assert.equal(statePath("r"), join(root, "r.state.json"));
  assert.equal(JSON.parse(read(join(root, "r.state.json"))).open.length, 1);
  assert.equal(existsSync(join(dir, ".relayflow")), false);
  assert.equal(prepare("r", "main", false, []).incremental, true, "the next round reads the state back from there");
});

test("hiddenByAttribute names exactly the -diff paths, and takes a list past any command line's limit", (t) => {
  const { dir, git } = repo(t);
  writeFileSync(join(dir, ".gitattributes"), "src/a.txt -diff\n");
  git("add", ".");
  assert.deepEqual(hiddenByAttribute(["src/a.txt", "src/b.txt"]), ["src/a.txt"]);
  writeFileSync(join(dir, ".gitattributes"), "src/a.txt -diff\nassets/last.png -diff\n");
  // Enough paths that one command line could not carry them: past this machine's ARG_MAX.
  const argMax = Number(execFileSync("getconf", ["ARG_MAX"], { encoding: "utf8" }).trim()) || 2_097_152;
  const count = Math.ceil(argMax / "assets/pack/icon-00000.png\0".length) + 1000;
  const many = [...Array.from({ length: count }, (_, i) => `assets/pack/icon-${i}.png`), "assets/last.png"];
  assert.deepEqual(hiddenByAttribute(many), ["assets/last.png"], `${count} paths, past ARG_MAX (${argMax}), are all checked`);
});
