import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { fetchBase, reviewerInputs, verifierAgent } from "./review.ts";

const opts = {
  flow: "adversarial-review", root: "/repo", dir: ".relayflow/adversarial-review", files: ".relayflow/adversarial-review/files.txt",
  lenses: [{ name: "correctness", brief: "find bugs" }, { name: "tests", brief: "judge tests" }, { name: "platform", brief: "cfg" }],
  clis: ["claude", "codex"], preamble: "You review.",
};

test("each lens gets its own reviewer input, CLIs round-robin, and the model is carried exactly when given", () => {
  const plain = reviewerInputs(opts);
  assert.deepEqual(plain.map((i) => [i.lens, i.cli]), [["correctness", "claude"], ["tests", "codex"], ["platform", "claude"]]);
  assert.ok(plain.every((i) => !("model" in i)), "no model key when none was given");
  assert.deepEqual(plain.map((i) => i.cwd), [".relayflow/adversarial-review/correctness", ".relayflow/adversarial-review/tests", ".relayflow/adversarial-review/platform"]);
  assert.ok(plain[0].findings.endsWith("/correctness/findings.json"));
  assert.ok(plain[1].task.includes("judge tests") && plain[1].task.includes("You review.") && plain[1].task.includes("TESTS"));
  const modelled = reviewerInputs({ ...opts, model: "claude-fable-5-1" });
  assert.ok(modelled.every((i) => i.model === "claude-fable-5-1"));
});

test("the verifier's agent options carry the model exactly when given, read from the flow's dir, and are read-only", () => {
  const base = { root: "/repo", dir: ".relayflow/x", files: "f.txt", scopeFiles: "s.txt", previous: "p.json", open: 2, paths: ["a.json"], cli: "claude", preamble: "P" };
  const plain = verifierAgent(base);
  assert.ok(!("model" in plain));
  assert.equal(plain.cli, "claude");
  assert.equal(plain.cwd, ".relayflow/x");
  assert.deepEqual(plain.permissions, { accessPreset: "readonly" });
  assert.ok(plain.task.includes("p.json holds 2 blocker/major") && plain.task.includes("a.json"));
  assert.equal(verifierAgent({ ...base, model: "claude-opus-5-5" }).model, "claude-opus-5-5");
});

test("the base fetch accepts a branch name and refuses a refspec that would move a local branch", () => {
  const sh = (command) => execFileSync("/bin/sh", ["-c", command], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  assert.equal(fetchBase("main"), undefined, "a local branch has nothing to fetch");
  assert.ok(fetchBase("origin/main")?.includes("$t env GIT_HTTP_LOW_SPEED_LIMIT=1000 GIT_HTTP_LOW_SPEED_TIME=20 git fetch --no-tags --quiet 'origin' 'main'"), "a stalled remote is given up on");
  assert.ok(fetchBase("origin/main")?.includes("t=$(command -v timeout || command -v gtimeout || true); t=${t:+$t 50};"), "and a black-holed one, where timeout exists");
  assert.equal(fetchBase("feature/x")?.includes("git remote | grep -qxF 'feature'"), true, "a slash alone does not make it remote; the remote check does");
  for (const bad of ["origin/+refs/heads/evil:refs/heads/main", "origin/-opt", "origin/a..b", 'origin/x"$(echo INJECTED >&2)']) {
    // The message quotes the base; a command substitution in it is printed, never run.
    assert.throws(() => sh(fetchBase(bad)), (error) => /not a branch name/.test(error.stderr) && !/^INJECTED$/m.test(error.stderr), bad);
  }
  assert.doesNotThrow(() => sh(fetchBase("nowhere/HEAD")), "a remote's HEAD is a valid base");
  // No remote named `nowhere` here, so the valid name exits 0 before fetching.
  assert.doesNotThrow(() => sh(fetchBase("nowhere/review/x")));
});

test("the fetch command runs against a real remote, with and without a timeout program on the PATH", () => {
  const root = mkdtempSync(join(tmpdir(), "fetch-base-"));
  const git = (dir, ...args) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  const upstream = join(root, "upstream");
  mkdirSync(upstream);
  git(upstream, "init", "-q", "-b", "main");
  git(upstream, "config", "user.email", "t@example.com");
  git(upstream, "config", "user.name", "t");
  writeFileSync(join(upstream, "a.txt"), "a\n");
  git(upstream, "add", ".");
  git(upstream, "commit", "-qm", "base");
  const clone = join(root, "clone");
  execFileSync("git", ["clone", "-q", upstream, clone]);
  writeFileSync(join(upstream, "a.txt"), "b\n");
  git(upstream, "commit", "-qam", "newer");
  // Two PATHs: one with a `timeout` that records it ran, one with none at all.
  const bare = join(root, "bare");
  const withShim = join(root, "with-shim");
  mkdirSync(bare);
  mkdirSync(withShim);
  for (const tool of ["git", "sh", "env", "grep", "sleep"]) {
    const real = execFileSync("sh", ["-c", `command -v ${tool}`], { encoding: "utf8" }).trim();
    symlinkSync(real, join(bare, tool));
    symlinkSync(real, join(withShim, tool));
  }
  const ran = join(root, "timeout-ran");
  writeFileSync(join(withShim, "timeout"), `#!/bin/sh\necho "$1" >> '${ran}'\nshift\nexec "$@"\n`, { mode: 0o755 });
  for (const path of [withShim, bare]) {
    execFileSync("/bin/sh", ["-c", fetchBase("origin/main")], { cwd: clone, env: { ...process.env, PATH: path }, stdio: "ignore" });
    assert.equal(git(clone, "rev-parse", "origin/main"), git(upstream, "rev-parse", "main"), path === bare ? "without timeout on the PATH" : "with it");
    git(clone, "update-ref", "-d", "refs/remotes/origin/main");
  }
  assert.equal(readFileSync(ran, "utf8"), "50\n", "the timeout program ran once, with the 50-second cap");
  // A cut-off (exit 124) is not retried: a slow but moving fetch has the whole cap, once.
  const cutting = join(root, "cutting");
  mkdirSync(cutting);
  for (const tool of ["git", "sh", "env", "grep", "sleep"]) symlinkSync(join(bare, tool), join(cutting, tool));
  writeFileSync(join(cutting, "timeout"), `#!/bin/sh\necho "$1" >> '${ran}'\nexit 124\n`, { mode: 0o755 });
  const cut = spawnSync("/bin/sh", ["-c", fetchBase("origin/main")], { cwd: clone, env: { ...process.env, PATH: cutting } });
  assert.equal(cut.status, 124);
  assert.equal(readFileSync(ran, "utf8"), "50\n50\n", "one attempt after the cut-off, none more");
  // Any other failure is tried three times in all: with the sleeps between, inside the 3-minute lease.
  const failing = join(root, "failing");
  mkdirSync(failing);
  for (const tool of ["sh", "env", "grep", "sleep"]) symlinkSync(join(bare, tool), join(failing, tool));
  writeFileSync(join(failing, "git"), `#!/bin/sh\ncase "$1" in fetch) echo fetch >> '${ran}'; exit 1;; esac\nexec '${join(bare, "git")}' "$@"\n`, { mode: 0o755 });
  const failed = spawnSync("/bin/sh", ["-c", fetchBase("origin/main")], { cwd: clone, env: { ...process.env, PATH: failing } });
  assert.equal(failed.status, 1);
  assert.equal(readFileSync(ran, "utf8"), "50\n50\nfetch\nfetch\nfetch\n", "three attempts on an ordinary failure");
});
