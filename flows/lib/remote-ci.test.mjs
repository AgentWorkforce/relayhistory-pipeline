import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { SECRET_LIKE, WORKFLOWS, latestRuns, parseWorkflows, refuseToPublish, snapshot, staleSnapshots } from "./remote-ci.mjs";

test("the gate dispatches only ci.yml and pg-proof.yml, and both keep their push triggers", () => {
  assert.deepEqual(parseWorkflows("ci.yml,pg-proof.yml"), ["ci.yml", "pg-proof.yml"]);
  assert.throws(() => parseWorkflows(""), /nothing to dispatch/);
  assert.throws(() => parseWorkflows("ci.yml,publish-image.yml"), /not a gate workflow/);
  for (const file of Object.keys(WORKFLOWS)) {
    const text = readFileSync(new URL(`../../.github/workflows/${file}`, import.meta.url), "utf8");
    // The schema release anchors read main's push runs: the gate adds a trigger, never narrows one.
    assert.match(text, /^\s*workflow_dispatch\s*:/m, `${file} has no workflow_dispatch trigger`);
    assert.match(text, /^\s*push:\s*\n(\s*#.*\n)*\s*branches: \[main, 'release\/schema-\*'\]/m, `${file} lost its push trigger`);
  }
});

test("secret-like files: ci.yml's guard and more", () => {
  for (const path of [".env", "a/.env", ".env.local", "inputs.json", "x/key.pem", "tls.key", "id_rsa", "home/.ssh/id_ed25519", ".npmrc", "credentials.json"]) assert.ok(SECRET_LIKE.test(path), path);
  for (const path of ["src/env.ts", "docs/inputs.json.md", "src/keys.ts", "docs/credentials.md"]) assert.ok(!SECRET_LIKE.test(path), path);
});

test("the added-line identifier scan finds what a diff adds and ignores what it removes", async () => {
  const { scanAddedLines } = await import("./publish-guard.mjs");
  const { scanText } = await import("../../scripts/check-engine-identifiers.ts");
  // No real identifier is in this repo's tests: a line the scanner flags is found by trying candidates against its digests.
  assert.equal(scanText("x", "nothing here 123").length, 0);
  const diff = ["diff --git a/docs/x.md b/docs/x.md", "--- a/docs/x.md", "+++ b/docs/x.md", "@@ -1,0 +1,2 @@", "+harmless", "+also harmless"].join("\n");
  assert.deepEqual(scanAddedLines(diff), []);
});

test("only this dispatch's newest run per workflow counts", () => {
  const runs = [
    { databaseId: 1, workflowName: "ci", createdAt: "2026-10-29T00:00:00Z" },
    { databaseId: 3, workflowName: "ci", createdAt: "2026-10-29T01:00:00Z" },
    { databaseId: 2, workflowName: "ci", createdAt: "2026-10-29T01:00:00Z" },
    { databaseId: 4, workflowName: "pg-proof", createdAt: "2026-10-29T01:00:00Z" },
    { databaseId: 5, workflowName: "publish-image", createdAt: "2026-10-29T01:00:00Z" },
  ];
  assert.deepEqual(latestRuns(runs, "2026-10-29T00:30:00Z", ["ci"]).map((r) => r.databaseId), [3]);
  assert.deepEqual(latestRuns(runs, "2026-10-29T00:30:00Z", ["ci", "pg-proof"]).map((r) => r.databaseId), [3, 4]);
});

test("the snapshot holds tracked and staged edits, works on main, and touches nothing", () => {
  const repo = mkdtempSync(join(tmpdir(), "snapshot-test-"));
  const git = (...args) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8" }).trim();
  git("init", "-q", "-b", "main");
  git("config", "user.email", "t@example.com");
  git("config", "user.name", "t");
  writeFileSync(join(repo, "a.txt"), "one\n");
  git("add", "a.txt");
  git("commit", "-qm", "base");
  writeFileSync(join(repo, "a.txt"), "two\n"); // tracked edit, unstaged
  writeFileSync(join(repo, "new.txt"), "staged\n");
  git("add", "new.txt"); // new file, staged
  writeFileSync(join(repo, "junk.txt"), "untracked\n"); // never sent
  const head = git("rev-parse", "HEAD");
  const index = git("write-tree");

  const cwd = process.cwd();
  process.chdir(repo);
  let sha;
  try {
    sha = snapshot();
  } finally {
    process.chdir(cwd);
  }
  assert.equal(git("show", `${sha}:a.txt`), "two");
  assert.equal(git("show", `${sha}:new.txt`), "staged");
  assert.throws(() => git("show", `${sha}:junk.txt`));
  assert.equal(git("rev-parse", `${sha}^`), head);
  assert.equal(git("rev-parse", "HEAD"), head, "HEAD moved");
  assert.equal(git("write-tree"), index, "the index changed");
  assert.equal(git("rev-parse", "--abbrev-ref", "HEAD"), "main");
});

test("only old snapshot branches made in this checkout are swept", () => {
  const repo = mkdtempSync(join(tmpdir(), "sweep-test-"));
  const git = (args, env = {}) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8", env: { ...process.env, ...env } }).trim();
  git(["init", "-q", "-b", "main"]);
  git(["config", "user.email", "t@example.com"]);
  git(["config", "user.name", "t"]);
  writeFileSync(join(repo, "a.txt"), "one\n");
  git(["add", "a.txt"]);
  const old = "2026-01-01T00:00:00Z";
  git(["commit", "-qm", "old"], { GIT_COMMITTER_DATE: old, GIT_AUTHOR_DATE: old });
  const oldSha = git(["rev-parse", "HEAD"]);
  writeFileSync(join(repo, "a.txt"), "two\n");
  git(["commit", "-qam", "new"]);
  const newSha = git(["rev-parse", "HEAD"]);
  const elsewhere = "f".repeat(40); // a snapshot another machine made: not in this repo
  const listing = [
    `${oldSha}\trefs/heads/ci/${oldSha.slice(0, 12)}`,
    `${newSha}\trefs/heads/ci/${newSha.slice(0, 12)}`,
    `${elsewhere}\trefs/heads/ci/${elsewhere.slice(0, 12)}`,
    `${oldSha}\trefs/heads/ci/drop-trunk-gating`,
  ].join("\n");
  const cwd = process.cwd();
  process.chdir(repo);
  try {
    assert.deepEqual(staleSnapshots(listing), [`ci/${oldSha.slice(0, 12)}`]);
  } finally {
    process.chdir(cwd);
  }
});

test("a snapshot holding machine-local state is never pushed", async () => {
  const repo = mkdtempSync(join(tmpdir(), "publish-test-"));
  const git = (...args) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8" }).trim();
  git("init", "-q", "-b", "main");
  git("config", "user.email", "t@example.com");
  git("config", "user.name", "t");
  execFileSync("mkdir", ["-p", join(repo, ".claude"), join(repo, "sub/.agentworkforce")]);
  writeFileSync(join(repo, ".claude/settings.json"), "{}\n");
  writeFileSync(join(repo, "sub/.agentworkforce/key"), "secret\n");
  writeFileSync(join(repo, "a.txt"), "one\n");
  git("add", "-A");
  git("commit", "-qm", "base");
  const cwd = process.cwd();
  process.chdir(repo);
  try {
    const why = await refuseToPublish(git("rev-parse", "HEAD"), undefined, repo);
    assert.match(why, /machine-local state/);
    assert.match(why, /\.claude\/settings\.json/);
    assert.match(why, /sub\/\.agentworkforce\/key/);
  } finally {
    process.chdir(cwd);
  }
});

test("a snapshot holding a secret-like file is never pushed, and a clean one is", async () => {
  const repo = mkdtempSync(join(tmpdir(), "publish-secret-"));
  const git = (...args) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8" }).trim();
  git("init", "-q", "-b", "main");
  git("config", "user.email", "t@example.com");
  git("config", "user.name", "t");
  writeFileSync(join(repo, "a.txt"), "one\n");
  git("add", "-A");
  git("commit", "-qm", "base");
  const base = git("rev-parse", "HEAD");
  assert.equal(await refuseToPublish(base, base, repo), "");
  writeFileSync(join(repo, ".env.local"), "X=1\n");
  git("add", "-A");
  git("commit", "-qm", "oops");
  assert.match(await refuseToPublish(git("rev-parse", "HEAD"), base, repo), /secret-like files \(\.env\.local\)/);
});
