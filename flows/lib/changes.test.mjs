import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { MACHINE_LOCAL, UNTRACKED } from "./changes.ts";
import { TREE_FINGERPRINT } from "./review.ts";

/** A repo on `work`, one commit past `main`, with a tracked edit not yet committed. */
function repo() {
  const dir = mkdtempSync(join(tmpdir(), "changes-test-"));
  const git = (...args) => execFileSync("git", ["-C", dir, ...args], { encoding: "utf8" }).trim();
  git("init", "-q", "-b", "main");
  git("config", "user.email", "t@example.com");
  git("config", "user.name", "t");
  mkdirSync(join(dir, "src"));
  writeFileSync(join(dir, "src/a.txt"), "one\n");
  git("add", ".");
  git("commit", "-qm", "base");
  git("checkout", "-qb", "work");
  writeFileSync(join(dir, "src/b.txt"), "committed\n");
  writeFileSync(join(dir, "Cargo.lock"), "lock\n".repeat(50));
  git("add", ".");
  git("commit", "-qm", "work");
  writeFileSync(join(dir, "src/a.txt"), "two\n");
  return { dir, git };
}
const sh = (dir, command) => execFileSync("/bin/sh", ["-c", command], { cwd: dir, encoding: "utf8" });

test("unstaged new files count anywhere, but the relay's and the CLIs' state does not", () => {
  const { dir } = repo();
  mkdirSync(join(dir, "tools"));
  writeFileSync(join(dir, "tools/build.sh"), "x\n");
  writeFileSync(join(dir, "MIGRATION.md"), "x\n");
  writeFileSync(join(dir, "src/new.txt"), "x\n");
  for (const local of MACHINE_LOCAL) {
    mkdirSync(join(dir, local));
    writeFileSync(join(dir, local, "state.json"), "{}\n");
  }
  assert.deepEqual(sh(dir, UNTRACKED).trim().split("\n").sort(), ["MIGRATION.md", "src/new.txt", "tools/build.sh"]);
});

test("the tree fingerprint moves with any edit a reviewer could make", () => {
  const { dir } = repo();
  mkdirSync(join(dir, "flows/node_modules/.bin"), { recursive: true });
  writeFileSync(join(dir, ".gitignore"), "flows/node_modules/\n/.claude/\n");
  mkdirSync(join(dir, ".claude"));
  writeFileSync(join(dir, ".claude/settings.json"), "{}\n");
  writeFileSync(join(dir, "untracked.txt"), "one\n");
  writeFileSync(join(dir, "flows/node_modules/.bin/flows"), "one\n");
  const print = () => sh(dir, TREE_FINGERPRINT).trim();
  const seen = new Set([print()]);
  assert.equal(print(), [...seen][0], "stable when nothing changed");
  mkdirSync(join(dir, ".claude/hooks"), { recursive: true });
  for (const [path, text] of [
    ["src/a.txt", "three\n"],
    ["untracked.txt", "two\n"],
    ["flows/node_modules/.bin/flows", "two\n"],
    [".claude/settings.json", '{"hooks":{}}\n'],
    [".claude/hooks/run.sh", "echo planted\n"],
    [".git/hooks/pre-commit", "echo planted\n"],
  ]) {
    writeFileSync(join(dir, path), text);
    const now = print();
    assert.match(now, /^[0-9a-f]{40}$/);
    assert.ok(!seen.has(now), `editing ${path} did not move it`);
    seen.add(now);
  }
  mkdirSync(join(dir, ".agentworkforce"));
  writeFileSync(join(dir, ".agentworkforce/inbox.json"), "{}\n");
  assert.ok(seen.has(print()), "the relay's own state must not trip it");
});

