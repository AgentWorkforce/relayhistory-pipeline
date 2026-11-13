import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { checkRoot, stateDir, stateRoot } from "./state-dir.mjs";

const CLI = fileURLToPath(new URL("./state-dir.mjs", import.meta.url));
const REPO = "/work/repo";

test("unset or blank, every flow keeps its state in the checkout's .relayflow, as before", () => {
  for (const env of [{}, { RELAYFLOW_STATE_DIR: "" }, { RELAYFLOW_STATE_DIR: "  " }]) {
    assert.equal(stateRoot(env, REPO), ".relayflow");
    assert.equal(stateDir("adversarial-review", env, REPO), ".relayflow/adversarial-review");
    assert.equal(stateDir("ci", env, REPO), ".relayflow/ci");
  }
});

test("set, each flow gets its own directory under it, so one run dir holds every flow's reports", () => {
  const env = { RELAYFLOW_STATE_DIR: "/runs/r1/" };
  assert.equal(stateRoot(env, REPO), "/runs/r1");
  assert.equal(stateDir("adversarial-review", env, REPO), "/runs/r1/adversarial-review");
  assert.equal(stateDir("context-review", env, REPO), "/runs/r1/context-review");
  assert.equal(stateDir("ci", env, REPO), "/runs/r1/ci");
});

test("a relative value is taken from the repository root and made absolute", () => {
  assert.equal(stateRoot({ RELAYFLOW_STATE_DIR: "../runs/r2" }, REPO), "/work/runs/r2");
  assert.equal(stateDir("ci", { RELAYFLOW_STATE_DIR: "state" }, REPO), "/work/repo/state/ci");
});

test("a flow name cannot climb out of the root", () => {
  for (const flow of ["", ".", "..", "../x", "a/b"]) assert.throws(() => stateDir(flow, {}, REPO), /not a flow name/);
});

/** A checkout that ignores `ignored/`; returns its path. */
function repo() {
  // Its real path, as the command's working directory reports it (macOS links /var).
  const dir = realpathSync(mkdtempSync(join(tmpdir(), "state-dir-test-")));
  execFileSync("git", ["-C", dir, "init", "-q"]);
  writeFileSync(join(dir, ".gitignore"), ".relayflow/\nignored/\n");
  return dir;
}

test("a root inside the checkout must be git-ignored; one outside it or the default is always fine", () => {
  const dir = repo();
  assert.doesNotThrow(() => checkRoot(".relayflow", dir));
  assert.doesNotThrow(() => checkRoot(join(dir, "ignored/run"), dir));
  assert.doesNotThrow(() => checkRoot(join(dir, "..", "elsewhere"), dir));
  assert.throws(() => checkRoot(join(dir, "tracked-state"), dir), /inside the checkout and not git-ignored/);
  assert.throws(() => checkRoot(dir, dir), /inside the checkout/);
});

test("the command prints the flow's directory, or the root, and refuses an unignored root in the checkout", () => {
  const dir = repo();
  const run = (env, ...args) =>
    spawnSync(process.execPath, [CLI, ...args], { cwd: dir, encoding: "utf8", env: { ...process.env, RELAYFLOW_STATE_DIR: "", ...env } });
  assert.equal(run({}, "ci").stdout.trim(), ".relayflow/ci");
  assert.equal(run({ RELAYFLOW_STATE_DIR: "/runs/r3" }, "context-review").stdout.trim(), "/runs/r3/context-review");
  assert.equal(run({ RELAYFLOW_STATE_DIR: "/runs/r3" }).stdout.trim(), "/runs/r3");
  assert.equal(run({ RELAYFLOW_STATE_DIR: "ignored" }, "ci").stdout.trim(), join(dir, "ignored", "ci"));
  const refused = run({ RELAYFLOW_STATE_DIR: "loose" }, "ci");
  assert.equal(refused.status, 1);
  assert.match(refused.stderr, /not git-ignored/);
});
