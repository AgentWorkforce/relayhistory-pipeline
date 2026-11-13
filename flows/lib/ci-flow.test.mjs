import assert from "node:assert/strict";
import test from "node:test";

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { applyOverride, cleanStateDir, jobs, STATE_MARKER } from "../ci.flow.ts";
import { classifyPaths, lanesFor } from "./classify.mjs";
import { LIB } from "./shell.ts";

const classification = (paths) => {
  const c = classifyPaths(paths);
  return { ...c, rulesVerdict: c.verdict, jev: { asked: false, reason: "test" }, lanes: lanesFor(c.verdict, paths), paths: paths.length };
};
const names = (js) => js.map((j) => j.name);

const steps = (js) => js.flatMap((j) => j.steps.map((s) => `${j.name}: ${s.name}`));

test("none runs the guards only", () => {
  const js = jobs(classification(["docs/x.md"]), ".relayflow/ci", undefined, "local", "ci.yml");
  assert.deepEqual(names(js), ["checks"]);
  assert.equal(js[0].steps.length, 1);
});

test("full runs every lane, pg-proof on 15/16/17 and the whole e2e, in five concurrent jobs", () => {
  const js = jobs(classification(["src/session-ingest/app.ts"]), ".relayflow/ci", undefined, "local", "ci.yml");
  assert.deepEqual(names(js), ["checks", "shards 1+3", "shards 2+4", "pg-proof", "e2e"]);
  const all = steps(js);
  for (const n of ["1", "2", "3", "4"]) assert.ok(all.some((s) => s.endsWith(`shard ${n} of 4`)), `shard ${n}`);
  for (const v of ["15", "16", "17"]) assert.ok(all.includes(`pg-proof: real PostgreSQL ${v}`), v);
  assert.ok(all.some((s) => s.startsWith("checks: package tests")), "aux");
  const e2e = js.find((j) => j.name === "e2e").steps[0].command;
  assert.match(e2e, /lane\.sh' e2e --target local$/);
  // The gate's own scripts are run from where the flow was loaded, never from the tree under review.
  assert.ok(e2e.includes(`'${LIB}/lane.sh'`), e2e);
  assert.doesNotMatch(e2e, /--steps/);
});

test("targeted runs the unit lanes and only the e2e steps the paths reach", () => {
  const js = jobs(classification(["src/bob-costas/summarizer.ts"]), ".relayflow/ci", undefined, "sandbox", "ci.yml");
  assert.ok(!names(js).some((n) => n.startsWith("pg-proof")));
  assert.match(js.find((j) => j.name === "e2e").steps[0].command, /--target sandbox --steps 'upload\.courier,index\.hoop,summary\.extractive'$/);
  const scriptsOnly = jobs(classification(["scripts/x.ts"]), ".relayflow/ci", undefined, "local", "ci.yml");
  assert.ok(!names(scriptsOnly).includes("e2e"), "no e2e step reaches scripts/");
});

test("the GitHub gate is a job only when a merge base is given", () => {
  const sha = "a".repeat(40);
  const js = jobs(classification(["src/session-ingest/app.ts"]), ".relayflow/ci", sha, "local", "ci.yml,pg-proof.yml");
  assert.match(js.at(-1).steps[0].command, new RegExp(`remote-ci\\.mjs' watch '${sha}' 'ci\\.yml,pg-proof\\.yml'$`));
  for (const job of js) for (const s of job.steps) assert.doesNotMatch(s.command, /(^|\s)(bash|node) flows\/lib\//, `${s.name} runs the tree's own gate script`);
});

test("the state dir is emptied only when this flow made it", () => {
  const sh = (cmd, cwd) => spawnSync("/bin/sh", ["-c", cmd], { cwd, encoding: "utf8" });
  const root = mkdtempSync(join(tmpdir(), "ci-state-"));
  const foreign = join(root, "home");
  mkdirSync(foreign);
  writeFileSync(join(foreign, "keep.txt"), "x");
  const refused = sh(cleanStateDir(foreign), root);
  assert.notEqual(refused.status, 0);
  assert.match(refused.stderr, /refusing to empty/);
  assert.ok(existsSync(join(foreign, "keep.txt")), "a directory the flow did not make survives");
  const ours = join(root, "ci");
  assert.equal(sh(cleanStateDir(ours), root).status, 0);
  assert.ok(existsSync(join(ours, STATE_MARKER)));
  writeFileSync(join(ours, "old-report.md"), "x");
  assert.equal(sh(cleanStateDir(ours), root).status, 0);
  assert.ok(!existsSync(join(ours, "old-report.md")), "its own earlier state is cleared");
});

test("an override raises the verdict and never lowers it", () => {
  const docs = classification(["docs/x.md"]);
  assert.equal(applyOverride(docs, "full").verdict, "full");
  assert.equal(applyOverride(docs, "full").lanes.pgProof, true);
  const full = classification(["src/hoopdb/indexer.ts"]);
  assert.equal(applyOverride(full, "targeted").verdict, "full");
});
