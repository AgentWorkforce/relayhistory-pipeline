import assert from "node:assert/strict";
import test from "node:test";

import { applyOverride, jobs } from "../ci.flow.ts";
import { classifyPaths, lanesFor } from "./classify.mjs";

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
  assert.match(e2e, /lane\.sh e2e --target local$/);
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
  assert.match(js.at(-1).steps[0].command, new RegExp(`remote-ci\\.mjs watch '${sha}' 'ci\\.yml,pg-proof\\.yml'$`));
});

test("an override raises the verdict and never lowers it", () => {
  const docs = classification(["docs/x.md"]);
  assert.equal(applyOverride(docs, "full").verdict, "full");
  assert.equal(applyOverride(docs, "full").lanes.pgProof, true);
  const full = classification(["src/hoopdb/indexer.ts"]);
  assert.equal(applyOverride(full, "targeted").verdict, "full");
});
