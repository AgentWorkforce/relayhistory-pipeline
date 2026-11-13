import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { askJev, classifyPaths, combine, JEV_MIN_CONFIDENCE, jevRequest, lanesFor } from "./classify.mjs";

const stepsTs = readFileSync(new URL("../../e2e/pipeline/steps.ts", import.meta.url), "utf8");
const knownSteps = new Set([...stepsTs.matchAll(/^\s+id: "([a-z0-9._]+)"/gm)].map((m) => m[1]));

test("docs only is none, and runs no unit lane or e2e", () => {
  const c = classifyPaths(["docs/local-stack.md", "README.md", "docs/design/x.png", "AGENTS.md"]);
  assert.equal(c.verdict, "none");
  assert.deepEqual(lanesFor(c.verdict, ["docs/local-stack.md"]), { unit: false, pgProof: false, flows: false, e2e: "none" });
});

test("the ingest, ledgers, HoopDb, migrations, stacks, the read API and the consumer force full", () => {
  for (const path of [
    "src/session-ingest/app.ts", "src/session-ingest/ledger-s3.ts", "packages/pipeline/src/lib/ledger-x.ts",
    "src/hoopdb/indexer.ts", "supabase/migrations/20261112000000_x.sql", "src/hoopdb/migrations/0009_x.sql",
    "stacks/aws/index.ts", "src/session-read-api/app.ts", "src/session-pipeline/consumer.ts", "src/shared/vendor.ts",
    "docker/services.Dockerfile", "e2e/pipeline/steps.ts", "bun.lock", ".github/workflows/ci.yml", "flows/ci.flow.ts",
    "flows/lib/classify.mjs", "flows/lib/lane.sh", "flows/lib/fleet.mjs",
  ]) {
    const c = classifyPaths([path, "docs/x.md"]);
    assert.equal(c.verdict, "full", path);
    assert.deepEqual(lanesFor(c.verdict, [path]).pgProof, true, path);
    assert.ok(c.reasons[0].startsWith(`full: ${path}`), `${path}: ${c.reasons[0]}`);
  }
});

test("a path no rule knows is full: the rules fail toward more testing", () => {
  const c = classifyPaths(["something-new/thing.ts"]);
  assert.equal(c.verdict, "full");
  assert.match(c.reasons[0], /no rule knows/);
});

test("targeted selects the e2e steps the paths reach, and only known steps", () => {
  const c = classifyPaths(["src/bob-costas/summarizer.ts", "src/referee/rules.ts"]);
  assert.equal(c.verdict, "targeted");
  assert.deepEqual(new Set(c.steps), new Set(["upload.courier", "index.hoop", "summary.extractive"]));
  for (const step of c.steps) assert.ok(knownSteps.has(step), `${step} is not an e2e/pipeline/steps.ts id`);
  const scripts = classifyPaths(["scripts/aws-hardening-mutations.ts"]);
  assert.equal(scripts.verdict, "targeted");
  assert.deepEqual(scripts.steps, [], "unit lanes only");
  assert.equal(lanesFor("targeted", ["flows/README.md", "flows/adversarial-review.flow.ts"]).flows, true);
});

test("full anywhere wins over targeted, and full carries no step subset", () => {
  const c = classifyPaths(["src/bob-costas/summarizer.ts", "src/session-ingest/app.ts"]);
  assert.equal(c.verdict, "full");
  assert.deepEqual(c.steps, []);
});

test("Jev escalates only, and only at the confidence floor", () => {
  const rules = classifyPaths(["scripts/x.ts"]);
  assert.equal(combine(rules, { asked: true, choice: "full", confidence: JEV_MIN_CONFIDENCE }).verdict, "full");
  assert.equal(combine(rules, { asked: true, choice: "full", confidence: JEV_MIN_CONFIDENCE - 0.01 }).verdict, "targeted");
  assert.equal(combine(rules, { asked: true, choice: "none", confidence: 0.99 }).verdict, "targeted", "never downgrades");
  const full = classifyPaths(["src/session-ingest/app.ts"]);
  assert.equal(combine(full, { asked: true, choice: "targeted", confidence: 1 }).verdict, "full");
  assert.equal(combine(rules, { asked: false, reason: "x" }).verdict, "targeted");
});

test("without JEV_TOKEN Jev is skipped and recorded, and nothing is fetched", async () => {
  let called = false;
  const r = await askJev(["a"], "", "none", {}, async () => { called = true; return new Response("{}"); });
  assert.equal(called, false);
  assert.deepEqual(r, { asked: false, reason: "JEV_TOKEN is not set: the Jev escalation step was skipped" });
});

test("Jev's answer is parsed; a bad answer, an HTTP error or a throw leaves the rules standing", async () => {
  const env = { JEV_TOKEN: "test-token" };
  const answer = (body, status = 200) => async (_url, init) => {
    assert.equal(init.headers.authorization, "Bearer test-token");
    const sent = JSON.parse(init.body);
    assert.equal(sent.model, "jev-latest");
    assert.deepEqual(Object.keys(sent.questions.verdict.criteria), ["none", "targeted", "full"]);
    return new Response(JSON.stringify(body), { status });
  };
  assert.deepEqual(await askJev(["a"], "", "none", env, answer({ answers: { verdict: { type: "choice", choice: "full", confidence: 0.9 } } })), { asked: true, choice: "full", confidence: 0.9 });
  assert.match((await askJev(["a"], "", "none", env, answer({ answers: { verdict: { type: "choice", choice: "huge", confidence: 1 } } }))).reason, /not a valid choice/);
  assert.match((await askJev(["a"], "", "none", env, answer({}, 401))).reason, /HTTP 401/);
  assert.match((await askJev(["a"], "", "none", env, async () => { throw new Error("down"); })).reason, /could not be reached/);
});

test("the Jev state stays under its limit and carries no token", () => {
  const body = jevRequest(Array.from({ length: 5000 }, (_, i) => `src/x${i}.ts`), "x".repeat(50_000), "full");
  assert.ok(body.state.length <= 19_000);
  assert.ok(!JSON.stringify(body).includes("JEV_TOKEN"));
});
