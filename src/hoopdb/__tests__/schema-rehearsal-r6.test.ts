/**
 * Rehearsal stage R6 (scripts/pg-proof/schema-rehearsal.ts) reads acceptedGenerations from the
 * grant-free src/session-ingest/accept-generations.ts, never from worker.ts, which imports the
 * hosted-worker module-load TS grant first (docs/scrub-engine.md "Default flip"). Static and
 * in-process only: no PostgreSQL, the stage itself runs in rehearse-session-schema.yml.
 */
import { expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { R6_ACCEPT_GENERATIONS_MODULE, R6_LEGACY_ACCEPT_GENERATIONS_MODULE, r6AcceptGenerationsModule } from "../../../scripts/pg-proof/schema-rehearsal-core.ts";
import { hostedGrantReach, trackedSources } from "../../../scripts/scrub-default-scan.ts";

const ROOT = join(import.meta.dir, "../../..");

test("R6 picks accept-generations.ts in this tree, worker.ts only for a tree from before the split", () => {
  expect(R6_ACCEPT_GENERATIONS_MODULE).toBe("src/session-ingest/accept-generations.ts");
  expect(r6AcceptGenerationsModule((rel) => existsSync(join(ROOT, rel)))).toBe("src/session-ingest/accept-generations.ts");
  expect(r6AcceptGenerationsModule(() => true)).toBe("src/session-ingest/accept-generations.ts");
  expect(r6AcceptGenerationsModule(() => false)).toBe(R6_LEGACY_ACCEPT_GENERATIONS_MODULE);
});

test("the R6 stage loads its module only through r6AcceptGenerationsModule", () => {
  const src = readFileSync(join(ROOT, "scripts/pg-proof/schema-rehearsal.ts"), "utf8");
  const start = src.indexOf('await stage("R6"');
  const end = src.indexOf("await stage(", start + 1);
  expect(start).toBeGreaterThan(0);
  const r6 = src.slice(start, end > start ? end : undefined);
  const loads = [...r6.matchAll(/\bmod\(([^;\n]*)\);/g)].map((m) => m[1]);
  expect(loads).toStrictEqual(['"stacks/cloudflare/session-ingest.ts"', "r6AcceptGenerationsModule((rel) => existsSync(join(ROOT, rel)))"]);
  expect(r6.replace(/\/\/[^\n]*/g, "")).not.toContain("worker.ts");
});

test("the module R6 loads is grant-free and exports acceptedGenerations; worker.ts is not", async () => {
  const reach = hostedGrantReach(trackedSources());
  expect(reach.has(R6_ACCEPT_GENERATIONS_MODULE)).toBe(false);
  expect(reach.has(R6_LEGACY_ACCEPT_GENERATIONS_MODULE)).toBe(true);
  const W = await import(join(ROOT, R6_ACCEPT_GENERATIONS_MODULE));
  expect(W.acceptedGenerations({ INGEST_EXPECT_GENERATION: "a".repeat(64), INGEST_ACCEPT_GENERATIONS: `${"a".repeat(64)},${"b".repeat(64)}` }, "buffered")).toStrictEqual(["a".repeat(64), "b".repeat(64)]);
});
