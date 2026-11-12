#!/usr/bin/env bun
/**
 * Negative reachability report for the relayscrubber 0.5.0-alpha.8 cutover
 * (docs/scrub-engine.md, "Cutover: pinned relayscrubber 0.5.0-alpha.8").
 *
 * Runs src/session-ingest/__tests__/scrub-cutover-reachability.test.ts (one file, no
 * other tests), which drives the ingest writer, publication image, routes/pipeline
 * and neon-s3-sync export with every TS scrub/seal entry point spied, then rechecks
 * its JSON rows here:
 *   - cutover (SCRUB_ENGINE=wasm-bindgen + SCRUB_ENGINE_REQUIRED=wasm-bindgen +
 *     READER_MAX_RULESET=10): routed sealer installed, zero TS calls, scrub version 10,
 *     crate profile, the fixture secret absent from the sealed bytes;
 *   - control (knob unset): no routed sealer, the TS sealer ran, version 8, no profile;
 *   - unwired (under the triple; publication canary, default pipelineRoutes, Worker
 *     deliverDepsFor): the seam refused exactly once with seal_path_not_routed, zero TS
 *     calls, no raw object written.
 * The same test file also scans the sources so every seal-capable construction is
 * classified as wired or unwired-refuses; a failure there fails the run.
 *
 * Usage: bun scripts/scrub-cutover-reachability.ts [--out <path>]   (default: scrub-cutover-reachability.json)
 * Prints the report; exits 1 on any failed test or invariant.
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const ROOT = resolve(import.meta.dir, "..");
const TEST = "./src/session-ingest/__tests__/scrub-cutover-reachability.test.ts";
const CALLERS = ["writer", "publication-image", "routes/pipeline", "export"];
const UNWIRED = ["publication-canary", "routes/pipeline-default", "worker-deliverDepsFor"];
const args = process.argv.slice(2);
const outIndex = args.indexOf("--out");
const out = resolve(outIndex >= 0 && args[outIndex + 1] ? args[outIndex + 1]! : "scrub-cutover-reachability.json");
const raw = `${out}.rows.json`;
rmSync(raw, { force: true });

const env: Record<string, string | undefined> = { ...process.env, SCRUB_REACHABILITY_REPORT: raw };
// The test sets the triple itself; an inherited selection must not leak in.
for (const k of ["SCRUB_ENGINE", "SCRUB_ENGINE_REQUIRED", "READER_MAX_RULESET", "SCRUB_BINDGEN_DIR"]) delete env[k];
const run = spawnSync(process.execPath, ["test", "--timeout", "300000", TEST], { cwd: ROOT, env, encoding: "utf8" });
const failures: string[] = [];
if (run.status !== 0) failures.push(`test run exited ${run.status}: ${(run.stderr || run.stdout).split("\n").filter((l) => /fail|error/i.test(l)).slice(0, 20).join(" | ")}`);
const rows: { caller: string; mode: string; routedSealer: boolean; tsCalls: Record<string, number>; scrubVersion?: string; scrubProfile?: string; secretInSealedBytes?: boolean; seamRefusals?: string[]; rawObjectsWritten?: number }[] = existsSync(raw) ? JSON.parse(readFileSync(raw, "utf8")).rows : [];
rmSync(raw, { force: true });

for (const mode of ["cutover", "control"])
  for (const caller of CALLERS) {
    const row = rows.find((r) => r.caller === caller && r.mode === mode);
    if (!row) { failures.push(`${mode}/${caller}: no row`); continue; }
    const ts = Object.values(row.tsCalls).reduce((a, b) => a + b, 0);
    if (mode === "cutover") {
      if (!row.routedSealer) failures.push(`${mode}/${caller}: no routed sealer`);
      if (ts !== 0) failures.push(`${mode}/${caller}: ${ts} TS scrub/seal calls`);
      if (row.scrubVersion !== "10" || row.scrubProfile !== "relayhistory-secrets-v10") failures.push(`${mode}/${caller}: marker ${row.scrubVersion}/${row.scrubProfile}`);
      if (row.secretInSealedBytes) failures.push(`${mode}/${caller}: secret survived sealing`);
    } else {
      if (row.routedSealer) failures.push(`${mode}/${caller}: routed sealer installed with the knob unset`);
      if (!(row.tsCalls["raw-chunks.ts#sealRawChunk"]! >= 1)) failures.push(`${mode}/${caller}: TS sealer not used`);
      if (row.scrubVersion !== "8" || row.scrubProfile !== undefined) failures.push(`${mode}/${caller}: marker ${row.scrubVersion}/${row.scrubProfile}`);
    }
  }
for (const caller of UNWIRED) {
  const row = rows.find((r) => r.caller === caller && r.mode === "unwired");
  if (!row) { failures.push(`unwired/${caller}: no row`); continue; }
  const ts = Object.values(row.tsCalls).reduce((a, b) => a + b, 0);
  if (row.routedSealer) failures.push(`unwired/${caller}: has a routed sealer`);
  if (ts !== 0) failures.push(`unwired/${caller}: ${ts} TS scrub/seal calls`);
  if (JSON.stringify(row.seamRefusals) !== JSON.stringify(["seal_path_not_routed"])) failures.push(`unwired/${caller}: seam refusals ${JSON.stringify(row.seamRefusals)}`);
  if (row.rawObjectsWritten !== 0) failures.push(`unwired/${caller}: ${row.rawObjectsWritten} raw objects written`);
}

const head = spawnSync("git", ["rev-parse", "HEAD"], { cwd: ROOT, encoding: "utf8" }).stdout.trim() || null;
const report = {
  schema: 1,
  kind: "scrub-cutover-reachability",
  generatedAt: new Date().toISOString(),
  gitHead: head,
  test: TEST,
  testSummary: (run.stdout + run.stderr).split("\n").filter((l) => /^\s*\d+ (pass|fail)|^Ran /.test(l)).map((l) => l.trim()),
  ok: failures.length === 0,
  failures,
  rows,
};
writeFileSync(out, JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(report, null, 2));
if (failures.length) process.exit(1);
