#!/usr/bin/env bun
/**
 * Teardown of the aws-rehearse-p1-1126 stage's Alchemy state only (stacks/aws/pippen-rehearsal.ts
 * teardownStageState): every object version and delete marker under exactly
 * `96bulls/96bulls-aws/aws-rehearse-p1-1126/` in the shared Pippen sandbox state bucket. Refuses
 * before deleting anything if a listed key is outside that prefix; never touches the bucket or a
 * sibling stage. The caller must be the stage's lease session (NINETY_SIX_BULLS_PIPPEN_SESSION).
 *
 *   bun scripts/rehearsal-state-teardown.ts             # dry run: list and count only
 *   bun scripts/rehearsal-state-teardown.ts --execute   # delete those versions
 *
 * Account and region come from .96bulls/aws-rehearse-p1-1126/aws-inputs.json (gitignored).
 */
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { REHEARSAL_STAGE, StateBucketError, assertLeaseCaller, teardownStageState, validatePippenStateBucket } from "../stacks/aws/pippen-rehearsal.ts";
import type { AwsRun } from "../stacks/aws/state-bucket.ts";

const args = process.argv.slice(2);
if (args.some((a) => a !== "--execute")) throw new Error("Usage: bun scripts/rehearsal-state-teardown.ts [--execute]");
const root = fileURLToPath(new URL("..", import.meta.url));
const inputs = JSON.parse(readFileSync(join(root, ".96bulls", REHEARSAL_STAGE, "aws-inputs.json"), "utf8")) as { accountId: string; region: string };
// Errors carry fixed codes only (pippen-rehearsal.ts): the CLI's own text is not passed on.
const aws: AwsRun = (a) => {
  const r = spawnSync("aws", a, { encoding: "utf8", env: process.env, maxBuffer: 256 * 1024 * 1024 });
  if (r.status !== 0) throw new Error(/NoSuchBucket|\(404\)/.test(r.stderr ?? "") ? "NoSuchBucket" : "aws call failed");
  return r.stdout;
};
try {
  assertLeaseCaller(aws, inputs.accountId, process.env);
  const bucket = validatePippenStateBucket(aws, inputs);
  const result = teardownStageState(aws, { bucket, region: inputs.region, stage: REHEARSAL_STAGE, execute: args.includes("--execute") });
  console.log(JSON.stringify({ stage: REHEARSAL_STAGE, ...result, executed: args.includes("--execute") }));
} catch (e) {
  console.error(e instanceof StateBucketError ? e.message : "96bulls: rehearsal teardown refused (unexpected error). Nothing was changed.");
  process.exit(1);
}
