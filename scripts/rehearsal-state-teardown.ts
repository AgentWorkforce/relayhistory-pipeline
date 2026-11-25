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
import { REHEARSAL_STAGE, StateBucketError, assertLeaseCaller, assertRehearsalResourcesGone, recordedMappingUuids, stateBackendFor, teardownStageState, validatePippenStateBucket } from "../stacks/aws/pippen-rehearsal.ts";
import type { AwsRun } from "../stacks/aws/state-bucket.ts";

const args = process.argv.slice(2);
if (args.some((a) => a !== "--execute")) throw new Error("Usage: bun scripts/rehearsal-state-teardown.ts [--execute]");
const root = fileURLToPath(new URL("..", import.meta.url));
// Errors carry fixed codes only (pippen-rehearsal.ts): the CLI's own text is not passed on.
const aws: AwsRun = (a) => {
  const r = spawnSync("aws", a, { encoding: "utf8", env: process.env, maxBuffer: 256 * 1024 * 1024 });
  if (r.status !== 0) {
    const kind = /NoSuchBucket|\(404\)/.test(r.stderr ?? "") ? "NoSuchBucket" : /ResourceNotFoundException|NonExistentQueue|QueueDoesNotExist|NoSuchKey/.exec(r.stderr ?? "")?.[0] ?? "aws call failed";
    throw new Error(kind);
  }
  return r.stdout;
};
try {
  let inputs: unknown;
  try {
    inputs = JSON.parse(readFileSync(join(root, ".96bulls", REHEARSAL_STAGE, "aws-inputs.json"), "utf8"));
  } catch {
    throw new StateBucketError("state_inputs_invalid");
  }
  // The same validation as the state layer: the pinned region, validate-only, a well-formed account.
  stateBackendFor(REHEARSAL_STAGE, inputs);
  const { accountId, region } = inputs as { accountId: string; region: string };
  // The account and lease are the broker's attested ones (PIPPEN_SANDBOX_ACCOUNT_ID / _SESSION_ID), and STS proves them.
  assertLeaseCaller(aws, accountId, process.env);
  const bucket = validatePippenStateBucket(aws, { accountId, region });
  const execute = args.includes("--execute");
  // Physical resources first: every recorded id must be gone before the state that records them is discarded.
  if (execute) assertRehearsalResourcesGone(aws, { region, stage: REHEARSAL_STAGE, mappingUuids: recordedMappingUuids(aws, { bucket, region, stage: REHEARSAL_STAGE }) });
  const result = teardownStageState(aws, { bucket, region, stage: REHEARSAL_STAGE, execute });
  console.log(JSON.stringify({ stage: REHEARSAL_STAGE, ...result, executed: execute }));
} catch (e) {
  console.error(e instanceof StateBucketError ? e.message : "96bulls: rehearsal teardown refused (unexpected error). Nothing was changed.");
  process.exit(1);
}
