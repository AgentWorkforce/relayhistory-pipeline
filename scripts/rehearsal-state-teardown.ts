#!/usr/bin/env bun
/**
 * Teardown of the aws-rehearse-p1-1126 stage's Alchemy state only (stacks/aws/pippen-rehearsal.ts
 * teardownStageState): every object version and delete marker under exactly
 * `96bulls/96bulls-aws/aws-rehearse-p1-1126/` in the shared Pippen sandbox state bucket. Refuses
 * before deleting anything if a listed key is outside that prefix; never touches the bucket or a
 * sibling stage. The caller must be the stage's lease session (NINETY_SIX_BULLS_PIPPEN_SESSION).
 *
 *   bun scripts/rehearsal-state-teardown.ts --manifest
 *       # BEFORE any destroy: read every resource id from AWS and write the physical manifest
 *       # (.96bulls/aws-rehearse-p1-1126/physical-manifest.json, gitignored); never overwritten
 *   bun scripts/rehearsal-state-teardown.ts             # dry run: list and count the state versions
 *   bun scripts/rehearsal-state-teardown.ts --execute   # after the destroy: every manifest id gone, then delete the state
 *
 * Account and region come from .96bulls/aws-rehearse-p1-1126/aws-inputs.json (gitignored).
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  REHEARSAL_STAGE,
  RUN_ID_ENV,
  StateBucketError,
  assertLeaseCaller,
  assertManifestComplete,
  assertManifestGone,
  manifestDigest,
  readPhysicalManifest,
  rehearsalRunContext,
  teardownStageState,
  validatePippenStateBucket,
} from "../stacks/aws/pippen-rehearsal.ts";
import type { AwsRun } from "../stacks/aws/state-bucket.ts";

const args = process.argv.slice(2);
const flag = (f: string) => args.includes(f);
if (args.some((a) => a !== "--execute" && a !== "--manifest") || (flag("--manifest") && flag("--execute")))
  throw new Error("Usage: bun scripts/rehearsal-state-teardown.ts [--manifest | --execute]   (with NINETY_SIX_BULLS_REHEARSAL_RUN_ID set)");
const root = fileURLToPath(new URL("..", import.meta.url));
const dir = join(root, ".96bulls", REHEARSAL_STAGE);
const manifestPath = join(dir, "physical-manifest.json");
// Errors carry fixed codes only (pippen-rehearsal.ts): the CLI's own text is not passed on.
const aws: AwsRun = (a) => {
  const r = spawnSync("aws", a, { encoding: "utf8", env: process.env, maxBuffer: 256 * 1024 * 1024 });
  if (r.status !== 0) {
    const kind = /NoSuchBucket|\(404\)/.test(r.stderr ?? "") ? "NoSuchBucket" : /ResourceNotFoundException|NonExistentQueue|QueueDoesNotExist|NoSuchEntity|NoSuchKey/.exec(r.stderr ?? "")?.[0] ?? "aws call failed";
    throw new Error(kind);
  }
  return r.stdout;
};
try {
  let inputs: unknown;
  try {
    inputs = JSON.parse(readFileSync(join(dir, "aws-inputs.json"), "utf8"));
  } catch {
    throw new StateBucketError("state_inputs_invalid");
  }
  // Before any AWS call: inputs, lease env, the issuer pin (account, boundary, role and its
  // disposition, lease) and the lease's end; then STS proves the session.
  const ctx = rehearsalRunContext(REHEARSAL_STAGE, inputs, process.env)!;
  const runId = process.env[RUN_ID_ENV] ?? "";
  assertLeaseCaller(aws, ctx.accountId, process.env);
  if (flag("--manifest")) {
    // Written once, before any destroy, and kept across partial destroys: never overwritten.
    if (existsSync(manifestPath)) throw new StateBucketError("state_manifest_invalid");
    const manifest = readPhysicalManifest(aws, ctx, runId);
    writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600, flag: "wx" });
    // The digest goes into the run's handoff and the broker receipt.
    console.log(JSON.stringify({ stage: REHEARSAL_STAGE, manifest: "written", digest: manifestDigest(manifest) }));
    process.exit(0);
  }
  const bucket = validatePippenStateBucket(aws, ctx);
  const execute = flag("--execute");
  if (execute) {
    // Physical resources first: every id the manifest recorded must be gone before the state that records them is discarded.
    let manifest: unknown;
    try {
      manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    } catch {
      throw new StateBucketError("state_manifest_invalid");
    }
    assertManifestComplete(manifest, ctx, runId);
    assertManifestGone(aws, manifest, ctx, runId);
  }
  const result = teardownStageState(aws, { bucket, region: ctx.region, stage: REHEARSAL_STAGE, execute });
  console.log(JSON.stringify({ stage: REHEARSAL_STAGE, ...result, executed: execute }));
} catch (e) {
  console.error(e instanceof StateBucketError ? e.message : "96bulls: rehearsal teardown refused (unexpected error). Nothing was changed.");
  process.exit(1);
}
