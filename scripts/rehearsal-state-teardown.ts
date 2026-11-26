#!/usr/bin/env bun
/**
 * The aws-rehearse-p1-1126 stage's state lifecycle in the shared Pippen sandbox state bucket
 * (stacks/aws/pippen-rehearsal.ts). Each step refuses with fixed codes; the CLI's own output is never
 * passed on. Account, region and the execution role come from .96bulls/aws-rehearse-p1-1126/aws-inputs.json
 * (gitignored); the lease env (PIPPEN_SANDBOX_*, PIPPEN_REHEARSAL_*) from `pippen sandbox exec`;
 * the run id from NINETY_SIX_BULLS_REHEARSAL_RUN_ID.
 *
 *   bun scripts/rehearsal-state-teardown.ts --claim
 *       # BEFORE the first capture (lease A): empty version inventory, names unused, then the
 *       # generation object created (the only write of the state tooling besides --execute)
 *   bun scripts/rehearsal-state-teardown.ts --manifest
 *       # BEFORE any destroy (lease A): read every resource id and write the physical manifest
 *       # (.96bulls/aws-rehearse-p1-1126/physical-manifest.json, gitignored); never overwritten;
 *       # prints its digest for the broker's receipt
 *   bun scripts/rehearsal-state-teardown.ts             # dry run: count the state versions
 *   bun scripts/rehearsal-state-teardown.ts --execute --expect-manifest-digest <sha256>
 *       --authorization <file> --expect-authorization-digest <sha256>
 *       # after the destroy, only under the authorized cleanup-only lease B (issued after lease A was
 *       # revoked, A's sessions expired and the drain was attested); every check before any deletion
 *   ... --execute ... --previous-authorization <file>
 *       # a successor lease B2 only: resume a tearing-down generation of the superseded lease B (its
 *       # record passed here; B2's authorization names its digest in `supersedes`)
 */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { REHEARSAL_STAGE, StateBucketError, assertLeaseCaller, claimStageState, manifestDigest, readPhysicalManifest, rehearsalRunContext, runRehearsalTeardown } from "../stacks/aws/pippen-rehearsal.ts";
import type { AwsRun } from "../stacks/aws/state-bucket.ts";

const args = process.argv.slice(2);
const valueOf = (f: string) => (args.includes(f) ? args[args.indexOf(f) + 1] : undefined);
const flags = ["--claim", "--manifest", "--execute"].filter((f) => args.includes(f));
const values = ["--expect-manifest-digest", "--authorization", "--expect-authorization-digest", "--previous-authorization"];
const known = new Set([...flags, ...values.filter((v) => args.includes(v)), ...values.map(valueOf).filter((v): v is string => v !== undefined)]);
const execute = args.includes("--execute");
if (
  args.some((a) => !known.has(a)) || flags.length > 1 ||
  (execute && (!valueOf("--expect-manifest-digest") || !valueOf("--authorization"))) || (!execute && values.some((v) => args.includes(v))) ||
  (args.includes("--authorization") !== args.includes("--expect-authorization-digest"))
)
  throw new Error("Usage: bun scripts/rehearsal-state-teardown.ts [--claim | --manifest | --execute --expect-manifest-digest <sha256> --authorization <file> --expect-authorization-digest <sha256> [--previous-authorization <file>]]");
const root = fileURLToPath(new URL("..", import.meta.url));
const dir = join(root, ".96bulls", REHEARSAL_STAGE);
const manifestPath = join(dir, "physical-manifest.json");
// Only the error class is passed on (never the CLI's text): not found, access denied, or a failure.
const aws: AwsRun = (a) => {
  const r = spawnSync("aws", a, { encoding: "utf8", env: process.env, maxBuffer: 256 * 1024 * 1024 });
  if (r.status !== 0) {
    const err = r.stderr ?? "";
    const kind = /NoSuchBucket|\(404\)/.test(err)
      ? "NoSuchBucket"
      : /AccessDenied|UnauthorizedOperation|not authorized/i.test(err)
        ? "AccessDenied"
        : (/ResourceNotFoundException|NonExistentQueue|QueueDoesNotExist|NoSuchEntity|NoSuchKey|PreconditionFailed/.exec(err)?.[0] ?? "aws call failed");
    throw new Error(kind);
  }
  return r.stdout;
};
const readJson = (path: string, code: "state_manifest_invalid" | "state_authorization_mismatch") => {
  try {
    return JSON.parse(readFileSync(path, "utf8")) as unknown;
  } catch {
    throw new StateBucketError(code);
  }
};
try {
  let inputs: unknown;
  try {
    inputs = JSON.parse(readFileSync(join(dir, "aws-inputs.json"), "utf8"));
  } catch {
    throw new StateBucketError("state_inputs_invalid");
  }
  if (args.includes("--claim") || args.includes("--manifest")) {
    // Lease A. Before any AWS call: inputs, lease env, the issuer pin and the lease's end.
    const ctx = rehearsalRunContext(REHEARSAL_STAGE, inputs, process.env)!;
    if (args.includes("--claim")) {
      const claimed = claimStageState(aws, ctx);
      console.log(JSON.stringify({ stage: REHEARSAL_STAGE, claimed: true, runId: ctx.runId }));
      void claimed;
    } else {
      assertLeaseCaller(aws, ctx.accountId, process.env);
      // Written once, before any destroy, and kept across partial destroys: never overwritten.
      if (existsSync(manifestPath)) throw new StateBucketError("state_manifest_invalid");
      const manifest = readPhysicalManifest(aws, ctx, ctx.runId);
      writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600, flag: "wx" });
      console.log(JSON.stringify({ stage: REHEARSAL_STAGE, manifest: "written", digest: manifestDigest(manifest) }));
    }
    process.exit(0);
  }
  const authorizationPath = valueOf("--authorization");
  const result = runRehearsalTeardown(aws, {
    inputs,
    env: process.env,
    execute,
    ...(execute ? { manifest: readJson(manifestPath, "state_manifest_invalid"), expectManifestDigest: valueOf("--expect-manifest-digest") } : {}),
    ...(authorizationPath ? { authorization: readJson(authorizationPath, "state_authorization_mismatch"), expectAuthorizationDigest: valueOf("--expect-authorization-digest") } : {}),
    ...(valueOf("--previous-authorization") ? { previousAuthorization: readJson(valueOf("--previous-authorization")!, "state_authorization_mismatch") } : {}),
  });
  console.log(JSON.stringify({ stage: REHEARSAL_STAGE, ...result, executed: execute }));
} catch (e) {
  console.error(e instanceof StateBucketError ? e.message : "96bulls: rehearsal state step refused (unexpected error). Nothing was changed.");
  process.exit(1);
}
