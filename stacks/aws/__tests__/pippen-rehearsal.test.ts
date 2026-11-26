/**
 * The P1 rehearsal in a Pippen sandbox (pippen-rehearsal.ts): backend selection from validated
 * inputs, the validate-only state bucket, caller (attested lease) and prefix guards, the stage-only
 * teardown with empty-prefix and physical-resource evidence, and the lease session policies.
 * Offline: every AWS call is a fake. Placeholder account 111122223333 only.
 */
import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  REHEARSAL_AUTHORITY_END,
  REHEARSAL_LEASE_EXPIRES_AT,
  REHEARSAL_LEASE_TTL_HOURS,
  REHEARSAL_STAGE,
  REHEARSAL_TAG,
  StateBucketError,
  ISSUER_PIN_ENV,
  ISSUED_AT_ENV,
  RUN_ID_ENV,
  assertManifestReceipt,
  assertTeardownAuthorization,
  authorizationDigest,
  claimStageState,
  runRehearsalTeardown,
  teardownPolicyDigest,
  generationBody,
  generationKey,
  teardownDeadline,
  rehearsalTeardownPolicies,
  TEARDOWN_DENIED_ACTIONS,
  manifestDigest,
  rehearsalRunContext,
  type RehearsalContext,
  assertManifestComplete,
  assertManifestGone,
  assertRehearsalNamesUnused,
  readPhysicalManifest,
  rehearsalIssuerPin,
  type PhysicalManifest,
  assertStateBucketMode,
  assertStatePrefix,
  assertValidateOnlyState,
  leaseCallerArn,
  listStageStateVersions,
  prepareStateBackend,
  rehearsalLeaseEnd,
  rehearsalLeasePolicies,
  rehearsalResourceNames,
  rehearsalStatePrefix,
  stateBackendFor,
  teardownStageState,
  validatePippenStateBucket,
} from "../pippen-rehearsal.ts";
import { pippenStateBucket, stackStateOptions, type AwsRun } from "../state-bucket.ts";
import { validateAwsInputs, type AwsInputs } from "../inputs.ts";
import { capturePlan } from "../../../src/cli/aws-provenance-io.ts";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const fixture = <T>(name: string): T => JSON.parse(readFileSync(fileURLToPath(new URL(`./fixtures/pippen-rehearsal/${name}`, import.meta.url)), "utf8")) as T;
const src = (path: string) => readFileSync(fileURLToPath(new URL(`../../../${path}`, import.meta.url)), "utf8");
const ACCOUNT = "111122223333";
const REGION = "us-west-2";
const BUCKET = pippenStateBucket(ACCOUNT, REGION);
const PREFIX = "96bulls/96bulls-aws/aws-rehearse-p1-1126/";
const SESSION = "0f1e2d3c-4b5a-4968-8776-a5b4c3d2e1f0";
const BOUNDARY = `arn:aws:iam::${ACCOUNT}:policy/boundary`;
const ROLE = `arn:aws:iam::${ACCOUNT}:role/pippen/workload/96bulls-aws-rehearse-p1-1126-session-pipeline`;
const ISSUED = "2026-11-26T18:00:00Z";
/** A moment inside the lease (issued + 1 h), so the tests do not depend on the clock. */
const NOW = Date.parse("2026-11-26T19:00:00Z");
const pinOf = (o: Partial<{ accountId: string; boundaryArn: string; roleArn: string; roleDisposition: "deleted" | "retained"; sessionId: string; issuedAt: string }> = {}) =>
  rehearsalIssuerPin({ stage: REHEARSAL_STAGE, region: REGION, accountId: ACCOUNT, boundaryArn: BOUNDARY, roleArn: ROLE, roleDisposition: "deleted", sessionId: SESSION, issuedAt: ISSUED, ...o });
const PIN = pinOf();
/** What `pippen sandbox exec` attests (lease, account, issuance time, the issuer's pin), plus the operator's lease id. */
const RUN = "66666666-6666-4666-8666-666666666666";
const ENV = { NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: ACCOUNT, [ISSUED_AT_ENV]: ISSUED, [ISSUER_PIN_ENV]: PIN, [RUN_ID_ENV]: RUN };
const CALLER = `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-${SESSION}`;
const IN_SHAPE = fixture<Record<string, unknown>>("state-bucket.in-shape.json");
const INPUTS = { accountId: ACCOUNT, region: REGION, pippenSandbox: { permissionsBoundaryArn: BOUNDARY, stateBucket: "validate-only", executionRole: { arn: ROLE, disposition: "deleted" } } };
const CTX: RehearsalContext = rehearsalRunContext(REHEARSAL_STAGE, INPUTS, ENV, NOW)!;

/** A fake aws CLI over the fixture responses; every call is recorded. */
function fakeBucket(
  over: Record<string, unknown> = {},
  opts: { absent?: boolean; caller?: string; account?: string; errors?: Record<string, string>; stateEmpty?: boolean; inUse?: string[]; composite?: string[]; listing?: unknown; alarmsReply?: unknown; groupsReply?: unknown; generation?: string | null; denied?: string } = {},
) {
  const calls: string[][] = [];
  const aws: AwsRun = (args) => {
    calls.push(args);
    const op = args[1]!;
    if (args[0] === "sts") return JSON.stringify({ Account: opts.account ?? ACCOUNT, Arn: opts.caller ?? CALLER });
    const line = args.join(" ");
    if (opts.denied && line.includes(opts.denied)) throw new Error("An error occurred (AccessDenied) when calling the operation");
    if (op === "list-objects-v2") return JSON.stringify(opts.listing ?? (opts.stateEmpty ? { KeyCount: 0 } : { KeyCount: 1, Contents: [{ Key: `${PREFIX}session-pipeline.json` }] }));
    if (op === "put-object") return JSON.stringify({ ETag: '"e"' });
    if (op === "get-object") {
      const body = opts.generation === undefined ? generationBody(CTX, "claimed") : opts.generation;
      if (body === null) throw new Error("An error occurred (NoSuchKey)");
      writeFileSync(args.at(-1)!, body);
      return JSON.stringify({ ETag: '"g"', VersionId: "g1" });
    }
    const used = (opts.inUse ?? []).some((n) => line.includes(n));
    if (op === "get-function-configuration" || op === "get-queue-url") {
      if (used) return "{}";
      throw new Error(args[0] === "sqs" ? "An error occurred (AWS.SimpleQueueService.NonExistentQueue)" : "An error occurred (ResourceNotFoundException)");
    }
    if (op === "describe-alarms") return JSON.stringify(opts.alarmsReply ?? { MetricAlarms: used ? [{ AlarmName: "x" }] : [], CompositeAlarms: (opts.composite ?? []).map((AlarmName) => ({ AlarmName })) });
    if (op === "describe-log-groups") return JSON.stringify(opts.groupsReply ?? { logGroups: used ? [{ logGroupName: args[args.indexOf("--log-group-name-prefix") + 1] }] : [] });
    if (opts.absent) throw new Error(`An error occurred (NoSuchBucket) when calling the ${op} operation: The specified bucket does not exist`);
    if (opts.errors?.[op]) throw new Error(opts.errors[op]);
    const body = { ...IN_SHAPE, ...over }[op];
    if (body === undefined) throw new Error(`unexpected call ${op}`);
    return JSON.stringify(body);
  };
  return { aws, calls, writes: () => calls.filter((c) => (c[0] === "s3api" && !/^(get-|list-|head-)/.test(c[1] ?? "")) || c[0] === "s3").map((c) => c[1]) };
}
const codeOf = (run: () => unknown) => {
  try {
    run();
    return "ok";
  } catch (e) {
    return e instanceof StateBucketError ? e.code : `other: ${(e as Error).message}`;
  }
};

describe("backend selection: decided from validated inputs, before any state backend is chosen", () => {
  it("a declared rehearsal stage needs well-formed inputs; missing or malformed inputs throw, never fall through to ensure or the default store", () => {
    expect(stateBackendFor(REHEARSAL_STAGE, INPUTS)).toBe("validate-only");
    const bad: [string, unknown][] = [
      ["no inputs", undefined],
      ["null", null],
      ["empty object", {}],
      ["no pippenSandbox", { accountId: ACCOUNT, region: REGION }],
      ["pippenSandbox without stateBucket", { ...INPUTS, pippenSandbox: { permissionsBoundaryArn: INPUTS.pippenSandbox.permissionsBoundaryArn } }],
      ["stateBucket ensure", { ...INPUTS, pippenSandbox: { ...INPUTS.pippenSandbox, stateBucket: "ensure" } }],
      ["another region", { ...INPUTS, region: "us-east-1" }],
      ["bad account", { ...INPUTS, accountId: "1234" }],
      ["boundary in another account", { ...INPUTS, pippenSandbox: { ...INPUTS.pippenSandbox, permissionsBoundaryArn: "arn:aws:iam::444455556666:policy/b" } }],
      ["pippenSandbox not an object", { ...INPUTS, pippenSandbox: "validate-only" }],
    ];
    for (const [why, inputs] of bad) expect([why, codeOf(() => stateBackendFor(REHEARSAL_STAGE, inputs))]).toEqual([why, "state_inputs_invalid"]);
    // Every other stage keeps its old choice.
    expect(stateBackendFor("aws-proof-sandbox", { accountId: ACCOUNT, region: REGION, pippenSandbox: { permissionsBoundaryArn: "x" } })).toBe("ensure");
    expect(stateBackendFor("aws-proof", { accountId: ACCOUNT, region: REGION, pippenSandbox: { permissionsBoundaryArn: "x" } })).toBe("ensure");
    expect(stateBackendFor("aws-sessions-prod", { accountId: ACCOUNT, region: "us-east-1" })).toBe("default");
    expect(stateBackendFor("aws-sessions-prod", undefined)).toBe("default");
    expect(codeOf(() => stateBackendFor("aws-proof-sandbox", { pippenSandbox: { stateBucket: "validate-only" } }))).toBe("state_inputs_invalid");
  });

  it("prepareStateBackend: zero AWS calls and no ensure for missing or malformed rehearsal inputs; validate-only never ensures", () => {
    for (const inputs of [undefined, {}, { accountId: ACCOUNT, region: REGION }, { ...INPUTS, region: "us-east-1" }]) {
      const f = fakeBucket();
      let ensured = 0;
      expect(codeOf(() => prepareStateBackend(f.aws, { stage: REHEARSAL_STAGE, inputs, env: ENV, now: NOW }, () => void ensured++))).toBe("state_inputs_invalid");
      expect([inputs, f.calls.length, ensured]).toEqual([inputs, 0, 0]);
    }
    const f = fakeBucket();
    let ensured = 0;
    expect(prepareStateBackend(f.aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV, now: NOW }, () => void ensured++)).toBe("validate-only");
    expect(ensured).toBe(0);
    expect(f.writes()).toEqual([]);
    // A drifted bucket: refused, still no ensure and no write.
    const drift = fakeBucket({ "get-bucket-versioning": { Status: "Suspended" } });
    expect(codeOf(() => prepareStateBackend(drift.aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV, now: NOW }, () => void ensured++))).toBe("state_bucket_versioning_drift");
    expect([ensured, drift.writes()]).toEqual([0, []]);
    // The proof stage still ensures, exactly once; a stage without pippenSandbox makes no call.
    const proof = fakeBucket();
    prepareStateBackend(proof.aws, { stage: "aws-proof-sandbox", inputs: { accountId: ACCOUNT, region: REGION, pippenSandbox: { permissionsBoundaryArn: "x" } }, env: {} }, () => void ensured++);
    expect([ensured, proof.calls.length]).toEqual([1, 0]);
    const plain = fakeBucket();
    expect(prepareStateBackend(plain.aws, { stage: "aws-sessions-prod", inputs: { accountId: ACCOUNT, region: "us-east-1" }, env: {} }, () => void ensured++)).toBe("default");
    expect([ensured, plain.calls.length]).toEqual([1, 0]);
  });

  it("stage A's entry (prepareStateBackend, the capture's and the deploy's state layer) is read-only: no generation refuses, tearing-down refuses, a generation-only state requires every name unused", () => {
    let ensured = 0;
    const run = (f: ReturnType<typeof fakeBucket>) => codeOf(() => prepareStateBackend(f.aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV, now: NOW }, () => void ensured++));
    const genOnly = { KeyCount: 1, Contents: [{ Key: generationKey(PREFIX) }] };
    // Unclaimed (no generation object): refused, nothing written. The claim is a separate, explicit step.
    for (const f of [fakeBucket({}, { stateEmpty: true, generation: null }), fakeBucket({}, { generation: null })]) {
      expect(run(f)).toBe("state_not_claimed");
      expect(f.writes()).toEqual([]);
    }
    // Claimed, stage A not applied (the generation object is the only object): every name must be unused.
    for (const used of ["96bulls-aws-rehearse-p1-1126-session-pipeline", "96bulls-aws-rehearse-p1-1126-session-search", "96bulls-aws-rehearse-p1-1126-session-search-backlog-age", "/aws/lambda/96bulls-aws-rehearse-p1-1126-session-pipeline"]) {
      const f = fakeBucket({}, { listing: genOnly, inUse: [used] });
      expect([used, run(f)]).toEqual([used, "state_resources_remain"]);
      expect(f.writes()).toEqual([]);
    }
    expect(run(fakeBucket({}, { listing: genOnly, composite: ["96bulls-aws-rehearse-p1-1126-session-pipeline-errors"] }))).toBe("state_resources_remain");
    expect(run(fakeBucket({}, { listing: genOnly, denied: "get-function-configuration" }))).toBe("state_gone_unproven");
    for (const alarmsReply of [{ MetricAlarms: [] }, { MetricAlarms: [], CompositeAlarms: [], NextToken: "t" }, { CompositeAlarms: [] }, { MetricAlarms: [{}], CompositeAlarms: [] }, []])
      expect([alarmsReply, run(fakeBucket({}, { listing: genOnly, alarmsReply }))]).toEqual([alarmsReply, "state_teardown_failed"]);
    for (const groupsReply of [{}, { logGroups: [], nextToken: "t" }, { logGroups: [{}] }]) expect([groupsReply, run(fakeBucket({}, { listing: genOnly, groupsReply }))]).toEqual([groupsReply, "state_teardown_failed"]);
    const clean = fakeBucket({}, { listing: genOnly });
    expect(run(clean)).toBe("ok");
    expect(clean.writes()).toEqual([]);
    // Alchemy's lease file alone does not mean stage A was applied: the names are still checked.
    expect(run(fakeBucket({}, { listing: { KeyCount: 2, Contents: [{ Key: `${PREFIX}__lease__.json` }, { Key: generationKey(PREFIX) }] }, inUse: ["96bulls-aws-rehearse-p1-1126-session-pipeline"] }))).toBe("state_resources_remain");
    // While a teardown is open (the generation is tearing-down), no capture or deploy runs, whatever else is there.
    const tearing = generationBody(CTX, "tearing-down", { manifestDigest: "a".repeat(64), authorizationDigest: "b".repeat(64) });
    for (const listing of [genOnly, { KeyCount: 2, Contents: [{ Key: generationKey(PREFIX) }, { Key: `${PREFIX}session-search.json` }] }]) {
      const f = fakeBucket({}, { listing, generation: tearing });
      expect([listing, run(f)]).toEqual([listing, "state_teardown_in_progress"]);
      expect(f.writes()).toEqual([]);
    }
    // Stage A applied (the generation and resources): resumed under this run's generation, no names check, no write.
    const later = fakeBucket({}, { listing: { KeyCount: 2, Contents: [{ Key: generationKey(PREFIX) }, { Key: `${PREFIX}session-search.json` }] }, inUse: ["96bulls-aws-rehearse-p1-1126-session-pipeline"] });
    expect(run(later)).toBe("ok");
    expect(later.calls.some((c) => c[1] === "get-function-configuration")).toBe(false);
    expect(later.writes()).toEqual([]);
    for (const generation of ["", "{", generationBody({ ...CTX, runId: "77777777-7777-4777-8777-777777777777" }, "claimed"), generationBody({ ...CTX, sessionId: "11111111-1111-4111-8111-111111111111" }, "claimed"), generationBody({ ...CTX, pin: "0".repeat(64) }, "claimed")])
      expect([generation, run(fakeBucket({}, { generation }))]).toEqual([generation, "state_provenance_mismatch"]);
    for (const listing of [{ KeyCount: 1, IsTruncated: true, Contents: [{ Key: `${PREFIX}a.json` }] }, { KeyCount: 1, NextContinuationToken: "t", Contents: [{ Key: `${PREFIX}a.json` }] }, { KeyCount: -1 }, { KeyCount: "1", Contents: [{ Key: `${PREFIX}a.json` }] }, { KeyCount: 1 }, { KeyCount: 0, Contents: [{ Key: `${PREFIX}a.json` }] }, { KeyCount: 1, Contents: [{ Key: "96bulls/96bulls-aws/aws-proof-sandbox/a.json" }] }, { KeyCount: 1, Contents: [{ Key: PREFIX }] }, []])
      expect([listing, run(fakeBucket({}, { listing }))]).toEqual([listing, "state_bucket_unreadable"]);
    expect(ensured).toBe(0);
  });

  it("the explicit claim: an empty VERSION inventory (no noncurrent versions or delete markers), names unused, then a create-only generation write", () => {
    const withVersions = (page: unknown, opts: Parameters<typeof fakeBucket>[1] = {}) => {
      const f = fakeBucket({}, opts);
      const aws: AwsRun = (args) => (args[1] === "list-object-versions" ? (f.calls.push(args), JSON.stringify(page)) : args[1] === "put-object" ? (f.calls.push(args), JSON.stringify({ ETag: '"e"', VersionId: "g1" })) : f.aws(args));
      return { ...f, aws };
    };
    const ok = withVersions({ IsTruncated: false });
    expect(claimStageState(ok.aws, CTX)).toEqual({ bucket: BUCKET, generation: generationKey(PREFIX) });
    expect(ok.writes()).toEqual(["put-object"]);
    const put = ok.calls.find((c) => c[1] === "put-object")!;
    expect(put[put.indexOf("--key") + 1]).toBe(generationKey(PREFIX));
    expect(put[put.indexOf("--if-none-match") + 1]).toBe("*");
    expect(put.includes("--if-match")).toBe(false);
    // Not a .json key: Alchemy's state store and the snapshot never see it.
    expect(generationKey(PREFIX).endsWith(".json")).toBe(false);
    expect(JSON.parse(generationBody(CTX, "claimed"))).toEqual({ schema: 1, stage: REHEARSAL_STAGE, runId: RUN, sessionId: SESSION, issuedAt: ISSUED, pin: PIN, state: "claimed", receipts: null });
    expect(codeOf(() => generationBody(CTX, "claimed", { manifestDigest: "a", authorizationDigest: "b" }))).toBe("state_teardown_failed");
    expect(codeOf(() => generationBody(CTX, "tearing-down"))).toBe("state_teardown_failed");
    // Prior history (current, noncurrent or deleted) cannot be claimed, and nothing is written.
    for (const page of [
      { IsTruncated: false, Versions: [{ Key: `${PREFIX}old.json`, VersionId: "v-old" }] },
      { IsTruncated: false, DeleteMarkers: [{ Key: `${PREFIX}old.json`, VersionId: "dm-old" }] },
      { IsTruncated: false, Versions: [{ Key: generationKey(PREFIX), VersionId: "g-old" }] },
    ]) {
      const f = withVersions(page);
      expect([page, codeOf(() => claimStageState(f.aws, CTX))]).toEqual([page, "state_provenance_mismatch"]);
      expect(f.writes()).toEqual([]);
    }
    const used = withVersions({ IsTruncated: false }, { inUse: ["96bulls-aws-rehearse-p1-1126-session-pipeline"] });
    expect(codeOf(() => claimStageState(used.aws, CTX))).toBe("state_resources_remain");
    expect(used.writes()).toEqual([]);
    // A generation object created in between (the create-only put fails): refused.
    const raced = withVersions({ IsTruncated: false });
    const racedAws: AwsRun = (args) => {
      if (args[1] === "put-object") throw new Error("PreconditionFailed");
      return raced.aws(args);
    };
    expect(codeOf(() => claimStageState(racedAws, CTX))).toBe("state_provenance_mismatch");
  });

  it("both entry points decide the backend from validated inputs before any selection, and ensure only inside prepareStateBackend", () => {
    const cap = src("src/cli/aws-provenance-io.ts");
    const decide = cap.indexOf("rehearsalRunContext(stage, inputs, env);");
    expect(decide).toBeGreaterThan(0);
    expect(decide).toBeLessThan(cap.indexOf('awsJson(root, ["sts", "get-caller-identity"], env)'));
    expect(decide).toBeLessThan(cap.indexOf("observeSourceAt(root, env)"));
    expect(cap).toContain('if (pippen) prepareStateBackend((args) => run("aws", args, root, env), { stage, inputs, env }, ensurePippenStateBucket);');
    expect(cap.match(/ensurePippenStateBucket\(/g) ?? []).toEqual([]);
    const stack = src("stacks/aws/alchemy.aws.run.ts");
    const guard = stack.indexOf("if (argvStage) rehearsalRunContext(argvStage, stateTarget, process.env);");
    expect(guard).toBeGreaterThan(0);
    expect(guard).toBeLessThan(stack.indexOf("export default Alchemy.Stack("));
    expect(stack.match(/ensurePippenStateBucket\(/g)).toEqual(["ensurePippenStateBucket("]);
    expect(stack.indexOf("ensurePippenStateBucket(run, target)")).toBeGreaterThan(stack.indexOf("prepareStateBackend("));
  });

  it("the inputs rule (validateAwsInputs) refuses the rehearsal stage without its validate-only inputs", () => {
    expect(() => assertStateBucketMode(REHEARSAL_STAGE, INPUTS)).not.toThrow();
    expect(() => assertStateBucketMode(REHEARSAL_STAGE, {})).toThrow(/runs only in a Pippen sandbox in us-west-2/);
    expect(() => assertStateBucketMode("aws-proof", { pippenSandbox: { permissionsBoundaryArn: "x" } })).not.toThrow();
    expect(() => assertStateBucketMode("aws-proof-sandbox", { pippenSandbox: { permissionsBoundaryArn: "x", stateBucket: "validate-only" } })).toThrow(/only for a declared validate-only stage/);
    const inputs = { accountId: ACCOUNT, region: REGION, budgetUsd: 50, budgetEmail: "budget@example.invalid", pippenSandbox: { permissionsBoundaryArn: `arn:aws:iam::${ACCOUNT}:policy/boundary` } } as unknown as AwsInputs;
    expect(() => validateAwsInputs(REHEARSAL_STAGE, inputs)).toThrow(/runs only in a Pippen sandbox/);
    expect(() => validateAwsInputs(REHEARSAL_STAGE, { ...inputs, pippenSandbox: { ...inputs.pippenSandbox!, stateBucket: "validate-only" } })).toThrow(/runs only in a Pippen sandbox/);
    expect(() => validateAwsInputs(REHEARSAL_STAGE, { ...inputs, pippenSandbox: { ...inputs.pippenSandbox!, stateBucket: "validate-only", executionRole: { arn: ROLE, disposition: "deleted" } } })).not.toThrow();
  });
});

describe("validate-only: the shared state bucket is read, never created or changed", () => {
  it("an in-shape bucket passes with reads only (no head-bucket, no create, no put)", () => {
    const f = fakeBucket();
    expect(assertValidateOnlyState(f.aws, CTX)).toEqual({ bucket: BUCKET, prefix: PREFIX });
    expect(f.writes()).toEqual([]);
    expect(f.calls.map((c) => `${c[0]} ${c[1]}`)).toEqual([
      "sts get-caller-identity",
      "s3api get-bucket-versioning",
      "s3api get-bucket-encryption",
      "s3api get-public-access-block",
      "s3api get-bucket-ownership-controls",
      "s3api get-bucket-tagging",
      "s3api list-objects-v2",
      "s3api get-object",
    ]);
  });

  it("bucket absent: refused with state_bucket_absent, nothing created", () => {
    const f = fakeBucket({}, { absent: true });
    expect(codeOf(() => validatePippenStateBucket(f.aws, { accountId: ACCOUNT, region: REGION }))).toBe("state_bucket_absent");
    expect(f.writes()).toEqual([]);
  });

  it("drift in each of the five settings is refused with its own code, and nothing is written", () => {
    const drift = fixture<Record<string, Record<string, unknown>>>("state-bucket.drift.json");
    const expected: Record<string, string> = {
      "versioning-suspended": "state_bucket_versioning_drift",
      "versioning-never": "state_bucket_versioning_drift",
      "encryption-kms": "state_bucket_encryption_drift",
      "encryption-bucket-key": "state_bucket_encryption_drift",
      "public-access-open": "state_bucket_public_access_drift",
      "ownership-writer": "state_bucket_ownership_drift",
      "tagging-no-sandbox-tag": "state_bucket_tagging_drift",
    };
    expect(Object.keys(drift).sort()).toEqual(Object.keys(expected).sort());
    for (const [name, over] of Object.entries(drift)) {
      const f = fakeBucket(over);
      expect([name, codeOf(() => validatePippenStateBucket(f.aws, { accountId: ACCOUNT, region: REGION }))]).toEqual([name, expected[name]]);
      expect(f.writes()).toEqual([]);
    }
    const missing: [string, string, string][] = [
      ["get-bucket-encryption", "An error occurred (ServerSideEncryptionConfigurationNotFoundError)", "state_bucket_encryption_drift"],
      ["get-public-access-block", "An error occurred (NoSuchPublicAccessBlockConfiguration)", "state_bucket_public_access_drift"],
      ["get-bucket-ownership-controls", "An error occurred (OwnershipControlsNotFoundError)", "state_bucket_ownership_drift"],
      ["get-bucket-tagging", "An error occurred (NoSuchTagSet)", "state_bucket_tagging_drift"],
      ["get-bucket-versioning", "An error occurred (AccessDenied) canary-text-91", "state_bucket_unreadable"],
    ];
    for (const [op, err, code] of missing) {
      const f = fakeBucket({}, { errors: { [op]: err } });
      let message = "";
      try {
        validatePippenStateBucket(f.aws, { accountId: ACCOUNT, region: REGION });
      } catch (e) {
        message = (e as Error).message;
        expect([op, (e as StateBucketError).code]).toEqual([op, code]);
      }
      expect(message).not.toContain("canary-text-91");
    }
  });
});

describe("the issuer pin and the run context: checked before any AWS call", () => {
  const OTHER = "444455556666";
  const other = { accountId: OTHER, region: REGION, pippenSandbox: { permissionsBoundaryArn: `arn:aws:iam::${OTHER}:policy/boundary`, stateBucket: "validate-only", executionRole: { arn: `arn:aws:iam::${OTHER}:role/pippen/workload/r`, disposition: "deleted" } } };
  const otherEnv = { ...ENV, PIPPEN_SANDBOX_ACCOUNT_ID: OTHER };

  it("a different well-formed account with a matching boundary is refused, even with matching lease env and STS", () => {
    expect(stateBackendFor(REHEARSAL_STAGE, other)).toBe("validate-only");
    const f = fakeBucket({}, { account: OTHER, caller: `arn:aws:sts::${OTHER}:assumed-role/agent-session/agent-${SESSION}` });
    let ensured = 0;
    expect(codeOf(() => prepareStateBackend(f.aws, { stage: REHEARSAL_STAGE, inputs: other, env: otherEnv, now: NOW }, () => void ensured++))).toBe("state_issuer_pin_mismatch");
    expect([f.calls.length, ensured]).toEqual([0, 0]);
  });

  it("the pin binds the boundary, the execution role and its disposition, the lease session and its issuance time", () => {
    expect(codeOf(() => rehearsalRunContext(REHEARSAL_STAGE, INPUTS, ENV, NOW))).toBe("ok");
    const variants: [string, unknown, Record<string, string | undefined>][] = [
      ["another boundary", { ...INPUTS, pippenSandbox: { ...INPUTS.pippenSandbox, permissionsBoundaryArn: `arn:aws:iam::${ACCOUNT}:policy/other` } }, ENV],
      ["another role", { ...INPUTS, pippenSandbox: { ...INPUTS.pippenSandbox, executionRole: { arn: `arn:aws:iam::${ACCOUNT}:role/pippen/workload/other`, disposition: "deleted" } } }, ENV],
      ["retained instead of deleted", { ...INPUTS, pippenSandbox: { ...INPUTS.pippenSandbox, executionRole: { arn: ROLE, disposition: "retained" } } }, ENV],
      ["another issuance time", INPUTS, { ...ENV, [ISSUED_AT_ENV]: "2026-11-26T18:00:01Z" }],
      ["another lease", INPUTS, { ...ENV, NINETY_SIX_BULLS_PIPPEN_SESSION: "11111111-1111-4111-8111-111111111111", PIPPEN_SANDBOX_SESSION_ID: "11111111-1111-4111-8111-111111111111" }],
      ["no pin", INPUTS, { ...ENV, [ISSUER_PIN_ENV]: undefined }],
      ["malformed pin", INPUTS, { ...ENV, [ISSUER_PIN_ENV]: PIN.toUpperCase() }],
      ["no issuance time", INPUTS, { ...ENV, [ISSUED_AT_ENV]: undefined }],
    ];
    for (const [why, inputs, env] of variants) expect([why, codeOf(() => rehearsalRunContext(REHEARSAL_STAGE, inputs, env, NOW))]).toEqual([why, "state_issuer_pin_mismatch"]);
    // The pin is a digest only.
    expect(PIN).toMatch(/^[0-9a-f]{64}$/);
    expect(PIN).not.toContain(ACCOUNT);
  });

  it("the lease must not have ended: before issuance, or at and after the earliest end, refuses", () => {
    for (const now of ["2026-11-26T17:59:59Z", "2026-11-27T00:00:00Z", "2026-11-27T08:00:00Z"]) expect([now, codeOf(() => rehearsalRunContext(REHEARSAL_STAGE, INPUTS, ENV, Date.parse(now)))]).toEqual([now, "state_lease_expired"]);
    expect(codeOf(() => rehearsalRunContext(REHEARSAL_STAGE, INPUTS, ENV, Date.parse("2026-11-26T23:59:59Z")))).toBe("ok");
  });

  it("the role input: in the account, retained only under /pippen/workload/, disposition deleted or retained", () => {
    for (const role of [
      { arn: `arn:aws:iam::${OTHER}:role/pippen/workload/r`, disposition: "deleted" },
      { arn: `arn:aws:iam::${ACCOUNT}:role/r`, disposition: "retained" },
      { arn: ROLE, disposition: "kept" },
      { arn: "not-an-arn", disposition: "deleted" },
      undefined,
    ])
      expect([role, codeOf(() => stateBackendFor(REHEARSAL_STAGE, { ...INPUTS, pippenSandbox: { ...INPUTS.pippenSandbox, executionRole: role } }))]).toEqual([role, "state_inputs_invalid"]);
  });

  it("through the FULL capture entry (capturePlan): malformed inputs, a wrong pin or an ended lease refuse with zero aws and git calls", () => {
    const root = mkdtempSync(join(tmpdir(), "96bulls-capture-"));
    try {
      const bin = join(root, "bin");
      const log = join(root, "calls.log");
      mkdirSync(bin);
      for (const tool of ["aws", "git", "bunx", "helm", "bun"]) {
        writeFileSync(join(bin, tool), `#!/bin/sh\necho "${tool} $*" >> "${log}"\nexit 1\n`);
        chmodSync(join(bin, tool), 0o755);
      }
      mkdirSync(join(root, ".96bulls", REHEARSAL_STAGE), { recursive: true });
      const inputsPath = join(root, ".96bulls", REHEARSAL_STAGE, "aws-inputs.json");
      const env = { ...ENV, PATH: bin };
      const cases: [string, unknown, Record<string, string | undefined>, string][] = [
        ["malformed inputs", { accountId: ACCOUNT, region: REGION }, env, "state_inputs_invalid"],
        ["another account", other, { ...env, PIPPEN_SANDBOX_ACCOUNT_ID: OTHER }, "state_issuer_pin_mismatch"],
        ["no pin", INPUTS, { ...env, [ISSUER_PIN_ENV]: undefined }, "state_issuer_pin_mismatch"],
        // The real clock (2026-11-25 or later) is outside a lease issued at 2026-11-26T18:00Z for any run before it.
        ["lease not yet valid or ended", INPUTS, { ...env, [ISSUED_AT_ENV]: "2026-01-01T00:00:00Z", [ISSUER_PIN_ENV]: pinOf({ issuedAt: "2026-01-01T00:00:00Z" }) }, "state_lease_expired"],
      ];
      for (const [why, inputs, e, code] of cases) {
        writeFileSync(inputsPath, JSON.stringify(inputs));
        expect([why, codeOf(() => capturePlan(root, REHEARSAL_STAGE, e))]).toEqual([why, code]);
        let calls = "";
        try {
          calls = readFileSync(log, "utf8");
        } catch {
          calls = "";
        }
        expect([why, calls]).toEqual([why, ""]);
      }
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("caller and prefix guards", () => {
  it("the caller must be the broker-attested lease session in the attested account", () => {
    expect(leaseCallerArn(ACCOUNT, ENV)).toBe(CALLER);
    const wrong = [
      `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-00000000-0000-4000-8000-000000000000`,
      `arn:aws:sts::444455556666:assumed-role/agent-session/agent-${SESSION}`,
      `arn:aws:sts::${ACCOUNT}:assumed-role/AdministratorAccess/agent-${SESSION}`,
      `arn:aws:iam::${ACCOUNT}:user/someone`,
    ];
    for (const caller of wrong) {
      const f = fakeBucket({}, { caller });
      expect([caller, codeOf(() => assertValidateOnlyState(f.aws, CTX))]).toEqual([caller, "state_caller_mismatch"]);
      expect(f.calls.filter((c) => c[0] === "s3api")).toEqual([]);
    }
    // STS reporting another account for the same ARN text is refused too.
    expect(codeOf(() => assertValidateOnlyState(fakeBucket({}, { account: "444455556666" }).aws, CTX))).toBe("state_caller_mismatch");
    // The attestation is required, not optional, and must match the inputs and the lease id.
    for (const env of [
      { NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION },
      { ...ENV, PIPPEN_SANDBOX_ACCOUNT_ID: undefined },
      { ...ENV, PIPPEN_SANDBOX_SESSION_ID: undefined },
      { ...ENV, PIPPEN_SANDBOX_SESSION_ID: "11111111-1111-4111-8111-111111111111" },
      { ...ENV, PIPPEN_SANDBOX_ACCOUNT_ID: "444455556666" },
      { ...ENV, NINETY_SIX_BULLS_PIPPEN_SESSION: "not-a-uuid" },
    ])
      expect(codeOf(() => leaseCallerArn(ACCOUNT, env))).toBe("state_caller_mismatch");
  });

  it("the prefix is exactly this stage's: a sibling stage or a parent prefix is refused", () => {
    expect(rehearsalStatePrefix(REHEARSAL_STAGE)).toBe(PREFIX);
    for (const prefix of ["96bulls/96bulls-aws/", "96bulls/", "", "96bulls/96bulls-aws/aws-proof-sandbox/", "96bulls/96bulls-aws/aws-rehearse-p1-1126", "96bulls/96bulls-aws/aws-rehearse-p1-11/"])
      expect([prefix, codeOf(() => assertStatePrefix(REHEARSAL_STAGE, prefix))]).toEqual([prefix, "state_prefix_mismatch"]);
    for (const stage of ["aws-proof-sandbox", "aws-rehearse-p1-1127", "aws-sessions-prod"]) expect(codeOf(() => rehearsalStatePrefix(stage))).toBe("state_prefix_mismatch");
    expect(stackStateOptions({ accountId: ACCOUNT, region: REGION, pippenSandbox: { stateBucket: "validate-only" } })).toEqual({ prefix: "96bulls", bucketName: BUCKET });
  });
});

/** This teardown's receipts in the state-level tests. */
const R = { manifestDigest: "a".repeat(64), authorizationDigest: "b".repeat(64) };
const CLAIMED = generationBody(CTX, "claimed");
const TEARING = generationBody(CTX, "tearing-down", R);

/**
 * A stateful fake of the stage prefix in a versioned bucket with conditional writes: each key's
 * versions (oldest first, each with its body and ETag); ListObjectVersions; DeleteObjects (deleting a
 * key's newest version makes the previous one current, as S3 does); PutObject with --if-none-match "*"
 * (fails if the key has a current version) or --if-match <ETag> (fails unless that is the current
 * ETag); GetObject (body to the outfile, ETag and VersionId on stdout).
 * `partialFirstDelete`: the first DeleteObjects removes only its first object and reports an error;
 * `loseLastDelete`: the DeleteObjects that empties the prefix succeeds but its response is lost;
 * `silentlyKeep`: DeleteObjects never removes that key but reports success.
 */
function statefulS3(initial: [string, string, string?][], opts: { partialFirstDelete?: boolean; loseLastDelete?: boolean; silentlyKeep?: string } = {}) {
  type V = { id: string; body: string; etag: string };
  const keys = new Map<string, V[]>();
  let n = 0;
  const add = (key: string, id: string, body: string) => {
    const list = keys.get(key) ?? [];
    list.push({ id, body, etag: `"e${++n}"` });
    keys.set(key, list);
  };
  for (const [key, id, body] of initial) add(key, id, body ?? "{}");
  const calls: string[][] = [];
  const batches: string[][] = [];
  let deletesSeen = 0;
  const versions = () => [...keys].flatMap(([key, list]) => list.map((v) => [key, v.id] as [string, string]));
  const aws: AwsRun = (args) => {
    calls.push(args);
    const op = `${args[0]} ${args[1]}`;
    if (op === "s3api list-object-versions")
      return JSON.stringify({ IsTruncated: false, Versions: versions().filter((v) => !v[1].startsWith("dm")).map(([Key, VersionId]) => ({ Key, VersionId })), DeleteMarkers: versions().filter((v) => v[1].startsWith("dm")).map(([Key, VersionId]) => ({ Key, VersionId })) });
    if (op === "s3api delete-objects") {
      const objects = (JSON.parse(args[args.indexOf("--delete") + 1]!) as { Objects: { Key: string; VersionId: string }[] }).Objects;
      batches.push(objects.map((o) => o.Key));
      deletesSeen++;
      const remove = opts.partialFirstDelete && deletesSeen === 1 ? objects.slice(0, 1) : objects.filter((o) => o.Key !== opts.silentlyKeep);
      for (const o of remove) {
        const list = (keys.get(o.Key) ?? []).filter((v) => v.id !== o.VersionId);
        if (list.length) keys.set(o.Key, list);
        else keys.delete(o.Key);
      }
      if (opts.partialFirstDelete && deletesSeen === 1) return JSON.stringify({ Errors: [{ Key: objects[1]?.Key, Code: "InternalError" }] });
      if (opts.loseLastDelete && versions().length === 0) throw new Error("connection reset");
      return "{}";
    }
    if (op === "s3api put-object") {
      const key = args[args.indexOf("--key") + 1]!;
      const current = keys.get(key)?.at(-1);
      if (args.includes("--if-none-match") && current) throw new Error("An error occurred (PreconditionFailed)");
      if (args.includes("--if-match") && current?.etag !== args[args.indexOf("--if-match") + 1]) throw new Error("An error occurred (PreconditionFailed)");
      add(key, `p${n + 1}`, readFileSync(args[args.indexOf("--body") + 1]!, "utf8"));
      const v = keys.get(key)!.at(-1)!;
      return JSON.stringify({ ETag: v.etag, VersionId: v.id });
    }
    if (op === "s3api get-object") {
      const key = args[args.indexOf("--key") + 1]!;
      const v = keys.get(key)?.at(-1);
      if (!v) throw new Error("An error occurred (NoSuchKey)");
      writeFileSync(args.at(-1)!, v.body);
      return JSON.stringify({ ETag: v.etag, VersionId: v.id });
    }
    throw new Error(`unexpected ${args.join(" ")}`);
  };
  /** A writer outside the executor under test (another run's claim). */
  const put = (key: string, body: string) => add(key, `x${n + 1}`, body);
  const body = (key: string) => keys.get(key)?.at(-1)?.body;
  return { aws, calls, versions, deleteBatches: () => batches, put, body };
}

/** The bucket, caller and names reads around a stateful prefix (for the claim). */
function withStateAndBucket(s3: AwsRun): AwsRun {
  const bucket = fakeBucket();
  return (args) => (["list-object-versions", "delete-objects", "put-object", "get-object"].includes(args[1]!) ? s3(args) : bucket.aws(args));
}

describe("teardown: only this stage's state keys, under the generation lock", () => {
  const fakeListing = (pages: unknown[], opts: { deleteErrors?: boolean } = {}) => {
    const calls: string[][] = [];
    let page = 0;
    const aws: AwsRun = (args) => {
      calls.push(args);
      if (args[1] === "list-object-versions") {
        const p = pages[page++] ?? { IsTruncated: false };
        return typeof p === "string" ? p : JSON.stringify(p);
      }
      if (args[1] === "delete-objects") return JSON.stringify(opts.deleteErrors ? { Errors: [{ Code: "AccessDenied" }] } : {});
      throw new Error(`unexpected ${args[1]}`);
    };
    return { aws, calls };
  };
  const target = { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE };
  const run = { ...target, execute: true, original: CTX, receipts: R };
  const GK = generationKey(PREFIX);
  const genOnly = (state: "claimed" | "tearing-down" = "claimed"): [string, string, string] => [GK, "g1", state === "claimed" ? CLAIMED : TEARING];

  it("lists every version and delete marker under the exact prefix across pages (dry run deletes nothing)", () => {
    const dry = fakeListing(fixture<unknown[]>("teardown.pages.json"));
    expect(teardownStageState(dry.aws, target)).toEqual({ listed: 5, deleted: 0, remaining: 5 });
    expect(dry.calls.every((c) => c[1] === "list-object-versions" && c[c.indexOf("--prefix") + 1] === PREFIX)).toBe(true);
    expect(dry.calls[1]!.slice(dry.calls[1]!.indexOf("--key-marker"))).toEqual(["--key-marker", `${PREFIX}b.json`, "--version-id-marker", "v-b1"]);
    expect(codeOf(() => teardownStageState(fakeListing(fixture<unknown[]>("teardown.pages.json")).aws, { ...target, execute: true }))).toBe("state_teardown_failed");
  });

  it("the transition claimed -> tearing-down by compare-and-swap comes first; then everything else; the generation object last and alone", () => {
    const s3 = statefulS3([genOnly(), [`${PREFIX}session-search.json`, "v1"], [`${PREFIX}session-search.json`, "v2"], [`${PREFIX}gone.json`, "dm1"]]);
    let casVersion = "";
    const aws: AwsRun = (args) => {
      const out = s3.aws(args);
      if (args[1] === "put-object") casVersion = (JSON.parse(out) as { VersionId: string }).VersionId;
      return out;
    };
    expect(teardownStageState(aws, run)).toEqual({ listed: 4, deleted: 5, remaining: 0 });
    // The CAS (If-Match the claimed ETag) before any delete.
    const ops = s3.calls.map((c) => c[1]);
    expect(ops.indexOf("put-object")).toBeLessThan(ops.indexOf("delete-objects"));
    const put = s3.calls.find((c) => c[1] === "put-object")!;
    expect([put[put.indexOf("--key") + 1], put[put.indexOf("--if-match") + 1]]).toEqual([GK, '"e1"']);
    // Others (including the generation's old claimed version) first; the generation's current version last, alone.
    expect(s3.deleteBatches()).toEqual([[GK, `${PREFIX}session-search.json`, `${PREFIX}session-search.json`, `${PREFIX}gone.json`], [GK]]);
    expect(s3.versions()).toEqual([]);
    // The last delete names exactly the version the compare-and-swap returned.
    expect(casVersion).not.toBe("");
    const last = s3.calls.filter((c) => c[1] === "delete-objects").at(-1)!;
    expect(JSON.parse(last[last.indexOf("--delete") + 1]!).Objects).toEqual([{ Key: GK, VersionId: casVersion }]);
  });

  it("partial batch success: the generation (tearing-down) survives, the retry with the same receipts resumes and completes", () => {
    const s3 = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"], [`${PREFIX}b.json`, "v2"]], { partialFirstDelete: true });
    expect(codeOf(() => teardownStageState(s3.aws, run))).toBe("state_teardown_failed");
    expect(s3.body(GK)).toBe(TEARING);
    expect(teardownStageState(s3.aws, run)).toMatchObject({ remaining: 0 });
    expect(s3.versions()).toEqual([]);
    // Other receipts cannot take over a tearing-down generation.
    const t = statefulS3([genOnly("tearing-down"), [`${PREFIX}a.json`, "v1"]]);
    expect(codeOf(() => teardownStageState(t.aws, { ...run, receipts: { ...R, authorizationDigest: "c".repeat(64) } }))).toBe("state_provenance_mismatch");
    expect(t.deleteBatches()).toEqual([]);
  });

  it("a delete that reports success but leaves a version, or a generation object that survives its delete, refuses success", () => {
    const keep = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"]], { silentlyKeep: `${PREFIX}a.json` });
    expect(codeOf(() => teardownStageState(keep.aws, run))).toBe("state_teardown_not_empty");
    expect(keep.body(GK)).toBe(TEARING);
    const gen = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"]], { silentlyKeep: GK });
    expect(codeOf(() => teardownStageState(gen.aws, run))).toBe("state_teardown_not_empty");
  });

  it("the broker's interleaving: T1 read A's generation; T2 completes; B claims; T1's compare-and-swap fails and B is untouched", () => {
    const B = generationBody({ ...CTX, runId: "77777777-7777-4777-8777-777777777777" }, "claimed");
    const s3 = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"]]);
    let paused = false;
    const t1: AwsRun = (args) => {
      // Pause T1 right after its generation read, before its compare-and-swap.
      if (args[1] === "put-object" && !paused) {
        paused = true;
        expect(teardownStageState(s3.aws, run)).toMatchObject({ remaining: 0 });
        expect(codeOf(() => claimStageState(withStateAndBucket(s3.aws), { ...CTX, runId: "77777777-7777-4777-8777-777777777777" }))).toBe("ok");
      }
      return s3.aws(args);
    };
    expect(codeOf(() => teardownStageState(t1, run))).toBe("state_provenance_mismatch");
    expect(s3.body(GK)).toBe(B);
    expect(s3.versions()).toEqual([[GK, expect.any(String)]]);
  });

  it("a stale executor stops at every later step: T2 completes and B claims while T1 waits before each delete; B is untouched", () => {
    const B = generationBody({ ...CTX, runId: "77777777-7777-4777-8777-777777777777" }, "claimed");
    for (const k of [1, 2]) {
      const s3 = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"], [`${PREFIX}b.json`, "v2"]]);
      let deletes = 0;
      let done = false;
      const t1: AwsRun = (args) => {
        if (args[1] === "delete-objects" && ++deletes === k && !done) {
          done = true;
          expect(teardownStageState(s3.aws, run)).toMatchObject({ remaining: 0 });
          s3.put(GK, B);
        }
        return s3.aws(args);
      };
      const code = codeOf(() => teardownStageState(t1, run));
      // T1's in-flight delete names only its own generation's version ids. After the first delete its
      // fence stops it (the generation is B's); after the last, the final re-list (B's generation remains).
      expect([k, code]).toEqual([k, k === 1 ? "state_provenance_mismatch" : "state_teardown_not_empty"]);
      expect([k, s3.body(GK)]).toEqual([k, B]);
    }
    // The fence catches a replaced generation (same body, another version) between the CAS and the first delete.
    const s3 = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"]]);
    let swapped = false;
    const t: AwsRun = (args) => {
      const out = s3.aws(args);
      if (args[1] === "put-object" && !swapped) {
        swapped = true;
        s3.put(GK, TEARING);
      }
      return out;
    };
    expect(codeOf(() => teardownStageState(t, run))).toBe("state_provenance_mismatch");
    expect(s3.deleteBatches()).toEqual([]);
    // Another run's generation at the start: refused before any write or delete.
    const other = statefulS3([[GK, "g9", B], [`${PREFIX}a.json`, "v1"]]);
    expect(codeOf(() => teardownStageState(other.aws, run))).toBe("state_provenance_mismatch");
    expect([other.deleteBatches(), other.calls.some((c) => c[1] === "put-object")]).toEqual([[], false]);
    // No generation object at all (an orphan version): refused.
    expect(codeOf(() => teardownStageState(statefulS3([[`${PREFIX}a.json`, "v1"]]).aws, run))).toBe("state_provenance_mismatch");
    // The caller re-check fails mid-teardown: refused before the next delete.
    const c = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"]]);
    let checks = 0;
    expect(codeOf(() => teardownStageState(c.aws, { ...run, assertCaller: () => { if (++checks > 2) throw new StateBucketError("state_caller_mismatch"); } }))).toBe("state_caller_mismatch");
    expect(c.versions().some(([key]) => key === GK)).toBe(true);
  });

  it("a claim cannot interleave: while any version or the generation object exists the claim refuses", () => {
    const s3 = statefulS3([genOnly(), [`${PREFIX}a.json`, "v1"]], { partialFirstDelete: true });
    expect(codeOf(() => teardownStageState(s3.aws, run))).toBe("state_teardown_failed");
    const other = { ...CTX, runId: "77777777-7777-4777-8777-777777777777" };
    expect(codeOf(() => claimStageState(withStateAndBucket(s3.aws), other))).toBe("state_provenance_mismatch");
    teardownStageState(s3.aws, run);
    expect(s3.versions()).toEqual([]);
    expect(codeOf(() => claimStageState(withStateAndBucket(s3.aws), other))).toBe("ok");
    // A second claim while that generation exists: refused before any write.
    expect(codeOf(() => claimStageState(withStateAndBucket(s3.aws), CTX))).toBe("state_provenance_mismatch");
  });

  it("refuses before deleting anything when any listed key is outside the prefix", () => {
    for (const [name, pages] of Object.entries(fixture<Record<string, unknown[]>>("teardown.outside-prefix.json"))) {
      const f = fakeListing(pages);
      expect([name, codeOf(() => teardownStageState(f.aws, run))]).toEqual([name, "state_teardown_outside_prefix"]);
      expect([name, f.calls.filter((c) => c[1] === "delete-objects").length]).toEqual([name, 0]);
      expect([name, codeOf(() => listStageStateVersions(fakeListing(pages).aws, target))]).toEqual([name, "state_teardown_outside_prefix"]);
    }
  });

  it("validates every page strictly: blank output, a missing or non-boolean IsTruncated, missing markers, non-arrays, unknown fields, another prefix", () => {
    for (const [name, pages] of Object.entries(fixture<Record<string, unknown[]>>("teardown.malformed.json"))) {
      const f = fakeListing(pages);
      expect([name, codeOf(() => teardownStageState(f.aws, run))]).toEqual([name, "state_teardown_failed"]);
      expect([name, f.calls.filter((c) => c[1] === "delete-objects").length]).toEqual([name, 0]);
    }
    const failing: AwsRun = () => {
      throw new Error("An error occurred (AccessDenied) secret-ish-canary");
    };
    let message = "";
    try {
      listStageStateVersions(failing, target);
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toMatch(/state_teardown_failed/);
    expect(message).not.toContain("secret-ish-canary");
    expect(codeOf(() => teardownStageState(fakeListing([]).aws, { ...run, stage: "aws-proof-sandbox" }))).toBe("state_prefix_mismatch");
  });

  const names = rehearsalResourceNames();
  const ROLE_NAME = "96bulls-aws-rehearse-p1-1126-session-pipeline";
  const U1 = "44444444-4444-4444-8444-444444444444";
  const U2 = "55555555-5555-4555-8555-555555555555";
  const FN = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${names.function}`;
  const qArn = (n: string) => `arn:aws:sqs:${REGION}:${ACCOUNT}:${n}`;
  /** A fake account where `present` ids still exist (by any substring of the call), for the manifest and its readback. */
  const fakeAccount = (
    present: Set<string>,
    opts: { alarms?: string[]; composite?: string[]; group?: boolean; role?: { Arn: string; Path: string; PermissionsBoundary?: { PermissionsBoundaryArn: string } }; fnRole?: string; mappings?: { UUID: string; EventSourceArn: string }[]; denied?: string; failing?: string } = {},
  ) => {
    const calls: string[][] = [];
    const aws: AwsRun = (args) => {
      calls.push(args);
      const line = args.join(" ");
      if (opts.denied && line.includes(opts.denied)) throw new Error("An error occurred (AccessDenied) canary-77");
      if (opts.failing && line.includes(opts.failing)) throw new Error("An error occurred (ThrottlingException) canary-78");
      const has = [...present].some((id) => line.includes(id));
      switch (`${args[0]} ${args[1]}`) {
        case "lambda get-function-configuration":
          if (!has) throw new Error("An error occurred (ResourceNotFoundException)");
          return JSON.stringify({ FunctionArn: FN, Role: opts.fnRole ?? ROLE });
        case "sqs get-queue-url": {
          const n = args[args.indexOf("--queue-name") + 1]!;
          if (!present.has(n)) throw new Error("An error occurred (AWS.SimpleQueueService.NonExistentQueue)");
          return JSON.stringify({ QueueUrl: `https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/${n}` });
        }
        case "sqs get-queue-attributes":
          if (!has) throw new Error("An error occurred (AWS.SimpleQueueService.NonExistentQueue)");
          return "{}";
        case "lambda list-event-source-mappings":
          return JSON.stringify({ EventSourceMappings: (opts.mappings ?? [{ UUID: U1, EventSourceArn: qArn(names.queues[0]!) }, { UUID: U2, EventSourceArn: qArn(names.queues[2]!) }]).map((m) => ({ ...m, FunctionArn: FN })) });
        case "lambda get-event-source-mapping":
          if (!has) throw new Error("An error occurred (ResourceNotFoundException)");
          return "{}";
        case "cloudwatch describe-alarms":
          return JSON.stringify({ MetricAlarms: (opts.alarms ?? []).map((AlarmName) => ({ AlarmName })), CompositeAlarms: (opts.composite ?? []).map((AlarmName) => ({ AlarmName })) });
        case "logs describe-log-groups":
          return JSON.stringify({ logGroups: opts.group ? [{ logGroupName: names.logGroup }] : [] });
        case "iam get-role":
          if (opts.role) return JSON.stringify({ Role: opts.role });
          if (!has) throw new Error("An error occurred (NoSuchEntity)");
          return JSON.stringify({ Role: { Arn: ROLE, Path: "/pippen/workload/" } });
      }
      throw new Error(`unexpected ${line}`);
    };
    return { aws, calls };
  };
  const live = () => new Set([names.function, ...names.queues, U1, U2, ROLE_NAME]);
  const T = { accountId: ACCOUNT, region: REGION, stage: REHEARSAL_STAGE };
  const manifest = () => readPhysicalManifest(fakeAccount(live(), { alarms: names.alarms, group: true }).aws, CTX, RUN);

  it("the pre-destroy manifest records every id, the pinned role, disposition and boundary, and this run's binding", () => {
    const m = manifest();
    expect(m.function).toEqual({ name: names.function, arn: FN });
    expect(m.role).toEqual({ arn: ROLE, disposition: "deleted", boundaryArn: BOUNDARY });
    expect(m.binding).toEqual({ runId: RUN, sessionId: SESSION, issuedAt: ISSUED, pin: PIN });
    expect(m.queues.map((q) => q.name)).toEqual(names.queues);
    expect(m.mappings.map((x) => x.uuid).sort()).toEqual([U1, U2]);
    expect(m.alarms).toEqual([...names.alarms].sort());
    expect(m.digest).toBe(manifestDigest(m));
    // Missing ids refuse at read time, and so does a function whose role is not the pinned one.
    for (const [why, f] of [
      ["one mapping", fakeAccount(live(), { alarms: names.alarms, group: true, mappings: [{ UUID: U1, EventSourceArn: qArn(names.queues[0]!) }] })],
      ["an alarm missing", fakeAccount(live(), { alarms: names.alarms.slice(1), group: true })],
      ["no log group", fakeAccount(live(), { alarms: names.alarms })],
      ["another role on the function", fakeAccount(live(), { alarms: names.alarms, group: true, fnRole: `arn:aws:iam::${ACCOUNT}:role/someone` })],
    ] as const)
      expect([why, codeOf(() => readPhysicalManifest(f.aws, CTX, RUN))]).toEqual([why, "state_manifest_invalid"]);
    expect(codeOf(() => readPhysicalManifest(fakeAccount(live(), { alarms: names.alarms, group: true }).aws, CTX, "not-a-uuid"))).toBe("state_manifest_invalid");
  });

  it("a missing, incomplete, stale or edited manifest refuses before anything is checked or deleted", () => {
    const m = manifest();
    expect(codeOf(() => assertManifestComplete(m, CTX, RUN))).toBe("ok");
    const sealed = (x: Record<string, unknown>) => ({ ...x, digest: manifestDigest(x as never) });
    const bad: [string, unknown, string?][] = [
      ["missing", undefined],
      ["empty", {}],
      ["one mapping", sealed({ ...m, mappings: m.mappings.slice(0, 1) })],
      ["duplicate mapping", sealed({ ...m, mappings: [m.mappings[0], m.mappings[0]] })],
      ["both mappings on the events queue", sealed({ ...m, mappings: m.mappings.map((x) => ({ ...x, eventSourceArn: qArn(names.queues[0]!) })) })],
      ["three queues", sealed({ ...m, queues: m.queues.slice(1) })],
      ["four alarms", sealed({ ...m, alarms: m.alarms.slice(1) })],
      ["another role", sealed({ ...m, role: { ...m.role, arn: `arn:aws:iam::${ACCOUNT}:role/pippen/workload/other` } })],
      ["another account's role", sealed({ ...m, role: { ...m.role, arn: `arn:aws:iam::444455556666:role/pippen/workload/${ROLE_NAME}` } })],
      ["a disposition the pin did not approve", sealed({ ...m, role: { ...m.role, disposition: "retained" } })],
      ["another boundary", sealed({ ...m, role: { ...m.role, boundaryArn: `arn:aws:iam::${ACCOUNT}:policy/other` } })],
      ["another run", sealed({ ...m, binding: { ...m.binding, runId: "77777777-7777-4777-8777-777777777777" } })],
      ["another lease", sealed({ ...m, binding: { ...m.binding, sessionId: "11111111-1111-4111-8111-111111111111" } })],
      ["another issuance", sealed({ ...m, binding: { ...m.binding, issuedAt: "2026-11-26T17:00:00Z" } })],
      ["another pin", sealed({ ...m, binding: { ...m.binding, pin: "0".repeat(64) } })],
      ["edited without its digest", { ...m, alarms: [...m.alarms.slice(1), m.alarms[0]] , logGroup: m.logGroup, mappings: [{ ...m.mappings[0]!, uuid: "88888888-8888-4888-8888-888888888888" }, m.mappings[1]] }],
      ["no digest", { ...m, digest: undefined }],
      ["another stage", sealed({ ...m, stage: "aws-proof-sandbox" })],
    ];
    for (const [why, x] of bad) expect([why, codeOf(() => assertManifestComplete(x, CTX, RUN))]).toEqual([why, "state_manifest_invalid"]);
    // Executed in another run (another run id in the environment): refused.
    expect(codeOf(() => assertManifestComplete(m, CTX, "77777777-7777-4777-8777-777777777777"))).toBe("state_manifest_invalid");
    expect(codeOf(() => assertManifestGone(fakeAccount(new Set()).aws, { ...m, mappings: [] }, CTX, RUN))).toBe("state_manifest_invalid");
  });

  it("execute needs the independent receipt: the digest the broker recorded at --manifest, not a local recomputation", () => {
    const m = manifest();
    expect(codeOf(() => assertManifestReceipt(m, m.digest))).toBe("ok");
    for (const expected of [undefined, "", "abc", "0".repeat(64), m.digest.toUpperCase()]) expect([expected, codeOf(() => assertManifestReceipt(m, expected))]).toEqual([expected, "state_receipt_mismatch"]);
    // A manifest edited and re-sealed locally (digest recomputed) no longer matches the receipt.
    const resealed = { ...m, mappings: [{ ...m.mappings[0]!, uuid: "88888888-8888-4888-8888-888888888888" }, m.mappings[1]!] };
    const sealed = { ...resealed, digest: manifestDigest(resealed as never) };
    expect(codeOf(() => assertManifestComplete(sealed, CTX, RUN))).toBe("ok");
    expect(codeOf(() => assertManifestReceipt(sealed, m.digest))).toBe("state_receipt_mismatch");
  });

  it("partial destroy: each recorded id is checked; a remaining mapping, queue, function, alarm, log group or role refuses until everything is gone", () => {
    const m: PhysicalManifest = manifest();
    for (const left of [U2, names.queues[3]!, names.function, ROLE_NAME])
      expect([left, codeOf(() => assertManifestGone(fakeAccount(new Set([left])).aws, m, CTX, RUN))]).toEqual([left, "state_resources_remain"]);
    expect(codeOf(() => assertManifestGone(fakeAccount(new Set(), { alarms: [names.alarms[4]!] }).aws, m, CTX, RUN))).toBe("state_resources_remain");
    expect(codeOf(() => assertManifestGone(fakeAccount(new Set(), { group: true }).aws, m, CTX, RUN))).toBe("state_resources_remain");
    const done = fakeAccount(new Set());
    expect(codeOf(() => assertManifestGone(done.aws, m, CTX, RUN))).toBe("ok");
    const checked = done.calls.map((c) => c.join(" "));
    for (const id of [FN, ...m.queues.map((q) => q.url), U1, U2]) expect([id, checked.some((c) => c.includes(id))]).toEqual([id, true]);
    // Deleted: GetRole on exactly the pinned role name.
    expect(checked.filter((c) => c.startsWith("iam get-role"))).toEqual([`iam get-role --role-name ${ROLE_NAME} --region ${REGION} --output json`]);
    // AccessDenied is never "gone" (a tag-conditioned read of a deleted resource): refused as unproven, for each readback.
    for (const denied of ["get-function-configuration", "get-queue-attributes", "get-event-source-mapping", "describe-alarms", "describe-log-groups", "iam get-role"]) {
      let message = "";
      try {
        assertManifestGone(fakeAccount(new Set(), { denied }).aws, m, CTX, RUN);
      } catch (e) {
        message = (e as Error).message;
      }
      expect([denied, message]).toEqual([denied, expect.stringMatching(/state_gone_unproven/)]);
      expect(message).not.toContain("canary-77");
    }
    // Another failure is not "gone" either.
    expect(codeOf(() => assertManifestGone(fakeAccount(new Set(), { failing: "get-event-source-mapping" }).aws, m, CTX, RUN))).toBe("state_teardown_failed");
    // A same-name composite alarm remaining is a remaining resource.
    expect(codeOf(() => assertManifestGone(fakeAccount(new Set(), { composite: [names.alarms[0]!] }).aws, m, CTX, RUN))).toBe("state_resources_remain");
  });

  it("a retained role must be exactly the pinned ARN and path, with the pinned permissions boundary", () => {
    const keptInputs = { ...INPUTS, pippenSandbox: { ...INPUTS.pippenSandbox, executionRole: { arn: ROLE, disposition: "retained" } } };
    const keptEnv = { ...ENV, [ISSUER_PIN_ENV]: pinOf({ roleDisposition: "retained" }) };
    const ctx = rehearsalRunContext(REHEARSAL_STAGE, keptInputs, keptEnv, NOW)!;
    const m = readPhysicalManifest(fakeAccount(live(), { alarms: names.alarms, group: true }).aws, ctx, RUN);
    const good = { Arn: ROLE, Path: "/pippen/workload/", PermissionsBoundary: { PermissionsBoundaryArn: BOUNDARY } };
    expect(codeOf(() => assertManifestGone(fakeAccount(new Set(), { role: good }).aws, m, ctx, RUN))).toBe("ok");
    for (const [why, role] of [
      ["no boundary", { Arn: ROLE, Path: "/pippen/workload/" }],
      ["another boundary", { ...good, PermissionsBoundary: { PermissionsBoundaryArn: `arn:aws:iam::${ACCOUNT}:policy/other` } }],
      ["another path", { ...good, Path: "/" }],
      ["another ARN", { ...good, Arn: `arn:aws:iam::${ACCOUNT}:role/pippen/workload/other` }],
    ] as const)
      expect([why, codeOf(() => assertManifestGone(fakeAccount(new Set(), { role: role as never }).aws, m, ctx, RUN))]).toEqual([why, "state_resources_remain"]);
    // A retained role that was deleted is not "retained".
    expect(codeOf(() => assertManifestGone(fakeAccount(new Set()).aws, m, ctx, RUN))).toBe("state_teardown_failed");
  });

  it("before stage A no stage name may be in use (no existing resource can be adopted by tagging)", () => {
    expect(codeOf(() => assertRehearsalNamesUnused(fakeAccount(new Set()).aws, { region: REGION, stage: REHEARSAL_STAGE }))).toBe("ok");
    for (const used of [names.function, names.queues[1]!]) expect([used, codeOf(() => assertRehearsalNamesUnused(fakeAccount(new Set([used])).aws, { region: REGION, stage: REHEARSAL_STAGE }))]).toEqual([used, "state_resources_remain"]);
    expect(codeOf(() => assertRehearsalNamesUnused(fakeAccount(new Set(), { alarms: [names.alarms[0]!] }).aws, { region: REGION, stage: REHEARSAL_STAGE }))).toBe("state_resources_remain");
    expect(codeOf(() => assertRehearsalNamesUnused(fakeAccount(new Set(), { group: true }).aws, { region: REGION, stage: REHEARSAL_STAGE }))).toBe("state_resources_remain");
  });

  it("the teardown script delegates every check to runRehearsalTeardown; the claim and the manifest are explicit, separate steps", () => {
    const script = src("scripts/rehearsal-state-teardown.ts");
    expect(script).toContain("runRehearsalTeardown(aws, {");
    expect(script).toContain("claimStageState(aws, ctx)");
    expect(script).toContain('if (existsSync(manifestPath)) throw new StateBucketError("state_manifest_invalid");');
    expect(script).toContain('flag: "wx"');
    // The CLI keeps the error class: AccessDenied is passed on as such (state_gone_unproven), never as a generic failure.
    expect(script).toContain('? "AccessDenied"');
  });
});

describe("the lease session policies", () => {
  const lease = rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE });
  const all = [lease.inline, ...lease.managed.map((m) => m.document)];
  const statements = all.flatMap((d) => d.Statement as Record<string, unknown>[]);
  const actions = (s: Record<string, unknown>) => ([] as string[]).concat((s.Action as string | string[] | undefined) ?? []);

  it("fits STS: a small inline session policy (<= 2048 packed chars) and managed policies (<= 6144 each) passed by ARN", () => {
    expect(JSON.stringify(lease.inline).length).toBeLessThanOrEqual(2048);
    expect(lease.managed.length).toBeLessThanOrEqual(10);
    for (const m of lease.managed) expect([m.name, JSON.stringify(m.document).length <= 6144]).toEqual([m.name, true]);
    expect(lease.managed.map((m) => m.name)).toEqual(["96bulls-rehearse-p1-1126-state", "96bulls-rehearse-p1-1126-create", "96bulls-rehearse-p1-1126-act"]);
  });

  it("state: objects and prefixed listing on this prefix only; the shared bucket never writable", () => {
    const state = lease.managed[0]!.document.Statement as Record<string, unknown>[];
    expect(state.find((s) => s.Sid === "Objects")).toMatchObject({ Effect: "Allow", Resource: `arn:aws:s3:::${BUCKET}/${PREFIX}*` });
    expect(state.find((s) => s.Sid === "List")).toMatchObject({ Resource: `arn:aws:s3:::${BUCKET}`, Condition: { StringLike: { "s3:prefix": [`${PREFIX}*`] } } });
    expect(state.find((s) => s.Sid === "OtherPrefixes")).toMatchObject({ Effect: "Deny", Condition: { StringNotLike: { "s3:prefix": [`${PREFIX}*`] } } });
    expect(state.find((s) => s.Sid === "NotThisPrefix")).toMatchObject({ Effect: "Deny", NotResource: `arn:aws:s3:::${BUCKET}/${PREFIX}*` });
    const deny = (lease.inline.Statement as Record<string, unknown>[]).find((s) => s.Sid === "SharedBucket")!;
    for (const a of ["s3:PutBucket*", "s3:DeleteBucket*", "s3:PutEncryptionConfiguration", "s3:CreateBucket"]) expect(actions(deny)).toContain(a);
    for (const s of statements.filter((x) => x.Effect === "Allow")) for (const a of actions(s)) expect([s.Sid, /^s3:(Put|Delete)Bucket|^s3:CreateBucket|^s3:PutEncryption/.test(a)]).toEqual([s.Sid, false]);
  });

  it("tags: create only with the rehearsal tag; act only on resources that carry it; the tag cannot be removed", () => {
    const create = lease.managed[1]!.document.Statement as Record<string, unknown>[];
    for (const s of create.filter((x) => x.Sid !== "Tags")) expect([s.Sid, (s.Condition as Record<string, Record<string, string>>).StringEquals]).toEqual([s.Sid, { [`aws:RequestTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value }]);
    // Tag calls only in their own statement, never beside a create action, and only on a resource that already carries the tag (no IfExists).
    for (const s of create.filter((x) => x.Sid !== "Tags")) expect([s.Sid, actions(s).some((a) => /Tag(Queue|Resource)$/.test(a))]).toEqual([s.Sid, false]);
    const tags = create.find((s) => s.Sid === "Tags")!;
    expect(tags.Condition).toEqual({ DateLessThan: { "aws:CurrentTime": lease.expiresAt }, StringEquals: { "aws:RequestTag/rehearsal": "p1-1126", "aws:ResourceTag/rehearsal": "p1-1126" } });
    expect(JSON.stringify(lease)).not.toContain("IfExists");
    // No mapping ARN in the tag statement (a mapping cannot be tagged standalone).
    expect((tags.Resource as string[]).some((r) => r.includes("event-source-mapping"))).toBe(false);
    const act = lease.managed[2]!.document.Statement as Record<string, unknown>[];
    for (const s of act.filter((x) => !["Reads", "NamedReads", "RoleRead", "GoneReads", "GoneMappings"].includes(x.Sid as string)))
      expect([s.Sid, (s.Condition as Record<string, Record<string, string>>).StringEquals]).toEqual([s.Sid, { [`aws:ResourceTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value }]);
    // Mappings: by their own tag and exactly the function (ArnEquals, no wildcard), on mapping ARNs only.
    const exactFn = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:96bulls-aws-rehearse-p1-1126-session-pipeline`;
    expect(act.find((s) => s.Sid === "Mappings")).toMatchObject({
      Resource: `arn:aws:lambda:${REGION}:${ACCOUNT}:event-source-mapping:*`,
      Condition: { StringEquals: { "aws:ResourceTag/rehearsal": "p1-1126" }, ArnEquals: { "lambda:FunctionArn": exactFn } },
    });
    expect(create.find((s) => s.Sid === "Mappings")).toMatchObject({ Resource: `arn:aws:lambda:${REGION}:${ACCOUNT}:event-source-mapping:*`, Condition: { ArnEquals: { "lambda:FunctionArn": exactFn } } });
    expect(act.find((s) => s.Sid === "Function")!.Resource).toBe(exactFn);
    expect((lease.inline.Statement as Record<string, unknown>[]).find((s) => s.Sid === "KeepTag")).toMatchObject({ Effect: "Deny", Condition: { "ForAnyValue:StringEquals": { "aws:TagKeys": ["rehearsal"] } } });
  });

  it("actions: what the rehearsal and its guards need, no EventBridge and no Secrets Manager", () => {
    const every = statements.flatMap(actions);
    for (const a of ["lambda:ListAliases", "lambda:ListVersionsByFunction", "lambda:PublishVersion", "lambda:GetFunctionConfiguration", "lambda:ListEventSourceMappings", "cloudwatch:GetMetricStatistics", "cloudwatch:GetMetricData", "logs:StartQuery", "logs:GetQueryResults", "logs:FilterLogEvents", "sqs:GetQueueUrl", "cloudwatch:DescribeAlarms", "logs:DescribeLogGroups", "lambda:GetEventSourceMapping"])
      expect([a, every.includes(a)]).toEqual([a, true]);
    expect(every.filter((a) => /^(events|secretsmanager|iam):/.test(a))).toEqual(["iam:GetRole"]);
    // The unscoped reads are exactly those IAM cannot narrow to a resource.
    const act = lease.managed[2]!.document.Statement as Record<string, unknown>[];
    expect(act.find((s) => s.Sid === "Reads")).toMatchObject({ Resource: "*" });
    expect(actions(act.find((s) => s.Sid === "Reads")!)).toEqual(["lambda:ListEventSourceMappings", "cloudwatch:GetMetricStatistics", "cloudwatch:GetMetricData", "logs:DescribeLogGroups", "logs:GetQueryResults"]);
    const names = rehearsalResourceNames();
    expect(act.find((s) => s.Sid === "NamedReads")!.Resource).toEqual([...names.queues.map((q) => `arn:aws:sqs:${REGION}:${ACCOUNT}:${q}`), ...names.alarms.map((x) => `arn:aws:cloudwatch:${REGION}:${ACCOUNT}:alarm:${x}`)]);
    // RoleRead: exactly the pinned execution role.
    expect(act.find((s) => s.Sid === "RoleRead")!.Resource).toBe(ROLE);
    // Every scoped resource is exact (no name wildcard), except mapping ARNs (UUIDs unknown before creation, conditioned on the exact function) and the state prefix.
    const exact = new Set([
      `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${names.function}`,
      ...names.queues.map((q) => `arn:aws:sqs:${REGION}:${ACCOUNT}:${q}`),
      ...names.alarms.map((x) => `arn:aws:cloudwatch:${REGION}:${ACCOUNT}:alarm:${x}`),
      `arn:aws:logs:${REGION}:${ACCOUNT}:log-group:${names.logGroup}`,
      `arn:aws:logs:${REGION}:${ACCOUNT}:log-group:${names.logGroup}:*`,
      `arn:aws:lambda:${REGION}:${ACCOUNT}:event-source-mapping:*`,
      `arn:aws:s3:::${BUCKET}`,
      `arn:aws:s3:::${BUCKET}/${PREFIX}*`,
      ROLE,
    ]);
    for (const s of statements.filter((x) => x.Effect === "Allow" && x.Resource !== "*"))
      for (const r of ([] as string[]).concat(s.Resource as string | string[])) expect([s.Sid, r, exact.has(r)]).toEqual([s.Sid, r, true]);
    expect(() => rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: `arn:aws:iam::444455556666:role/x` })).toThrow();
    // Absence readbacks by exact name, untagged, so a deleted resource reads NotFound, not AccessDenied.
    expect(act.find((s) => s.Sid === "GoneReads")).toMatchObject({ Action: ["lambda:GetFunctionConfiguration", "sqs:GetQueueAttributes"], Resource: [`arn:aws:lambda:${REGION}:${ACCOUNT}:function:${names.function}`, ...names.queues.map((q) => `arn:aws:sqs:${REGION}:${ACCOUNT}:${q}`)] });
    expect(act.find((s) => s.Sid === "GoneMappings")).toBeUndefined();
    const U = ["44444444-4444-4444-8444-444444444444", "55555555-5555-4555-8555-555555555555"];
    const teardown = rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE, mappingUuids: U });
    expect((teardown.managed[2]!.document.Statement as Record<string, unknown>[]).find((s) => s.Sid === "GoneMappings")).toMatchObject({ Action: ["lambda:GetEventSourceMapping"], Resource: U.map((u) => `arn:aws:lambda:${REGION}:${ACCOUNT}:event-source-mapping:${u}`) });
    expect(JSON.stringify(teardown.managed[2]!.document).length).toBeLessThanOrEqual(6144);
    expect(() => rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE, mappingUuids: ["*"] })).toThrow();
    // The policies' inventory: five alarms (the three the pipeline always has plus the two search alarms), matching the doc.
    expect(names.alarms.length).toBe(5);
    const doc = src("docs/handoffs/pippen-rehearsal-p1-1126.md");
    for (const n of [names.function, ...names.queues, ...names.alarms, names.logGroup]) expect([n, doc.includes(`\`${n}\``)]).toEqual([n, true]);
    expect(doc).toContain("five alarms");
  });

  /** Evaluates a statement's date condition at `t` (the only conditions that depend on time). */
  const applies = (s: Record<string, unknown>, t: string) => {
    const c = (s.Condition ?? {}) as Record<string, Record<string, string>>;
    const now = Date.parse(t);
    if (c.DateLessThan && !(now < Date.parse(c.DateLessThan["aws:CurrentTime"]!))) return false;
    if (c.DateGreaterThanEquals && !(now >= Date.parse(c.DateGreaterThanEquals["aws:CurrentTime"]!))) return false;
    return true;
  };

  it("the cutoff is exclusive at 2026-11-27T08:00:00Z: allowed at 07:59:59, denied at 08:00:00", () => {
    expect(REHEARSAL_AUTHORITY_END).toBe("2026-11-27T08:00:00Z");
    expect(Date.parse(REHEARSAL_LEASE_EXPIRES_AT)).toBeLessThanOrEqual(Date.parse("2026-11-27T08:00:00Z"));
    expect(lease.expiresAt).toBe("2026-11-27T08:00:00Z");
    const allows = statements.filter((s) => s.Effect === "Allow");
    const end = (lease.inline.Statement as Record<string, unknown>[]).find((s) => s.Sid === "End")!;
    expect(allows.length).toBeGreaterThan(0);
    expect(allows.every((s) => applies(s, "2026-11-27T07:59:59Z"))).toBe(true);
    expect(applies(end, "2026-11-27T07:59:59Z")).toBe(false);
    expect(allows.some((s) => applies(s, "2026-11-27T08:00:00Z"))).toBe(false);
    expect(applies(end, "2026-11-27T08:00:00Z")).toBe(true);
    expect(end).toMatchObject({ Effect: "Deny", Action: "*", Condition: { DateGreaterThanEquals: { "aws:CurrentTime": "2026-11-27T08:00:00Z" } } });
  });

  it("the effective end is the earliest of the cutoff, issuance + 6 h and teardown; a later caller value is refused", () => {
    expect(REHEARSAL_LEASE_TTL_HOURS).toBe(6);
    expect(rehearsalLeaseEnd()).toBe("2026-11-27T08:00:00Z");
    expect(rehearsalLeaseEnd({ issuedAt: "2026-11-26T18:00:00Z" })).toBe("2026-11-27T00:00:00Z");
    expect(rehearsalLeaseEnd({ issuedAt: "2026-11-27T05:00:00Z" })).toBe("2026-11-27T08:00:00Z");
    expect(rehearsalLeaseEnd({ issuedAt: "2026-11-26T18:00:00Z", teardownCompletedAt: "2026-11-26T20:30:00Z" })).toBe("2026-11-26T20:30:00Z");
    expect(rehearsalLeaseEnd({ issuedAt: "2026-11-26T18:00:00Z", requested: "2026-11-26T22:00:00Z" })).toBe("2026-11-26T22:00:00Z");
    for (const requested of ["2026-11-27T00:00:01Z", "2026-11-27T08:00:01Z", "2026-11-28T00:00:00Z"])
      expect([requested, codeOf(() => rehearsalLeaseEnd({ issuedAt: "2026-11-26T18:00:00Z", requested }))]).toEqual([requested, "state_lease_expired"]);
    expect(codeOf(() => rehearsalLeaseEnd({ requested: "2026-11-27T08:00:01Z" }))).toBe("state_lease_expired");
    expect(codeOf(() => rehearsalLeaseEnd({ issuedAt: "2026-11-27T08:00:00Z" }))).toBe("state_lease_expired");
    expect(codeOf(() => rehearsalLeaseEnd({ requested: "tomorrow" }))).toBe("state_lease_expired");
    // The policies carry the effective end.
    const issued = rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE, issuedAt: "2026-11-26T18:00:00Z" });
    expect(issued.expiresAt).toBe("2026-11-27T00:00:00Z");
    expect(JSON.stringify(issued)).not.toContain("2026-11-27T08:00:00Z");
    expect(codeOf(() => rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE, expiresAt: "2026-11-28T00:00:00Z" }))).toBe("state_lease_expired");
    expect(codeOf(() => rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE, region: "us-east-1" }))).toBe("state_inputs_invalid");
    expect(codeOf(() => rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE, stage: "aws-proof-sandbox" }))).toBe("state_prefix_mismatch");
  });

  it("the checked-in document is the generator's template output (no issuance: the absolute cutoff), with placeholders", () => {
    const doc = src("docs/handoffs/pippen-rehearsal-p1-1126.lease-policy.json");
    const TEMPLATE_ROLE = "arn:aws:iam::000000000000:role/pippen/workload/execution-role";
    expect(doc).toBe(`${JSON.stringify(rehearsalLeasePolicies({ accountId: "000000000000", roleArn: TEMPLATE_ROLE }), null, 2).replaceAll(TEMPLATE_ROLE, "<execution-role-arn>").replaceAll("000000000000", "<account>")}\n`);
    expect(doc).not.toMatch(/\d{12}/);
    expect(doc).not.toContain("2026-11-28");
  });
});

describe("teardown under an authorized teardown lease (B) of a run captured under lease A", () => {
  const names = rehearsalResourceNames();
  const U1 = "44444444-4444-4444-8444-444444444444";
  const U2 = "55555555-5555-4555-8555-555555555555";
  const FN = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${names.function}`;
  const qArn = (n: string) => `arn:aws:sqs:${REGION}:${ACCOUNT}:${n}`;
  // Lease A issued 2026-11-26T18:00Z, so the run deadline is A + 6 h = 2026-11-27T00:00Z.
  const SESSION_B = "99999999-9999-4999-8999-999999999999";
  const ISSUED_B = "2026-11-26T20:00:00Z";
  const NOW_B = Date.parse("2026-11-26T20:30:00Z");
  const WINDOW_END = "2026-11-26T23:45:00Z";
  const EXPIRES_B = "2026-11-26T23:30:00Z";
  const envB = (sessionId = SESSION_B, issuedAt = ISSUED_B) => ({ NINETY_SIX_BULLS_PIPPEN_SESSION: sessionId, PIPPEN_SANDBOX_SESSION_ID: sessionId, PIPPEN_SANDBOX_ACCOUNT_ID: ACCOUNT, [ISSUED_AT_ENV]: issuedAt, [ISSUER_PIN_ENV]: pinOf({ sessionId, issuedAt }), [RUN_ID_ENV]: RUN });
  const ENV_B = envB();
  const CTX_B = rehearsalRunContext(REHEARSAL_STAGE, INPUTS, ENV_B, NOW_B)!;
  const CALLER_B = `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-${SESSION_B}`;
  // The broker's attestation: lease A revoked, and A's sessions expired, before B was issued.
  const REVOKED_A = "2026-11-26T19:30:00Z";
  const SESSIONS_EXPIRE_A = "2026-11-26T19:45:00Z";

  /** The manifest lease A wrote before the destroy. */
  const manifestA = (): PhysicalManifest => {
    const aws: AwsRun = (args) => {
      switch (`${args[0]} ${args[1]}`) {
        case "lambda get-function-configuration":
          return JSON.stringify({ FunctionArn: FN, Role: ROLE });
        case "sqs get-queue-url":
          return JSON.stringify({ QueueUrl: `https://sqs.${REGION}.amazonaws.com/${ACCOUNT}/${args[args.indexOf("--queue-name") + 1]}` });
        case "lambda list-event-source-mappings":
          return JSON.stringify({ EventSourceMappings: [{ UUID: U1, EventSourceArn: qArn(names.queues[0]!), FunctionArn: FN }, { UUID: U2, EventSourceArn: qArn(names.queues[2]!), FunctionArn: FN }] });
        case "cloudwatch describe-alarms":
          return JSON.stringify({ MetricAlarms: names.alarms.map((AlarmName) => ({ AlarmName })), CompositeAlarms: [] });
        case "logs describe-log-groups":
          return JSON.stringify({ logGroups: [{ logGroupName: names.logGroup }] });
      }
      throw new Error(`unexpected ${args.join(" ")}`);
    };
    return readPhysicalManifest(aws, CTX, RUN);
  };
  const authorization = (m: PhysicalManifest, over: Record<string, unknown> = {}, teardown: Record<string, unknown> = {}, ctxB = CTX_B) => {
    const t = { sessionId: ctxB.sessionId, callerArn: `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-${ctxB.sessionId}`, issuedAt: ctxB.issuedAt, windowEnd: WINDOW_END, expiresAt: EXPIRES_B, ...teardown };
    return {
      schema: 1,
      stage: REHEARSAL_STAGE,
      runId: RUN,
      original: { sessionId: SESSION, issuedAt: ISSUED, pin: PIN, revokedAt: REVOKED_A, sessionsExpireAt: SESSIONS_EXPIRE_A },
      manifestDigest: m.digest,
      mappingUuids: [U1, U2],
      ...over,
      teardown: { ...t, policyDigest: (teardown.policyDigest as string | undefined) ?? teardownPolicyDigest(ctxB, (over.mappingUuids as string[] | undefined) ?? [U1, U2], t.expiresAt as string) },
    };
  };
  /** The account after the destroy: every recorded id gone; the stage prefix stateful (A's claimed generation, one state version). */
  const fakeAfterDestroy = (o: { caller?: string | ((n: number) => string); generation?: string; remaining?: string; s3Opts?: Parameters<typeof statefulS3>[1]; initial?: [string, string, string?][] } = {}) => {
    const s3 = statefulS3(o.initial ?? [[generationKey(PREFIX), "g1", o.generation ?? generationBody(CTX, "claimed")], [`${PREFIX}session-search.json`, "v1"]], o.s3Opts ?? {});
    const calls: string[][] = [];
    let sts = 0;
    const aws: AwsRun = (args) => {
      calls.push(args);
      const line = args.join(" ");
      if (args[0] === "sts") return JSON.stringify({ Account: ACCOUNT, Arn: typeof o.caller === "function" ? o.caller(++sts) : (o.caller ?? CALLER_B) });
      if (["list-object-versions", "delete-objects", "put-object", "get-object"].includes(args[1]!)) return s3.aws(args);
      if (o.remaining && line.includes(o.remaining)) return "{}";
      const body = IN_SHAPE[args[1]!];
      if (body !== undefined) return JSON.stringify(body);
      if (args[1] === "describe-alarms") return JSON.stringify({ MetricAlarms: [], CompositeAlarms: [] });
      if (args[1] === "describe-log-groups") return JSON.stringify({ logGroups: [] });
      throw new Error(args[0] === "sqs" ? "An error occurred (AWS.SimpleQueueService.NonExistentQueue)" : args[0] === "iam" ? "An error occurred (NoSuchEntity)" : "An error occurred (ResourceNotFoundException)");
    };
    return { aws, calls, s3, deletes: () => s3.deleteBatches().length };
  };
  const teardown = (f: ReturnType<typeof fakeAfterDestroy>, i: { env?: Record<string, string | undefined>; now?: number; manifest?: unknown; expectManifestDigest?: string; authorization?: unknown; expectAuthorizationDigest?: string }) =>
    codeOf(() => runRehearsalTeardown(f.aws, { inputs: INPUTS, env: i.env ?? ENV_B, now: i.now ?? NOW_B, execute: true, ...i }));
  const withAuth = (m: PhysicalManifest, x: Record<string, unknown>) => ({ manifest: m, expectManifestDigest: m.digest, authorization: x, expectAuthorizationDigest: authorizationDigest(x) });

  it("captured under lease A, torn down under the authorized cleanup-only lease B: succeeds, the generation deleted last", () => {
    const m = manifestA();
    const auth = authorization(m);
    const f = fakeAfterDestroy();
    let result: unknown;
    expect(codeOf(() => (result = runRehearsalTeardown(f.aws, { inputs: INPUTS, env: ENV_B, now: NOW_B, execute: true, ...withAuth(m, auth) })))).toBe("ok");
    expect(result).toEqual({ listed: 2, deleted: 3, remaining: 0 });
    expect(f.s3.deleteBatches()).toEqual([[generationKey(PREFIX), `${PREFIX}session-search.json`], [generationKey(PREFIX)]]);
    const ops = f.calls.map((c) => `${c[0]} ${c[1]}`);
    // The caller is B at the start, before the physical readback, and before each delete.
    const firstRead = ops.findIndex((o) => o === "lambda get-function-configuration");
    expect(ops.slice(0, firstRead).filter((o) => o === "sts get-caller-identity").length).toBe(2);
    expect(ops.lastIndexOf("sts get-caller-identity", firstRead)).toBe(firstRead - 1);
    for (const [k, o] of ops.entries()) if (o === "s3api delete-objects") expect(ops.slice(0, k).lastIndexOf("sts get-caller-identity")).toBeGreaterThan(ops.slice(0, k).lastIndexOf("s3api delete-objects"));
    // The tearing-down generation records this teardown's receipts.
    const put = f.calls.find((c) => c[1] === "put-object")!;
    expect(put[put.indexOf("--if-match") + 1]).toBe('"e1"');
    // Lease A itself (no authorization): refused, nothing written or deleted.
    const a = fakeAfterDestroy({ caller: CALLER });
    expect(teardown(a, { env: ENV, now: NOW, manifest: m, expectManifestDigest: m.digest })).toBe("state_authorization_mismatch");
    expect([a.deletes(), a.calls.some((c) => c[1] === "put-object")]).toEqual([0, false]);
  });

  it("lease A's revocation is required: revoked and its sessions expired no later than B's issuance", () => {
    const m = manifestA();
    const orig = (o: Record<string, unknown>) => ({ original: { sessionId: SESSION, issuedAt: ISSUED, pin: PIN, revokedAt: REVOKED_A, sessionsExpireAt: SESSIONS_EXPIRE_A, ...o } });
    for (const [why, over] of [
      ["no revocation time", orig({ revokedAt: undefined })],
      ["no session expiry", orig({ sessionsExpireAt: undefined })],
      ["revoked after B was issued", orig({ revokedAt: "2026-11-26T20:00:01Z" })],
      ["A's sessions outlive B's issuance", orig({ sessionsExpireAt: "2026-11-26T20:00:01Z" })],
      ["revoked before A was issued", orig({ revokedAt: "2026-11-26T17:59:59Z" })],
      ["a malformed revocation time", orig({ revokedAt: "soon" })],
    ] as const) {
      const f = fakeAfterDestroy();
      expect([why, teardown(f, withAuth(m, authorization(m, over)))]).toEqual([why, "state_authorization_mismatch"]);
      expect([why, f.deletes(), f.calls.some((c) => c[1] === "put-object")]).toEqual([why, 0, false]);
    }
    // The boundary: revoked and expired exactly at B's issuance holds.
    expect(teardown(fakeAfterDestroy(), withAuth(m, authorization(m, orig({ revokedAt: ISSUED_B, sessionsExpireAt: ISSUED_B }))))).toBe("ok");
  });

  it("the caller must stay B: a change before the physical readback or before a delete refuses", () => {
    const m = manifestA();
    const auth = authorization(m);
    for (const at of [2, 3, 4, 5]) {
      const f = fakeAfterDestroy({ caller: (n) => (n >= at ? CALLER : CALLER_B) });
      expect([at, teardown(f, withAuth(m, auth))]).toEqual([at, "state_caller_mismatch"]);
      // Before the readback (2): nothing physical read. Later: the generation survives.
      if (at === 2) expect(f.calls.some((c) => c[1] === "get-function-configuration")).toBe(false);
      expect([at, f.s3.versions().some(([key]) => key === generationKey(PREFIX))]).toEqual([at, true]);
    }
  });

  it("lost final response and retries: completion is recognised under the same receipts; a different receipt is not", () => {
    const m = manifestA();
    const auth = authorization(m);
    // The last DeleteObjects (the generation object) succeeds but its response is lost.
    const f = fakeAfterDestroy({ s3Opts: { loseLastDelete: true } });
    expect(teardown(f, withAuth(m, auth))).toBe("state_teardown_failed");
    expect(f.s3.versions()).toEqual([]);
    // Retry with the same receipts: recognised as complete, nothing deleted again.
    const before = f.deletes();
    let result: unknown;
    expect(codeOf(() => (result = runRehearsalTeardown(f.aws, { inputs: INPUTS, env: ENV_B, now: NOW_B, execute: true, ...withAuth(m, auth) })))).toBe("ok");
    expect(result).toEqual({ listed: 0, deleted: 0, remaining: 0 });
    expect(f.deletes()).toBe(before);
    // Retry after a partial first batch: resumes under the tearing-down generation with the same receipts.
    const p = fakeAfterDestroy({ initial: [[generationKey(PREFIX), "g1", generationBody(CTX, "claimed")], [`${PREFIX}a.json`, "v1"], [`${PREFIX}b.json`, "v2"]], s3Opts: { partialFirstDelete: true } });
    expect(teardown(p, withAuth(m, auth))).toBe("state_teardown_failed");
    expect(teardown(p, withAuth(m, auth))).toBe("ok");
    expect(p.s3.versions()).toEqual([]);
    // Only the tearing-down generation left: resumed under the same receipts...
    const tdOnly = () => fakeAfterDestroy({ initial: [[generationKey(PREFIX), "g1", generationBody(CTX, "tearing-down", { manifestDigest: m.digest, authorizationDigest: authorizationDigest(auth) })]] });
    const t = tdOnly();
    expect(teardown(t, withAuth(m, auth))).toBe("ok");
    expect(t.s3.versions()).toEqual([]);
    // ...but not under another teardown's receipts, and a state version without a generation is never resumed.
    const other = authorization(m, {}, { expiresAt: "2026-11-26T23:00:00Z" });
    const t2 = tdOnly();
    expect(teardown(t2, withAuth(m, other))).toBe("state_provenance_mismatch");
    expect(t2.deletes()).toBe(0);
    const orphan = fakeAfterDestroy({ initial: [[`${PREFIX}a.json`, "v1"]] });
    expect(teardown(orphan, withAuth(m, auth))).toBe("state_provenance_mismatch");
    expect(orphan.deletes()).toBe(0);
  });

  it("an unrelated or altered teardown lease is refused, and nothing is deleted", () => {
    const m = manifestA();
    const auth = authorization(m);
    const D = authorizationDigest(auth);
    const cases: [string, Record<string, unknown>][] = [
      ["no authorization under lease B (the manifest is lease A's)", { manifest: m, expectManifestDigest: m.digest }],
      ["an authorization for another teardown session", withAuth(m, authorization(m, {}, { sessionId: "88888888-8888-4888-8888-888888888888", callerArn: `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-88888888-8888-4888-8888-888888888888` }))],
      ["only the session differs", withAuth(m, authorization(m, {}, { sessionId: "88888888-8888-4888-8888-888888888888" }))],
      ["only the issuance differs", withAuth(m, authorization(m, {}, { issuedAt: "2026-11-26T20:00:01Z" }))],
      ["a receipt digest that is not the record's", { ...withAuth(m, auth), expectAuthorizationDigest: "0".repeat(64) }],
      ["no receipt digest", { manifest: m, expectManifestDigest: m.digest, authorization: auth }],
      ["another policy digest", withAuth(m, authorization(m, {}, { policyDigest: "0".repeat(64) }))],
      ["the full rehearsal policies (with creates and updates) as B's policy", withAuth(m, authorization(m, {}, { policyDigest: createHashHex(JSON.stringify(rehearsalLeasePolicies({ accountId: ACCOUNT, roleArn: ROLE, issuedAt: ISSUED_B, mappingUuids: [U1, U2] }))) }))],
      ["the cleanup policy digest for another expiry", withAuth(m, authorization(m, {}, { policyDigest: teardownPolicyDigest(CTX_B, [U1, U2], "2026-11-26T23:00:00Z") }))],
      ["an expiry past the run deadline (A + 6 h)", withAuth(m, authorization(m, {}, { windowEnd: "2026-11-27T05:00:00Z", expiresAt: "2026-11-27T00:00:01Z" }))],
      ["an expiry past the approved teardown window", withAuth(m, authorization(m, {}, { expiresAt: "2026-11-26T23:50:00Z" }))],
      ["no teardown window", withAuth(m, authorization(m, {}, { windowEnd: undefined }))],
      ["an original pin that is not the approved one", withAuth(m, authorization(m, { original: { sessionId: SESSION, issuedAt: ISSUED, pin: "1".repeat(64), revokedAt: REVOKED_A, sessionsExpireAt: SESSIONS_EXPIRE_A } }))],
      ["mapping UUIDs other than the manifest's", withAuth(m, authorization(m, { mappingUuids: [U1, "77777777-7777-4777-8777-777777777777"] }))],
      ["another manifest digest in the authorization", withAuth(m, authorization(m, { manifestDigest: "2".repeat(64) }))],
      ["another run", withAuth(m, authorization(m, { runId: "77777777-7777-4777-8777-777777777777" }))],
    ];
    for (const [why, i] of cases) {
      const f = fakeAfterDestroy();
      expect([why, teardown(f, i)]).toEqual([why, expect.stringMatching(/^state_(authorization_mismatch|manifest_invalid|receipt_mismatch)$/)]);
      expect([why, f.deletes()]).toEqual([why, 0]);
    }
    // A consistent forgery naming another pin for lease A: only recomputing A's pin refuses it.
    {
      const X = "3".repeat(64);
      const body = { ...m, binding: { ...m.binding, pin: X } };
      const forged = { ...body, digest: manifestDigest(body as never) };
      const x = authorization(forged as never, { original: { sessionId: SESSION, issuedAt: ISSUED, pin: X, revokedAt: REVOKED_A, sessionsExpireAt: SESSIONS_EXPIRE_A } });
      const f = fakeAfterDestroy({ generation: generationBody({ ...CTX, pin: X }, "claimed") });
      expect(teardown(f, { manifest: forged, expectManifestDigest: forged.digest, authorization: x, expectAuthorizationDigest: authorizationDigest(x) })).toBe("state_authorization_mismatch");
      expect(f.deletes()).toBe(0);
    }
    // The broker's reproduction: A issued 08:00Z, B issued 13:00Z with expiry 19:00Z, checked at 15:00Z (A + 7 h): refused.
    {
      const A = "2026-11-26T08:00:00Z", B = "2026-11-26T13:00:00Z";
      const ctxB = rehearsalRunContext(REHEARSAL_STAGE, INPUTS, envB(SESSION_B, B), Date.parse("2026-11-26T15:00:00Z"))!;
      const revoked = { revokedAt: "2026-11-26T12:00:00Z", sessionsExpireAt: "2026-11-26T12:30:00Z" };
      const x = authorization(m, { original: { sessionId: SESSION, issuedAt: A, pin: pinOf({ issuedAt: A }), ...revoked } }, { windowEnd: "2026-11-26T19:00:00Z", expiresAt: "2026-11-26T19:00:00Z" }, ctxB);
      expect(codeOf(() => assertTeardownAuthorization(ctxB, x, authorizationDigest(x), Date.parse("2026-11-26T15:00:00Z")))).toBe("state_authorization_mismatch");
      // Within A + 6 h (before 14:00Z, expiry capped at 14:00Z) it holds.
      const y = authorization(m, { original: { sessionId: SESSION, issuedAt: A, pin: pinOf({ issuedAt: A }), ...revoked } }, { windowEnd: "2026-11-26T19:00:00Z", expiresAt: "2026-11-26T14:00:00Z" }, ctxB);
      expect(codeOf(() => assertTeardownAuthorization(ctxB, y, authorizationDigest(y), Date.parse("2026-11-26T13:30:00Z")))).toBe("ok");
      expect(teardownDeadline(A, "2026-11-26T19:00:00Z")).toBe("2026-11-26T14:00:00Z");
    }
    // After B's expiry; the wrong STS caller; a foreign generation; a remaining mapping: refused, nothing deleted.
    for (const [why, f, i, code] of [
      ["after expiry", fakeAfterDestroy(), { ...withAuth(m, auth), now: Date.parse(EXPIRES_B) }, /^state_(authorization_mismatch|lease_expired)$/],
      ["wrong caller", fakeAfterDestroy({ caller: CALLER }), withAuth(m, auth), /^state_caller_mismatch$/],
      ["foreign generation", fakeAfterDestroy({ generation: generationBody({ ...CTX, runId: "77777777-7777-4777-8777-777777777777" }, "claimed") }), withAuth(m, auth), /^state_provenance_mismatch$/],
      ["remaining mapping", fakeAfterDestroy({ remaining: U2 }), withAuth(m, auth), /^state_resources_remain$/],
    ] as const) {
      expect([why, teardown(f, i as never)]).toEqual([why, expect.stringMatching(code)]);
      expect([why, f.deletes()]).toEqual([why, 0]);
    }
    expect(assertTeardownAuthorization(CTX_B, auth, D, NOW_B)).toMatchObject({ sessionId: SESSION, issuedAt: ISSUED, pin: PIN, runId: RUN });
  });

  it("the teardown lease's policies are cleanup-only, carry B's effective expiry, and their digest changes with it", () => {
    const t = rehearsalTeardownPolicies({ accountId: ACCOUNT, roleArn: ROLE, roleDisposition: "deleted", expiresAt: EXPIRES_B, mappingUuids: [U1, U2] });
    const all = [t.inline, ...t.managed.map((x) => x.document)];
    const allows = all.flatMap((d) => d.Statement as Record<string, unknown>[]).filter((x) => x.Effect === "Allow");
    const actions = allows.flatMap((x) => ([] as string[]).concat(x.Action as string | string[]));
    // No create, update, tag, publish or invoke; the only put is the generation object (its compare-and-swap).
    expect(actions.filter((a) => !/:(Get|List|Describe)/.test(a) && /Create|Update|Tag|Publish|Invoke|PutMetric|PutRetention|SetQueue|SendMessage|PassRole|PutRole|AttachRole/.test(a))).toEqual([]);
    const puts = allows.filter((x) => actions.includes("s3:PutObject") && ([] as string[]).concat(x.Action as string[]).includes("s3:PutObject"));
    expect(puts.map((x) => x.Resource)).toEqual([`arn:aws:s3:::${BUCKET}/${generationKey(PREFIX)}`]);
    expect((t.inline.Statement as Record<string, unknown>[]).find((x) => x.Sid === "CleanupOnly")).toMatchObject({ Effect: "Deny", Action: TEARDOWN_DENIED_ACTIONS });
    // Deletes and the exact-UUID mapping reads.
    for (const a of ["sqs:DeleteQueue", "lambda:DeleteFunction", "lambda:DeleteEventSourceMapping", "cloudwatch:DeleteAlarms", "logs:DeleteLogGroup", "iam:DeleteRole", "s3:DeleteObjectVersion", "lambda:GetEventSourceMapping"]) expect([a, actions.includes(a)]).toEqual([a, true]);
    const mappings = allows.find((x) => x.Sid === "Mappings")!;
    expect(mappings.Resource).toEqual([U1, U2].map((u) => `arn:aws:lambda:${REGION}:${ACCOUNT}:event-source-mapping:${u}`));
    // A retained role is never deleted.
    const kept = rehearsalTeardownPolicies({ accountId: ACCOUNT, roleArn: ROLE, roleDisposition: "retained", expiresAt: EXPIRES_B, mappingUuids: [U1, U2] });
    for (const x of ["iam:DeleteRole", "iam:DetachRolePolicy", "iam:DeleteRolePolicy", "iam:ListAttachedRolePolicies", "iam:ListRolePolicies"]) expect([x, JSON.stringify(kept.managed).includes(x)]).toEqual([x, false]);
    // The deleted disposition's role rights are scoped to exactly the pinned role ARN.
    const role = allows.find((x) => x.Sid === "Role")!;
    expect(role).toMatchObject({ Action: ["iam:DeleteRole", "iam:ListAttachedRolePolicies", "iam:DetachRolePolicy", "iam:ListRolePolicies", "iam:DeleteRolePolicy"], Resource: ROLE });
    expect(allows.filter((x) => ([] as string[]).concat(x.Action as string[]).some((y) => y.startsWith("iam:"))).map((x) => [x.Sid, x.Resource])).toEqual([["Role", ROLE], ["RoleRead", ROLE]]);
    // The only wildcard resources are the two discovery reads IAM cannot narrow (broker decisions).
    expect(allows.filter((x) => x.Resource === "*").flatMap((x) => x.Action as string[])).toEqual(["lambda:ListEventSourceMappings", "logs:DescribeLogGroups"]);
    // The expiry is in every Allow and the inline Deny; the digest changes with it.
    expect(allows.every((x) => (x.Condition as Record<string, Record<string, string>>).DateLessThan["aws:CurrentTime"] === EXPIRES_B)).toBe(true);
    expect((t.inline.Statement as Record<string, unknown>[]).find((x) => x.Sid === "End")).toMatchObject({ Condition: { DateGreaterThanEquals: { "aws:CurrentTime": EXPIRES_B } } });
    expect(teardownPolicyDigest(CTX_B, [U1, U2], EXPIRES_B)).not.toBe(teardownPolicyDigest(CTX_B, [U1, U2], "2026-11-26T23:00:00Z"));
    // Sizes fit STS.
    expect(JSON.stringify(t.inline).length).toBeLessThanOrEqual(2048);
    for (const x of t.managed) expect(JSON.stringify(x.document).length).toBeLessThanOrEqual(6144);
    expect(codeOf(() => rehearsalTeardownPolicies({ accountId: ACCOUNT, roleArn: ROLE, roleDisposition: "deleted", expiresAt: "2026-11-27T08:00:01Z", mappingUuids: [U1, U2] }))).toBe("state_inputs_invalid");
    expect(codeOf(() => rehearsalTeardownPolicies({ accountId: ACCOUNT, roleArn: ROLE, roleDisposition: "deleted", expiresAt: EXPIRES_B, mappingUuids: [U1] }))).toBe("state_inputs_invalid");
  });
});

/** sha256 hex of a string (for a digest the generator would never produce). */
function createHashHex(text: string): string {
  return new Bun.CryptoHasher("sha256").update(text).digest("hex");
}
