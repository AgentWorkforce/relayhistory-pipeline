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
const ENV = { NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: ACCOUNT, [ISSUED_AT_ENV]: ISSUED, [ISSUER_PIN_ENV]: PIN };
const CALLER = `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-${SESSION}`;
const IN_SHAPE = fixture<Record<string, unknown>>("state-bucket.in-shape.json");
const INPUTS = { accountId: ACCOUNT, region: REGION, pippenSandbox: { permissionsBoundaryArn: BOUNDARY, stateBucket: "validate-only", executionRole: { arn: ROLE, disposition: "deleted" } } };
const CTX: RehearsalContext = rehearsalRunContext(REHEARSAL_STAGE, INPUTS, ENV, NOW)!;

/** A fake aws CLI over the fixture responses; every call is recorded. */
function fakeBucket(over: Record<string, unknown> = {}, opts: { absent?: boolean; caller?: string; account?: string; errors?: Record<string, string>; stateEmpty?: boolean; inUse?: string[] } = {}) {
  const calls: string[][] = [];
  const aws: AwsRun = (args) => {
    calls.push(args);
    const op = args[1]!;
    if (args[0] === "sts") return JSON.stringify({ Account: opts.account ?? ACCOUNT, Arn: opts.caller ?? CALLER });
    if (op === "list-objects-v2") return JSON.stringify({ KeyCount: opts.stateEmpty ? 0 : 1 });
    const line = args.join(" ");
    const used = (opts.inUse ?? []).some((n) => line.includes(n));
    if (op === "get-function-configuration" || op === "get-queue-url") {
      if (used) return "{}";
      throw new Error(args[0] === "sqs" ? "An error occurred (AWS.SimpleQueueService.NonExistentQueue)" : "An error occurred (ResourceNotFoundException)");
    }
    if (op === "describe-alarms") return JSON.stringify({ MetricAlarms: used ? [{ AlarmName: "x" }] : [] });
    if (op === "describe-log-groups") return JSON.stringify({ logGroups: used ? [{ logGroupName: args[args.indexOf("--log-group-name-prefix") + 1] }] : [] });
    if (opts.absent) throw new Error(`An error occurred (NoSuchBucket) when calling the ${op} operation: The specified bucket does not exist`);
    if (opts.errors?.[op]) throw new Error(opts.errors[op]);
    const body = { ...IN_SHAPE, ...over }[op];
    if (body === undefined) throw new Error(`unexpected call ${op}`);
    return JSON.stringify(body);
  };
  return { aws, calls, writes: () => calls.filter((c) => c[0] === "s3api" && !/^(get-|list-|head-)/.test(c[1] ?? "")).map((c) => c[1]) };
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

  it("stage A's entry (prepareStateBackend, used by the capture and the deploy's state layer): an empty stage state requires every name unused", () => {
    let ensured = 0;
    // Empty state (stage A not applied), a name in use: refused before anything else is done.
    for (const used of ["96bulls-aws-rehearse-p1-1126-session-pipeline", "96bulls-aws-rehearse-p1-1126-session-search", "96bulls-aws-rehearse-p1-1126-session-search-backlog-age", "/aws/lambda/96bulls-aws-rehearse-p1-1126-session-pipeline"]) {
      const f = fakeBucket({}, { stateEmpty: true, inUse: [used] });
      expect([used, codeOf(() => prepareStateBackend(f.aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV, now: NOW }, () => void ensured++))]).toEqual([used, "state_resources_remain"]);
      expect(f.writes()).toEqual([]);
    }
    // Empty state, every name unused: accepted. Non-empty state (this stage's own resources recorded): no names check.
    expect(prepareStateBackend(fakeBucket({}, { stateEmpty: true }).aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV, now: NOW }, () => void ensured++)).toBe("validate-only");
    const later = fakeBucket({}, { inUse: ["96bulls-aws-rehearse-p1-1126-session-pipeline"] });
    expect(prepareStateBackend(later.aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV, now: NOW }, () => void ensured++)).toBe("validate-only");
    expect(later.calls.some((c) => c[1] === "get-function-configuration")).toBe(false);
    expect(ensured).toBe(0);
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

describe("teardown: only this stage's state keys, with evidence", () => {
  const fakeListing = (pages: unknown[], opts: { deleteErrors?: boolean; after?: unknown[] } = {}) => {
    const calls: string[][] = [];
    let page = 0;
    let deleted = false;
    let afterPage = 0;
    const aws: AwsRun = (args) => {
      calls.push(args);
      if (args[1] === "list-object-versions") {
        if (deleted && opts.after) return JSON.stringify(opts.after[afterPage++] ?? { IsTruncated: false });
        const p = pages[page++] ?? { IsTruncated: false };
        return typeof p === "string" ? p : JSON.stringify(p);
      }
      if (args[1] === "delete-objects") {
        deleted = true;
        return JSON.stringify(opts.deleteErrors ? { Errors: [{ Code: "AccessDenied" }] } : {});
      }
      throw new Error(`unexpected ${args[1]}`);
    };
    return { aws, calls };
  };
  const target = { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE };

  it("lists every version and delete marker under the exact prefix across pages, deletes them by key and version, and re-lists to prove the prefix empty", () => {
    const dry = fakeListing(fixture<unknown[]>("teardown.pages.json"));
    expect(teardownStageState(dry.aws, target)).toEqual({ listed: 5, deleted: 0, remaining: 5 });
    expect(dry.calls.every((c) => c[1] === "list-object-versions" && c[c.indexOf("--prefix") + 1] === PREFIX)).toBe(true);
    expect(dry.calls[1]!.slice(dry.calls[1]!.indexOf("--key-marker"))).toEqual(["--key-marker", `${PREFIX}b.json`, "--version-id-marker", "v-b1"]);

    const run = fakeListing(fixture<unknown[]>("teardown.pages.json"));
    expect(teardownStageState(run.aws, { ...target, execute: true })).toEqual({ listed: 5, deleted: 5, remaining: 0 });
    const ops = run.calls.map((c) => c[1]);
    // list (2 pages), delete, then the independent re-list.
    expect(ops).toEqual(["list-object-versions", "list-object-versions", "delete-objects", "list-object-versions"]);
    const objects = JSON.parse(run.calls[2]![run.calls[2]!.indexOf("--delete") + 1]).Objects as { Key: string; VersionId: string }[];
    expect(objects.map((o) => `${o.Key}@${o.VersionId}`)).toEqual([`${PREFIX}a.json@v-a1`, `${PREFIX}a.json@v-a2`, `${PREFIX}b.json@v-b1`, `${PREFIX}c/d.json@v-c1`, `${PREFIX}b.json@dm-b2`]);
    expect(run.calls.some((c) => /delete-bucket|put-bucket|create-bucket/.test(c[1] ?? ""))).toBe(false);
  });

  it("refuses success when the re-list after deleting still shows a version or a delete marker", () => {
    for (const left of [{ IsTruncated: false, Versions: [{ Key: `${PREFIX}a.json`, VersionId: "v-a3" }] }, { IsTruncated: false, DeleteMarkers: [{ Key: `${PREFIX}b.json`, VersionId: "dm-new" }] }]) {
      const f = fakeListing(fixture<unknown[]>("teardown.pages.json"), { after: [left] });
      expect(codeOf(() => teardownStageState(f.aws, { ...target, execute: true }))).toBe("state_teardown_not_empty");
    }
  });

  it("refuses before deleting anything when any listed key is outside the prefix", () => {
    for (const [name, pages] of Object.entries(fixture<Record<string, unknown[]>>("teardown.outside-prefix.json"))) {
      const f = fakeListing(pages);
      expect([name, codeOf(() => teardownStageState(f.aws, { ...target, execute: true }))]).toEqual([name, "state_teardown_outside_prefix"]);
      expect([name, f.calls.filter((c) => c[1] === "delete-objects").length]).toEqual([name, 0]);
      expect([name, codeOf(() => listStageStateVersions(fakeListing(pages).aws, target))]).toEqual([name, "state_teardown_outside_prefix"]);
    }
  });

  it("validates every page strictly: blank output, a missing or non-boolean IsTruncated, missing markers, non-arrays, unknown fields, another prefix", () => {
    for (const [name, pages] of Object.entries(fixture<Record<string, unknown[]>>("teardown.malformed.json"))) {
      const f = fakeListing(pages);
      expect([name, codeOf(() => teardownStageState(f.aws, { ...target, execute: true }))]).toEqual([name, "state_teardown_failed"]);
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
    expect(codeOf(() => teardownStageState(fakeListing(fixture<unknown[]>("teardown.pages.json"), { deleteErrors: true }).aws, { ...target, execute: true }))).toBe("state_teardown_failed");
    expect(codeOf(() => teardownStageState(fakeListing([]).aws, { ...target, stage: "aws-proof-sandbox", execute: true }))).toBe("state_prefix_mismatch");
  });

  const names = rehearsalResourceNames();
  const ROLE_NAME = "96bulls-aws-rehearse-p1-1126-session-pipeline";
  const U1 = "44444444-4444-4444-8444-444444444444";
  const U2 = "55555555-5555-4555-8555-555555555555";
  const RUN = "66666666-6666-4666-8666-666666666666";
  const FN = `arn:aws:lambda:${REGION}:${ACCOUNT}:function:${names.function}`;
  const qArn = (n: string) => `arn:aws:sqs:${REGION}:${ACCOUNT}:${n}`;
  /** A fake account where `present` ids still exist (by any substring of the call), for the manifest and its readback. */
  const fakeAccount = (
    present: Set<string>,
    opts: { alarms?: string[]; group?: boolean; role?: { Arn: string; Path: string; PermissionsBoundary?: { PermissionsBoundaryArn: string } }; fnRole?: string; mappings?: { UUID: string; EventSourceArn: string }[]; denied?: string } = {},
  ) => {
    const calls: string[][] = [];
    const aws: AwsRun = (args) => {
      calls.push(args);
      const line = args.join(" ");
      if (opts.denied && line.includes(opts.denied)) throw new Error("An error occurred (AccessDenied) canary-77");
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
          return JSON.stringify({ MetricAlarms: (opts.alarms ?? []).map((AlarmName) => ({ AlarmName })), CompositeAlarms: [] });
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
    let message = "";
    try {
      assertManifestGone(fakeAccount(new Set(), { denied: "get-event-source-mapping" }).aws, m, CTX, RUN);
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toMatch(/state_teardown_failed/);
    expect(message).not.toContain("canary-77");
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

  it("the teardown script checks the run context (inputs, lease, pin), the caller and the bucket, then every manifest id, before any delete", () => {
    const script = src("scripts/rehearsal-state-teardown.ts");
    const order = [
      "rehearsalRunContext(REHEARSAL_STAGE, inputs, process.env)",
      "assertLeaseCaller(aws, ctx.accountId, process.env);",
      "validatePippenStateBucket(aws, ctx);",
      "assertManifestComplete(manifest, ctx, runId);",
      "assertManifestGone(aws, manifest, ctx, runId);",
      "teardownStageState(aws,",
    ].map((s) => script.indexOf(s));
    // The manifest is written once (never overwritten) before any destroy.
    expect(script).toContain('if (existsSync(manifestPath)) throw new StateBucketError("state_manifest_invalid");');
    expect(script).toContain('flag: "wx"');
    expect(order.every((n) => n > 0)).toBe(true);
    expect([...order].sort((a, b) => a - b)).toEqual(order);
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
    for (const s of act.filter((x) => !["Reads", "NamedReads", "RoleRead"].includes(x.Sid as string)))
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
