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
  assertRehearsalResourcesGone,
  assertStateBucketMode,
  assertStatePrefix,
  assertValidateOnlyState,
  leaseCallerArn,
  listStageStateVersions,
  prepareStateBackend,
  recordedMappingUuids,
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

const fixture = <T>(name: string): T => JSON.parse(readFileSync(fileURLToPath(new URL(`./fixtures/pippen-rehearsal/${name}`, import.meta.url)), "utf8")) as T;
const src = (path: string) => readFileSync(fileURLToPath(new URL(`../../../${path}`, import.meta.url)), "utf8");
const ACCOUNT = "111122223333";
const REGION = "us-west-2";
const BUCKET = pippenStateBucket(ACCOUNT, REGION);
const PREFIX = "96bulls/96bulls-aws/aws-rehearse-p1-1126/";
const SESSION = "0f1e2d3c-4b5a-4968-8776-a5b4c3d2e1f0";
/** What `pippen sandbox exec` attests, plus the operator's lease id. */
const ENV = { NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION, PIPPEN_SANDBOX_SESSION_ID: SESSION, PIPPEN_SANDBOX_ACCOUNT_ID: ACCOUNT };
const CALLER = `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-${SESSION}`;
const IN_SHAPE = fixture<Record<string, unknown>>("state-bucket.in-shape.json");
const INPUTS = { accountId: ACCOUNT, region: REGION, pippenSandbox: { permissionsBoundaryArn: `arn:aws:iam::${ACCOUNT}:policy/boundary`, stateBucket: "validate-only" } };

/** A fake aws CLI over the fixture responses; every call is recorded. */
function fakeBucket(over: Record<string, unknown> = {}, opts: { absent?: boolean; caller?: string; account?: string; errors?: Record<string, string> } = {}) {
  const calls: string[][] = [];
  const aws: AwsRun = (args) => {
    calls.push(args);
    const op = args[1]!;
    if (args[0] === "sts") return JSON.stringify({ Account: opts.account ?? ACCOUNT, Arn: opts.caller ?? CALLER });
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
      expect(codeOf(() => prepareStateBackend(f.aws, { stage: REHEARSAL_STAGE, inputs, env: ENV }, () => void ensured++))).toBe("state_inputs_invalid");
      expect([inputs, f.calls.length, ensured]).toEqual([inputs, 0, 0]);
    }
    const f = fakeBucket();
    let ensured = 0;
    expect(prepareStateBackend(f.aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV }, () => void ensured++)).toBe("validate-only");
    expect(ensured).toBe(0);
    expect(f.writes()).toEqual([]);
    // A drifted bucket: refused, still no ensure and no write.
    const drift = fakeBucket({ "get-bucket-versioning": { Status: "Suspended" } });
    expect(codeOf(() => prepareStateBackend(drift.aws, { stage: REHEARSAL_STAGE, inputs: INPUTS, env: ENV }, () => void ensured++))).toBe("state_bucket_versioning_drift");
    expect([ensured, drift.writes()]).toEqual([0, []]);
    // The proof stage still ensures, exactly once; a stage without pippenSandbox makes no call.
    const proof = fakeBucket();
    prepareStateBackend(proof.aws, { stage: "aws-proof-sandbox", inputs: { accountId: ACCOUNT, region: REGION, pippenSandbox: { permissionsBoundaryArn: "x" } }, env: {} }, () => void ensured++);
    expect([ensured, proof.calls.length]).toEqual([1, 0]);
    const plain = fakeBucket();
    expect(prepareStateBackend(plain.aws, { stage: "aws-sessions-prod", inputs: { accountId: ACCOUNT, region: "us-east-1" }, env: {} }, () => void ensured++)).toBe("default");
    expect([ensured, plain.calls.length]).toEqual([1, 0]);
  });

  it("both entry points decide the backend from validated inputs before any selection, and ensure only inside prepareStateBackend", () => {
    const cap = src("src/cli/aws-provenance-io.ts");
    const decide = cap.indexOf("stateBackendFor(stage, inputs);");
    expect(decide).toBeGreaterThan(0);
    expect(decide).toBeLessThan(cap.indexOf('awsJson(root, ["sts", "get-caller-identity"], env)'));
    expect(cap).toContain('if (pippen) prepareStateBackend((args) => run("aws", args, root, env), { stage, inputs, env }, ensurePippenStateBucket);');
    expect(cap.match(/ensurePippenStateBucket\(/g) ?? []).toEqual([]);
    const stack = src("stacks/aws/alchemy.aws.run.ts");
    const guard = stack.indexOf("if (argvStage) stateBackendFor(argvStage, stateTarget);");
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
    expect(() => validateAwsInputs(REHEARSAL_STAGE, { ...inputs, pippenSandbox: { ...inputs.pippenSandbox!, stateBucket: "validate-only" } })).not.toThrow();
  });
});

describe("validate-only: the shared state bucket is read, never created or changed", () => {
  it("an in-shape bucket passes with reads only (no head-bucket, no create, no put)", () => {
    const f = fakeBucket();
    expect(assertValidateOnlyState(f.aws, { accountId: ACCOUNT, region: REGION, stage: REHEARSAL_STAGE, env: ENV })).toEqual({ bucket: BUCKET, prefix: PREFIX });
    expect(f.writes()).toEqual([]);
    expect(f.calls.map((c) => `${c[0]} ${c[1]}`)).toEqual([
      "sts get-caller-identity",
      "s3api get-bucket-versioning",
      "s3api get-bucket-encryption",
      "s3api get-public-access-block",
      "s3api get-bucket-ownership-controls",
      "s3api get-bucket-tagging",
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
      expect([caller, codeOf(() => assertValidateOnlyState(f.aws, { accountId: ACCOUNT, region: REGION, stage: REHEARSAL_STAGE, env: ENV }))]).toEqual([caller, "state_caller_mismatch"]);
      expect(f.calls.filter((c) => c[0] === "s3api")).toEqual([]);
    }
    // STS reporting another account for the same ARN text is refused too.
    expect(codeOf(() => assertValidateOnlyState(fakeBucket({}, { account: "444455556666" }).aws, { accountId: ACCOUNT, region: REGION, stage: REHEARSAL_STAGE, env: ENV }))).toBe("state_caller_mismatch");
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

  it("the physical-resource readback: every recorded id must read back as gone before the state is discarded", () => {
    const names = rehearsalResourceNames();
    const U = "44444444-4444-4444-8444-444444444444";
    const fakeGone = (remaining: string[] = [], other: string[] = [], alarms: unknown[] = [], groups: unknown[] = []) => {
      const calls: string[][] = [];
      const aws: AwsRun = (args) => {
        calls.push(args);
        const key = `${args[0]} ${args[1]} ${args[3]}`;
        if (other.some((o) => key.includes(o))) throw new Error("An error occurred (AccessDenied) canary-77");
        if (args[1] === "describe-alarms") return JSON.stringify({ MetricAlarms: alarms, CompositeAlarms: [] });
        if (args[1] === "describe-log-groups") return JSON.stringify({ logGroups: groups });
        if (remaining.some((r) => key.includes(r))) return "{}";
        throw new Error(args[0] === "sqs" ? "An error occurred (AWS.SimpleQueueService.NonExistentQueue)" : "An error occurred (ResourceNotFoundException)");
      };
      return { aws, calls };
    };
    const ok = fakeGone();
    expect(codeOf(() => assertRehearsalResourcesGone(ok.aws, { region: REGION, stage: REHEARSAL_STAGE, mappingUuids: [U] }))).toBe("ok");
    expect(ok.calls.map((c) => `${c[0]} ${c[1]}`)).toEqual([
      "lambda get-function-configuration",
      ...names.queues.map(() => "sqs get-queue-url"),
      "lambda get-event-source-mapping",
      "cloudwatch describe-alarms",
      "logs describe-log-groups",
    ]);
    for (const left of [names.function, names.queues[2]!, U])
      expect([left, codeOf(() => assertRehearsalResourcesGone(fakeGone([left]).aws, { region: REGION, stage: REHEARSAL_STAGE, mappingUuids: [U] }))]).toEqual([left, "state_resources_remain"]);
    expect(codeOf(() => assertRehearsalResourcesGone(fakeGone([], [], [{ AlarmName: names.alarms[0] }]).aws, { region: REGION, stage: REHEARSAL_STAGE, mappingUuids: [] }))).toBe("state_resources_remain");
    expect(codeOf(() => assertRehearsalResourcesGone(fakeGone([], [], [], [{ logGroupName: names.logGroup }]).aws, { region: REGION, stage: REHEARSAL_STAGE, mappingUuids: [] }))).toBe("state_resources_remain");
    // A different log group under the same prefix is not this one.
    expect(codeOf(() => assertRehearsalResourcesGone(fakeGone([], [], [], [{ logGroupName: `${names.logGroup}-other` }]).aws, { region: REGION, stage: REHEARSAL_STAGE, mappingUuids: [] }))).toBe("ok");
    // An access error is not "gone".
    let message = "";
    try {
      assertRehearsalResourcesGone(fakeGone([], [names.function]).aws, { region: REGION, stage: REHEARSAL_STAGE, mappingUuids: [] });
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toMatch(/state_teardown_failed/);
    expect(message).not.toContain("canary-77");
    expect(codeOf(() => assertRehearsalResourcesGone(ok.aws, { region: REGION, stage: REHEARSAL_STAGE, mappingUuids: ["not-a-uuid"] }))).toBe("state_teardown_failed");
  });

  it("the recorded mapping UUIDs come from this stage's state objects only", () => {
    const U = "44444444-4444-4444-8444-444444444444";
    const calls: string[][] = [];
    const aws: AwsRun = (args) => {
      calls.push(args);
      if (args[2]!.endsWith("session-events-mapping.json")) return JSON.stringify({ status: "created", attr: { uuid: U } });
      throw new Error("An error occurred (404) NoSuchKey");
    };
    expect(recordedMappingUuids(aws, target)).toEqual([U]);
    expect(calls.map((c) => c[2])).toEqual([`s3://${BUCKET}/${PREFIX}session-events-mapping.json`, `s3://${BUCKET}/${PREFIX}session-search-mapping.json`]);
    expect(codeOf(() => recordedMappingUuids(() => JSON.stringify({ attr: { uuid: "x" } }), target))).toBe("state_teardown_failed");
  });

  it("the teardown script checks inputs, the attested caller and the bucket, then the resources, before any delete", () => {
    const script = src("scripts/rehearsal-state-teardown.ts");
    const order = ["stateBackendFor(REHEARSAL_STAGE, inputs);", "assertLeaseCaller(aws, accountId, process.env);", "validatePippenStateBucket(aws, { accountId, region });", "assertRehearsalResourcesGone(aws,", "teardownStageState(aws,"].map((s) => script.indexOf(s));
    expect(order.every((n) => n > 0)).toBe(true);
    expect([...order].sort((a, b) => a - b)).toEqual(order);
  });
});

describe("the lease session policies", () => {
  const lease = rehearsalLeasePolicies({ accountId: ACCOUNT });
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
    for (const s of create) expect([s.Sid, (s.Condition as Record<string, Record<string, string>>).StringEquals]).toEqual([s.Sid, { [`aws:RequestTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value }]);
    const act = lease.managed[2]!.document.Statement as Record<string, unknown>[];
    for (const s of act.filter((x) => x.Sid !== "Reads" && x.Sid !== "Mappings"))
      expect([s.Sid, (s.Condition as Record<string, Record<string, string>>).StringEquals]).toEqual([s.Sid, { [`aws:ResourceTag/${REHEARSAL_TAG.key}`]: REHEARSAL_TAG.value }]);
    expect(act.find((s) => s.Sid === "Mappings")).toMatchObject({ Condition: { ArnLike: { "lambda:FunctionArn": `arn:aws:lambda:${REGION}:${ACCOUNT}:function:96bulls-aws-rehearse-p1-1126-*` } } });
    expect((lease.inline.Statement as Record<string, unknown>[]).find((s) => s.Sid === "KeepTag")).toMatchObject({ Effect: "Deny", Condition: { "ForAnyValue:StringEquals": { "aws:TagKeys": ["rehearsal"] } } });
  });

  it("actions: what the rehearsal and its guards need, no EventBridge and no Secrets Manager", () => {
    const every = statements.flatMap(actions);
    for (const a of ["lambda:ListAliases", "lambda:ListVersionsByFunction", "lambda:PublishVersion", "lambda:GetFunctionConfiguration", "lambda:ListEventSourceMappings", "cloudwatch:GetMetricStatistics", "cloudwatch:GetMetricData", "logs:StartQuery", "logs:GetQueryResults", "logs:FilterLogEvents", "sqs:GetQueueUrl", "cloudwatch:DescribeAlarms", "logs:DescribeLogGroups", "lambda:GetEventSourceMapping"])
      expect([a, every.includes(a)]).toEqual([a, true]);
    expect(every.filter((a) => /^(events|secretsmanager|iam):/.test(a))).toEqual([]);
    // Every scoped resource is this stage's, in us-west-2 and the account.
    for (const s of statements.filter((x) => x.Effect === "Allow" && x.Resource !== "*"))
      for (const r of ([] as string[]).concat(s.Resource as string | string[]))
        expect([s.Sid, r.includes("96bulls-aws-rehearse-p1-1126-") || r.includes(PREFIX) || r === `arn:aws:s3:::${BUCKET}`]).toEqual([s.Sid, true]);
    // The policies' inventory: five alarms (the three the pipeline always has plus the two search alarms), matching the doc.
    const names = rehearsalResourceNames();
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
    const issued = rehearsalLeasePolicies({ accountId: ACCOUNT, issuedAt: "2026-11-26T18:00:00Z" });
    expect(issued.expiresAt).toBe("2026-11-27T00:00:00Z");
    expect(JSON.stringify(issued)).not.toContain("2026-11-27T08:00:00Z");
    expect(codeOf(() => rehearsalLeasePolicies({ accountId: ACCOUNT, expiresAt: "2026-11-28T00:00:00Z" }))).toBe("state_lease_expired");
    expect(codeOf(() => rehearsalLeasePolicies({ accountId: ACCOUNT, region: "us-east-1" }))).toBe("state_inputs_invalid");
    expect(codeOf(() => rehearsalLeasePolicies({ accountId: ACCOUNT, stage: "aws-proof-sandbox" }))).toBe("state_prefix_mismatch");
  });

  it("the checked-in document is the generator's template output (no issuance: the absolute cutoff), with placeholders", () => {
    const doc = src("docs/handoffs/pippen-rehearsal-p1-1126.lease-policy.json");
    expect(doc).toBe(`${JSON.stringify(rehearsalLeasePolicies({ accountId: "000000000000" }), null, 2).replaceAll("000000000000", "<account>")}\n`);
    expect(doc).not.toMatch(/\d{12}/);
    expect(doc).not.toContain("2026-11-28");
  });
});
