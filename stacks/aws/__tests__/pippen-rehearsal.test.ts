/**
 * The P1 rehearsal in a Pippen sandbox (pippen-rehearsal.ts): validate-only state bucket, caller and
 * prefix guards, the stage-only teardown and the lease policy. Offline: every AWS call is a fake.
 * Placeholder account 111122223333 only.
 */
import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import {
  REHEARSAL_LEASE_EXPIRES_AT,
  REHEARSAL_STAGE,
  StateBucketError,
  assertStateBucketMode,
  assertStatePrefix,
  assertValidateOnlyState,
  leaseCallerArn,
  listStageStateVersions,
  rehearsalLeasePolicy,
  rehearsalResourceNames,
  rehearsalStatePrefix,
  stateBucketMode,
  teardownStageState,
  validatePippenStateBucket,
} from "../pippen-rehearsal.ts";
import { pippenStateBucket, stackStateOptions, type AwsRun } from "../state-bucket.ts";
import { validateAwsInputs, type AwsInputs } from "../inputs.ts";

const fixture = <T>(name: string): T => JSON.parse(readFileSync(fileURLToPath(new URL(`./fixtures/pippen-rehearsal/${name}`, import.meta.url)), "utf8")) as T;
const ACCOUNT = "111122223333";
const REGION = "us-west-2";
const BUCKET = pippenStateBucket(ACCOUNT, REGION);
const PREFIX = "96bulls/96bulls-aws/aws-rehearse-p1-1126/";
const SESSION = "0f1e2d3c-4b5a-4968-8776-a5b4c3d2e1f0";
const ENV = { NINETY_SIX_BULLS_PIPPEN_SESSION: SESSION };
const CALLER = `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-${SESSION}`;
const IN_SHAPE = fixture<Record<string, unknown>>("state-bucket.in-shape.json");

/** A fake aws CLI over the fixture responses; every call is recorded, and any write is a test failure. */
function fakeBucket(over: Record<string, unknown> = {}, opts: { absent?: boolean; caller?: string; errors?: Record<string, string> } = {}) {
  const calls: string[][] = [];
  const aws: AwsRun = (args) => {
    calls.push(args);
    const op = args[1]!;
    if (args[0] === "sts") return JSON.stringify({ Account: ACCOUNT, Arn: opts.caller ?? CALLER });
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
    expect(f.calls.filter((c) => c[0] === "s3api").every((c) => c[c.indexOf("--bucket") + 1] === BUCKET)).toBe(true);
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
    // A missing configuration is drift too; an access error is "unreadable", and no CLI text is carried.
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
  it("the caller must be this stage's lease session in this account", () => {
    expect(leaseCallerArn(ACCOUNT, ENV)).toBe(CALLER);
    const wrong = [
      `arn:aws:sts::${ACCOUNT}:assumed-role/agent-session/agent-00000000-0000-4000-8000-000000000000`, // another lease
      `arn:aws:sts::444455556666:assumed-role/agent-session/agent-${SESSION}`, // another account
      `arn:aws:sts::${ACCOUNT}:assumed-role/AdministratorAccess/agent-${SESSION}`, // another role
      `arn:aws:iam::${ACCOUNT}:user/someone`,
    ];
    for (const caller of wrong) {
      const f = fakeBucket({}, { caller });
      expect([caller, codeOf(() => assertValidateOnlyState(f.aws, { accountId: ACCOUNT, region: REGION, stage: REHEARSAL_STAGE, env: ENV }))]).toEqual([caller, "state_caller_mismatch"]);
      expect(f.calls.filter((c) => c[0] === "s3api")).toEqual([]);
    }
    for (const env of [{}, { NINETY_SIX_BULLS_PIPPEN_SESSION: "not-a-uuid" }, { ...ENV, PIPPEN_SANDBOX_SESSION_ID: "11111111-1111-4111-8111-111111111111" }, { ...ENV, PIPPEN_SANDBOX_ACCOUNT_ID: "444455556666" }])
      expect(codeOf(() => leaseCallerArn(ACCOUNT, env))).toBe("state_caller_mismatch");
  });

  it("the prefix is exactly this stage's: a sibling stage or a parent prefix is refused", () => {
    expect(rehearsalStatePrefix(REHEARSAL_STAGE)).toBe(PREFIX);
    expect(codeOf(() => assertStatePrefix(REHEARSAL_STAGE, PREFIX))).toBe("ok");
    for (const prefix of ["96bulls/96bulls-aws/", "96bulls/", "", "96bulls/96bulls-aws/aws-proof-sandbox/", "96bulls/96bulls-aws/aws-rehearse-p1-1126", "96bulls/96bulls-aws/aws-rehearse-p1-11/"])
      expect([prefix, codeOf(() => assertStatePrefix(REHEARSAL_STAGE, prefix))]).toEqual([prefix, "state_prefix_mismatch"]);
    // Only a declared stage has a prefix in this mode.
    for (const stage of ["aws-proof-sandbox", "aws-rehearse-p1-1127", "aws-sessions-prod"]) expect(codeOf(() => rehearsalStatePrefix(stage))).toBe("state_prefix_mismatch");
    // Alchemy keeps the stage under the same prefix: AWS.state({ prefix: "96bulls" }) adds 96bulls-aws/<stage>/.
    expect(stackStateOptions({ accountId: ACCOUNT, region: REGION, pippenSandbox: { stateBucket: "validate-only" } })).toEqual({ prefix: "96bulls", bucketName: BUCKET });
  });

  it("the inputs rule: the rehearsal must be validate-only; the proof stage and other stages keep ensure", () => {
    expect(stateBucketMode(undefined)).toBe("ensure");
    expect(stateBucketMode({ permissionsBoundaryArn: "x" })).toBe("ensure");
    expect(stateBucketMode({ permissionsBoundaryArn: "x", stateBucket: "validate-only" })).toBe("validate-only");
    expect(() => assertStateBucketMode("aws-proof-sandbox", { permissionsBoundaryArn: "x" })).not.toThrow();
    expect(() => assertStateBucketMode("aws-proof-sandbox", { permissionsBoundaryArn: "x", stateBucket: "validate-only" })).toThrow(/keeps its own state bucket path/);
    // An existing fresh-account sandbox stage (aws-proof, inputs.test.ts) is unchanged.
    expect(() => assertStateBucketMode("aws-proof", { permissionsBoundaryArn: "x" })).not.toThrow();
    expect(() => assertStateBucketMode(REHEARSAL_STAGE, undefined)).toThrow(/runs only in a Pippen sandbox/);
    expect(() => assertStateBucketMode(REHEARSAL_STAGE, { permissionsBoundaryArn: "x" })).toThrow(/needs pippenSandbox.stateBucket "validate-only"/);
    expect(() => assertStateBucketMode(REHEARSAL_STAGE, { permissionsBoundaryArn: "x", stateBucket: "validate-only" })).not.toThrow();
    expect(() => assertStateBucketMode(REHEARSAL_STAGE, { permissionsBoundaryArn: "x", stateBucket: "ensure" })).toThrow(/may only be "validate-only"/);
    expect(() => assertStateBucketMode("aws-other-sandbox", { permissionsBoundaryArn: "x", stateBucket: "validate-only" })).toThrow(/no declared state prefix/);
    // A stage without pippenSandbox is untouched.
    expect(() => assertStateBucketMode("aws-sessions-dev", undefined)).not.toThrow();
    const inputs = { accountId: ACCOUNT, region: REGION, budgetUsd: 50, budgetEmail: "budget@example.invalid", pippenSandbox: { permissionsBoundaryArn: `arn:aws:iam::${ACCOUNT}:policy/boundary` } } as unknown as AwsInputs;
    expect(() => validateAwsInputs(REHEARSAL_STAGE, inputs)).toThrow(/needs pippenSandbox.stateBucket "validate-only"/);
    expect(() => validateAwsInputs(REHEARSAL_STAGE, { ...inputs, pippenSandbox: { ...inputs.pippenSandbox!, stateBucket: "validate-only" } })).not.toThrow();
  });
});

describe("teardown: only this stage's state keys", () => {
  /** A fake list-object-versions over pages, keyed by the markers each call passes. */
  const fakeListing = (pages: unknown[], opts: { deleteErrors?: boolean } = {}) => {
    const calls: string[][] = [];
    let page = 0;
    const aws: AwsRun = (args) => {
      calls.push(args);
      if (args[1] === "list-object-versions") return JSON.stringify(pages[page++] ?? { IsTruncated: false });
      if (args[1] === "delete-objects") return JSON.stringify(opts.deleteErrors ? { Errors: [{ Code: "AccessDenied" }] } : {});
      throw new Error(`unexpected ${args[1]}`);
    };
    return { aws, calls };
  };

  it("lists every version and delete marker under the exact prefix across pages, and deletes them by key and version only", () => {
    const f = fakeListing(fixture<unknown[]>("teardown.pages.json"));
    const dry = teardownStageState(f.aws, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE });
    expect(dry).toEqual({ listed: 5, deleted: 0 });
    expect(f.calls.every((c) => c[1] === "list-object-versions")).toBe(true);
    expect(f.calls.every((c) => c[c.indexOf("--prefix") + 1] === PREFIX)).toBe(true);
    // The second page continues from both markers.
    expect(f.calls[1]!.slice(f.calls[1]!.indexOf("--key-marker"))).toEqual(["--key-marker", `${PREFIX}b.json`, "--version-id-marker", "v-b1"]);

    const run = fakeListing(fixture<unknown[]>("teardown.pages.json"));
    expect(teardownStageState(run.aws, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE, execute: true })).toEqual({ listed: 5, deleted: 5 });
    const del = run.calls.filter((c) => c[1] === "delete-objects");
    expect(del.length).toBe(1);
    const objects = JSON.parse(del[0]![del[0]!.indexOf("--delete") + 1]).Objects as { Key: string; VersionId: string }[];
    expect(objects.map((o) => `${o.Key}@${o.VersionId}`)).toEqual([`${PREFIX}a.json@v-a1`, `${PREFIX}a.json@v-a2`, `${PREFIX}b.json@v-b1`, `${PREFIX}c/d.json@v-c1`, `${PREFIX}b.json@dm-b2`]);
    // Never the bucket itself.
    expect(run.calls.some((c) => /delete-bucket|put-bucket|create-bucket/.test(c[1] ?? ""))).toBe(false);
  });

  it("refuses before deleting anything when any listed key is outside the prefix (sibling stage, longer sibling, parent, the prefix itself, a later page)", () => {
    for (const [name, pages] of Object.entries(fixture<Record<string, unknown[]>>("teardown.outside-prefix.json"))) {
      const f = fakeListing(pages);
      expect([name, codeOf(() => teardownStageState(f.aws, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE, execute: true }))]).toEqual([name, "state_teardown_outside_prefix"]);
      expect([name, f.calls.filter((c) => c[1] === "delete-objects").length]).toEqual([name, 0]);
    }
    // The listing itself refuses (not only the delete batches).
    for (const [name, pages] of Object.entries(fixture<Record<string, unknown[]>>("teardown.outside-prefix.json")))
      expect([name, codeOf(() => listStageStateVersions(fakeListing(pages).aws, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE }))]).toEqual([name, "state_teardown_outside_prefix"]);
  });

  it("refuses a truncated page without a marker, a malformed entry, a failed list or a partial delete", () => {
    expect(codeOf(() => listStageStateVersions(fakeListing([{ IsTruncated: true, Versions: [] }]).aws, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE }))).toBe("state_teardown_failed");
    expect(codeOf(() => listStageStateVersions(fakeListing([{ IsTruncated: false, Versions: [{ Key: `${PREFIX}a` }] }]).aws, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE }))).toBe("state_teardown_failed");
    const failing: AwsRun = () => {
      throw new Error("An error occurred (AccessDenied) secret-ish-canary");
    };
    let message = "";
    try {
      listStageStateVersions(failing, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE });
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toMatch(/state_teardown_failed/);
    expect(message).not.toContain("secret-ish-canary");
    expect(codeOf(() => teardownStageState(fakeListing(fixture<unknown[]>("teardown.pages.json"), { deleteErrors: true }).aws, { bucket: BUCKET, region: REGION, stage: REHEARSAL_STAGE, execute: true }))).toBe("state_teardown_failed");
    // Another stage cannot be torn down by this module at all.
    expect(codeOf(() => teardownStageState(fakeListing([]).aws, { bucket: BUCKET, region: REGION, stage: "aws-proof-sandbox", execute: true }))).toBe("state_prefix_mismatch");
  });
});

describe("the lease policy", () => {
  const policy = rehearsalLeasePolicy({ accountId: ACCOUNT });
  const statement = (sid: string) => policy.Statement.find((s) => s.Sid === sid)! as Record<string, unknown>;

  it("state objects and listing only under this prefix; the bucket's settings readable, never writable", () => {
    expect(statement("StateObjectsOnlyThisPrefix")).toMatchObject({ Effect: "Allow", Resource: `arn:aws:s3:::${BUCKET}/${PREFIX}*` });
    expect(statement("StateObjectsOnlyThisPrefix").Action).toEqual(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"]);
    expect(statement("StateListOnlyThisPrefix")).toMatchObject({ Resource: `arn:aws:s3:::${BUCKET}`, Condition: { StringLike: { "s3:prefix": [`${PREFIX}*`] } } });
    expect(statement("NeverListOtherPrefixes")).toMatchObject({ Effect: "Deny", Condition: { StringNotLike: { "s3:prefix": [`${PREFIX}*`] } } });
    const deny = statement("NeverChangeTheSharedBucket");
    expect(deny.Effect).toBe("Deny");
    for (const a of ["s3:PutBucket*", "s3:DeleteBucket*", "s3:PutEncryptionConfiguration", "s3:PutBucketVersioning", "s3:PutBucketPublicAccessBlock", "s3:PutBucketOwnershipControls", "s3:PutBucketTagging", "s3:CreateBucket"])
      expect(deny.Action as string[]).toContain(a);
    expect(statement("NeverOtherStateObjects")).toMatchObject({ Effect: "Deny", NotResource: [`arn:aws:s3:::${BUCKET}/${PREFIX}*`, "arn:aws:s3:::96bulls-aws-rehearse-p1-1126-*/*", "arn:aws:s3:::pippen-sbx-96bulls-aws-rehearse-p1-1126-*/*"] });
    // No Allow grants a bucket-level write.
    for (const s of policy.Statement.filter((x) => x.Effect === "Allow")) for (const a of ([] as string[]).concat((s as { Action?: string | string[] }).Action ?? [])) expect([s.Sid, /^s3:(Put|Delete)Bucket|^s3:CreateBucket|^s3:PutEncryption/.test(a)]).toEqual([s.Sid, false]);
  });

  it("v2 resources only by this stage's names, in us-west-2 only, nothing after the stated expiry", () => {
    const allows = policy.Statement.filter((s) => s.Effect === "Allow" && s.Sid.startsWith("V2") && s.Sid !== "V2ReadOnlyLists");
    for (const s of allows) {
      const resources = ([] as string[]).concat((s as { Resource: string | string[] }).Resource);
      const scoped = resources.every((r) => r.includes("96bulls-aws-rehearse-p1-1126-") || r.includes("96bulls/aws-rehearse-p1-1126/") || r === "*");
      expect([s.Sid, scoped]).toEqual([s.Sid, true]);
      expect([s.Sid, resources.every((r) => r === "*" || r.includes(`:${REGION}:${ACCOUNT}:`))]).toEqual([s.Sid, true]);
      expect((s as { Condition: Record<string, unknown> }).Condition).toMatchObject({ DateLessThan: { "aws:CurrentTime": REHEARSAL_LEASE_EXPIRES_AT }, StringEquals: { "aws:RequestedRegion": REGION } });
    }
    expect(statement("V2EventSourceMappingsOnTheFunction").Condition).toMatchObject({ ArnLike: { "lambda:FunctionArn": `arn:aws:lambda:${REGION}:${ACCOUNT}:function:96bulls-aws-rehearse-p1-1126-*` } });
    expect(statement("OnlyThisRegion")).toMatchObject({ Effect: "Deny", Condition: { StringNotEquals: { "aws:RequestedRegion": REGION } } });
    expect(statement("NothingAfterExpiry")).toMatchObject({ Effect: "Deny", Action: "*", Condition: { DateGreaterThan: { "aws:CurrentTime": REHEARSAL_LEASE_EXPIRES_AT } } });
    expect(() => rehearsalLeasePolicy({ accountId: ACCOUNT, region: "us-east-1" })).toThrow();
    expect(() => rehearsalLeasePolicy({ accountId: ACCOUNT, stage: "aws-proof-sandbox" })).toThrow();
    // The resource names the policy scopes to are the stage's.
    const names = rehearsalResourceNames();
    for (const n of [names.function, ...names.queues, ...names.rules, ...names.alarms]) expect(n.startsWith("96bulls-aws-rehearse-p1-1126-")).toBe(true);
    expect(names.secret).toBe("96bulls/aws-rehearse-p1-1126/session-pipeline");
  });

  it("the checked-in policy document is the generator's output with placeholders", () => {
    const doc = readFileSync(fileURLToPath(new URL("../../../docs/handoffs/pippen-rehearsal-p1-1126.lease-policy.json", import.meta.url)), "utf8");
    expect(doc).toBe(`${JSON.stringify(rehearsalLeasePolicy({ accountId: "000000000000" }), null, 2).replaceAll("000000000000", "<account>")}\n`);
    expect(doc).not.toMatch(/\d{12}/);
  });
});
