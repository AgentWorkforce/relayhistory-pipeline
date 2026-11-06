import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { buildRhcAuthStorePlan, canonicalPolicyJson, E_CANDIDATE_ROOT, RhcAuthStorePolicyError } from "../rhc-auth-store-policy.ts";
import { runRhcAuthStorePlan } from "../../../scripts/ops/rhc-auth-store-plan.ts";
const account = "000000000000", region = "us-east-1";
const input = () => ({ version: 1, accountId: account, region, bucketArn: "arn:aws:s3:::example-c-auth-only", keyRoot: "auth/private-v1", kmsKeyArn: `arn:aws:kms:${region}:${account}:key/11111111-1111-1111-1111-111111111111`, runtimePrincipalArn: `arn:aws:iam::${account}:user/c-auth`, ownerBootstrapPrincipalArn: `arn:aws:iam::${account}:role/auth-bootstrap`, keyAdminPrincipalArn: `arn:aws:iam::${account}:role/auth-key-admin`, ePrincipalArn: `arn:aws:iam::${account}:user/e-writer`, eBucketArn: "arn:aws:s3:::example-e-data", eAuthorityKeyRoot: "96bulls/authority-v1", eCandidateKeyRoot: E_CANDIDATE_ROOT, transcriptBucketArn: "arn:aws:s3:::example-transcripts" });
type Statement = Record<string, any>;
const array = (v: any): any[] => Array.isArray(v) ? v : [v];
const glob = (p: string, v: string) => new RegExp("^" + p.replace(/[.+^${}()|[\]\\]/g, "\\$&").replaceAll("*", ".*").replaceAll("?", ".") + "$", "s").test(v);
/** Bounded statement-shape oracle, NOT an AWS IAM/404/KMS qualification. Unknown conditions fail. */
function effects(statements: Statement[], action: string, resource: string, context: Record<string, string> = {}) {
  return statements.filter(s => {
    if (!array(s.Action).some(a => glob(a.toLowerCase(), action.toLowerCase()))) return false;
    if (s.Principal && s.Principal !== "*" && !array(s.Principal.AWS).includes(context["aws:PrincipalArn"])) return false;
    if (s.Resource && !array(s.Resource).some(r => glob(r, resource))) return false;
    if (s.NotResource && array(s.NotResource).some(r => glob(r, resource))) return false;
    return Object.entries(s.Condition ?? {}).every(([op, conditions]) => Object.entries(conditions as object).every(([key, v]) => {
      const want = array(v), actual = context[key];
      if (op === "StringEquals" || op === "Bool") return actual !== undefined && want.includes(actual);
      if (op === "StringNotEquals" || op === "ArnNotEquals") return actual === undefined || !want.includes(actual);
      if (op === "StringLike") return actual !== undefined && want.some(p => glob(p, actual));
      throw Error("unmodeled condition");
    }));
  }).map(s => s.Effect);
}
const permitted = (ss: Statement[], action: string, resource: string, ctx: Record<string, string> = {}) => {
  const e = effects(ss, action, resource, ctx); return e.includes("Allow") && !e.includes("Deny");
};
const request = (principal: string, key: string) => ({ "aws:PrincipalArn": principal, "aws:SecureTransport": "true", "s3:x-amz-server-side-encryption": "aws:kms", "s3:x-amz-server-side-encryption-aws-kms-key-id": key });
async function* stdin(value: unknown) { yield JSON.stringify(value); }

describe("C auth offline policy proposal", () => {
  test("captures an unqualified deterministic immutable proposal and hashes its entire public body", () => {
    const v = input(), a = buildRhcAuthStorePlan(v), reversed = Object.fromEntries(Object.entries(v).reverse());
    expect(buildRhcAuthStorePlan(reversed)).toEqual(a);
    const { sha256, ...body } = a;
    expect(sha256).toBe(createHash("sha256").update(canonicalPolicyJson(body)).digest("hex"));
    expect(a).toMatchObject({ deployable: false, status: "unqualified-proposal" });
    expect(a.holds).toContain("effective-S3-404-403-and-conditional-write-proof");
    expect(a.holds).toContain("versioning-retention-erasure-and-restore");
    v.keyRoot = "changed"; expect(a.input.keyRoot).toBe("auth/private-v1");
    expect(Object.isFrozen(a.policies.runtimePolicy.Statement[0]!.Resource)).toBe(true);
    expect(() => (a.input as any).keyRoot = "forged").toThrow();
    expect(() => canonicalPolicyJson([undefined])).toThrow();
    expect(() => canonicalPolicyJson(new Array(1))).toThrow();
  });
  test("explicit reads, conditional-code writes and bucket-only listing retain the exact family boundary", () => {
    const i = input(), p = buildRhcAuthStorePlan(i), s = p.policies.runtimePolicy.Statement;
    const keys = ["sessions/cli/org/session.json", "sessions/service/org/session.json", "index/id/hash.json", "index/access/hash.json", "index/refresh/hash.json", "auth-transitions/session/active.json", "auth-transitions/session/intent.json", "auth-transitions/session/intent.complete.json"];
    for (const k of keys) {
      const arn = `${i.bucketArn}/${i.keyRoot}/${k}`;
      expect(permitted(s, "s3:GetObject", arn)).toBe(true);
      expect(permitted(s, "s3:PutObject", arn, request(i.runtimePrincipalArn, i.kmsKeyArn))).toBe(true);
      expect(permitted(s, "s3:PutObject", arn)).toBe(false);
    }
    expect(permitted(s, "s3:GetObject", p.keyFamilies.seal)).toBe(true);
    expect(permitted(s, "s3:PutObject", p.keyFamilies.seal, request(i.runtimePrincipalArn, i.kmsKeyArn))).toBe(false);
    expect(permitted(s, "s3:ListBucket", i.bucketArn)).toBe(true);
    const list = s.find(s => s.Sid === "ListDedicatedAuthBucket")!;
    expect(list.Resource).toEqual([i.bucketArn]); expect(list.Condition).toBeUndefined();
    expect(p.keyFamilies.serviceListPrefix).toBe("auth/private-v1/sessions/service/<sha256(org)>/");
    for (const arn of [i.eBucketArn, i.transcriptBucketArn, `${i.eBucketArn}/${i.eAuthorityKeyRoot}/root.json`, `${i.bucketArn}/other-prefix/session.json`, `${i.bucketArn}/${i.keyRoot}/authority-evil.json`]) {
      for (const action of ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]) expect(permitted(s, action, arn, request(i.runtimePrincipalArn, i.kmsKeyArn))).toBe(false);
    }
  });
  test("owner can read inventory/seal without write headers; runtime and E cannot write the seal", () => {
    const i = input(), p = buildRhcAuthStorePlan(i), seal = p.keyFamilies.seal;
    expect(permitted(p.policies.ownerBootstrapPolicy.Statement, "s3:GetObject", seal)).toBe(true);
    expect(permitted(p.policies.ownerBootstrapPolicy.Statement, "s3:PutObject", seal, request(i.ownerBootstrapPrincipalArn, i.kmsKeyArn))).toBe(true);
    expect(effects(p.policies.bucketPolicy.Statement, "s3:PutObject", seal, request(i.ownerBootstrapPrincipalArn, i.kmsKeyArn))).not.toContain("Deny");
    for (const principal of [i.runtimePrincipalArn, i.ePrincipalArn]) expect(effects(p.policies.bucketPolicy.Statement, "s3:PutObject", seal, request(principal, i.kmsKeyArn))).toContain("Deny");
    // An accidental broad E grant still cannot expose C auth data or key names.
    for (const action of ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetObjectVersion", "s3:ListBucketVersions", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutBucketPolicy"]) expect(effects(p.policies.bucketPolicy.Statement, action, ["s3:ListBucket", "s3:ListBucketVersions", "s3:PutBucketPolicy"].includes(action) ? i.bucketArn : seal, request(i.ePrincipalArn, i.kmsKeyArn))).toContain("Deny");
  });
  test("runtime cannot delete/history/admin; wrong-key or non-TLS writes remain refused", () => {
    const i = input(), p = buildRhcAuthStorePlan(i), object = p.keyFamilies.mutable[0]!.replaceAll("*", "a");
    for (const action of ["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:ListBucketVersions", "s3:PutBucketPolicy", "s3:PutObjectAcl", "s3:PutLifecycleConfiguration"]) expect(permitted(p.policies.runtimePolicy.Statement, action, object)).toBe(false);
    const wrong = request(i.runtimePrincipalArn, i.kmsKeyArn.replace("11111111", "22222222"));
    expect(permitted(p.policies.runtimePolicy.Statement, "s3:PutObject", object, wrong)).toBe(false);
    expect(effects(p.policies.bucketPolicy.Statement, "s3:PutObject", object, wrong)).toContain("Deny");
    expect(effects(p.policies.bucketPolicy.Statement, "s3:PutObject", object, {})).toContain("Deny");
    expect(effects(p.policies.bucketPolicy.Statement, "s3:GetObject", object, { "aws:SecureTransport": "false" })).toContain("Deny");
  });
  test("E's exact principal is denied every C-bucket action despite an accidental broad grant", () => {
    const i = input(), p = buildRhcAuthStorePlan(i), ss = p.policies.bucketPolicy.Statement;
    const broadGrant = { Effect: "Allow", Action: ["s3:*"], Resource: ["*"] };
    const requests = [
      ["s3:GetBucketPolicy", i.bucketArn], ["s3:PutBucketCORS", i.bucketArn],
      ["s3:GetObjectAcl", p.keyFamilies.seal], ["s3:GetObject", p.keyFamilies.seal],
      ["s3:PutObject", p.keyFamilies.mutable[0]!.replaceAll("*", "a")],
      ["s3:ListBucket", i.bucketArn], ["s3:DeleteObjectVersion", p.keyFamilies.seal],
    ];
    const ctx = request(i.ePrincipalArn, i.kmsKeyArn);
    for (const [action, resource] of requests) {
      expect(permitted([broadGrant, ...ss], action!, resource!, ctx)).toBe(false);
      // Removing precisely the E denial restores these otherwise-valid broad grants.
      expect(permitted([broadGrant, ...ss.filter(s => s.Sid !== "EHasNoCAuthAccess")], action!, resource!, ctx)).toBe(true);
    }
    for (const principal of [i.runtimePrincipalArn, i.ownerBootstrapPrincipalArn, i.keyAdminPrincipalArn])
      expect(effects(ss, "s3:GetBucketPolicy", i.bucketArn, request(principal, i.kmsKeyArn))).not.toContain("Deny");
    for (const resource of [i.eBucketArn, i.transcriptBucketArn, `${i.bucketArn}-sibling`, `${i.bucketArn}-sibling/object`])
      expect(effects(ss, "s3:GetBucketPolicy", resource, ctx)).not.toContain("Deny");
  });
  test("KMS is exact-key, exact-account, regional S3 and object-context; admins receive no crypto grant", () => {
    const i = input(), p = buildRhcAuthStorePlan(i), ctx = { "aws:PrincipalArn": i.runtimePrincipalArn, "kms:ViaService": `s3.${region}.amazonaws.com`, "kms:CallerAccount": account, "kms:EncryptionContext:aws:s3:arn": `${i.bucketArn}/${i.keyRoot}/sessions/cli/o/id.json` };
    for (const a of ["kms:GenerateDataKey", "kms:Decrypt"]) {
      expect(permitted(p.policies.runtimePolicy.Statement, a, i.kmsKeyArn, ctx)).toBe(true);
      for (const delta of [{ "kms:ViaService": "lambda.us-east-1.amazonaws.com" }, { "kms:CallerAccount": "111111111111" }, { "kms:EncryptionContext:aws:s3:arn": i.bucketArn }, { "kms:EncryptionContext:aws:s3:arn": `${i.eBucketArn}/anything` }]) expect(permitted(p.policies.runtimePolicy.Statement, a, i.kmsKeyArn, { ...ctx, ...delta })).toBe(false);
      expect(permitted(p.policies.runtimePolicy.Statement, a, i.kmsKeyArn.replace("11111111", "22222222"), ctx)).toBe(false);
      expect(permitted(p.policies.kmsKeyPolicy.Statement, a, i.kmsKeyArn, { ...ctx, "aws:PrincipalArn": i.keyAdminPrincipalArn })).toBe(false);
      expect(permitted(p.policies.kmsKeyPolicy.Statement, a, i.kmsKeyArn, { ...ctx, "aws:PrincipalArn": i.ePrincipalArn })).toBe(false);
    }
    for (const [name, policy] of Object.entries(p.policies)) for (const statement of policy.Statement) {
      if (name === "bucketPolicy" && statement.Sid === "EHasNoCAuthAccess") {
        expect(statement).toEqual({ Sid: "EHasNoCAuthAccess", Effect: "Deny", Principal: { AWS: i.ePrincipalArn }, Action: ["s3:*"], Resource: [i.bucketArn, `${i.bucketArn}/*`] });
      } else for (const action of array(statement.Action)) expect(action).not.toMatch(/[*?]/);
    }
    expect(p.resources.bucket).toMatchObject({ bucketKeyEnabled: false, lifecycleRules: [], objectLockEnabled: false });
    // DeleteBucketOwnershipControls API is authorized by PutBucketOwnershipControls, not a new IAM verb.
    const denied = p.policies.runtimePolicy.Statement.find(s => s.Sid === "NoAuthHistoryDeletionOrAdministration")!;
    expect(denied.Action).toContain("s3:PutBucketOwnershipControls");
    expect(denied.Action).not.toContain("s3:DeleteBucketOwnershipControls");
  });
  test("rejects malformed, wildcard, cross-account and incomplete inputs without echoing values", () => {
    const bad: unknown[] = [null, [], "input", new Date(), { ...input(), extra: true }, { ...input(), accessKeyId: "forbidden" }];
    for (const [field, values] of Object.entries({ version: [2, "1"], accountId: ["x", "00000000000*"], region: ["us-west-2"], bucketArn: ["*", "arn:aws:s3:::abc/*", "arn:aws:s3:::127.0.0.1"], keyRoot: ["", "a/*", "a?", "a//b", "a/../b", "a/", "a".repeat(201)], kmsKeyArn: ["*", input().kmsKeyArn.replace(region, "us-west-2"), input().kmsKeyArn.replace(account, "111111111111")], runtimePrincipalArn: [input().ownerBootstrapPrincipalArn, `arn:aws:sts::${account}:assumed-role/c/session`, `arn:aws:iam::111111111111:user/c`, `arn:aws:iam::${account}:user/*`], ownerBootstrapPrincipalArn: [input().runtimePrincipalArn], keyAdminPrincipalArn: [input().ePrincipalArn] })) for (const value of values) bad.push({ ...input(), [field]: value });
    for (const v of bad) expect(() => buildRhcAuthStorePlan(v)).toThrow(RhcAuthStorePolicyError);
    const marker = "AKIA" + "A".repeat(16);
    try { buildRhcAuthStorePlan({ ...input(), keyRoot: marker }); throw Error("not refused"); }
    catch (e) { expect(e).toBeInstanceOf(RhcAuthStorePolicyError); expect(String(e)).not.toContain(marker); }
  });
  test("requires dedicated buckets, logical root disjointness and distinct C/E/owner principals", () => {
    for (const change of [{ bucketArn: input().eBucketArn }, { bucketArn: input().transcriptBucketArn }, { keyRoot: "96bulls" }, { keyRoot: input().eAuthorityKeyRoot }, { keyRoot: input().eAuthorityKeyRoot + "/nested" }, { keyRoot: E_CANDIDATE_ROOT + "/nested" }, { eAuthorityKeyRoot: E_CANDIDATE_ROOT }, { eCandidateKeyRoot: "other/root" }, { runtimePrincipalArn: input().ePrincipalArn }]) expect(() => buildRhcAuthStorePlan({ ...input(), ...change })).toThrow();
  });
  test("refuses accessors/prototypes/symbols without invoking hidden getters or serialization hooks", () => {
    let accessed = 0;
    const x = { ...input() }; Object.defineProperty(x, "keyRoot", { enumerable: true, get() { accessed++; throw Error("secret"); } });
    for (const v of [x, Object.assign(Object.create({ inherited: true }), input()), { ...input(), [Symbol("hidden")]: true }, { ...input(), toJSON() { accessed++; } }]) expect(() => buildRhcAuthStorePlan(v)).toThrow();
    expect(accessed).toBe(0);
  });
  test("CLI defaults to a non-effect plan; no execute flag, oversized/read/JSON errors are sanitized", async () => {
    const logs: string[] = [], output = (s: string) => { logs.push(s); };
    const oldFetch = globalThis.fetch; globalThis.fetch = (() => { throw Error("network forbidden"); }) as typeof fetch;
    try {
      expect(await runRhcAuthStorePlan([], stdin(input()), output)).toBe(0);
      expect(JSON.parse(logs.pop()!)).toEqual(buildRhcAuthStorePlan(input()));
      expect(await runRhcAuthStorePlan(["--execute"], stdin(input()), output)).toBe(1);
      expect(JSON.parse(logs.pop()!)).toEqual({ status: "refused", error: "arguments" });
      async function* tooBig() { yield "x".repeat(65_537); }
      expect(await runRhcAuthStorePlan([], tooBig(), output)).toBe(1); expect(logs.pop()).toContain("input_bound");
      const marker = "private-input-marker";
      async function* failed() { throw Error(marker); yield ""; }
      expect(await runRhcAuthStorePlan([], failed(), output)).toBe(1); expect(logs.pop()).not.toContain(marker);
      async function* badJson() { yield marker; }
      expect(await runRhcAuthStorePlan([], badJson(), output)).toBe(1); expect(logs.pop()).toContain("input_json");
    } finally { globalThis.fetch = oldFetch; }
  });
  test("actual CLI pipe emits only public JSON and never invokes a provider or leaks refused input", () => {
    const script = new URL("../../../scripts/ops/rhc-auth-store-plan.ts", import.meta.url).pathname;
    const result = Bun.spawnSync([process.execPath, script], { stdin: Buffer.from(JSON.stringify(input())), stdout: "pipe", stderr: "pipe", env: {} });
    expect(result.exitCode).toBe(0); expect(result.stderr.toString()).toBe(""); expect(JSON.parse(result.stdout.toString()).deployable).toBe(false);
    const marker = "ASIA" + "B".repeat(16);
    const rejected = Bun.spawnSync([process.execPath, script], { stdin: Buffer.from(JSON.stringify({ ...input(), keyRoot: marker })), stdout: "pipe", stderr: "pipe", env: {} });
    expect(rejected.exitCode).toBe(1); expect(rejected.stdout.toString()).not.toContain(marker); expect(rejected.stderr.toString()).toBe("");
    for (const file of [script, new URL("../rhc-auth-store-policy.ts", import.meta.url).pathname]) {
      const code = readFileSync(file, "utf8"); expect(code).not.toMatch(/process\.env|writeFile|@alchemy|aws-sdk|execSync|spawn\(/);
    }
  });
});
