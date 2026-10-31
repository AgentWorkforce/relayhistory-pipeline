import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { manifestKey, rawChunkKey, type SessionLocation } from "../../../packages/object-store/src/layout.ts";
import { NON_DESTROYABLE } from "../deploy-guard.ts";
import { isProductionLike } from "../hardening.ts";
import { validateAwsInputs, type AwsInputs } from "../inputs.ts";
import { TENANT_KEY_BASE } from "../tenants.ts";
import {
  ABORT_INCOMPLETE_UPLOAD_DAYS,
  ERASURE_SLA_HOURS,
  accessLogBucketPolicy,
  accessLogLifecycleRules,
  transcriptBucketPolicy,
  transcriptEraserPolicy,
  transcriptKeyPolicy,
  transcriptLifecycleRules,
  transcriptNames,
  transcriptReaderPolicy,
  transcriptTrailSelectors,
  transcriptWriterPolicy,
  userArn,
} from "../transcript-store.ts";

const STAGE = "aws-sessions-prod";
const ACCOUNT = "941433935742";
const REGION = "us-east-1";
const n = transcriptNames(STAGE, ACCOUNT);
const KEY = `arn:aws:kms:${REGION}:${ACCOUNT}:key/1111`;
const writerArn = userArn(ACCOUNT, n.userPath, n.writer);
const readerArn = userArn(ACCOUNT, n.userPath, n.reader);
const eraserArn = userArn(ACCOUNT, n.userPath, n.eraser);
const src = readFileSync(fileURLToPath(new URL("../transcript-store.ts", import.meta.url)), "utf8");
const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
const example = JSON.parse(readFileSync(fileURLToPath(new URL("../examples/aws-sessions-prod.inputs.example.json", import.meta.url)), "utf8")) as AwsInputs;

type Stmt = { Sid?: string; Effect: "Allow" | "Deny"; Principal?: unknown; Action: string[]; Resource: string | string[]; Condition?: Record<string, Record<string, unknown>> };
const res = (s: Stmt) => (Array.isArray(s.Resource) ? s.Resource : [s.Resource]);
/** IAM resource glob -> RegExp (`*` matches across `/`, as in IAM). */
const glob = (pattern: string) => new RegExp(`^${pattern.replace(/[.+?^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*")}$`);
const covers = (pattern: string, action: string) => glob(pattern.replace(/\*/g, "*")).test(action) || pattern === action;
/** Effective decision for an identity policy alone: explicit Deny wins, then any Allow. */
const decide = (policy: { Statement: Stmt[] }, action: string, resource: string) => {
  const hit = (s: Stmt) => s.Action.some((a) => covers(a, action)) && res(s).some((r) => glob(r).test(resource));
  if (policy.Statement.some((s) => s.Effect === "Deny" && hit(s))) return "deny";
  return policy.Statement.some((s) => s.Effect === "Allow" && hit(s)) ? "allow" : "implicit-deny";
};

const loc: SessionLocation = { prefix: `${TENANT_KEY_BASE}/org_abc`, workspaceId: "ws1", machineId: "m1", harness: "claude", startDate: "2026-10-31", sessionId: "s1" };
const obj = (key: string) => `${n.bucketArn}/${key}`;
const RAW = obj(rawChunkKey(loc, 0, 0, "a".repeat(64), "jsonl"));
const MANIFEST = obj(manifestKey(loc));
const SUMMARY = obj(`${TENANT_KEY_BASE}/org_abc/v1/ws=ws1/m=m1/h=claude/d=2026/10/31/s=s1/summary/short.json.enc`);
const OUTSIDE = obj("v1/ws=ws1/m=m1/h=claude/d=2026/10/31/s=s1/manifest.json");
const allowedActions = (p: { Statement: Stmt[] }) => p.Statement.filter((s) => s.Effect === "Allow").flatMap((s) => s.Action);

describe("transcript store: names and layout", () => {
  it("names follow the stack convention and fit S3/IAM/KMS limits", () => {
    expect(n.bucket).toBe("96bulls-aws-sessions-prod-transcripts-941433935742");
    expect(n.accessLogBucket).toBe("96bulls-aws-sessions-prod-access-logs-941433935742");
    for (const b of [n.bucket, n.accessLogBucket, n.trailBucket]) expect(b).toMatch(/^(?!xn--)[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/);
    for (const u of [n.writer, n.reader, n.eraser]) expect(u).toMatch(/^[\w+=,.@-]{1,64}$/);
    expect(n.keyAlias).toMatch(/^alias\/[a-zA-Z0-9/_-]{1,250}$/);
    expect(n.budget).toMatch(/^[^:\\]{1,100}$/);
  });

  it("the 96Bulls pipeline keys land at 96bulls/<org>/v1/...", () => {
    expect(manifestKey(loc).startsWith(`${TENANT_KEY_BASE}/org_abc/v1/ws=ws1/`)).toBe(true);
  });

  it("is a production stage: never destroyed, never purged", () => {
    expect(NON_DESTROYABLE(STAGE)).toBe(true);
    expect(isProductionLike(STAGE)).toBe(true);
    expect(src).not.toMatch(/forceDestroy/);
  });
});

describe("transcript store: bucket hardening", () => {
  const policy = transcriptBucketPolicy({ bucketArn: n.bucketArn, keyArn: KEY, eraserArn }) as Stmt[];
  const sid = (s: string) => {
    const found = policy.find((p) => p.Sid === s);
    expect(found).toBeDefined();
    return found!;
  };

  it("denies every non-TLS request and TLS below 1.2, on the bucket and its objects", () => {
    const tls = sid("DenyInsecureTransport");
    expect(tls.Effect).toBe("Deny");
    expect(tls.Principal).toBe("*");
    expect(tls.Action).toEqual(["s3:*"]);
    expect(res(tls)).toEqual([n.bucketArn, `${n.bucketArn}/*`]);
    expect(tls.Condition).toEqual({ Bool: { "aws:SecureTransport": "false" } });
    expect(sid("DenyTlsBelow12").Condition).toEqual({ NumericLessThan: { "s3:TlsVersion": "1.2" } });
  });

  it("has no Allow statement at all: nothing is public and nothing is granted by the bucket", () => {
    expect(policy.filter((s) => s.Effect === "Allow")).toEqual([]);
  });

  it("only the eraser deletes objects or versions", () => {
    const d = sid("OnlyTheEraserDeletes");
    expect(d.Effect).toBe("Deny");
    expect(d.Action.sort()).toEqual(["s3:DeleteObject", "s3:DeleteObjectVersion"]);
    expect(d.Condition).toEqual({ ArnNotEquals: { "aws:PrincipalArn": eraserArn } });
  });

  it("refuses a PUT that names other encryption or another KMS key (only when the header is present)", () => {
    expect(sid("DenyOtherEncryption").Condition).toEqual({ StringNotEquals: { "s3:x-amz-server-side-encryption": "aws:kms" }, Null: { "s3:x-amz-server-side-encryption": "false" } });
    expect(sid("DenyOtherKmsKey").Condition).toEqual({ StringNotEquals: { "s3:x-amz-server-side-encryption-aws-kms-key-id": KEY }, Null: { "s3:x-amz-server-side-encryption-aws-kms-key-id": "false" } });
  });

  it("is versioned, SSE-KMS with its own key and no bucket key, SSE-C blocked, all public access blocked, owner-enforced, Object Lock off, logged", () => {
    const block = src.slice(src.indexOf('AWS.S3.Bucket("transcripts"'), src.indexOf('yield* AWS.Budgets.Budget("budget"'));
    expect(block).toContain('versioning: "Enabled"');
    expect(block).toContain('encryption: { sseAlgorithm: "aws:kms", kmsMasterKeyId: key.keyArn, bucketKeyEnabled: false, blockedEncryptionTypes: ["SSE-C"] }');
    expect(block).toContain("publicAccessBlock: PUBLIC_ACCESS_BLOCK");
    expect(block).toContain('objectOwnership: "BucketOwnerEnforced"');
    expect(block).toContain("objectLockEnabled: false");
    expect(block).not.toContain("objectLockConfiguration");
    expect(block).toContain("logging: { targetBucket: accessLogs.bucketName");
    expect(src).toContain("const PUBLIC_ACCESS_BLOCK = { blockPublicAcls: true, ignorePublicAcls: true, blockPublicPolicy: true, restrictPublicBuckets: true } as const;");
  });

  it("the access-log bucket takes S3 server access logs from the transcripts bucket only, over TLS", () => {
    const p = accessLogBucketPolicy({ logBucketArn: "arn:aws:s3:::logs", sourceBucketArn: n.bucketArn, accountId: ACCOUNT }) as Stmt[];
    expect(p.find((s) => s.Sid === "DenyInsecureTransport")).toBeDefined();
    const allow = p.filter((s) => s.Effect === "Allow");
    expect(allow).toHaveLength(1);
    expect(allow[0]!.Principal).toEqual({ Service: "logging.s3.amazonaws.com" });
    expect(allow[0]!.Action).toEqual(["s3:PutObject"]);
    expect(allow[0]!.Condition).toEqual({ ArnLike: { "aws:SourceArn": n.bucketArn }, StringEquals: { "aws:SourceAccount": ACCOUNT } });
  });
});

describe("transcript store: lifecycle fits the K-45 erasure SLA", () => {
  const rules = transcriptLifecycleRules() as { ID: string; Status: string; Filter: { Prefix: string }; NoncurrentVersionExpiration?: { NoncurrentDays: number }; AbortIncompleteMultipartUpload?: { DaysAfterInitiation: number }; Expiration?: { ExpiredObjectDeleteMarker?: boolean } }[];

  it("expires noncurrent versions so they are due within 48 h (days + midnight rounding), bucket-wide", () => {
    const r = rules.find((x) => x.NoncurrentVersionExpiration)!;
    expect(r.Status).toBe("Enabled");
    expect(r.Filter).toEqual({ Prefix: "" });
    expect((r.NoncurrentVersionExpiration!.NoncurrentDays + 1) * 24).toBeLessThanOrEqual(ERASURE_SLA_HOURS);
  });

  it("aborts incomplete multipart uploads within the same bound and removes expired delete markers", () => {
    const a = rules.find((x) => x.AbortIncompleteMultipartUpload)!;
    expect((a.AbortIncompleteMultipartUpload!.DaysAfterInitiation + 1) * 24).toBeLessThanOrEqual(ERASURE_SLA_HOURS);
    expect(ABORT_INCOMPLETE_UPLOAD_DAYS).toBe(1);
    expect(rules.find((x) => x.Expiration?.ExpiredObjectDeleteMarker)?.Status).toBe("Enabled");
  });

  it("access logs expire and their noncurrent versions too", () => {
    const l = accessLogLifecycleRules() as { NoncurrentVersionExpiration?: { NoncurrentDays: number }; Expiration?: { Days: number } }[];
    expect(l.find((x) => x.Expiration)?.Expiration?.Days).toBeGreaterThan(0);
    expect(l.find((x) => x.NoncurrentVersionExpiration)!.NoncurrentVersionExpiration!.NoncurrentDays).toBe(1);
  });
});

describe("transcript store: KMS key policy is least privilege", () => {
  const kp = transcriptKeyPolicy({ accountId: ACCOUNT, region: REGION, bucketName: n.bucket, writerArn, readerArn });
  const st = kp.Statement as Stmt[];

  it("never grants kms:* or a wildcard principal; the root administers but cannot use the key", () => {
    for (const s of st) {
      expect(s.Action).not.toContain("kms:*");
      expect(JSON.stringify(s.Principal)).not.toBe('{"AWS":"*"}');
      expect(s.Principal).not.toBe("*");
    }
    const root = st.filter((s) => JSON.stringify(s.Principal).includes(":root"));
    expect(root).toHaveLength(1);
    for (const use of ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:ReEncryptFrom", "kms:ReEncryptTo"]) expect(root[0]!.Action.some((a) => covers(a, use))).toBe(false);
  });

  it("only the writer and reader use it, only via S3 in us-east-1, only for objects under 96bulls/ in this bucket", () => {
    const users = st.filter((s) => !JSON.stringify(s.Principal).includes(":root"));
    expect(users.map((s) => (s.Principal as { AWS: string }).AWS).sort()).toEqual([readerArn, writerArn].sort());
    for (const s of users) {
      expect(s.Condition).toEqual({
        StringEquals: { "kms:ViaService": `s3.${REGION}.amazonaws.com`, "kms:CallerAccount": ACCOUNT },
        StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${n.bucket}/${TENANT_KEY_BASE}/*` },
      });
    }
    expect(users.find((s) => (s.Principal as { AWS: string }).AWS === writerArn)!.Action.sort()).toEqual(["kms:Decrypt", "kms:GenerateDataKey"]);
    expect(users.find((s) => (s.Principal as { AWS: string }).AWS === readerArn)!.Action).toEqual(["kms:Decrypt"]);
    expect(JSON.stringify(kp)).not.toContain(eraserArn);
  });
});

describe("transcript store: IAM users are least privilege", () => {
  const writer = transcriptWriterPolicy(n.bucketArn) as { Statement: Stmt[] };
  const reader = transcriptReaderPolicy(n.bucketArn) as { Statement: Stmt[] };
  const eraser = transcriptEraserPolicy(n.bucketArn) as { Statement: Stmt[] };

  it("writer puts raw chunks, manifests and summaries under 96bulls/<org>/v1/ and reads them, nothing else", () => {
    for (const k of [RAW, MANIFEST, SUMMARY]) expect(decide(writer, "s3:PutObject", k)).toBe("allow");
    expect(decide(writer, "s3:GetObject", MANIFEST)).toBe("allow");
    expect(decide(writer, "s3:PutObject", OUTSIDE)).toBe("implicit-deny");
    expect(decide(writer, "s3:PutObject", obj(`${TENANT_KEY_BASE}/org_abc/v1/ws=ws1/m=m1/h=claude/d=2026/10/31/s=s1/other.bin`))).toBe("implicit-deny");
    expect(decide(writer, "s3:PutObject", obj("anything"))).toBe("implicit-deny");
  });

  it("writer and reader never delete, never read old versions, never change the bucket", () => {
    for (const p of [writer, reader])
      for (const a of ["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:PutBucketPolicy", "s3:PutLifecycleConfiguration", "s3:PutBucketVersioning"])
        expect(decide(p, a, a.includes("Object") ? MANIFEST : n.bucketArn)).toBe("deny");
  });

  it("reader lists and reads, never writes", () => {
    expect(decide(reader, "s3:GetObject", RAW)).toBe("allow");
    expect(decide(reader, "s3:ListBucket", n.bucketArn)).toBe("allow");
    expect(decide(reader, "s3:PutObject", RAW)).toBe("deny");
  });

  it("eraser deletes every version under 96bulls/ and cannot read or write content", () => {
    expect(decide(eraser, "s3:DeleteObjectVersion", RAW)).toBe("allow");
    expect(decide(eraser, "s3:DeleteObject", MANIFEST)).toBe("allow");
    expect(decide(eraser, "s3:DeleteObject", OUTSIDE)).toBe("implicit-deny");
    for (const a of ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"]) expect(decide(eraser, a, RAW)).toBe("deny");
    const list = eraser.Statement.find((s) => s.Sid === "ListVersionsUnderPrefix")!;
    expect(list.Condition).toEqual({ StringLike: { "s3:prefix": [`${TENANT_KEY_BASE}/*`] } });
  });

  it("no user policy has a wildcard action, a wildcard resource, or any KMS or IAM grant", () => {
    for (const p of [writer, reader, eraser])
      for (const a of allowedActions(p)) {
        expect(a).not.toMatch(/\*/);
        expect(a.startsWith("s3:")).toBe(true);
      }
    for (const p of [writer, reader, eraser]) for (const s of p.Statement.filter((x) => x.Effect === "Allow")) for (const r of res(s)) expect(r.startsWith(n.bucketArn)).toBe(true);
  });

  it("creates the key after the users it names and after the trail that audits it", () => {
    const at = (s: string) => src.indexOf(s);
    expect(at('AWS.KMS.Key("transcripts-key"')).toBeGreaterThan(at('user("transcript-reader"'));
    expect(src).toContain('"96bulls:audit-trail": trail.trailName');
    expect(at('AWS.KMS.Key("transcripts-key"')).toBeGreaterThan(at('AWS.CloudTrail.Trail("trail"'));
  });

  it("declares no access keys: they would land in plaintext Alchemy state", () => {
    expect(src).not.toMatch(/IAM\.AccessKey|AccessKey\(/);
    expect(stack).not.toMatch(/IAM\.AccessKey/);
  });
});

describe("transcript store: CloudTrail data events", () => {
  it("logs S3 object events for the transcripts bucket only, plus management events", () => {
    const sel = transcriptTrailSelectors(n.bucket, true);
    expect(sel.map((s) => s.name)).toEqual(["management-events", "transcript-object-events"]);
    expect(sel[1]!.fieldSelectors).toEqual([
      { field: "eventCategory", equals: ["Data"] },
      { field: "resources.type", equals: ["AWS::S3::Object"] },
      { field: "resources.ARN", startsWith: [`arn:aws:s3:::${n.bucket}/`] },
    ]);
    expect(transcriptTrailSelectors(n.bucket, false).map((s) => s.name)).toEqual(["transcript-object-events"]);
  });
});

describe("transcript store: inputs", () => {
  const filled = (patch: (i: AwsInputs & Record<string, unknown>) => void = () => {}) => {
    const i = structuredClone(example) as AwsInputs & Record<string, unknown>;
    delete i.$comment;
    i.approval = { ...i.approval!, decision: "approved", date: "2026-10-31", quote: "let's start the production s3 work now", capture: { provenanceSha256: "a".repeat(64), planSha256: "b".repeat(64), inputsSha256: "c".repeat(64), sourceCommit: "d".repeat(40) } };
    patch(i);
    return i;
  };

  it("validates once filled; the committed example still refuses its placeholders", () => {
    expect(() => validateAwsInputs(STAGE, filled())).not.toThrow();
    expect(() => validateAwsInputs(STAGE, example)).toThrow(/placeholder values/);
  });

  it("refuses anything but us-east-1, sandboxes, and any baseline/EKS/purge input", () => {
    expect(() => validateAwsInputs(STAGE, filled((i) => (i.region = "us-west-2")))).toThrow(/must stay in us-east-1/);
    expect(() => validateAwsInputs("aws-sandbox-x", filled())).toThrow(/sandbox account never holds transcripts/);
    for (const [k, v] of [["eks", {}], ["supabase", {}], ["accountShared", { ownerStage: "aws-sessions-dev" }], ["lockerNoncurrentVersionDays", 30], ["hoopdbSecretArn", "arn:x"]] as const)
      expect(() => validateAwsInputs(STAGE, filled((i) => ((i as Record<string, unknown>)[k] = v)))).toThrow(new RegExp(`${k} is not allowed`));
    expect(() => validateAwsInputs(STAGE, filled((i) => (i.teardown = { purgeBucketsOnDestroy: true })))).toThrow(/never allowed on a production-looking stage/);
    expect(() => validateAwsInputs(STAGE, filled((i) => ((i.transcriptStore as Record<string, unknown>).objectLock = true)))).toThrow(/not a known setting/);
  });

  it("the stack declares the store instead of the baseline", () => {
    const at = stack.indexOf("if (inputs.transcriptStore)");
    expect(at).toBeGreaterThan(0);
    expect(at).toBeLessThan(stack.indexOf('AWS.KMS.Key("locker-key"'));
  });
});
