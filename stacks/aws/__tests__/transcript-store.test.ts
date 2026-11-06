import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { formatDeclaredPropertyYaml } from "alchemy/Cli/PropertyDiff";
import * as Effect from "effect/Effect";
import { manifestKey, rawChunkKey, type SessionLocation } from "../../../packages/object-store/src/layout.ts";
import { NON_DESTROYABLE } from "../deploy-guard.ts";
import { isProductionLike, trailKeyPolicy } from "../hardening.ts";
import { validateAwsInputs, type AwsInputs } from "../inputs.ts";
import { TENANT_KEY_BASE } from "../tenants.ts";
import { SESSION_PIPELINE_RESOURCES } from "../session-pipeline.ts";
import {
  ABORT_INCOMPLETE_UPLOAD_DAYS,
  ERASURE_SLA_HOURS,
  INBOX_CANDIDATE_KEY_ROOT,
  accessLogBucketPolicy,
  accessLogLifecycleRules,
  assertTranscriptStorePlanContract,
  transcriptBucketPolicy,
  transcriptEraserPolicy,
  transcriptKeyPolicy,
  transcriptLifecycleRules,
  transcriptNames,
  transcriptReaderPolicy,
  srvWriterObjects,
  transcriptSrvWriterPolicy,
  transcriptStoreStage,
  transcriptTrailSelectors,
  transcriptWriterPolicy,
  trailLogBucketPolicy,
  userArn,
} from "../transcript-store.ts";

const STAGE = "aws-sessions-prod";
const ACCOUNT = "941433935742";
const REGION = "us-east-1";
const n = transcriptNames(STAGE, ACCOUNT);
const KEY = `arn:aws:kms:${REGION}:${ACCOUNT}:key/1111`;
const AUTHORITY_ROOT = "96bulls/inbox-authority/v1";
const writerArn = userArn(ACCOUNT, n.userPath, n.writer);
const readerArn = userArn(ACCOUNT, n.userPath, n.reader);
const eraserArn = userArn(ACCOUNT, n.userPath, n.eraser);
const srvWriterArn = userArn(ACCOUNT, n.userPath, n.srvWriter);
const src = readFileSync(fileURLToPath(new URL("../transcript-store.ts", import.meta.url)), "utf8");
const stack = readFileSync(fileURLToPath(new URL("../alchemy.aws.run.ts", import.meta.url)), "utf8");
const captureWorkflow = readFileSync(fileURLToPath(new URL("../../../src/cli/aws-provenance-io.ts", import.meta.url)), "utf8");
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
    expect(srvWriterArn).toBe("arn:aws:iam::941433935742:user/96bulls/96bulls-aws-sessions-prod-transcript-srv-writer");
    for (const u of [n.writer, n.reader, n.eraser, n.srvWriter]) expect(u).toMatch(/^[\w+=,.@-]{1,64}$/);
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

  it("refuses a PUT that names other encryption, another KMS key, or aws:kms with no key id (aws/s3)", () => {
    expect(sid("DenyOtherEncryption").Condition).toEqual({ StringNotEquals: { "s3:x-amz-server-side-encryption": "aws:kms" }, Null: { "s3:x-amz-server-side-encryption": "false" } });
    const k = sid("SseHeaderMustNameThisKey");
    expect(k.Effect).toBe("Deny");
    expect(k.Action).toEqual(["s3:PutObject"]);
    expect(k.Condition).toEqual({ Null: { "s3:x-amz-server-side-encryption": "false" }, StringNotEqualsIfExists: { "s3:x-amz-server-side-encryption-aws-kms-key-id": KEY } });
    // Evaluate the condition for the three request shapes that matter.
    const denies = (headers: Record<string, string>) => {
      const sseSent = "s3:x-amz-server-side-encryption" in headers;
      const keyId = headers["s3:x-amz-server-side-encryption-aws-kms-key-id"];
      return sseSent && (keyId === undefined || keyId !== KEY);
    };
    expect(denies({})).toBe(false); // bucket default: this key
    expect(denies({ "s3:x-amz-server-side-encryption": "aws:kms" })).toBe(true); // would be aws/s3
    expect(denies({ "s3:x-amz-server-side-encryption": "aws:kms", "s3:x-amz-server-side-encryption-aws-kms-key-id": KEY })).toBe(false);
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
  const kp = transcriptKeyPolicy({ accountId: ACCOUNT, region: REGION, bucketName: n.bucket, writerArn, srvWriterArn, readerArn });
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

  it("only the writer, srv-writer and reader use it, only via S3 in us-east-1, only for objects under 96bulls/ in this bucket", () => {
    const users = st.filter((s) => !JSON.stringify(s.Principal).includes(":root"));
    expect(users.map((s) => (s.Principal as { AWS: string }).AWS).sort()).toEqual([readerArn, srvWriterArn, writerArn].sort());
    for (const s of users) {
      expect(s.Condition).toEqual({
        StringEquals: { "kms:ViaService": `s3.${REGION}.amazonaws.com`, "kms:CallerAccount": ACCOUNT },
        StringLike: { "kms:EncryptionContext:aws:s3:arn": `arn:aws:s3:::${n.bucket}/${TENANT_KEY_BASE}/*` },
      });
    }
    expect(users.find((s) => (s.Principal as { AWS: string }).AWS === writerArn)!.Action.sort()).toEqual(["kms:Decrypt", "kms:GenerateDataKey"]);
    expect(users.find((s) => (s.Principal as { AWS: string }).AWS === srvWriterArn)!.Action.sort()).toEqual(["kms:Decrypt", "kms:GenerateDataKey"]);
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

  it("no writer/reader/eraser policy has a wildcard action, a wildcard resource, or any KMS or IAM grant", () => {
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
    expect(at('AWS.KMS.Key("transcripts-key"')).toBeGreaterThan(at('"transcript-srv-writer",'));
    expect(src).toContain("srvWriterArn: srvWriter.userArn as unknown as string,");
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

  it("binds both CloudTrail bucket service grants to the exact trail and account", () => {
    const trailArn = `arn:aws:cloudtrail:${REGION}:${ACCOUNT}:trail/${n.trail}`;
    const policy = trailLogBucketPolicy({ bucketArn: `arn:aws:s3:::${n.trailBucket}`, trailArn, accountId: ACCOUNT }) as Stmt[];
    const grants = policy.filter((s) => s.Effect === "Allow");
    expect(grants).toHaveLength(2);
    expect(grants[0]!.Condition).toEqual({ StringEquals: { "aws:SourceArn": trailArn, "aws:SourceAccount": ACCOUNT } });
    expect(grants[1]!.Condition).toEqual({ StringEquals: { "aws:SourceArn": trailArn, "aws:SourceAccount": ACCOUNT, "s3:x-amz-acl": "bucket-owner-full-control" } });
  });

  it("has no trail-log decrypt path unless one exact audit role is configured", () => {
    const closed = trailKeyPolicy(ACCOUNT, REGION, n.trail, null);
    expect(JSON.stringify(closed)).not.toMatch(/kms:Decrypt|kms:ReEncrypt/);
    expect(JSON.stringify(closed)).not.toContain('"AWS":"*"');
    const audit = `arn:aws:iam::${ACCOUNT}:role/security/96bulls-cloudtrail-auditor`;
    const configured = trailKeyPolicy(ACCOUNT, REGION, n.trail, audit);
    const reader = configured.Statement.find((s) => s.Sid === "AccountReadsLogs")!;
    expect(reader.Principal).toEqual({ AWS: audit });
    expect(reader.Action).toEqual(["kms:Decrypt", "kms:ReEncryptFrom"]);
  });
});

describe("transcript store: srv-writer (relayhistory-api live writer) is least privilege", () => {
  const p = transcriptSrvWriterPolicy({ bucketArn: n.bucketArn, accountId: ACCOUNT, region: REGION, keyAlias: n.keyAlias }) as { Statement: Stmt[] };
  const allow = p.Statement.filter((s) => s.Effect === "Allow");
  const sid = (x: string) => p.Statement.find((s) => s.Sid === x)!;
  const objects = [
    `${n.bucketArn}/${TENANT_KEY_BASE}/*/v1/ws=*/m=srv_*`,
    `${n.bucketArn}/${TENANT_KEY_BASE}/*/live-sessions/*`,
    `${n.bucketArn}/${TENANT_KEY_BASE}/*/live-batches/*`,
  ];
  const srvLoc: SessionLocation = { ...loc, machineId: "srv_0123abcd" };
  const SRV_MANIFEST = obj(manifestKey(srvLoc));
  const SRV_RAW = obj(rawChunkKey(srvLoc, 0, 0, "b".repeat(64), "ndjson"));
  const LIVE_SESSION = obj(`${TENANT_KEY_BASE}/org_abc/live-sessions/s1.json`);
  const LIVE_BATCH = obj(`${TENANT_KEY_BASE}/org_abc/live-batches/2026/11/01/b1.ndjson.zst`);

  it("is named like the other users and tagged with its purpose", () => {
    expect(n.srvWriter).toBe("96bulls-aws-sessions-prod-transcript-srv-writer");
    expect(src).toContain('"relayhistory-api-worker-writes-live-transcripts"');
  });

  it("allows exactly s3:GetObject and s3:PutObject on the srv_ machine, live-sessions and live-batches prefixes, ListBucket for 404s, plus key use", () => {
    expect(allow.map((s) => s.Sid)).toEqual(["ReadLiveObjects", "WriteLiveObjectsSseKmsOnly", "UseTranscriptKeyViaS3", "ListForMissingKeySemantics"]);
    // A missing key must 404 (the Worker's create-only reads), which S3
    // decides by s3:ListBucket on the bucket; no prefix condition can apply.
    expect(sid("ListForMissingKeySemantics").Action).toEqual(["s3:ListBucket"]);
    expect(res(sid("ListForMissingKeySemantics"))).toEqual([n.bucketArn]);
    expect(sid("ListForMissingKeySemantics").Condition).toBeUndefined();
    expect(sid("ReadLiveObjects").Action).toEqual(["s3:GetObject"]);
    expect(sid("WriteLiveObjectsSseKmsOnly").Action).toEqual(["s3:PutObject"]);
    expect(sid("UseTranscriptKeyViaS3").Action.sort()).toEqual(["kms:Decrypt", "kms:GenerateDataKey"]);
    expect(res(sid("ReadLiveObjects"))).toEqual(objects);
    expect(res(sid("WriteLiveObjectsSseKmsOnly"))).toEqual(objects);
    expect(res(sid("UseTranscriptKeyViaS3"))).toEqual([`arn:aws:kms:${REGION}:${ACCOUNT}:key/*`]);
    expect([...new Set(allowedActions(p))].sort()).toEqual(["kms:Decrypt", "kms:GenerateDataKey", "s3:GetObject", "s3:ListBucket", "s3:PutObject"]);
  });

  it("keeps inbox authority grants dormant by default and scopes an enabled grant to exactly the reviewed roots", () => {
    const enabled = transcriptSrvWriterPolicy({
      bucketArn: n.bucketArn,
      accountId: ACCOUNT,
      region: REGION,
      keyAlias: n.keyAlias,
      inboxAuthorityKeyRoot: AUTHORITY_ROOT,
    }) as { Statement: Stmt[] };
    const enabledObjects = [
      ...objects,
      `${n.bucketArn}/${AUTHORITY_ROOT}/*`,
      `${n.bucketArn}/${INBOX_CANDIDATE_KEY_ROOT}/*`,
    ];
    expect(srvWriterObjects(n.bucketArn)).toEqual(objects);
    expect(srvWriterObjects(n.bucketArn, AUTHORITY_ROOT)).toEqual(enabledObjects);
    expect(src).toContain("inboxAuthorityKeyRoot: i.transcriptStore.inboxAuthorityKeyRoot");
    expect(res(enabled.Statement.find((statement) => statement.Sid === "ReadLiveObjects")!)).toEqual(enabledObjects);
    expect(res(enabled.Statement.find((statement) => statement.Sid === "WriteLiveObjectsSseKmsOnly")!)).toEqual(enabledObjects);
    expect([...new Set(allowedActions(enabled))].sort()).toEqual(["kms:Decrypt", "kms:GenerateDataKey", "s3:GetObject", "s3:ListBucket", "s3:PutObject"]);

    for (const root of [AUTHORITY_ROOT, INBOX_CANDIDATE_KEY_ROOT]) {
      const key = obj(`${root}/tenant/head.json`);
      expect(decide(enabled, "s3:GetObject", key)).toBe("allow");
      expect(decide(enabled, "s3:PutObject", key)).toBe("allow");
    }
    for (const key of [
      obj("96bulls/inbox-authority-other/v1/head.json"),
      obj(`${AUTHORITY_ROOT}-other/head.json`),
      obj("96bulls/upload-inbox/v10/candidate"),
      obj("96bulls/upload-inbox/v1-sibling/candidate"),
      obj("outside/inbox-authority/v1/head.json"),
    ]) {
      expect(decide(enabled, "s3:GetObject", key)).toBe("implicit-deny");
      expect(decide(enabled, "s3:PutObject", key)).toBe("implicit-deny");
    }
    for (const bad of ["96bulls", "96bulls/", "96bulls//authority", "96bulls/../authority", INBOX_CANDIDATE_KEY_ROOT, "96bulls/upload-inbox"])
      expect(() => srvWriterObjects(n.bucketArn, bad)).toThrow(/inboxAuthorityKeyRoot/);
  });

  it("reaches the Worker's srv_ manifests and raw chunks and the live prefixes, nothing else", () => {
    for (const k of [SRV_MANIFEST, SRV_RAW, LIVE_SESSION, LIVE_BATCH])
      for (const a of ["s3:PutObject", "s3:GetObject"]) expect(decide(p, a, k)).toBe("allow");
    for (const k of [MANIFEST, RAW, SUMMARY, OUTSIDE, obj("anything"), obj("live-sessions/x"), obj(`${TENANT_KEY_BASE}/org_abc/v1/ws=ws1/m=m1/h=claude/d=2026/10/31/s=s1/raw/x`)])
      for (const a of ["s3:PutObject", "s3:GetObject"]) expect(decide(p, a, k)).toBe("implicit-deny");
  });

  it("never deletes, lists versions, reads old versions or changes the bucket; lists current keys only", () => {
    for (const a of ["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:GetObjectVersion", "s3:PutBucketPolicy", "s3:PutObjectAcl"]) expect(decide(p, a, SRV_MANIFEST)).toBe("deny");
    for (const a of ["s3:ListBucketVersions", "s3:PutLifecycleConfiguration", "s3:DeleteBucket"]) expect(decide(p, a, n.bucketArn)).toBe("deny");
    expect(decide(p, "s3:ListBucket", n.bucketArn)).toBe("allow");
    for (const a of allowedActions(p)) expect(a).not.toMatch(/Delete|Versions|\*/);
  });

  it("PUT requires SSE-KMS with a key in this account and region; the bucket policy pins it to the transcripts CMK", () => {
    expect(sid("WriteLiveObjectsSseKmsOnly").Condition).toEqual({
      StringEquals: { "s3:x-amz-server-side-encryption": "aws:kms" },
      StringLike: { "s3:x-amz-server-side-encryption-aws-kms-key-id": `arn:aws:kms:${REGION}:${ACCOUNT}:key/*` },
    });
    const bucket = transcriptBucketPolicy({ bucketArn: n.bucketArn, keyArn: KEY, eraserArn }) as Stmt[];
    // Identity Allow and bucket Deny together: SSE-KMS naming exactly KEY, or nothing.
    const allowed = (h: Record<string, string>) => {
      const sse = h["s3:x-amz-server-side-encryption"];
      const keyId = h["s3:x-amz-server-side-encryption-aws-kms-key-id"];
      const identity = sse === "aws:kms" && keyId !== undefined && glob(`arn:aws:kms:${REGION}:${ACCOUNT}:key/*`).test(keyId);
      const bucketDenies = sse !== undefined && (sse !== "aws:kms" || keyId !== KEY);
      return identity && !bucketDenies;
    };
    expect(bucket.find((s) => s.Sid === "SseHeaderMustNameThisKey")!.Condition).toEqual({ Null: { "s3:x-amz-server-side-encryption": "false" }, StringNotEqualsIfExists: { "s3:x-amz-server-side-encryption-aws-kms-key-id": KEY } });
    expect(allowed({})).toBe(false);
    expect(allowed({ "s3:x-amz-server-side-encryption": "aws:kms" })).toBe(false);
    expect(allowed({ "s3:x-amz-server-side-encryption": "AES256" })).toBe(false);
    expect(allowed({ "s3:x-amz-server-side-encryption": "aws:kms", "s3:x-amz-server-side-encryption-aws-kms-key-id": `arn:aws:kms:${REGION}:${ACCOUNT}:key/2222` })).toBe(false);
    expect(allowed({ "s3:x-amz-server-side-encryption": "aws:kms", "s3:x-amz-server-side-encryption-aws-kms-key-id": KEY })).toBe(true);
  });

  it("key use is only via S3 in us-east-1 and only on the transcripts key's alias", () => {
    expect(sid("UseTranscriptKeyViaS3").Condition).toEqual({
      StringEquals: { "kms:ViaService": `s3.${REGION}.amazonaws.com` },
      "ForAnyValue:StringEquals": { "kms:ResourceAliases": n.keyAlias },
    });
  });

  it("the bucket admits it without a new grant: the bucket policy has no Allow and denies deletes to all but the eraser", () => {
    const bucket = transcriptBucketPolicy({ bucketArn: n.bucketArn, keyArn: KEY, eraserArn }) as Stmt[];
    expect(bucket.filter((s) => s.Effect === "Allow")).toEqual([]);
    expect(JSON.stringify(bucket)).not.toContain(srvWriterArn);
    expect(bucket.find((s) => s.Sid === "OnlyTheEraserDeletes")!.Condition).toEqual({ ArnNotEquals: { "aws:PrincipalArn": eraserArn } });
  });
});

describe("transcript store: deploy plan contract", () => {
  const resources = [
    "access-logs", "budget", "trail", "trail-key", "trail-key-alias", "trail-logs",
    "transcript-eraser", "transcript-reader", "transcript-srv-writer", "transcript-writer", "transcripts",
    "transcripts-key", "transcripts-key-alias",
  ];
  const h = (text: string) => createHash("sha256").update(text).digest("hex");
  const plan = (actions: Record<string, string>, summary?: string, extraResources: readonly string[] = []) => {
    const counts = new Map<string, number>();
    for (const action of Object.values(actions)) counts.set(action, (counts.get(action) ?? 0) + 1);
    const inferred = counts.size ? `Plan: ${[...counts].map(([action, count]) => `${count} to ${action}`).join(", ")}` : "Plan: 0 changes";
    return `${summary ?? inferred}\n${[...resources, ...extraResources].map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n`;
  };
  const detailed = (compact: string) => compact.split("\n").map((line) => line.startsWith("[") ? `  ${line}` : line).join("\n");
  const provenance = (compact: string, full = detailed(compact), patch: Record<string, unknown> = {}) => JSON.stringify({
    schema: 1,
    stage: STAGE,
    outputs: {
      plan: { path: `docs/plans/${STAGE}.plan.txt`, sha256: h(compact) },
      detailed: { path: `docs/plans/${STAGE}.plan.detailed.txt`, sha256: h(full) },
    },
    summary: compact.split("\n")[0],
    ...patch,
  });
  const check = (compact: string, full = detailed(compact), record = provenance(compact, full)) =>
    assertTranscriptStorePlanContract(compact, { detailedPlanText: full, provenanceText: record });

  it("accepts only the exact initial, recovery, two-policy remediation, srv-writer addition, srv-writer recovery, and final all-noop phases", () => {
    expect(check(plan(Object.fromEntries(resources.map((resource) => [resource, "create"]))))).toBe("initial");
    expect(check(plan({ transcripts: "create", "transcripts-key": "create", "transcripts-key-alias": "create" }))).toBe("principal-propagation recovery");
    expect(check(plan({ "trail-key": "update", "trail-logs": "update" }))).toBe("post-merge policy remediation");
    expect(check(plan({ "transcript-srv-writer": "create", "transcripts-key": "update" }))).toBe("srv-writer addition");
    expect(check(plan({}))).toBe("post-apply");
  });

  it("the srv-writer addition is exactly the new user plus the key policy update", () => {
    for (const actions of [
      { "transcript-srv-writer": "create" },
      { "transcript-srv-writer": "create", "transcripts-key": "update", transcripts: "update" },
      { "transcript-srv-writer": "update", "transcripts-key": "update" },
    ]) expect(() => check(plan(actions))).toThrow(/action set does not match/);
  });

  // A detailed plan whose transcripts-key update shows the live policy (-) and the planned policy (+).
  const keyDiff = (live: string[], planned: string[]) => [
    "  properties:", "    policy:",
    ...live.map((sid) => `  -       - Sid: placeholder\n  -         Sid: ${sid}`),
    ...planned.map((sid) => `  +         Sid: ${sid}`),
  ].join("\n");
  const withKeyDiff = (compact: string, diff: string) =>
    detailed(compact).replace("  [transcripts-key] update", `  [transcripts-key] update\n${diff}`);
  type PolicyStatementFixture = { Sid: string; Effect: string; Action: string[]; Resource: string[]; Condition?: Record<string, unknown> };
  type PolicyFixture = { Version: string; Statement: PolicyStatementFixture[] };
  const inboxPolicy = (authorityRoot?: string) => transcriptSrvWriterPolicy({
    bucketArn: n.bucketArn,
    accountId: ACCOUNT,
    region: REGION,
    keyAlias: n.keyAlias,
    inboxAuthorityKeyRoot: authorityRoot,
  }) as unknown as PolicyFixture;
  const policyStatement = (policy: PolicyFixture, sid: string) => policy.Statement.find((statement) => statement.Sid === sid)!;
  const withNativeInboxPolicyDiff = (
    compact: string,
    authorityRoot = AUTHORITY_ROOT,
    mutate?: (oldPolicy: PolicyFixture, newPolicy: PolicyFixture) => void,
  ) => {
    const oldPolicy = structuredClone(inboxPolicy());
    const newPolicy = structuredClone(inboxPolicy(authorityRoot));
    // The real helper deliberately reuses one Resource array for read/write;
    // detach fixture arrays so one causal mutation cannot change two statements.
    for (const policy of [oldPolicy, newPolicy])
      policy.Statement = policy.Statement.map((statement) => ({
        ...statement,
        Action: [...statement.Action],
        Resource: [...statement.Resource],
        ...(statement.Condition ? { Condition: structuredClone(statement.Condition) } : {}),
      }));
    mutate?.(oldPolicy, newPolicy);
    const document = formatDeclaredPropertyYaml({ policy: oldPolicy }, { policy: newPolicy }, "update");
    if (!document) throw new Error("native inbox policy fixture unexpectedly has no diff");
    const rendered = document.lines.map((line) => `  ${line}`).join("\n");
    return detailed(compact).replace("  [transcript-srv-writer] update", `  [transcript-srv-writer] update\n${rendered}`);
  };
  const LIVE = ["AccountAdministersKey", "WriterEncryptsAndReadsViaS3", "ReaderDecryptsViaS3"];
  const PLANNED = ["AccountAdministersKey", "WriterEncryptsAndReadsViaS3", "SrvWriterEncryptsAndReadsViaS3", "ReaderDecryptsViaS3"];

  it("srv-writer recovery: accepts a lone transcripts-key update that adds the srv-writer's key statement", () => {
    const compact = plan({ "transcripts-key": "update" });
    const full = withKeyDiff(compact, keyDiff(LIVE, PLANNED));
    expect(check(compact, full, provenance(compact, full))).toBe("srv-writer propagation recovery");
  });

  it("srv-writer recovery: rejects a key update that does not add exactly the srv-writer statement, and any other single update", () => {
    const compact = plan({ "transcripts-key": "update" });
    for (const [diff, why] of [
      [keyDiff(LIVE, LIVE), /must add exactly one SrvWriterEncryptsAndReadsViaS3/],
      [keyDiff(PLANNED, PLANNED), /already has SrvWriterEncryptsAndReadsViaS3/],
      [keyDiff(LIVE, [...PLANNED, "SrvWriterEncryptsAndReadsViaS3"]), /must add exactly one/],
    ] as const) {
      const full = withKeyDiff(compact, diff);
      expect(() => check(compact, full, provenance(compact, full))).toThrow(why);
    }
    expect(() => check(compact)).toThrow(/must add exactly one/);
    for (const resource of resources.filter((r) => r !== "transcripts-key" && r !== "transcript-srv-writer"))
      expect(() => check(plan({ [resource]: "update" }))).toThrow(/action set does not match/);
    for (const actions of [
      { "transcripts-key": "update", "transcript-srv-writer": "update" },
      { "transcripts-key": "update", transcripts: "update" },
    ]) expect(() => check(plan(actions))).toThrow(/action set does not match/);
  });

  it("accepts only the exact dormant-to-enabled srv-writer inbox policy expansion", () => {
    const compact = plan({ "transcript-srv-writer": "update" });
    const full = withNativeInboxPolicyDiff(compact);
    expect(check(compact, full, provenance(compact, full))).toBe("srv-writer inbox policy update");
    const additions = [
      `${n.bucketArn}/${AUTHORITY_ROOT}/*`,
      `${n.bucketArn}/${INBOX_CANDIDATE_KEY_ROOT}/*`,
      `${n.bucketArn}/${AUTHORITY_ROOT}/*`,
      `${n.bucketArn}/${INBOX_CANDIDATE_KEY_ROOT}/*`,
    ];
    for (const bad of [
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (_old, next) => {
        policyStatement(next, "WriteLiveObjectsSseKmsOnly").Resource.splice(-2);
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (_old, next) => {
        const resources = policyStatement(next, "WriteLiveObjectsSseKmsOnly").Resource;
        resources[resources.length - 2] = `${n.bucketArn}/96bulls/other-authority/v1/*`;
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (_old, next) => {
        for (const sid of ["ReadLiveObjects", "WriteLiveObjectsSseKmsOnly"]) {
          const resources = policyStatement(next, sid).Resource;
          resources[resources.length - 2] = `${n.bucketArn}/96bulls/upload-inbox/*`;
        }
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (_old, next) => {
        policyStatement(next, "ReadLiveObjects").Action.push("s3:DeleteObject");
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (_old, next) => {
        policyStatement(next, "WriteLiveObjectsSseKmsOnly").Resource.splice(-2);
        policyStatement(next, "ReadLiveObjects").Resource.push(...additions.slice(2));
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (_old, next) => {
        policyStatement(next, "ReadLiveObjects").Resource.splice(-2);
        policyStatement(next, "WriteLiveObjectsSseKmsOnly").Resource.splice(-2);
        policyStatement(next, "UseTranscriptKeyViaS3").Resource.push(...additions);
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (old, next) => {
        policyStatement(old, "ReadLiveObjects").Action = ["kms:Decrypt"];
        policyStatement(next, "ReadLiveObjects").Action = ["kms:Decrypt"];
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (_old, next) => {
        policyStatement(next, "ReadLiveObjects").Resource.splice(-2);
        policyStatement(next, "WriteLiveObjectsSseKmsOnly").Resource.splice(-2);
        policyStatement(next, "ReadLiveObjects").Condition = { StringEquals: { Injected: additions.slice(0, 2) } };
        policyStatement(next, "WriteLiveObjectsSseKmsOnly").Condition = { Injected: additions.slice(2) };
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (old, next) => {
        policyStatement(old, "WriteLiveObjectsSseKmsOnly").Sid = "ReadLiveObjects";
        policyStatement(next, "WriteLiveObjectsSseKmsOnly").Sid = "ReadLiveObjects";
      }),
      withNativeInboxPolicyDiff(compact, AUTHORITY_ROOT, (old, next) => {
        policyStatement(old, "ReadLiveObjects").Condition = { StringEquals: { Sid: "WriteLiveObjectsSseKmsOnly" } };
        policyStatement(next, "ReadLiveObjects").Condition = { StringEquals: { Sid: "WriteLiveObjectsSseKmsOnly" } };
      }),
    ])
      expect(() => check(compact, bad, provenance(compact, bad))).toThrow(/srv-writer inbox policy update/);
  });

  it("reaches the srv-writer inbox phase when every deployed pipeline resource is present as noop", () => {
    const compact = plan({ "transcript-srv-writer": "update" }, undefined, SESSION_PIPELINE_RESOURCES);
    const full = withNativeInboxPolicyDiff(compact);
    expect(check(compact, full, provenance(compact, full))).toBe("srv-writer inbox policy update");

    const mixed = plan({
      "transcript-srv-writer": "update",
      [SESSION_PIPELINE_RESOURCES[0]]: "update",
    }, undefined, SESSION_PIPELINE_RESOURCES);
    const mixedFull = withNativeInboxPolicyDiff(mixed);
    expect(() => check(mixed, mixedFull, provenance(mixed, mixedFull))).toThrow(/session pipeline rollout, update or post-apply phase/);
  });

  // Alchemy persisted the failed update: the plan shows no property change, the state holds the pending props.
  const SRV = `arn:aws:iam::${ACCOUNT}:user/96bulls/96bulls-aws-sessions-prod-transcript-srv-writer`;
  const stmt = (Sid: string, AWS = `arn:aws:iam::${ACCOUNT}:root`) => ({ Sid, Effect: "Allow", Principal: { AWS }, Action: ["kms:Decrypt"], Resource: "*" });
  const keyProps = (statements: object[]) => ({ deletionWindow: 30, enableKeyRotation: true, policy: { Version: "2012-10-17", Statement: statements } });
  const LIVE_STMTS = LIVE.map((sid) => stmt(sid));
  const SRV_STMT = stmt("SrvWriterEncryptsAndReadsViaS3", SRV);
  const snapshot = (o: { status?: string; props?: object; old?: object; user?: object } = {}) => JSON.stringify({
    schema: 1,
    objects: [
      { key: `96bulls/96bulls-aws/${STAGE}/transcripts-key.json`, text: JSON.stringify({
        status: o.status ?? "updating",
        props: o.props ?? keyProps([LIVE_STMTS[0], LIVE_STMTS[1], SRV_STMT, LIVE_STMTS[2]]),
        old: { status: "created", props: o.old ?? keyProps(LIVE_STMTS) },
      }) },
      { key: `96bulls/96bulls-aws/${STAGE}/transcript-srv-writer.json`, text: JSON.stringify(o.user ?? { status: "created", attr: { userArn: SRV } }) },
    ],
  });
  const pending = (stateSnapshotText?: string) => {
    const compact = plan({ "transcripts-key": "update" });
    const full = withKeyDiff(compact, "    no declared property changes");
    return assertTranscriptStorePlanContract(compact, { detailedPlanText: full, provenanceText: provenance(compact, full), stateSnapshotText });
  };

  it("srv-writer recovery: accepts a persisted pending key update that adds only the srv-writer's statement", () => {
    expect(pending(snapshot())).toBe("srv-writer propagation recovery");
    // The deploy has no snapshot; it is bound to the captured state by the version digest.
    expect(pending()).toBe("srv-writer propagation recovery");
  });

  it("srv-writer recovery: rejects a pending key update that is not exactly the srv-writer statement", () => {
    for (const [snap, why] of [
      [snapshot({ status: "created" }), /not a persisted pending update/],
      [snapshot({ props: keyProps(LIVE_STMTS) }), /must add exactly one/],
      [snapshot({ old: keyProps([...LIVE_STMTS, SRV_STMT]) }), /must add exactly one/],
      [snapshot({ props: keyProps([...LIVE_STMTS, stmt("SrvWriterEncryptsAndReadsViaS3", "*")]) }), /must name the srv-writer user/],
      [snapshot({ props: keyProps([...LIVE_STMTS.slice(0, 2), SRV_STMT]) }), /changes more than the srv-writer statement/],
      [snapshot({ props: { ...keyProps([...LIVE_STMTS, SRV_STMT]), enableKeyRotation: false } }), /changes more than the srv-writer statement/],
      [snapshot({ user: { status: "creating" } }), /transcript-srv-writer is not created/],
      ["not json", /not valid JSON/],
    ] as const) expect(() => pending(snap)).toThrow(why);
  });

  it("permits exactly two named updates, rejecting one, three, or a different resource", () => {
    for (const actions of [
      { "trail-key": "update" },
      { "trail-key": "update", "trail-logs": "update", trail: "update" },
      { "trail-key": "update", trail: "update" },
    ]) expect(() => check(plan(actions))).toThrow(/action set does not match/);
  });

  it("refuses replacements, deletes, imports/adoptions, unexpected partial creates, missing/extra resources, duplicates, and lying counts", () => {
    for (const action of ["replace", "delete", "adopt", "import"])
      expect(() => check(plan({ trail: action }))).toThrow(/forbidden action/);
    expect(() => check(plan({ trail: "create" }))).toThrow(/action set/);
    const allNoop = plan({});
    expect(() => check(allNoop.replace("[trail] noop\n", ""))).toThrow(/missing resource trail/);
    expect(() => check(allNoop + "[other] noop\n")).toThrow(/unexpected resource other/);
    expect(() => check(allNoop + "[trail] noop\n")).toThrow(/more than once/);
    expect(() => check(plan({}, "Plan: 1 to create"))).toThrow(/summary says 1 create/);
    expect(() => check(plan({ "trail-key": "update", "trail-logs": "update" }, "Plan: 2 to update plus something else"))).toThrow(/unrecognized summary/);
  });

  it("requires the detailed repair actions and exact compact/detailed provenance hashes and paths", () => {
    const compact = plan({ "trail-key": "update", "trail-logs": "update" });
    const full = detailed(compact);
    const otherUpdates = full.replace("[trail] noop", "[trail] update").replace("[trail-logs] update", "[trail-logs] noop");
    expect(() => check(compact, otherUpdates, provenance(compact, otherUpdates))).toThrow(/disagree on trail/);
    expect(() => check(compact, `${full}review-note\n`, provenance(compact, full))).toThrow(/detailed plan does not match/);
    expect(() => check(compact, full, provenance(compact, full, { stage: "aws-other" }))).toThrow(/schema 1 for aws-sessions-prod/);
    const badPath = JSON.stringify({
      ...JSON.parse(provenance(compact, full)),
      outputs: { ...JSON.parse(provenance(compact, full)).outputs, detailed: { path: "elsewhere", sha256: h(full) } },
    });
    expect(() => check(compact, full, badPath)).toThrow(/detailed plan path must be exactly/);
  });

  it("supports the required remediation reconciliation followed by an exact all-noop capture", () => {
    const phases = [
      check(plan({ "trail-key": "update", "trail-logs": "update" })),
      check(plan({})),
    ];
    expect(phases).toEqual(["post-merge policy remediation", "post-apply"]);
  });

  it("checks the production contract before a capture can replace reviewed files, and again before deploy", () => {
    const captureCheck = captureWorkflow.indexOf("if (stage === TRANSCRIPT_STAGE) assertTranscriptStorePlanContract(plan, { detailedPlanText: detailed, provenanceText, stateSnapshotText: snapText });");
    expect(captureCheck).toBeGreaterThan(0);
    expect(captureCheck).toBeLessThan(captureWorkflow.indexOf("writeFileSync(p.plan, plan)"));
    expect(stack).toContain("assertTranscriptStorePlanContract(planText, { detailedPlanText, provenanceText })");
  });
});

describe("transcript store: inputs", () => {
  const filled = (patch: (i: AwsInputs & Record<string, unknown>) => void = () => {}) => {
    const i = structuredClone(example) as AwsInputs & Record<string, unknown>;
    delete i.$comment;
    i.transcriptStore!.bufferedGcRoleArn = `arn:aws:iam::${ACCOUNT}:role/test-buffered-gc`;
    i.approval = { ...i.approval!, decision: "approved", date: "2026-10-31", quote: "let's start the production s3 work now", capture: { provenanceSha256: "a".repeat(64), planSha256: "b".repeat(64), inputsSha256: "c".repeat(64), sourceCommit: "d".repeat(40) } };
    patch(i);
    return i;
  };

  it("validates once filled; the committed example still refuses its placeholders", () => {
    expect(() => validateAwsInputs(STAGE, filled())).not.toThrow();
    expect(() => validateAwsInputs(STAGE, filled((i) => { i.transcriptStore!.inboxAuthorityKeyRoot = AUTHORITY_ROOT; }))).not.toThrow();
    expect(() => validateAwsInputs(STAGE, example)).toThrow(/placeholder values/);
  });

  it("keeps the inbox grant default-off and refuses broad, overlapping or malformed authority roots", () => {
    expect((filled().transcriptStore as Record<string, unknown>).inboxAuthorityKeyRoot).toBeUndefined();
    for (const root of [
      "96bulls",
      "96bulls/",
      "96bulls//authority",
      "96bulls/../authority",
      "outside/inbox-authority/v1",
      INBOX_CANDIDATE_KEY_ROOT,
      "96bulls/upload-inbox",
      "96bulls/upload-inbox/v1/nested",
    ])
      expect(() => validateAwsInputs(STAGE, filled((i) => { i.transcriptStore!.inboxAuthorityKeyRoot = root; }))).toThrow(/inboxAuthorityKeyRoot/);
  });

  it("refuses an invalid authority root before a direct stage caller can request a provider", async () => {
    await expect(Effect.runPromise(transcriptStoreStage({
      stage: STAGE,
      accountId: ACCOUNT,
      region: REGION,
      budgetUsd: 1,
      budgetEmail: "test@example.invalid",
      transcriptStore: {
        bufferedGcRoleArn: `arn:aws:iam::${ACCOUNT}:role/test-buffered-gc`,
        inboxAuthorityKeyRoot: INBOX_CANDIDATE_KEY_ROOT,
      },
      tags: () => ({}),
    }))).rejects.toThrow(/inboxAuthorityKeyRoot/);
  });

  it("requires the exact production target, commercial partition, and a plain transcriptStore object", () => {
    expect(() => validateAwsInputs(STAGE, filled((i) => (i.accountId = "111111111111")))).toThrow(/accountId must be exactly 941433935742/);
    expect(() => validateAwsInputs(STAGE, filled((i) => (i.region = "us-west-2")))).toThrow(/must stay in us-east-1/);
    for (const partition of [undefined, "aws-cn", "aws-us-gov"]) expect(() => validateAwsInputs(STAGE, filled((i) => ((i as Record<string, unknown>).partition = partition)))).toThrow(/partition must be exactly aws/);
    expect(() => validateAwsInputs("aws-sandbox-x", filled())).toThrow(/reserved for aws-sessions-prod/);
    for (const bad of [null, [], "yes", 1]) expect(() => validateAwsInputs(STAGE, filled((i) => ((i as Record<string, unknown>).transcriptStore = bad)))).toThrow(/plain object/);
    expect(() => validateAwsInputs(STAGE, filled((i) => ((i as Record<string, unknown>).transcriptStore = new (class Store {})())))).toThrow(/plain object/);
    expect(() => validateAwsInputs(STAGE, filled((i) => delete (i as Record<string, unknown>).transcriptStore))).toThrow(/transcriptStore is required/);
  });

  it("forbids transcriptStore on every other stage and refuses every legacy-baseline option", () => {
    for (const stage of ["aws-sessions-dev", "aws-production", "aws-sandbox-x", "aws-proof", "aws-sessions-prod-copy"])
      expect(() => validateAwsInputs(stage, filled())).toThrow(/reserved for aws-sessions-prod/);
    for (const [k, v] of [["eks", {}], ["supabase", {}], ["accountShared", { ownerStage: "aws-sessions-dev" }], ["lockerNoncurrentVersionDays", 30], ["hoopdbSecretArn", "arn:x"]] as const)
      expect(() => validateAwsInputs(STAGE, filled((i) => ((i as Record<string, unknown>)[k] = v)))).toThrow(new RegExp(`${k} is not allowed`));
    expect(() => validateAwsInputs(STAGE, filled((i) => (i.teardown = { purgeBucketsOnDestroy: true })))).toThrow(/never allowed on a production-looking stage/);
    expect(() => validateAwsInputs(STAGE, filled((i) => ((i.transcriptStore as Record<string, unknown>).objectLock = true)))).toThrow(/not a known setting/);
  });

  it("the stack declares the store instead of the baseline", () => {
    const at = stack.indexOf("if (stage === TRANSCRIPT_STAGE)");
    expect(at).toBeGreaterThan(0);
    expect(at).toBeLessThan(stack.indexOf('AWS.KMS.Key("locker-key"'));
    expect(stack).not.toContain("if (inputs.transcriptStore)");
    expect(stack).toContain("assertTranscriptStorePlanContract(planText, { detailedPlanText, provenanceText })");
  });

  it("accepts only an exact named audit role in the intended account and partition", () => {
    expect(() => validateAwsInputs(STAGE, filled((i) => ((i.transcriptStore as Record<string, unknown>).auditPrincipalArn = `arn:aws:iam::${ACCOUNT}:role/security/auditor`)))).not.toThrow();
    for (const arn of [`arn:aws:iam::111111111111:role/auditor`, `arn:aws-us-gov:iam::${ACCOUNT}:role/auditor`, `arn:aws:iam::${ACCOUNT}:user/auditor`, "*"])
      expect(() => validateAwsInputs(STAGE, filled((i) => ((i.transcriptStore as Record<string, unknown>).auditPrincipalArn = arn)))).toThrow(/named IAM role/);
  });
});
